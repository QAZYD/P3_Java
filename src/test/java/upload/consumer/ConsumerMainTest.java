package upload.consumer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import upload.grpc.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerMainTest {
    @TempDir Path directory;

    @Test void configuration() throws Exception {
        var defaults = ConsumerMain.Settings.parse();
        assertEquals(50051, defaults.port());
        assertEquals(4, defaults.grpcThreads());
        assertTrue(defaults.uploadDirectory().isAbsolute());
        for (String[] args : new String[][]{{"0"}, {"70000"}, {"-1"}, {"a"}, {"50051", "0"},
                {"50051", "-1"}, {"50051", "x"}, {"1", "1", "x", "extra"}, {"1", "1", ""}, {"1", "1", "\0"}}) {
            assertThrows(IllegalArgumentException.class, () -> ConsumerMain.Settings.parse(args));
        }
        Path file = Files.writeString(directory.resolve("file"), "not a directory");
        assertThrows(java.io.IOException.class,
                () -> ConsumerMain.Settings.parse("50051", "4", file.toString()).prepareDirectory());
        var settings = ConsumerMain.Settings.parse("--gui", "12345", "2", directory.resolve("new").toString());
        assertTrue(settings.gui());
        settings.prepareDirectory();
        assertTrue(Files.isDirectory(settings.uploadDirectory()));
    }

    private ConsumerMain.Settings settings(int port) {
        // Port zero is reserved for test-only ephemeral binding; CLI rejects it.
        return new ConsumerMain.Settings(port, 4, directory, false);
    }

    @Test void startupPortConflictAndIdempotentShutdown() throws Exception {
        var runtime = ConsumerMain.ServerRuntime.start(settings(0), new TestService(0), 1000);
        int port = runtime.port();
        assertTrue(port > 0);
        try {
            assertThrows(java.io.IOException.class,
                    () -> ConsumerMain.ServerRuntime.start(settings(port), new TestService(0), 1000));
        } finally { runtime.close(); runtime.close(); }
        assertTrue(runtime.isTerminated());
        try (var restarted = ConsumerMain.ServerRuntime.start(settings(port), new TestService(0), 1000)) {
            assertEquals(port, restarted.port());
        }
    }

    @Test void sixRequestsOverlapOnFourThreads() throws Exception {
        TestService service = new TestService(3000);
        var runtime = ConsumerMain.ServerRuntime.start(settings(0), service, 1000);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", runtime.port()).usePlaintext().build();
        long start = System.nanoTime();
        try {
            List<CompletableFuture<UploadReply>> replies = new ArrayList<>();
            for (int i = 0; i < 6; i++) replies.add(request(channel));
            for (var reply : replies) assertTrue(reply.get(20, TimeUnit.SECONDS).getAccepted());
            assertEquals(4, service.peak.get(), "Four requests must execute concurrently");
            assertEquals(6, service.completed.get());
            System.out.printf("[TEST] Six requests, four threads: %.2f seconds; peak=%d; completed=%d%n",
                    (System.nanoTime() - start) / 1e9, service.peak.get(), service.completed.get());
        } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            runtime.close();
        }
        assertTrue(runtime.isTerminated());
    }

    @Test void gracefulShutdownDrainsRequestAndConcurrentCloseIsSafe() throws Exception {
        TestService service = new TestService(300);
        var runtime = ConsumerMain.ServerRuntime.start(settings(0), service, 2000);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", runtime.port()).usePlaintext().build();
        ExecutorService closers = Executors.newFixedThreadPool(2);
        try {
            var reply = request(channel);
            assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            Future<?> first = closers.submit(runtime::close);
            Future<?> second = closers.submit(runtime::close);
            assertTrue(reply.get(5, TimeUnit.SECONDS).getAccepted());
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertTrue(runtime.isTerminated());
        } finally {
            runtime.close();
            closers.shutdownNow();
            assertTrue(closers.awaitTermination(5, TimeUnit.SECONDS));
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test void forcedShutdownInterruptsActiveWork() throws Exception {
        TestService service = new TestService(30000);
        var runtime = ConsumerMain.ServerRuntime.start(settings(0), service, 100);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", runtime.port()).usePlaintext().build();
        try {
            var reply = request(channel);
            assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            runtime.close();
            assertTrue(runtime.isTerminated());
            assertThrows(ExecutionException.class, () -> reply.get(5, TimeUnit.SECONDS));
        } finally {
            runtime.close();
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test void shutdownPreservesInterrupt() throws Exception {
        var runtime = ConsumerMain.ServerRuntime.start(settings(0), new TestService(0), 1000);
        Thread.currentThread().interrupt();
        try { runtime.close(); assertTrue(Thread.currentThread().isInterrupted()); }
        finally { Thread.interrupted(); }
    }

    private CompletableFuture<UploadReply> request(ManagedChannel channel) {
        CompletableFuture<UploadReply> future = new CompletableFuture<>();
        var stream = VideoUploadGrpc.newStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS).upload(new StreamObserver<>() {
            private UploadReply reply;
            public void onNext(UploadReply value) { reply = value; }
            public void onError(Throwable t) { future.completeExceptionally(t); }
            public void onCompleted() { future.complete(reply); }
        });
        stream.onNext(UploadChunk.newBuilder().setFilename("test-only.mp4").build());
        stream.onCompleted();
        return future;
    }

    /** Test-only transport probe: no production upload or file-writing implementation. */
    private static final class TestService extends VideoUploadGrpc.VideoUploadImplBase {
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final AtomicInteger completed = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final long delay;
        TestService(long delay) { this.delay = delay; }
        @Override public StreamObserver<UploadChunk> upload(StreamObserver<UploadReply> response) {
            return new StreamObserver<>() {
                public void onNext(UploadChunk chunk) {}
                public void onError(Throwable t) {}
                public void onCompleted() {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    entered.countDown();
                    try {
                        Thread.sleep(delay);
                        completed.incrementAndGet();
                        response.onNext(UploadReply.newBuilder().setAccepted(true).build());
                        response.onCompleted();
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { active.decrementAndGet(); }
                }
            };
        }
    }
}
