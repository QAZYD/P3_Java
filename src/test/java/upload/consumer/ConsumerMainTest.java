package upload.consumer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;
import upload.grpc.VideoUploadGrpc;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerMainTest {
    @TempDir Path directory;

    @Test void invalidArguments() {
        for (String[] args : new String[][]{{"0"}, {"70000"}, {"-1"}, {"x"},
                {"50051", "0"}, {"50051", "-1"}, {"50051", "x"},
                {"50051", "4", ""}, {"1", "1", "uploads", "extra"}}) {
            assertThrows(IllegalArgumentException.class, () -> new ConsumerMain().configure(args));
        }
    }

    @Test void startsAndStopsWithFourConcurrentCallbacks() throws Exception {
        ConsumerMain consumer = new ConsumerMain();
        TestService service = new TestService();
        java.net.ServerSocket reservation = new java.net.ServerSocket(0);
        int port = reservation.getLocalPort();
        reservation.close();
        consumer.configure(Integer.toString(port), "4", directory.toString());
        consumer.startServer(service);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        try {
            List<CompletableFuture<UploadReply>> replies = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                CompletableFuture<UploadReply> result = new CompletableFuture<>();
                StreamObserver<UploadChunk> request = VideoUploadGrpc.newStub(channel)
                        .withDeadlineAfter(20, TimeUnit.SECONDS)
                        .upload(new StreamObserver<>() {
                            public void onNext(UploadReply reply) { result.complete(reply); }
                            public void onError(Throwable error) { result.completeExceptionally(error); }
                            public void onCompleted() {}
                        });
                request.onNext(UploadChunk.newBuilder().setFilename("test.mp4").build());
                request.onCompleted();
                replies.add(result);
            }
            for (CompletableFuture<UploadReply> reply : replies) {
                assertTrue(reply.get(20, TimeUnit.SECONDS).getAccepted());
            }
            assertEquals(4, service.peak.get());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            consumer.stopServer();
            consumer.stopServer();
        }
    }

    private static class TestService extends VideoUploadGrpc.VideoUploadImplBase {
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();

        @Override
        public StreamObserver<UploadChunk> upload(StreamObserver<UploadReply> response) {
            return new StreamObserver<>() {
                public void onNext(UploadChunk chunk) {}
                public void onError(Throwable error) {}
                public void onCompleted() {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    try {
                        Thread.sleep(300);
                        response.onNext(UploadReply.newBuilder().setAccepted(true).build());
                        response.onCompleted();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        active.decrementAndGet();
                    }
                }
            };
        }
    }
}
