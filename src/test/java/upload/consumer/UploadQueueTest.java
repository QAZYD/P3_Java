package upload.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Leaky-bucket queue: capacity q, c workers, drops when full. */
class UploadQueueTest {

    @TempDir
    Path root;

    private Path staged(String name, String content) throws Exception {
        Path staging = Files.createDirectories(root.resolve("staging"));
        return Files.writeString(staging.resolve(name), content);
    }

    @Test
    void dropsWhenQueueIsFull() throws Exception {
        Path uploads = root.resolve("uploads");
        CountDownLatch workerBusy = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // The single worker blocks after saving its first video, so the queue can fill up.
        UploadQueue queue = new UploadQueue(1, 1, uploads, saved -> {
            workerBusy.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        queue.start();
        try {
            assertEquals(UploadSink.Result.ACCEPTED, queue.submit(staged("a.mp4", "a"), "a.mp4"));
            assertTrue(workerBusy.await(10, TimeUnit.SECONDS));                // worker is busy with a
            assertEquals(UploadSink.Result.ACCEPTED, queue.submit(staged("b.mp4", "b"), "b.mp4")); // fills q = 1
            assertEquals(UploadSink.Result.QUEUE_FULL, queue.submit(staged("c.mp4", "c"), "c.mp4")); // dropped
        } finally {
            release.countDown();
        }
        waitFor(() -> Files.exists(uploads.resolve("b.mp4")));
        queue.shutdown(5, TimeUnit.SECONDS);

        assertEquals("a", Files.readString(uploads.resolve("a.mp4")));
        assertEquals("b", Files.readString(uploads.resolve("b.mp4")));
        assertFalse(Files.exists(uploads.resolve("c.mp4")));
    }

    @Test
    void workersSaveEveryVideoAndRenameDuplicates() throws Exception {
        Path uploads = root.resolve("uploads");
        List<Path> saved = new CopyOnWriteArrayList<>();
        UploadQueue queue = new UploadQueue(10, 3, uploads, saved::add);
        queue.start();
        for (int i = 0; i < 5; i++) {
            assertEquals(UploadSink.Result.ACCEPTED, queue.submit(staged("clip" + i, "data" + i), "clip.mp4"));
        }
        waitFor(() -> saved.size() == 5);
        queue.shutdown(5, TimeUnit.SECONDS);

        assertTrue(Files.exists(uploads.resolve("clip.mp4")));
        for (int n = 1; n <= 4; n++) {
            assertTrue(Files.exists(uploads.resolve("clip (" + n + ").mp4")), "missing clip (" + n + ").mp4");
        }
        try (var staging = Files.list(root.resolve("staging"))) {
            assertTrue(staging.findAny().isEmpty(), "staged files should have been moved");
        }
    }

    @Test
    void rejectsAfterShutdown() throws Exception {
        UploadQueue queue = new UploadQueue(2, 1, root.resolve("uploads"), saved -> {});
        queue.start();
        queue.shutdown(5, TimeUnit.SECONDS);
        assertEquals(UploadSink.Result.STOPPED, queue.submit(staged("late.mp4", "x"), "late.mp4"));
    }

    @Test
    void serviceRepliesDroppedAndDeletesFileWhenQueueIsFull() throws Exception {
        Path uploads = root.resolve("uploads");
        Path staging = Files.createDirectories(root.resolve("incoming"));
        // Worker count 1 but not started: nothing drains the queue, so capacity 1 fills after one upload.
        UploadQueue queue = new UploadQueue(1, 1, uploads, saved -> {});
        VideoUploadService service = new VideoUploadService(staging, queue);

        UploadReply first = upload(service, "one.mp4", "first");
        UploadReply second = upload(service, "two.mp4", "second");

        assertTrue(first.getAccepted());
        assertFalse(second.getAccepted());
        assertTrue(second.getMessage().startsWith("Dropped: queue full"), second.getMessage());
        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(staging.resolve("one.mp4")));
        assertFalse(Files.exists(staging.resolve("two.mp4")), "dropped upload must be deleted");
        queue.shutdown(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(staging.resolve("one.mp4")), "unsaved queued upload is discarded at shutdown");
    }

    private static UploadReply upload(VideoUploadService service, String name, String content) {
        UploadReply[] reply = new UploadReply[1];
        StreamObserver<UploadChunk> request = service.upload(new StreamObserver<>() {
            public void onNext(UploadReply value) { reply[0] = value; }
            public void onError(Throwable t) { throw new AssertionError(t); }
            public void onCompleted() {}
        });
        request.onNext(UploadChunk.newBuilder().setFilename(name).build());
        request.onNext(UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(content, StandardCharsets.UTF_8)).build());
        request.onCompleted();
        return reply[0];
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting");
            }
            Thread.sleep(20);
        }
    }
}
