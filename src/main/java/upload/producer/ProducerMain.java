package upload.producer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import upload.common.Config;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;
import upload.grpc.VideoUploadGrpc;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * OWNER: Person 3
 *
 * SHARED CONTRACT (per file, one gRPC stream each):
 *   1. connect to Config.HOST : Config.PORT
 *   2. send one UploadChunk with setFilename(...)
 *   3. send the file as UploadChunks with setChunk(...), Config.CHUNK_SIZE bytes each
 *   4. call onCompleted() and wait for the UploadReply
 *
 * Reads MP4s from Config.VIDEOS_DIR.
 * Run with: mvn exec:java -Dexec.mainClass=upload.producer.ProducerMain
 */
public class ProducerMain {

    public static void main(String[] args) {
        // Read p (number of producer threads) from config
        int p = Config.PRODUCER_THREADS;
        System.out.printf("Starting %d producer thread(s) connecting to %s:%d...%n", p, Config.HOST, Config.PORT);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(Config.HOST, Config.PORT)
                .usePlaintext()
                .build();

        VideoUploadGrpc.VideoUploadStub asyncStub = VideoUploadGrpc.newStub(channel);
        ExecutorService pool = Executors.newFixedThreadPool(p);

        // Spec: Each thread i reads from a separate folder (e.g., videos/p1, videos/p2, ...)
        for (int i = 1; i <= p; i++) {
            final int producerId = i;
            pool.submit(() -> {
                Path folder = Config.VIDEOS_DIR.resolve("p" + producerId);
                if (!Files.exists(folder) || !Files.isDirectory(folder)) {
                    System.err.printf("[Producer %d] Folder does not exist: %s%n", producerId, folder);
                    return;
                }

                try (Stream<Path> paths = Files.list(folder)) {
                    List<Path> mp4Files = paths
                            .filter(file -> file.toString().toLowerCase().endsWith(".mp4"))
                            .toList();

                    if (mp4Files.isEmpty()) {
                        System.out.printf("[Producer %d] No MP4 files found in %s%n", producerId, folder);
                    }

                    for (Path file : mp4Files) {
                        uploadFile(producerId, file, asyncStub);
                    }
                } catch (Exception e) {
                    System.err.printf("[Producer %d] Error scanning folder: %s%n", producerId, e.getMessage());
                }
            });
        }

        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.MINUTES)) {
                pool.shutdownNow();
            }
            channel.shutdown();
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("All producer threads finished.");
    }

    private static void uploadFile(int producerId, Path file, VideoUploadGrpc.VideoUploadStub stub) {
        String fileName = file.getFileName().toString();
        System.out.printf("[Producer %d] Starting upload for: %s%n", producerId, fileName);
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<UploadReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(UploadReply reply) {
                System.out.printf("[Producer %d] [RESULT] %s -> Accepted: %b | Message: %s%n",
                        producerId, fileName, reply.getAccepted(), reply.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                System.err.printf("[Producer %d] [ERROR] Stream failed for %s: %s%n", producerId, fileName, t.getMessage());
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        StreamObserver<UploadChunk> requestObserver = null;
        boolean completed = false;

        try {
            requestObserver = stub.upload(responseObserver);

            try (InputStream inputStream = Files.newInputStream(file)) {
                // 1. Send filename first via oneof data case
                UploadChunk filenameChunk = UploadChunk.newBuilder()
                        .setFilename(fileName)
                        .build();
                requestObserver.onNext(filenameChunk);

                // 2. Stream file bytes in Config.CHUNK_SIZE chunks
                byte[] buffer = new byte[Config.CHUNK_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    UploadChunk chunk = UploadChunk.newBuilder()
                            .setChunk(ByteString.copyFrom(buffer, 0, bytesRead))
                            .build();
                    requestObserver.onNext(chunk);
                }
            }

            // 3. Complete stream and wait for reply
            requestObserver.onCompleted();
            completed = true;

            if (!latch.await(2, TimeUnit.MINUTES)) {
                System.err.printf("[Producer %d] [TIMEOUT] No reply received for %s%n", producerId, fileName);
            }

        } catch (InterruptedException e) {
            System.err.printf("[Producer %d] [INTERRUPTED] Upload interrupted for %s: %s%n", producerId, fileName, e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.printf("[Producer %d] [EXCEPTION] Failed uploading %s: %s%n", producerId, fileName, e.getMessage());
            if (requestObserver != null && !completed) {
                try {
                    requestObserver.onError(e);
                } catch (Exception ignored) {
                    // Stream may already be terminated by server/transport
                }
            }
        }
    }
}
