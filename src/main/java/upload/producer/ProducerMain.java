package upload.producer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import upload.common.Config;
import upload.common.UserInput;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;
import upload.grpc.VideoUploadGrpc;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class ProducerMain {

    public static void main(String[] args) {
        List<String> positional = new ArrayList<>();
        Integer pOption = null;
        try {
            for (String arg : args) {
                if (arg.startsWith("--p=")) pOption = UserInput.parsePositiveOption(arg, "Producer threads (p)");
                else positional.add(arg);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("[CONFIG] " + e.getMessage());
            return;
        }
        int p = pOption != null
                ? pOption
                : UserInput.askPositiveInt("Number of producer threads (p)", Config.PRODUCER_THREADS);

        Config.Setting host = Config.consumerHost(positional.toArray(new String[0]));
        Config.Setting portSetting = Config.consumerPort();
        int port;
        try {
            port = portSetting.asPort();
        } catch (IllegalArgumentException e) {
            System.err.println("[CONFIG] " + e.getMessage());
            return;
        }
        System.out.printf("Starting %d producer thread(s) connecting to %s:%d...%n", p, host.value(), port);
        System.out.printf("  host from %s, port from %s%n", host.source(), portSetting.source());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host.value(), port)
                .usePlaintext()
                .build();

        VideoUploadGrpc.VideoUploadStub asyncStub = VideoUploadGrpc.newStub(channel);
        ExecutorService pool = Executors.newFixedThreadPool(p);

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
                        // Stop early if the pool is being shut down.
                        if (Thread.currentThread().isInterrupted()) {
                            System.err.printf("[Producer %d] Interrupted, skipping remaining files%n", producerId);
                            break;
                        }
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
        Object readyLock = new Object();

        ClientResponseObserver<UploadChunk, UploadReply> responseObserver = new ClientResponseObserver<>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<UploadChunk> requestStream) {
                requestStream.setOnReadyHandler(() -> wake(readyLock));
            }

            @Override
            public void onNext(UploadReply reply) {
                System.out.printf("[Producer %d] [RESULT] %s -> Accepted: %b | Message: %s%n",
                        producerId, fileName, reply.getAccepted(), reply.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                System.err.printf("[Producer %d] [ERROR] Stream failed for %s: %s%n", producerId, fileName, t.getMessage());
                latch.countDown();
                wake(readyLock); 
            }

            @Override
            public void onCompleted() {
                latch.countDown();
                wake(readyLock);
            }
        };

        ClientCallStreamObserver<UploadChunk> requestObserver = null;
        boolean completed = false;

        try {
            requestObserver = (ClientCallStreamObserver<UploadChunk>) stub.upload(responseObserver);

            try (InputStream inputStream = Files.newInputStream(file)) {
                UploadChunk filenameChunk = UploadChunk.newBuilder()
                        .setFilename(fileName)
                        .build();
                requestObserver.onNext(filenameChunk);

                byte[] buffer = new byte[Config.CHUNK_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    if (!waitUntilReady(requestObserver, latch, readyLock)) {
                        return; 
                    }
                    UploadChunk chunk = UploadChunk.newBuilder()
                            .setChunk(ByteString.copyFrom(buffer, 0, bytesRead))
                            .build();
                    requestObserver.onNext(chunk);
                }
            }

            requestObserver.onCompleted();
            completed = true;

            if (!latch.await(2, TimeUnit.MINUTES)) {
                System.err.printf("[Producer %d] [TIMEOUT] No reply received for %s%n", producerId, fileName);
                requestObserver.cancel("Timed out waiting for reply", null);
            }

        } catch (InterruptedException e) {
            System.err.printf("[Producer %d] [INTERRUPTED] Upload interrupted for %s: %s%n", producerId, fileName, e.getMessage());
            if (requestObserver != null) {
                requestObserver.cancel("Producer interrupted", e);
            }
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.printf("[Producer %d] [EXCEPTION] Failed uploading %s: %s%n", producerId, fileName, e.getMessage());
            if (requestObserver != null && !completed) {
                try {
                    requestObserver.onError(e);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static boolean waitUntilReady(ClientCallStreamObserver<UploadChunk> stream,
                                          CountDownLatch latch, Object readyLock) throws InterruptedException {
        synchronized (readyLock) {
            while (!stream.isReady()) {
                if (latch.getCount() == 0) {
                    return false;
                }
                readyLock.wait(1000);
            }
        }
        return latch.getCount() > 0;
    }

    private static void wake(Object readyLock) {
        synchronized (readyLock) {
            readyLock.notifyAll();
        }
    }
}
