package upload.consumer;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;
import upload.common.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Person 2: create the window and server, then stop both when the app closes. */
public class ConsumerMain extends Application implements Consumer<Path>, Runnable {
    private int port = Config.PORT;
    private String portSource = "default";
    private int grpcThreads = 4;
    private Path uploadDirectory = Config.UPLOADS_DIR.toAbsolutePath().normalize();
    private PreviewWindow window;
    private Server server;
    private ExecutorService executor;
    private Thread shutdownHook;
    private boolean stopped;

    // JavaFX creates this object using this constructor.
    public ConsumerMain() {}

    // Read optional port, thread count, and upload directory.
    void configure(String... args) {
        if (args.length > 3) throw new IllegalArgumentException("Expected at most three arguments.");
        if (args.length == 0) {
            // No port argument: CONSUMER_PORT env var > network.properties > Config.PORT
            Config.Setting setting = Config.consumerPort();
            port = setting.asPort();
            portSource = setting.source();
        } else {
            portSource = "command line";
        }
        try {
            if (args.length > 0) port = Integer.parseInt(args[0]);
            if (args.length > 1) grpcThreads = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Port and thread count must be integers.");
        }
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Port must be 1-65535.");
        if (grpcThreads < 1) throw new IllegalArgumentException("Thread count must be positive.");
        if (args.length > 2) {
            if (args[2].isBlank()) throw new IllegalArgumentException("Upload directory cannot be blank.");
            uploadDirectory = Path.of(args[2]).toAbsolutePath().normalize();
        }
    }

    // 1. JavaFX calls start(Stage). Create Person 4's window on the JavaFX thread.
    // 2. Give Person 1's upload service this object's accept(Path) callback.
    // 3. Start gRPC with a fixed executor.
    @Override
    public void start(Stage stage) {
        try {
            configure(getParameters().getRaw().toArray(new String[0]));
            window = new PreviewWindow(stage);
            startServer(new VideoUploadService(uploadDirectory, this));
        } catch (IOException | RuntimeException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            Platform.exit();
        }
    }

    private void prepareDirectory() throws IOException {
        Files.createDirectories(uploadDirectory);
        if (!Files.isDirectory(uploadDirectory) || !Files.isWritable(uploadDirectory)) {
            throw new IOException("Upload path is not a writable directory: " + uploadDirectory);
        }
        Path check = Files.createTempFile(uploadDirectory, ".write-check-", ".tmp");
        Files.delete(check);
    }

    // A test can pass a small service while Person 1's upload handler is unfinished.
    synchronized void startServer(BindableService service) throws IOException {
        if (server != null || stopped) throw new IllegalStateException("Server already started or stopped.");
        prepareDirectory();
        System.out.println("[SERVER] Starting consumer...");
        System.out.println("[SERVER] Port: " + port + " (from " + portSource + ")");
        System.out.println("[SERVER] gRPC executor threads: " + grpcThreads);
        System.out.println("[SERVER] Upload directory: " + uploadDirectory);
        executor = Executors.newFixedThreadPool(grpcThreads);
        try {
            server = ServerBuilder.forPort(port).executor(executor).addService(service).build().start();
            shutdownHook = new Thread(this, "consumer-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            System.out.println("[SERVER] Ready to receive uploads");
        } catch (IOException | RuntimeException e) {
            stopServer();
            throw e;
        }
    }

    // 4. JavaFX calls stop() when the window closes. Cleanup may wait, so use a thread.
    @Override
    public void stop() {
        new Thread(this, "consumer-window-shutdown").start();
    }

    // Runnable lets the window-close thread and JVM shutdown hook share cleanup.
    @Override
    public void run() {
        stopServer();
    }

    synchronized void stopServer() {
        if (stopped) return;
        stopped = true;
        System.out.println("[SERVER] Shutting down...");
        boolean interrupted = false;
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                    server.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                interrupted = true;
                server.shutdownNow();
            }
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                interrupted = true;
                executor.shutdownNow();
            }
        }
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown has already begun.
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        System.out.println("[SERVER] Shutdown complete");
    }

    // Person 1 calls accept() after a video is complete.
    @Override
    public void accept(Path file) {
        if (window == null) System.out.println("[UPLOAD COMPLETE] " + file);
        else window.addVideo(file);
    }

    // Run with the JavaFX window by default. Use --headless until Person 4 finishes it.
    public static void main(String[] args) {
        if (args.length == 0 || !args[0].equals("--headless")) {
            launch(args);
            return;
        }
        ConsumerMain consumer = new ConsumerMain();
        try {
            String[] options = java.util.Arrays.copyOfRange(args, 1, args.length);
            consumer.configure(options);
            consumer.startServer(new VideoUploadService(consumer.uploadDirectory, consumer));
            consumer.server.awaitTermination();
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            System.err.println("Usage: ConsumerMain [--headless] [port] [grpcThreads] [uploadDirectory]");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.stopServer();
        }
    }
}
