package upload.consumer;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;
import upload.common.Config;
import upload.common.UserInput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Person 2: create the window and server, then stop both when the app closes. */
public class ConsumerMain extends Application implements Consumer<Path>, Runnable {
    private int port = Config.PORT;
    private String portSource = "default";
    private int grpcThreads = Config.GRPC_THREADS;
    private int consumerThreads = Config.CONSUMER_THREADS; // c
    private int queueCapacity = Config.QUEUE_CAPACITY;     // q
    private Path uploadDirectory = Config.UPLOADS_DIR.toAbsolutePath().normalize();
    private PreviewWindow window;
    private UploadQueue queue;
    private Server server;
    private ExecutorService executor;
    private Thread shutdownHook;
    private boolean stopped;

    // JavaFX creates this object using this constructor.
    public ConsumerMain() {}

    // Read --c=N and --q=N, then optional port, gRPC thread count, and upload directory.
    void configure(String... rawArgs) {
        List<String> positional = new ArrayList<>();
        for (String arg : rawArgs) {
            if (arg.startsWith("--c=")) consumerThreads = UserInput.parsePositiveOption(arg, "Consumer threads (c)");
            else if (arg.startsWith("--q=")) queueCapacity = UserInput.parsePositiveOption(arg, "Queue capacity (q)");
            else positional.add(arg);
        }
        String[] args = positional.toArray(new String[0]);
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
            startServer(createQueuedService());
        } catch (IOException | RuntimeException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            Platform.exit();
        }
    }

    // Uploads are received into a staging folder next to the uploads folder (e.g. "uploads-incoming"),
    // so the uploads folder only ever contains fully saved videos.
    Path stagingDirectory() {
        Path name = uploadDirectory.getFileName();
        return name == null
                ? uploadDirectory.resolve(".incoming")
                : uploadDirectory.resolveSibling(name + "-incoming");
    }

    // Leaky-bucket queue (capacity q) drained by c worker threads, which save into the uploads folder.
    synchronized VideoUploadService createQueuedService() {
        if (queue != null) throw new IllegalStateException("Queue already created.");
        queue = new UploadQueue(queueCapacity, consumerThreads, uploadDirectory, this);
        return new VideoUploadService(stagingDirectory(), queue);
    }

    private void prepareDirectory() throws IOException {
        Files.createDirectories(uploadDirectory);
        if (!Files.isDirectory(uploadDirectory) || !Files.isWritable(uploadDirectory)) {
            throw new IOException("Upload path is not a writable directory: " + uploadDirectory);
        }
        Path check = Files.createTempFile(uploadDirectory, ".write-check-", ".tmp");
        Files.delete(check);
        if (queue != null) {
            // Leftovers in staging are unfinished uploads from a previous run that was killed.
            Path staging = stagingDirectory();
            Files.createDirectories(staging);
            try (Stream<Path> leftovers = Files.list(staging)) {
                for (Path leftover : leftovers.toList()) {
                    Files.deleteIfExists(leftover);
                }
            }
        }
    }

    // A test can pass a small service while Person 1's upload handler is unfinished.
    synchronized void startServer(BindableService service) throws IOException {
        if (server != null || stopped) throw new IllegalStateException("Server already started or stopped.");
        prepareDirectory();
        System.out.println("[SERVER] Starting consumer...");
        System.out.println("[SERVER] Port: " + port + " (from " + portSource + ")");
        System.out.println("[SERVER] gRPC executor threads: " + grpcThreads);
        System.out.println("[SERVER] Upload directory: " + uploadDirectory);
        if (queue != null) {
            System.out.println("[SERVER] Consumer threads (c): " + consumerThreads);
            System.out.println("[SERVER] Queue capacity (q): " + queueCapacity
                    + (Config.PROCESSING_DELAY_MS > 0 ? " (processing delay " + Config.PROCESSING_DELAY_MS + " ms)" : ""));
        }
        executor = Executors.newFixedThreadPool(grpcThreads);
        try {
            if (queue != null) queue.start();
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
        // No more uploads can arrive now; let workers finish, discard videos still waiting.
        if (queue != null) queue.shutdown(5, TimeUnit.SECONDS);
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
        args = withPromptedQueueSettings(args);
        if (args.length == 0 || !args[0].equals("--headless")) {
            launch(args);
            return;
        }
        ConsumerMain consumer = new ConsumerMain();
        try {
            String[] options = Arrays.copyOfRange(args, 1, args.length);
            consumer.configure(options);
            consumer.startServer(consumer.createQueuedService());
            consumer.server.awaitTermination();
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            System.err.println("Usage: ConsumerMain [--headless] [--c=N] [--q=N] [port] [grpcThreads] [uploadDirectory]");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.stopServer();
        }
    }

    // Spec: c and q are inputs from the user. Ask for any not given as --c=N / --q=N.
    static String[] withPromptedQueueSettings(String[] args) {
        List<String> all = new ArrayList<>(Arrays.asList(args));
        boolean hasC = all.stream().anyMatch(a -> a.startsWith("--c="));
        boolean hasQ = all.stream().anyMatch(a -> a.startsWith("--q="));
        if (!hasC) all.add("--c=" + UserInput.askPositiveInt("Number of consumer threads (c)", Config.CONSUMER_THREADS));
        if (!hasQ) all.add("--q=" + UserInput.askPositiveInt("Max queue length (q)", Config.QUEUE_CAPACITY));
        return all.toArray(new String[0]);
    }
}
