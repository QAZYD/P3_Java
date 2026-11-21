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

public class ConsumerMain extends Application implements Consumer<Path>, Runnable {
    private int port = Config.PORT;
    private String portSource = "default";
    private int grpcThreads = Config.GRPC_THREADS;
    private int consumerThreads = Config.CONSUMER_THREADS;
    private int queueCapacity = Config.QUEUE_CAPACITY;     
    private Path uploadDirectory = Config.UPLOADS_DIR.toAbsolutePath().normalize();
    private PreviewWindow window;
    private UploadQueue queue;
    private Server server;
    private ExecutorService executor;
    private Thread shutdownHook;
    private boolean stopped;

    public ConsumerMain() {}

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

    Path stagingDirectory() {
        Path name = uploadDirectory.getFileName();
        return name == null
                ? uploadDirectory.resolve(".incoming")
                : uploadDirectory.resolveSibling(name + "-incoming");
    }

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
            Path staging = stagingDirectory();
            Files.createDirectories(staging);
            try (Stream<Path> leftovers = Files.list(staging)) {
                for (Path leftover : leftovers.toList()) {
                    Files.deleteIfExists(leftover);
                }
            }
        }
    }

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

    @Override
    public void stop() {
        new Thread(this, "consumer-window-shutdown").start();
    }

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
        if (queue != null) queue.shutdown(5, TimeUnit.SECONDS);
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        System.out.println("[SERVER] Shutdown complete");
    }

    @Override
    public void accept(Path file) {
        if (window == null) System.out.println("[UPLOAD COMPLETE] " + file);
        else window.addVideo(file);
    }

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

    static String[] withPromptedQueueSettings(String[] args) {
        List<String> all = new ArrayList<>(Arrays.asList(args));
        boolean hasC = all.stream().anyMatch(a -> a.startsWith("--c="));
        boolean hasQ = all.stream().anyMatch(a -> a.startsWith("--q="));
        if (!hasC) all.add("--c=" + UserInput.askPositiveInt("Number of consumer threads (c)", Config.CONSUMER_THREADS));
        if (!hasQ) all.add("--q=" + UserInput.askPositiveInt("Max queue length (q)", Config.QUEUE_CAPACITY));
        return all.toArray(new String[0]);
    }
}
