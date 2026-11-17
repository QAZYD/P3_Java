package upload.consumer;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;
import javafx.event.EventHandler;
import upload.common.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Person 2: configuration, server lifecycle and component wiring. */
public final class ConsumerMain {
    private ConsumerMain() {}

    // An ordinary class holds the command-line settings.
    static final class Settings {
        private final int port;
        private final int grpcThreads;
        private final Path uploadDirectory;
        private final boolean gui;

        Settings(int port, int grpcThreads, Path uploadDirectory, boolean gui) {
            this.port = port;
            this.grpcThreads = grpcThreads;
            this.uploadDirectory = uploadDirectory;
            this.gui = gui;
        }

        int port() {
            return port;
        }

        int grpcThreads() {
            return grpcThreads;
        }

        Path uploadDirectory() {
            return uploadDirectory;
        }

        boolean gui() {
            return gui;
        }

        static Settings parse(String... args) {
            boolean gui = args.length > 0 && args[0].equals("--gui");
            int first = 0;
            if (gui) {
                first = 1;
            }
            if (args.length - first > 3) {
                throw new IllegalArgumentException("Expected at most three positional arguments.");
            }
            int port = number(args, first, Config.PORT, "Port");
            int threads = number(args, first + 1, 4, "gRPC thread count");
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535.");
            }
            if (threads < 1) {
                throw new IllegalArgumentException("gRPC thread count must be greater than zero.");
            }
            String directory = Config.UPLOADS_DIR.toString();
            if (args.length > first + 2) {
                directory = args[first + 2];
            }
            if (directory.isBlank()) throw new IllegalArgumentException("Upload directory must not be blank.");
            return new Settings(port, threads, Path.of(directory).toAbsolutePath().normalize(), gui);
        }

        private static int number(String[] args, int index, int fallback, String label) {
            if (args.length <= index) {
                return fallback;
            }
            try {
                return Integer.parseInt(args[index]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(label + " must be an integer.");
            }
        }

        void prepareDirectory() throws IOException {
            Files.createDirectories(uploadDirectory);
            if (!Files.isDirectory(uploadDirectory) || !Files.isWritable(uploadDirectory)) {
                throw new IOException("Upload path must be a writable directory: " + uploadDirectory);
            }
            // Check actual creation permissions, including filesystem ACLs.
            Path probe = Files.createTempFile(uploadDirectory, ".consumer-write-check-", ".tmp");
            Files.delete(probe);
        }
    }

    /** Injectable service boundary permits independent tests without implementing uploads. */
    static final class ServerRuntime implements AutoCloseable {
        private final Server server;
        private final ExecutorService executor;
        private final long timeoutMillis;
        private boolean closed;

        private ServerRuntime(Server server, ExecutorService executor, long timeoutMillis) {
            this.server = server;
            this.executor = executor;
            this.timeoutMillis = timeoutMillis;
        }

        static ServerRuntime start(Settings settings, BindableService service, long timeoutMillis) throws IOException {
            settings.prepareDirectory();
            System.out.println("[SERVER] Starting consumer...");
            System.out.println("[SERVER] Port: " + settings.port());
            System.out.println("[SERVER] gRPC executor threads: " + settings.grpcThreads());
            System.out.println("[SERVER] Upload directory: " + settings.uploadDirectory());
            ExecutorService executor = Executors.newFixedThreadPool(settings.grpcThreads());
            Server server = null;
            try {
                server = ServerBuilder.forPort(settings.port()).executor(executor).addService(service).build();
                server.start();
                System.out.println("[SERVER] Ready to receive uploads");
                return new ServerRuntime(server, executor, timeoutMillis);
            } catch (IOException | RuntimeException e) {
                if (server != null) server.shutdownNow();
                executor.shutdownNow();
                throw e;
            }
        }

        int port() {
            return server.getPort();
        }

        boolean isTerminated() {
            return server.isTerminated() && executor.isTerminated();
        }

        void awaitTermination() throws InterruptedException {
            server.awaitTermination();
        }

        // Synchronization makes concurrent callers wait for the same cleanup to finish.
        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            System.out.println("[SERVER] Shutting down...");
            boolean interrupted = false;
            server.shutdown();
            try {
                stopServer();
            } catch (InterruptedException e) {
                interrupted = true;
                server.shutdownNow();
            }

            try {
                stopExecutor();
            } catch (InterruptedException e) {
                interrupted = true;
                executor.shutdownNow();
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            System.out.println("[SERVER] Shutdown complete (termination requested for all resources)");
        }

        private void stopServer() throws InterruptedException {
            if (!server.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                server.shutdownNow();
                if (!server.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    System.err.println("[SERVER] gRPC termination timed out.");
                }
            }
        }

        private void stopExecutor() throws InterruptedException {
            executor.shutdown();
            if (!executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    System.err.println("[SERVER] Executor tasks did not respond to interruption.");
                }
            }
        }
    }

    private static ServerRuntime start(Settings settings, Consumer<Path> callback) throws IOException {
        return ServerRuntime.start(settings, new VideoUploadService(settings.uploadDirectory(), callback), 5000);
    }

    private static Thread registerHook(ServerRuntime runtime) {
        Thread hook = new Thread(new ShutdownTask(runtime, null), "consumer-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(hook);
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
        return hook;
    }

    private static void removeHook(Thread hook) {
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException ignored) {
            // JVM shutdown already owns the hook.
        }
    }

    public static void main(String[] args) {
        try {
            Settings settings = Settings.parse(args);
            if (settings.gui()) {
                Application.launch(GuiApplication.class, args);
                return;
            }
            ServerRuntime runtime = start(settings, new UploadCallback(null));
            Thread hook = registerHook(runtime);
            try {
                runtime.awaitTermination();
            } finally {
                runtime.close();
                removeHook(hook);
            }
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            System.err.println("Usage: ConsumerMain [--gui] [port] [grpcThreads] [uploadDirectory]");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // The upload service calls accept() when a file is complete.
    private static final class UploadCallback implements Consumer<Path> {
        private final PreviewWindow window;

        UploadCallback(PreviewWindow window) {
            this.window = window;
        }

        @Override
        public void accept(Path path) {
            if (window == null) {
                System.out.println("[UPLOAD COMPLETE] " + path);
            } else {
                window.addVideo(path);
            }
        }
    }

    // Both the JVM hook and GUI cleanup thread run this same task.
    private static final class ShutdownTask implements Runnable {
        private final ServerRuntime runtime;
        private final Thread hook;

        ShutdownTask(ServerRuntime runtime, Thread hook) {
            this.runtime = runtime;
            this.hook = hook;
        }

        @Override
        public void run() {
            runtime.close();
            if (hook != null) {
                removeHook(hook);
            }
        }
    }

    private static final class WindowCloseHandler implements EventHandler<WindowEvent> {
        @Override
        public void handle(WindowEvent event) {
            Platform.exit();
        }
    }

    /** Lifecycle adapter only; Person 4 owns all window content and media behavior. */
    public static final class GuiApplication extends Application {
        private ServerRuntime runtime;
        private Thread hook;

        @Override public void start(Stage stage) {
            try {
                Settings settings = Settings.parse(getParameters().getRaw().toArray(new String[0]));
                PreviewWindow window = new PreviewWindow(stage);
                runtime = ConsumerMain.start(settings, new UploadCallback(window));
                hook = registerHook(runtime);
                stage.setOnCloseRequest(new WindowCloseHandler());
                System.out.println("[SERVER] GUI lifecycle connected; PreviewWindow must show its stage.");
            } catch (IOException | RuntimeException e) {
                System.err.println("[SERVER] Cannot start consumer GUI: " + e.getMessage());
                if (runtime != null) closeInBackground();
                Platform.exit();
            }
        }

        private void closeInBackground() {
            ShutdownTask task = new ShutdownTask(runtime, hook);
            Thread cleanupThread = new Thread(task, "consumer-window-shutdown");
            cleanupThread.start();
        }

        @Override public void stop() {
            // Never wait for RPCs or executor tasks on the JavaFX application thread.
            if (runtime != null) closeInBackground();
        }
    }
}
