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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Person 2: configuration, server lifecycle and component wiring. */
public final class ConsumerMain {
    private ConsumerMain() {}

    record Settings(int port, int grpcThreads, Path uploadDirectory, boolean gui) {
        static Settings parse(String... args) {
            boolean gui = args.length > 0 && args[0].equals("--gui");
            String[] values = gui ? Arrays.copyOfRange(args, 1, args.length) : args;
            if (values.length > 3) throw new IllegalArgumentException("Expected at most three positional arguments.");
            int port = number(values, 0, Config.PORT, "Port");
            int threads = number(values, 1, 4, "gRPC thread count");
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Port must be between 1 and 65535.");
            if (threads < 1) throw new IllegalArgumentException("gRPC thread count must be greater than zero.");
            String directory = values.length > 2 ? values[2] : Config.UPLOADS_DIR.toString();
            if (directory.isBlank()) throw new IllegalArgumentException("Upload directory must not be blank.");
            return new Settings(port, threads, Path.of(directory).toAbsolutePath().normalize(), gui);
        }

        private static int number(String[] args, int index, int fallback, String label) {
            if (args.length <= index) return fallback;
            try { return Integer.parseInt(args[index]); }
            catch (NumberFormatException e) { throw new IllegalArgumentException(label + " must be an integer."); }
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

        int port() { return server.getPort(); }
        boolean isTerminated() { return server.isTerminated() && executor.isTerminated(); }
        void awaitTermination() throws InterruptedException { server.awaitTermination(); }

        // Synchronization makes concurrent callers wait for the same cleanup to finish.
        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            System.out.println("[SERVER] Shutting down...");
            boolean interrupted = false;
            server.shutdown();
            try {
                if (!server.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    server.shutdownNow();
                    if (!server.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS))
                        System.err.println("[SERVER] gRPC termination timed out.");
                }
            } catch (InterruptedException e) {
                interrupted = true;
                server.shutdownNow();
            } finally {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                        executor.shutdownNow();
                        if (!executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS))
                            System.err.println("[SERVER] Executor tasks did not respond to interruption.");
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                    executor.shutdownNow();
                }
                if (interrupted) Thread.currentThread().interrupt();
                System.out.println("[SERVER] Shutdown complete (termination requested for all resources)");
            }
        }
    }

    private static ServerRuntime start(Settings settings, Consumer<Path> callback) throws IOException {
        return ServerRuntime.start(settings, new VideoUploadService(settings.uploadDirectory(), callback), 5000);
    }

    private static Thread registerHook(ServerRuntime runtime) {
        Thread hook = new Thread(runtime::close, "consumer-shutdown");
        try { Runtime.getRuntime().addShutdownHook(hook); }
        catch (RuntimeException e) { runtime.close(); throw e; }
        return hook;
    }

    private static void removeHook(Thread hook) {
        try { Runtime.getRuntime().removeShutdownHook(hook); }
        catch (IllegalStateException ignored) { /* JVM shutdown already owns the hook. */ }
    }

    public static void main(String[] args) {
        try {
            Settings settings = Settings.parse(args);
            if (settings.gui()) {
                Application.launch(GuiApplication.class, args);
                return;
            }
            ServerRuntime runtime = start(settings, path -> System.out.println("[UPLOAD COMPLETE] " + path));
            Thread hook = registerHook(runtime);
            try (runtime) { runtime.awaitTermination(); }
            finally { removeHook(hook); }
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("[SERVER] Cannot start consumer: " + e.getMessage());
            System.err.println("Usage: ConsumerMain [--gui] [port] [grpcThreads] [uploadDirectory]");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Lifecycle adapter only; Person 4 owns all window content and media behavior. */
    public static final class GuiApplication extends Application {
        private ServerRuntime runtime;
        private Thread hook;

        @Override public void start(Stage stage) {
            try {
                Settings settings = Settings.parse(getParameters().getRaw().toArray(String[]::new));
                PreviewWindow window = new PreviewWindow(stage);
                runtime = ConsumerMain.start(settings, window::addVideo);
                hook = registerHook(runtime);
                stage.setOnCloseRequest(event -> Platform.exit());
                System.out.println("[SERVER] GUI lifecycle connected; PreviewWindow must show its stage.");
            } catch (IOException | RuntimeException e) {
                System.err.println("[SERVER] Cannot start consumer GUI: " + e.getMessage());
                if (runtime != null) closeInBackground();
                Platform.exit();
            }
        }

        private void closeInBackground() {
            new Thread(() -> {
                runtime.close();
                if (hook != null) removeHook(hook);
            }, "consumer-window-shutdown").start();
        }

        @Override public void stop() {
            // Never wait for RPCs or executor tasks on the JavaFX application thread.
            if (runtime != null) closeInBackground();
        }
    }
}
