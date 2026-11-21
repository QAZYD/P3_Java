package upload.common;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class Config {

    private Config() {} 

    public static final String HOST = "localhost";
  
    public static final int PORT = 50051;

    public static final int CHUNK_SIZE = 64 * 1024;

    public static final Path VIDEOS_DIR = Path.of("videos");

    public static final Path UPLOADS_DIR = Path.of("uploads");

    public static final int PREVIEW_SECONDS = 10;

    public static final int PRODUCER_THREADS = 3;

    public static final int CONSUMER_THREADS = 2;

    public static final int QUEUE_CAPACITY = 5;

    public static final int GRPC_THREADS = 4;

    public static final long PROCESSING_DELAY_MS = 0;

    
    public static final Path LOCAL_SETTINGS = Path.of("network.properties");

    private static final Properties LOCAL = loadLocalSettings();

    public record Setting(String value, String source) {
        public int asPort() {
            try {
                int port = Integer.parseInt(value.trim());
                if (port >= 1 && port <= 65535) {
                    return port;
                }
            } catch (NumberFormatException ignored) {
            }
            throw new IllegalArgumentException("Invalid port '" + value + "' from " + source + " (expected 1-65535)");
        }
    }

    public static Setting consumerHost(String[] args) {
        String fromArgs = (args != null && args.length > 0) ? args[0] : null;
        return lookup(fromArgs, "CONSUMER_HOST", "consumer.host", HOST);
    }

    public static Setting consumerPort() {
        return lookup(null, "CONSUMER_PORT", "consumer.port", String.valueOf(PORT));
    }

    private static Setting lookup(String argValue, String envName, String propertyName, String defaultValue) {
        if (argValue != null && !argValue.isBlank()) {
            return new Setting(argValue.trim(), "command line");
        }
        String env = System.getenv(envName);
        if (env != null && !env.isBlank()) {
            return new Setting(env.trim(), "environment variable " + envName);
        }
        String local = LOCAL.getProperty(propertyName);
        if (local != null && !local.isBlank()) {
            return new Setting(local.trim(), LOCAL_SETTINGS.toString());
        }
        return new Setting(defaultValue, "default");
    }

    private static Properties loadLocalSettings() {
        Properties properties = new Properties();
        if (Files.isRegularFile(LOCAL_SETTINGS)) {
            try (Reader reader = Files.newBufferedReader(LOCAL_SETTINGS)) {
                properties.load(reader);
            } catch (IOException e) {
                System.err.println("[CONFIG] Could not read " + LOCAL_SETTINGS.toAbsolutePath() + ": " + e.getMessage());
            }
        }
        return properties;
    }
}
