package upload.common;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * SHARED CONTRACT: constants used by both the producer and the consumer.
 * Change these only after agreeing as a team.
 */
public final class Config {

    private Config() {} // constants only, no instances

    /** Default consumer address; producer connects here unless overridden (see consumerHost). */
    public static final String HOST = "localhost";
    /** Default port; both sides use it unless overridden (see consumerPort). */
    public static final int PORT = 50051;

    /** Size of each byte chunk the producer sends (64 KB, well under gRPC's 4 MB limit). */
    public static final int CHUNK_SIZE = 64 * 1024;

    /** Producer reads MP4s from here. */
    public static final Path VIDEOS_DIR = Path.of("videos");

    /** Consumer saves received MP4s here. */
    public static final Path UPLOADS_DIR = Path.of("uploads");

    /** Preview length in seconds. */
    public static final int PREVIEW_SECONDS = 10;

    /** Number of producer threads.(p) */
    public static final int PRODUCER_THREADS = 3;

    // ---------------------------------------------------------------------------------------
    // Network settings that differ per setup (same machine, VM, teammate's laptop, Docker).
    // Each is looked up in this order, first match wins:
    //   1. command-line argument
    //   2. environment variable       (CONSUMER_HOST / CONSUMER_PORT)
    //   3. local settings file        (network.properties in the folder you run from; not committed)
    //   4. the default constant above
    // ---------------------------------------------------------------------------------------

    /** Optional per-machine settings file. Copy network.properties.example to create it. */
    public static final Path LOCAL_SETTINGS = Path.of("network.properties");

    private static final Properties LOCAL = loadLocalSettings();

    /** A resolved value plus where it came from, so startup logs can show it. */
    public record Setting(String value, String source) {
        /** Parses the value as a TCP port, with a message naming the source if it is invalid. */
        public int asPort() {
            try {
                int port = Integer.parseInt(value.trim());
                if (port >= 1 && port <= 65535) {
                    return port;
                }
            } catch (NumberFormatException ignored) {
                // fall through to the error below
            }
            throw new IllegalArgumentException("Invalid port '" + value + "' from " + source + " (expected 1-65535)");
        }
    }

    /** Address of the consumer. Producer: pass its main() args; args[0], if given, is the host. */
    public static Setting consumerHost(String[] args) {
        String fromArgs = (args != null && args.length > 0) ? args[0] : null;
        return lookup(fromArgs, "CONSUMER_HOST", "consumer.host", HOST);
    }

    /** Port the consumer listens on and the producer connects to (command-line port handled by the caller). */
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
