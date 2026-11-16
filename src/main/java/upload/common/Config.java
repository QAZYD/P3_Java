package upload.common;

import java.nio.file.Path;

/**
 * SHARED CONTRACT: constants used by both the producer and the consumer.
 * Change these only after agreeing as a team.
 */
public final class Config {

    private Config() {} // constants only, no instances

    /** Consumer listens here; producer connects here. */
    public static final String HOST = "localhost";
    public static final int PORT = 50051;

    /** Size of each byte chunk the producer sends (64 KB, well under gRPC's 4 MB limit). */
    public static final int CHUNK_SIZE = 64 * 1024;

    /** Producer reads MP4s from here. */
    public static final Path VIDEOS_DIR = Path.of("videos");

    /** Consumer saves received MP4s here. */
    public static final Path UPLOADS_DIR = Path.of("uploads");

    /** Preview length in seconds. */
    public static final int PREVIEW_SECONDS = 10;
}
