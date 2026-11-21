package upload.consumer;

import java.nio.file.Path;

/**
 * What VideoUploadService does with a fully received, closed file.
 * Direct mode saves it on the spot; UploadQueue puts it in the leaky-bucket queue.
 */
@FunctionalInterface
public interface UploadSink {

    enum Result {
        /** The sink now owns the file. */
        ACCEPTED,
        /** Queue is at capacity (q): the upload is dropped and the caller deletes the file. */
        QUEUE_FULL,
        /** The consumer is shutting down: the caller deletes the file. */
        STOPPED
    }

    Result submit(Path file, String filename);
}
