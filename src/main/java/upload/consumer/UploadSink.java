package upload.consumer;

import java.nio.file.Path;

@FunctionalInterface
public interface UploadSink {

    enum Result {
        ACCEPTED,
        QUEUE_FULL,
        STOPPED
    }

    Result submit(Path file, String filename);
}
