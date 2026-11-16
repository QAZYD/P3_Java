package upload.producer;

/**
 * OWNER: Person 3
 *
 * SHARED CONTRACT (per file, one gRPC stream each):
 *   1. connect to Config.HOST : Config.PORT
 *   2. send one UploadChunk with setFilename(...)
 *   3. send the file as UploadChunks with setChunk(...), Config.CHUNK_SIZE bytes each
 *   4. call onCompleted() and wait for the UploadReply
 *
 * Reads MP4s from Config.VIDEOS_DIR.
 * Run with: mvn exec:java -Dexec.mainClass=upload.producer.ProducerMain
 */
public class ProducerMain {

    public static void main(String[] args) {
        // TODO (Person 3)
    }
}
