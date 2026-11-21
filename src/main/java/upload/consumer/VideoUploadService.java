package upload.consumer;
import io.grpc.stub.StreamObserver;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;
import upload.grpc.VideoUploadGrpc;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.nio.file.StandardOpenOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;


/**
 * Consumer side of the upload.
 *
 * gRPC calls upload() ONCE per incoming upload (i.e. once per producer stream).
 * We return an object (a StreamObserver) that gRPC will then feed messages into:
 *
 *     onNext(chunk)   -> called many times: first the filename, then the byte chunks
 *     onCompleted()   -> called once if the producer finished normally
 *     onError(t)      -> called once if something went wrong (e.g. producer disconnected)
 *
 * Exactly ONE of onCompleted / onError happens at the end, never both.
 */
public class VideoUploadService extends VideoUploadGrpc.VideoUploadImplBase {

    /** Folder incoming bytes are written to: the uploads folder (direct mode) or a staging folder (queue mode). */
    private final Path uploadDir;

    /** Receives each fully uploaded, closed file. */
    private final UploadSink sink;

    /** Start of the reply message when the sink accepts the file. */
    private final String acceptedMessage;

    /**
     * Direct mode (no queue): files are saved straight into uploadDir.
     * SHARED CONTRACT: onUploadComplete is called with the saved file's path once an upload completes
     * successfully. Used by the unit tests.
     */
    public VideoUploadService(Path uploadDir, Consumer<Path> onUploadComplete) {
        this(uploadDir, (file, name) -> {
            try {
                onUploadComplete.accept(file);
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
            return UploadSink.Result.ACCEPTED;
        }, "Upload Successful: ");
    }

    /**
     * Queue mode (what the app uses): files are received into stagingDir, then handed to the
     * leaky-bucket queue. The queue's c workers move them into the uploads folder.
     */
    public VideoUploadService(Path stagingDir, UploadQueue queue) {
        this(stagingDir, queue, "Received, queued for saving: ");
    }

    private VideoUploadService(Path uploadDir, UploadSink sink, String acceptedMessage) {
        this.uploadDir = uploadDir;
        this.sink = sink;
        this.acceptedMessage = acceptedMessage;
    }

  
    @Override
    public StreamObserver<UploadChunk> upload(StreamObserver<UploadReply> responseObserver) {

        // Everything inside this anonymous class belongs to ONE upload.
        return new StreamObserver<UploadChunk>() {
            // TODO 1: What state does a single upload need to remember between onNext calls?
            //         Declare those fields here.
            private OutputStream out;
            private Path targetPath;
            private boolean failed = false;
            private String failureReason; // first reason this upload was rejected, sent back to the producer
            private String filename;

            /** Marks the upload as failed (keeping the first reason) and deletes any partial file. */
            private void reject(String reason) {
                if (failureReason == null) {
                    failureReason = reason;
                }
                failed = true;
                cleanupUpload();
            }

            @Override
            public void onNext(UploadChunk msg) {
                switch (msg.getDataCase()) {
                    case FILENAME -> {
                        // TODO 2: The first message. Decide where the file goes and open it for writing.
                        //         Hint: Files.newOutputStream(...) and uploadDir.resolve(...)
                        if (failed || filename != null || out != null){
                            reject("filename sent more than once");
                            return;
                        }

                        try {
                            Path incomingPath = Path.of(msg.getFilename());

                            if (incomingPath.getFileName() == null) {
                                reject("invalid filename: '" + msg.getFilename() + "'");
                                return;
                            }

                            filename = incomingPath.getFileName().toString();

                        } catch (InvalidPathException e) {
                            reject("invalid filename: '" + msg.getFilename() + "'");
                            return;
                        }

                        if (filename.isBlank() || filename.equals(".") || filename.equals("..")){
                            reject("invalid filename: '" + msg.getFilename() + "'");
                            return;
                        }
                        int dotIndex = filename.lastIndexOf('.');
                        String baseName = dotIndex > 0 ? filename.substring(0, dotIndex) : filename;
                        String extension = dotIndex > 0 ? filename.substring(dotIndex) : "";
                        try{
                              Files.createDirectories(uploadDir);
                        
                        int counter = 0;
                        boolean created = false;
                        while(!created){
                            String candidateName;
                            if (counter == 0){
                                candidateName = filename;
                            } else{
                                candidateName = baseName + " (" + counter + ")" + extension;
                            }
                            Path candidatePath = uploadDir.resolve(candidateName);
                            try{
                                out = Files.newOutputStream(candidatePath, StandardOpenOption.CREATE_NEW);
                                targetPath = candidatePath; 
                                created = true;
                            } catch(FileAlreadyExistsException e){
                                counter++;
                            } 
                        }
                    
                        
                        } catch(IOException e){
                            reject("server could not create the file: " + e.getMessage());
                        }
                    }

                    case CHUNK -> {
                        // TODO 3: Write the bytes to the file.
                        //         Hint: msg.getChunk() returns a ByteString, which has writeTo(OutputStream).
                        if (failed || out == null){
                            reject("file data received before the filename");
                            return;
                        }
                        try{
                            msg.getChunk().writeTo(out);
                        }catch(IOException e){
                            reject("server could not write the file: " + e.getMessage());
                        }
                    }
                    case DATA_NOT_SET -> {
                        reject("empty message (neither filename nor data)");
                    }
                }
            }

            @Override
            public void onCompleted() {
                
                // TODO 5: The producer is done. Close the file, then reply to the producer:
                //         responseObserver.onNext(UploadReply.newBuilder()...build());
                //         responseObserver.onCompleted();
                if(failed || out ==  null){
                    reject("no filename was sent");
                    UploadReply reply = UploadReply.newBuilder()
                        .setAccepted(false)
                        .setMessage("Upload rejected: " + failureReason)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }
                try{
                out.close();
                }catch(IOException e){
                    reject("server could not save the file: " + e.getMessage());
                     UploadReply reply = UploadReply.newBuilder()
                        .setAccepted(false)
                        .setMessage("Upload rejected: " + failureReason)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }
                // The whole file has arrived: hand it to the sink (direct save, or the leaky-bucket queue).
                UploadSink.Result result = sink.submit(targetPath, filename);
                UploadReply reply;
                if (result == UploadSink.Result.ACCEPTED) {
                    // The sink owns the file now; don't delete it.
                    reply = UploadReply.newBuilder()
                        .setAccepted(true)
                        .setMessage(acceptedMessage + filename)
                        .build();
                } else {
                    // Leaky bucket: no room (or shutting down), so the video is dropped and its file deleted.
                    String reason = result == UploadSink.Result.QUEUE_FULL
                        ? "queue full"
                        : "consumer is shutting down";
                    reject(reason);
                    reply = UploadReply.newBuilder()
                        .setAccepted(false)
                        .setMessage("Dropped: " + reason + " (" + filename + ")")
                        .build();
                }
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                // TODO 6 (design): The upload broke halfway. What should happen to the half-written file?
                failed = true;
                cleanupUpload();
            }

            private void cleanupUpload() {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    out = null;
                }

                if (targetPath != null) {
                    try {
                        Files.deleteIfExists(targetPath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    targetPath = null;
                }
        }
        };
    }
}
