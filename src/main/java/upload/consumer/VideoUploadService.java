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


public class VideoUploadService extends VideoUploadGrpc.VideoUploadImplBase {

    private final Path uploadDir;

    private final UploadSink sink;

    private final String acceptedMessage;

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

        return new StreamObserver<UploadChunk>() {
            private OutputStream out;
            private Path targetPath;
            private boolean failed = false;
            private String failureReason; 
            private String filename;

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
                UploadSink.Result result = sink.submit(targetPath, filename);
                UploadReply reply;
                if (result == UploadSink.Result.ACCEPTED) {
                    reply = UploadReply.newBuilder()
                        .setAccepted(true)
                        .setMessage(acceptedMessage + filename)
                        .build();
                } else {
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
