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
import com.google.protobuf.ByteString.Output;

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

    private final Path uploadDir;

    /**
     * SHARED CONTRACT: called with the saved file's path once an upload completes successfully.
     * ConsumerMain passes in previewWindow::addVideo here, so this class never touches JavaFX.
     */
    private final Consumer<Path> onUploadComplete;

    public VideoUploadService(Path uploadDir, Consumer<Path> onUploadComplete) {
        this.uploadDir = uploadDir;
        this.onUploadComplete = onUploadComplete;


    }

  
    @Override
    public StreamObserver<UploadChunk> upload(StreamObserver<UploadReply> responseObserver) {

        // Everything inside this anonymous class belongs to ONE upload.
        return new StreamObserver<UploadChunk>() {
            // TODO 1: What state does a single upload need to remember between onNext calls?
            //         Declare those fields here.
            private OutputStream out;
            private Path targetPath;
            private long bytesWritten = 0;
            private boolean failed = false;
            private String filename;

            @Override
            public void onNext(UploadChunk msg) {
                switch (msg.getDataCase()) {
                    case FILENAME -> {
                        // TODO 2: The first message. Decide where the file goes and open it for writing.
                        //         Hint: Files.newOutputStream(...) and uploadDir.resolve(...)
                        filename = Path.of(msg.getFilename()).getFileName().toString();
                        if (filename.isBlank() || filename.equals(".") || filename.equals("..")){
                            failed = true;
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
                            targetPath = uploadDir.resolve(candidateName);
                            try{
                                out = Files.newOutputStream(targetPath, StandardOpenOption.CREATE_NEW);
                                created = true;
                            } catch(FileAlreadyExistsException e){
                                counter++;
                            } 
                        }
                    
                        
                        } catch(IOException e){
                            //ERROR HANDLER
                        }  
                    }

                    case CHUNK -> {
                        // TODO 3: Write the bytes to the file.
                        //         Hint: msg.getChunk() returns a ByteString, which has writeTo(OutputStream).
                        if (failed || out == null){
                            failed = true;
                            return;
                        }
                        try{
                            msg.getChunk().writeTo(out);
                            bytesWritten += msg.getChunk().size();
                        }catch(IOException e){
                            failed = true;
                        }
                    }
                    case DATA_NOT_SET -> {
                        // TODO 4 (design): Is an empty message an error? What should we do?
                    }
                }
            }

            @Override
            public void onCompleted() {
                // TODO 5: The producer is done. Close the file, then reply to the producer:
                //         responseObserver.onNext(UploadReply.newBuilder()...build());
                //         responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                // TODO 6 (design): The upload broke halfway. What should happen to the half-written file?
            }
        };
    }
}
