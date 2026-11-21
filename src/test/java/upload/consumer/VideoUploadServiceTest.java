
package upload.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import upload.grpc.UploadChunk;
import upload.grpc.UploadReply;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

public class VideoUploadServiceTest {

    @TempDir
    Path uploadDir;

    private static class TestResponseObserver implements StreamObserver<UploadReply> {

        private UploadReply reply;
        private boolean completed = false;

        @Override
        public void onNext(UploadReply value) {
            reply = value;
        }

        @Override
        public void onError(Throwable t) {
            fail("Unexpected error: " + t.getMessage());
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    @Test
    void successfulUpload() throws Exception {

        VideoUploadService service = new VideoUploadService(
            uploadDir,
            path -> {}
        );

        TestResponseObserver response = new TestResponseObserver();

        StreamObserver<UploadChunk> request = service.upload(response);

        request.onNext(
            UploadChunk.newBuilder()
                .setFilename("test.mp4")
                .build()
        );

        byte[] testData = "Hello video upload".getBytes(StandardCharsets.UTF_8);

        request.onNext(
            UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(testData))
                .build()
        );

        request.onCompleted();

        assertTrue(response.completed);
        assertNotNull(response.reply);
        assertTrue(response.reply.getAccepted());

        Path savedFile = uploadDir.resolve("test.mp4");

        assertTrue(Files.exists(savedFile));
        assertArrayEquals(testData, Files.readAllBytes(savedFile));
    }

    @Test
    void duplicateFilenameUpload() throws Exception {

        VideoUploadService service = new VideoUploadService(
            uploadDir,
            path -> {}
        );

        byte[] firstData = "First video".getBytes(StandardCharsets.UTF_8);
        byte[] secondData = "Second video".getBytes(StandardCharsets.UTF_8);

        TestResponseObserver firstResponse = new TestResponseObserver();
        TestResponseObserver secondResponse = new TestResponseObserver();

        StreamObserver<UploadChunk> firstRequest = service.upload(firstResponse);
        StreamObserver<UploadChunk> secondRequest = service.upload(secondResponse);

        firstRequest.onNext(
            UploadChunk.newBuilder()
                .setFilename("test.mp4")
                .build()
        );

        firstRequest.onNext(
            UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(firstData))
                .build()
        );

        firstRequest.onCompleted();

        secondRequest.onNext(
            UploadChunk.newBuilder()
                .setFilename("test.mp4")
                .build()
        );

        secondRequest.onNext(
            UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(secondData))
                .build()
        );

        secondRequest.onCompleted();

        assertTrue(firstResponse.completed);
        assertTrue(secondResponse.completed);

        assertNotNull(firstResponse.reply);
        assertNotNull(secondResponse.reply);

        assertTrue(firstResponse.reply.getAccepted());
        assertTrue(secondResponse.reply.getAccepted());

        Path firstFile = uploadDir.resolve("test.mp4");
        Path secondFile = uploadDir.resolve("test (1).mp4");

        assertTrue(Files.exists(firstFile));
        assertTrue(Files.exists(secondFile));

        assertArrayEquals(firstData, Files.readAllBytes(firstFile));
        assertArrayEquals(secondData, Files.readAllBytes(secondFile));
    }

    @Test
    void chunkBeforeFilename() throws Exception {

        VideoUploadService service = new VideoUploadService(
            uploadDir,
            path -> {}
        );

        TestResponseObserver response = new TestResponseObserver();
        StreamObserver<UploadChunk> request = service.upload(response);

        byte[] testData = "Invalid upload".getBytes(StandardCharsets.UTF_8);

        request.onNext(
            UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(testData))
                .build()
        );

        request.onCompleted();

        assertTrue(response.completed);
        assertNotNull(response.reply);
        assertTrue(!response.reply.getAccepted());

        try (var files = Files.list(uploadDir)) {
            assertTrue(files.findAny().isEmpty());
        }
    }


@Test
void interruptedUpload() throws Exception {

    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("interrupted.mp4")
            .build()
    );

    request.onNext(
        UploadChunk.newBuilder()
            .setChunk(ByteString.copyFrom(
                "Partial video data".getBytes(StandardCharsets.UTF_8)
            ))
            .build()
    );

    request.onError(new RuntimeException("Connection interrupted"));

    Path interruptedFile = uploadDir.resolve("interrupted.mp4");

    assertTrue(Files.notExists(interruptedFile));
}




@Test
void invalidFilename() throws Exception {

    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("")
            .build()
    );

    request.onCompleted();

    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}


@Test
void emptyUpload() throws Exception {

    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    request.onCompleted();

    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}


@Test
void repeatedFilenameMessage() throws Exception {

    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("first.mp4")
            .build()
    );

    request.onNext(
        UploadChunk.newBuilder()
            .setChunk(ByteString.copyFrom(
                "Partial video data".getBytes(StandardCharsets.UTF_8)
            ))
            .build()
    );

    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("second.mp4")
            .build()
    );

    request.onCompleted();

    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}

@Test
void filenameSanitization() throws Exception {

    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    byte[] testData = "Sanitized video".getBytes(StandardCharsets.UTF_8);

    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("../sanitized.mp4")
            .build()
    );

    request.onNext(
        UploadChunk.newBuilder()
            .setChunk(ByteString.copyFrom(testData))
            .build()
    );

    request.onCompleted();

    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(response.reply.getAccepted());

    Path savedFile = uploadDir.resolve("sanitized.mp4");

    assertTrue(Files.exists(savedFile));
    assertArrayEquals(testData, Files.readAllBytes(savedFile));

    assertTrue(
        savedFile.toAbsolutePath().normalize().getParent()
            .equals(uploadDir.toAbsolutePath().normalize())
    );
}



}
