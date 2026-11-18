
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

        // Arrange
        VideoUploadService service = new VideoUploadService(
            uploadDir,
            path -> {}
        );

        TestResponseObserver response = new TestResponseObserver();

        StreamObserver<UploadChunk> request = service.upload(response);

        // Act
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

        // Assert
        assertTrue(response.completed);
        assertNotNull(response.reply);
        assertTrue(response.reply.getAccepted());

        Path savedFile = uploadDir.resolve("test.mp4");

        assertTrue(Files.exists(savedFile));
        assertArrayEquals(testData, Files.readAllBytes(savedFile));
    }

    @Test
    void duplicateFilenameUpload() throws Exception {

        // Arrange
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

        // Act: First upload
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

        // Act: Second upload with the same filename
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

        // Assert
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

        // Arrange
        VideoUploadService service = new VideoUploadService(
            uploadDir,
            path -> {}
        );

        TestResponseObserver response = new TestResponseObserver();
        StreamObserver<UploadChunk> request = service.upload(response);

        byte[] testData = "Invalid upload".getBytes(StandardCharsets.UTF_8);

        // Act: Send a chunk without sending a filename first
        request.onNext(
            UploadChunk.newBuilder()
                .setChunk(ByteString.copyFrom(testData))
                .build()
        );

        request.onCompleted();

        // Assert
        assertTrue(response.completed);
        assertNotNull(response.reply);
        assertTrue(!response.reply.getAccepted());

        try (var files = Files.list(uploadDir)) {
            assertTrue(files.findAny().isEmpty());
        }
    }


@Test
void interruptedUpload() throws Exception {

    // Arrange
    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    // Act: Start an upload and write some data
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

    // Simulate the producer's connection failing
    request.onError(new RuntimeException("Connection interrupted"));

    // Assert: The partially uploaded file should be deleted
    Path interruptedFile = uploadDir.resolve("interrupted.mp4");

    assertTrue(Files.notExists(interruptedFile));
}




@Test
void invalidFilename() throws Exception {

    // Arrange
    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    // Act: Send an empty filename
    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("")
            .build()
    );

    request.onCompleted();

    // Assert
    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}


@Test
void emptyUpload() throws Exception {

    // Arrange
    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    // Act: Complete without sending any messages
    request.onCompleted();

    // Assert
    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}


@Test
void repeatedFilenameMessage() throws Exception {

    // Arrange
    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    // Act: Send the first filename and some data
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

    // Send another filename during the same upload
    request.onNext(
        UploadChunk.newBuilder()
            .setFilename("second.mp4")
            .build()
    );

    request.onCompleted();

    // Assert
    assertTrue(response.completed);
    assertNotNull(response.reply);
    assertTrue(!response.reply.getAccepted());

    try (var files = Files.list(uploadDir)) {
        assertTrue(files.findAny().isEmpty());
    }
}

@Test
void filenameSanitization() throws Exception {

    // Arrange
    VideoUploadService service = new VideoUploadService(
        uploadDir,
        path -> {}
    );

    TestResponseObserver response = new TestResponseObserver();
    StreamObserver<UploadChunk> request = service.upload(response);

    byte[] testData = "Sanitized video".getBytes(StandardCharsets.UTF_8);

    // Act
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

    // Assert
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
