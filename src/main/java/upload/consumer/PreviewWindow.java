package upload.consumer;

import javafx.stage.Stage;

import java.nio.file.Path;

/**
 * OWNER: Person 4
 *
 * SHARED CONTRACT:
 *   - Created by ConsumerMain inside JavaFX's start(Stage), so the constructor runs on the JavaFX thread.
 *   - addVideo(Path) may be called from ANY thread (gRPC worker threads), so it must
 *     hand its UI work to the JavaFX thread with Platform.runLater(...).
 */
public class PreviewWindow {

    private final Stage stage;

    public PreviewWindow(Stage stage) {
        this.stage = stage;
        // TODO (Person 4): build the window (layout, title, size) and call stage.show()
    }

    /** Adds a finished upload to the window and sets up its 10-second preview. Thread-safe. */
    public void addVideo(Path file) {
        // TODO (Person 4): replace this placeholder with Platform.runLater(() -> { ... })
        System.out.println("[PreviewWindow] received: " + file);
    }
}
