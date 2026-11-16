package upload.consumer;

import javafx.application.Application;
import javafx.stage.Stage;

/**
 * OWNER: Person 2
 *
 * SHARED CONTRACT (wiring order):
 *   1. JavaFX calls start(stage)                 -> create PreviewWindow(stage)
 *   2. create VideoUploadService(Config.UPLOADS_DIR, previewWindow::addVideo)
 *   3. start the gRPC server on Config.PORT with that service and a fixed thread pool
 *   4. stop() shuts down the server and the thread pool when the window closes
 *
 * Run with: mvn javafx:run
 */
public class ConsumerMain extends Application {

    @Override
    public void start(Stage stage) {
        // TODO (Person 2): steps 1-3 above
    }

    @Override
    public void stop() {
        // TODO (Person 2): step 4 above
    }

    public static void main(String[] args) {
        launch(args);
    }
}
