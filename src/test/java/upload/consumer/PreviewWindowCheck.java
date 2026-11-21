package upload.consumer;

import javafx.animation.AnimationTimer;
import javafx.application.Platform;
import javafx.event.Event;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.Slider;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.image.PixelFormat;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.FlowPane;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.stage.Stage;

import java.nio.file.Path;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;


public final class PreviewWindowCheck {
    private static Stage stage;
    private static PreviewWindow window;

    private PreviewWindowCheck() {}

    public static void main(String[] args) throws Exception {
        boolean show = args.length > 0 && args[0].equals("--show");
        int offset = show ? 1 : 0;
        if (args.length - offset != 3) {
            throw new IllegalArgumentException("Supply long, short, and corrupt local video paths");
        }
        Path longClip = Path.of(args[offset]);
        Path shortClip = Path.of(args[offset + 1]);
        Path corruptClip = Path.of(args[offset + 2]);
        CompletableFuture<Void> started = new CompletableFuture<>();
        Platform.startup(() -> {
            try {
                Platform.setImplicitExit(show);
                stage = new Stage();
                if (!show) {
                    stage.setX(-10000);
                    stage.setY(-10000);
                }
                window = new PreviewWindow(stage);
                started.complete(null);
            } catch (Throwable failure) {
                started.completeExceptionally(failure);
            }
        });

        try {
            started.get(10, TimeUnit.SECONDS);
            window.addVideo(longClip);
            window.addVideo(shortClip);
            window.addVideo(corruptClip);
            if (show) {
                return;
            }
            runChecks(longClip);
            System.out.println("PASS: gallery callbacks, duplicate/partial filtering, hover limits, "
                    + "stale callbacks, full playback, pause/seek, media errors, and close cleanup");
        } finally {
            if (!show) {
                fx(() -> { stage.close(); return null; });
                Platform.exit();
            }
        }
    }

    private static void runChecks(Path longClip) throws Exception {
        window.addVideo(longClip.toAbsolutePath().normalize());
        window.addVideo(longClip.resolveSibling("incomplete.part"));
        fx(() -> {
            stage.getScene().getRoot().applyCss();
            stage.getScene().getRoot().layout();
            require(((FlowPane) find("video-gallery")).getChildren().size() == 3,
                    "Completed callbacks must produce three distinct gallery entries");
            require(!find("empty-gallery").isVisible(), "Empty state must disappear");
            require(preview(1) == null && preview(2) == null, "Gallery must load players lazily");
            stage.setWidth(540);
            stage.setHeight(420);
            return null;
        });
        saveSnapshot("gallery");
        fx(() -> {
            FlowPane gallery = (FlowPane) find("video-gallery");
            gallery.applyCss();
            gallery.layout();
            for (Node tile : gallery.getChildren()) {
                require(tile.getLayoutX() + tile.getLayoutBounds().getMaxX()
                                <= gallery.getWidth() - gallery.getPadding().getRight() + 1,
                        "Video cards must fit the gallery width after resizing");
                Node viewport = tile.lookup(".tile-viewport");
                double width = viewport.getLayoutBounds().getWidth();
                double height = viewport.getLayoutBounds().getHeight();
                require(Math.abs(width / height - 16.0 / 9) < 0.02,
                        "Video cards must preserve their 16:9 preview area");
            }
            return null;
        });

        fx(() -> {
            for (int i = 0; i < 8; i++) {
                hover(1, true);
                hover(1, false);
            }
            hover(1, true);
            return null;
        });
        await("Background media initialization must attach the selected preview", () -> preview(1) != null);
        MediaPlayer abandoned = fx(() -> preview(1));
        CompletableFuture<Void> abandonedDisposal = observeDisposal(abandoned);
        Runnable lateReady = fx(abandoned::getOnReady);
        fx(() -> {
            hover(1, false);
            lateReady.run();
            require(preview(1) == null, "Exit before media readiness must prevent late playback");
            hover(1, true);
            return null;
        });
        abandonedDisposal.get(10, TimeUnit.SECONDS);
        await("Long preview must start", () -> playing(preview(1)));
        MediaPlayer longPreview = fx(() -> preview(1));
        CompletableFuture<Void> previewDisposal = observeDisposal(longPreview);
        fx(() -> {
            require(longPreview.isMute(), "Hover must be muted");
            require(longPreview.getStopTime().toSeconds() == 10, "Long preview limit must be 10 seconds");
            return null;
        });
        await("Preview progress must advance", () -> ((ProgressBar) find("video-1")
                .lookup(".preview-progress")).getProgress() > 0);
        fx(() -> {
            Node title = find("gallery-title");
            require(title.localToScene(title.getBoundsInLocal()).getMinY() >= 20,
                    "Gallery title must remain inside the window padding during previews");
            return null;
        });
        await("Long preview must finish automatically", () -> preview(1) == null);
        previewDisposal.get(10, TimeUnit.SECONDS);

        fx(() -> { hover(2, true); return null; });
        await("Short preview must start", () -> playing(preview(2)));
        MediaPlayer shortPreview = fx(() -> preview(2));
        fx(() -> {
            double length = shortPreview.getMedia().getDuration().toSeconds();
            require(length > 0 && length < 10, "Short fixture must be shorter than 10 seconds");
            require(Math.abs(shortPreview.getStopTime().toSeconds() - length) < 0.05,
                    "Short preview must use the actual clip length");
            return null;
        });
        await("Short preview must finish normally", () -> preview(2) == null);

        Button longTile = fx(() -> (Button) find("video-1"));
        fx(() -> {
            hover(1, true);
            longTile.fire();
            return null;
        });
        await("Click must start full playback", () -> playing(fullPlayer()));
        MediaPlayer full = fx(PreviewWindowCheck::fullPlayer);
        CompletableFuture<Void> fullDisposal = observeDisposal(full);
        saveSnapshot("playback");
        window.addVideo(longClip.resolveSibling("new upload.mp4"));
        fx(() -> {
            Node title = stage.getScene().lookup(".playback-title");
            require(title.localToScene(title.getBoundsInLocal()).getMinY() >= 20,
                    "Player title must remain inside the window padding");
            require(fullPlayer() == full, "A completed-upload notification must preserve current playback");
            require(!full.isMute(), "Full playback must enable sound");
            Button mute = (Button) find("mute-video");
            mute.fire();
            require(full.isMute() && mute.getAccessibleText().equals("Unmute"),
                    "Mute control must update sound state and its accessible label");
            mute.fire();
            require(!full.isMute(), "Unmute must restore full playback sound");
            require(full.getMedia().getDuration().toSeconds() > 12, "Long fixture must exceed 12 seconds");
            require(full.getStopTime().toSeconds() > 10, "Full playback must extend past the preview");
            ((Button) find("play-pause")).fire();
            return null;
        });
        if (Boolean.getBoolean("preview.checkFullscreen")) {
            fx(() -> { ((Button) find("full-screen")).fire(); return null; });
            await("Full screen must open", stage::isFullScreen);
            fx(() -> {
                Event.fireEvent(stage.getScene().getRoot(), new KeyEvent(KeyEvent.KEY_PRESSED,
                        "", "", KeyCode.ESCAPE, false, false, false, false));
                return null;
            });
            await("Escape must leave full screen", () -> !stage.isFullScreen());
            require(fx(() -> fullPlayer() == full), "Leaving full screen must preserve the video");
        }
        await("Pause must work", () -> full.getStatus() == MediaPlayer.Status.PAUSED);
        fx(() -> {
            Slider slider = (Slider) find("playback-position");
            slider.setValue(11);
            Event.fireEvent(slider, new KeyEvent(KeyEvent.KEY_RELEASED, "", "", KeyCode.RIGHT,
                    false, false, false, false));
            ((Button) find("play-pause")).fire();
            return null;
        });
        await("Seek and playback must pass 10 seconds", () -> full.getCurrentTime().toSeconds() > 10.5);
        fx(() -> { ((Button) find("back-to-gallery")).fire(); return null; });
        fullDisposal.get(10, TimeUnit.SECONDS);

        fx(() -> { hover(3, true); return null; });
        await("Corrupt preview must show an error", () -> hasError(find("video-3")));
        fx(() -> { ((Button) find("video-3")).fire(); return null; });
        await("Corrupt full playback must show an error", () -> ((Button) find("play-pause")).isDisabled());
        fx(() -> { ((Button) find("back-to-gallery")).fire(); hover(1, true); return null; });
        await("Valid playback must recover after errors", () -> playing(preview(1)));
        MediaPlayer atClose = fx(() -> preview(1));
        CompletableFuture<Void> closeDisposal = observeDisposal(atClose);
        fx(() -> { stage.close(); return null; });
        closeDisposal.get(10, TimeUnit.SECONDS);
        window.addVideo(longClip.resolveSibling("after-close.mp4"));
        fx(() -> {
            require(((FlowPane) find("video-gallery")).getChildren().size() == 4,
                    "Late callback must leave the closed gallery alone");
            return null;
        });
    }

    private static void hover(int number, boolean entering) {
        Event.fireEvent(find("video-" + number), new MouseEvent(
                entering ? MouseEvent.MOUSE_ENTERED : MouseEvent.MOUSE_EXITED,
                0, 0, 0, 0, MouseButton.NONE, 0, false, false, false, false,
                false, false, false, false, false, false, null));
    }

    private static Node find(String id) {
        Node node = stage.getScene().lookup("#" + id);
        if (node == null) {
            throw new AssertionError("Missing UI element " + id);
        }
        return node;
    }

    private static MediaPlayer preview(int number) {
        return ((MediaView) find("preview-" + number)).getMediaPlayer();
    }

    private static MediaPlayer fullPlayer() {
        return ((MediaView) find("full-video")).getMediaPlayer();
    }

    private static boolean playing(MediaPlayer player) {
        return player != null && player.getStatus() == MediaPlayer.Status.PLAYING;
    }

    private static boolean hasError(Node tile) {
        return tile.lookupAll(".video-message").stream()
                .anyMatch(node -> node instanceof Label label && label.getText().contains("unavailable"));
    }

    private static <T> T fx(Callable<T> action) throws Exception {
        FutureTask<T> task = new FutureTask<>(action);
        Platform.runLater(task);
        return task.get(10, TimeUnit.SECONDS);
    }

    private static CompletableFuture<Void> observeDisposal(MediaPlayer player) throws Exception {
        CompletableFuture<Void> disposed = new CompletableFuture<>();
        fx(() -> {
            player.statusProperty().addListener((observable, previous, status) -> {
                if (status == MediaPlayer.Status.DISPOSED) {
                    disposed.complete(null);
                }
            });
            if (player.getStatus() == MediaPlayer.Status.DISPOSED) {
                disposed.complete(null);
            }
            return null;
        });
        return disposed;
    }

    private static void saveSnapshot(String name) throws Exception {
        String folder = System.getProperty("preview.snapshotFolder");
        if (folder == null) {
            return;
        }
        CompletableFuture<WritableImage> captured = new CompletableFuture<>();
        fx(() -> {
            stage.getScene().getRoot().applyCss();
            stage.getScene().getRoot().layout();
            stage.getScene().snapshot(result -> {
                captured.complete(result.getImage());
                return null;
            }, null);
            return null;
        });
        WritableImage snapshot = captured.get(10, TimeUnit.SECONDS);
        int width = (int) snapshot.getWidth();
        int height = (int) snapshot.getHeight();
        int[] pixels = new int[width * height];
        snapshot.getPixelReader().getPixels(0, 0, width, height,
                PixelFormat.getIntArgbInstance(), pixels, 0, width);
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        image.setRGB(0, 0, width, height, pixels, 0, width);
        ImageIO.write(image, "png", Path.of(folder, name + ".png").toFile());
    }

    private static void await(String description, BooleanSupplier condition) throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        fx(() -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(18);
            new AnimationTimer() {
                @Override
                public void handle(long now) {
                    try {
                        if (condition.getAsBoolean()) {
                            stop();
                            result.complete(null);
                        } else if (System.nanoTime() >= deadline) {
                            throw new AssertionError(description);
                        }
                    } catch (Throwable failure) {
                        stop();
                        result.completeExceptionally(failure);
                    }
                }
            }.start();
            return null;
        });
        result.get(20, TimeUnit.SECONDS);
        System.out.println("PASS: " + description);
    }

    private static void require(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }
}
