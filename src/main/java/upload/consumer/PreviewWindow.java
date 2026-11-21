package upload.consumer;

import javafx.application.Platform;
import javafx.css.PseudoClass;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.geometry.Orientation;
import javafx.scene.Scene;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Separator;
import javafx.scene.control.Slider;
import javafx.scene.control.Tooltip;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;
import javafx.util.Duration;
import upload.common.Config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OWNER: Person 4
 *
 * SHARED CONTRACT:
 *   - Created by ConsumerMain inside JavaFX's start(Stage), so the constructor runs on the JavaFX thread.
 *   - addVideo(Path) may be called from ANY thread (gRPC worker threads), so it must
 *     hand its UI work to the JavaFX thread with Platform.runLater(...).
 */
public class PreviewWindow {

    private static final PseudoClass PREVIEWING = PseudoClass.getPseudoClass("previewing");

    private final Stage stage;
    private final BorderPane root = new BorderPane();
    private final FlowPane tiles = new FlowPane(20, 20);
    private final Map<Path, VideoTile> videos = new LinkedHashMap<>();
    private final Label videoCount = new Label("0 videos");
    private final VBox emptyMessage = new VBox(10);
    private final VBox gallery;
    private final BorderPane playback = new BorderPane();
    private final MediaView fullView = new MediaView();
    private final Label playbackTitle = new Label();
    private final Label playbackDuration = new Label();
    private final Label playbackMessage = new Label();
    private final Button playPause = new Button();
    private final Button mute = new Button();
    private final Button fullScreen = new Button();
    private final Slider position = new Slider();
    private final Label time = new Label("00:00 / 00:00");
    private final ThreadPoolExecutor mediaLoader = new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
                Thread thread = new Thread(task, "video-media-loader");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.DiscardOldestPolicy());

    // The gallery and attached players belong to the JavaFX thread. Pending construction
    // crosses from the loader through Playback.pendingPlayer's atomic ownership handoff.
    private Playback active;
    private volatile boolean closed;
    private double tileWidth = 276;

    public PreviewWindow(Stage stage) {
        if (!Platform.isFxApplicationThread()) {
            throw new IllegalStateException("Create PreviewWindow on the JavaFX application thread");
        }
        this.stage = stage;

        Label title = new Label("Uploaded videos");
        title.setId("gallery-title");
        title.getStyleClass().add("heading");
        Region headingSpace = new Region();
        HBox.setHgrow(headingSpace, Priority.ALWAYS);
        videoCount.getStyleClass().add("video-count");
        HBox heading = new HBox(16, title, headingSpace, videoCount);
        heading.setAlignment(Pos.BASELINE_LEFT);
        Label hint = new Label("Hover to preview for 10 seconds · Click to open");
        hint.getStyleClass().add("secondary");
        hint.setWrapText(true);

        tiles.setPadding(new Insets(8));
        tiles.setId("video-gallery");
        VBox galleryContents = new VBox(16, tiles, hint);
        galleryContents.setMinWidth(0);
        ScrollPane scroll = new ScrollPane(galleryContents);
        scroll.setMinSize(0, 0);
        scroll.getStyleClass().add("gallery-scroll");
        scroll.setFitToWidth(true);
        scroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
        tiles.widthProperty().addListener((observable, previous, width) ->
                resizeTiles(width.doubleValue(), scroll.getViewportBounds().getHeight()));
        scroll.viewportBoundsProperty().addListener((observable, previous, bounds) ->
                resizeTiles(tiles.getWidth(), bounds.getHeight()));
        StackPane galleryContent = new StackPane(scroll, emptyMessage);
        galleryContent.setMinSize(0, 0);
        emptyMessage.setMouseTransparent(true);
        emptyMessage.setAlignment(Pos.CENTER);
        Label emptyTitle = new Label("No uploaded videos yet");
        emptyTitle.getStyleClass().add("empty-title");
        Label emptyHint = new Label("Completed uploads are added automatically.");
        emptyHint.getStyleClass().add("secondary");
        emptyHint.setWrapText(true);
        emptyMessage.getChildren().addAll(emptyTitle, emptyHint);
        emptyMessage.setId("empty-gallery");
        hint.visibleProperty().bind(emptyMessage.visibleProperty().not());
        hint.managedProperty().bind(hint.visibleProperty());
        gallery = new VBox(16, heading, galleryContent);
        gallery.setMinSize(0, 0);
        VBox.setVgrow(galleryContent, Priority.ALWAYS);

        buildPlaybackView();
        root.setCenter(gallery);
        root.setPadding(new Insets(30));
        root.setOnKeyPressed(event -> {
            if (event.getCode() == KeyCode.ESCAPE) {
                if (stage.isFullScreen()) {
                    stage.setFullScreen(false);
                } else if (root.getCenter() == playback) {
                    showGallery();
                }
                event.consume();
            }
        });
        Scene scene = new Scene(root, 980, 660);
        scene.getStylesheets().add(Objects.requireNonNull(
                PreviewWindow.class.getResource("preview-window.css")).toExternalForm());
        stage.setTitle("Video Consumer");
        stage.setMinWidth(540);
        stage.setMinHeight(420);
        stage.setScene(scene);
        stage.setFullScreenExitHint("");
        stage.setFullScreenExitKeyCombination(KeyCombination.NO_MATCH);
        stage.fullScreenProperty().addListener((observable, previous, maximized) ->
                setIcon(fullScreen, maximized ? "minimize" : "maximize",
                        maximized ? "Exit full screen" : "Full screen"));
        // addEventHandler (not setOnHidden) so this doesn't replace any other window handlers.
        // Stops media here; ConsumerMain shuts the server down in Application.stop().
        stage.addEventHandler(WindowEvent.WINDOW_HIDDEN, event -> {
            closed = true;
            stopPlayback();
            mediaLoader.shutdownNow();
        });
        stage.show();
    }

    /**
     * Adds a completed local file. May be called from any thread after JavaFX starts.
     * Only called after an upload has fully finished, so the file is complete.
     * Duplicate paths and notifications after window closure are ignored.
     */
    public void addVideo(Path file) {
        Path path = Objects.requireNonNull(file, "file").toAbsolutePath().normalize();
        if (path.getFileName() == null) {
            throw new IllegalArgumentException("Expected a video file path");
        }
        if (closed) {
            return;
        }
        Platform.runLater(() -> {
            if (closed || videos.containsKey(path)) {
                return;
            }
            VideoTile tile = new VideoTile(path, videos.size() + 1);
            videos.put(path, tile);
            tile.resize(tileWidth);
            tiles.getChildren().add(tile.button);
            videoCount.setText(videos.size() + (videos.size() == 1 ? " video" : " videos"));
            emptyMessage.setVisible(false);
        });
    }

    private void buildPlaybackView() {
        Button back = new Button("Videos", FluentIcons.create("back"));
        back.setId("back-to-gallery");
        back.getStyleClass().add("back-button");
        back.setGraphicTextGap(10);
        back.setAccessibleText("Back to videos");
        back.setOnAction(event -> showGallery());
        playbackTitle.getStyleClass().add("playback-title");
        playbackTitle.setMaxWidth(Double.MAX_VALUE);
        playbackDuration.getStyleClass().add("secondary");
        VBox metadata = new VBox(3, playbackTitle, playbackDuration);
        metadata.setMinWidth(0);
        HBox.setHgrow(metadata, Priority.ALWAYS);
        Separator divider = new Separator(Orientation.VERTICAL);
        divider.setPrefHeight(32);
        divider.setMaxHeight(32);
        HBox heading = new HBox(20, back, divider, metadata);
        heading.setAlignment(Pos.CENTER_LEFT);
        heading.setPadding(new Insets(0, 0, 18, 0));

        fullView.setPreserveRatio(true);
        fullView.setId("full-video");
        playbackMessage.setWrapText(true);
        playbackMessage.getStyleClass().add("video-message");
        StackPane viewport = new StackPane(fullView, playbackMessage);
        viewport.setMinSize(0, 0);
        fullView.fitWidthProperty().bind(viewport.widthProperty());
        fullView.fitHeightProperty().bind(viewport.heightProperty());

        playPause.setId("play-pause");
        playPause.getStyleClass().add("media-control");
        setIcon(playPause, "play", "Play");
        playPause.setOnAction(event -> togglePlayback());
        mute.setId("mute-video");
        mute.getStyleClass().add("media-control");
        setIcon(mute, "speaker", "Mute");
        mute.setOnAction(event -> {
            if (active != null && active.player != null && !active.preview) {
                active.player.setMute(!active.player.isMute());
                setIcon(mute, active.player.isMute() ? "muted" : "speaker",
                        active.player.isMute() ? "Unmute" : "Mute");
            }
        });
        fullScreen.setId("full-screen");
        fullScreen.getStyleClass().add("media-control");
        setIcon(fullScreen, "maximize", "Full screen");
        fullScreen.setOnAction(event -> stage.setFullScreen(!stage.isFullScreen()));
        position.setId("playback-position");
        position.getStyleClass().add("seek-slider");
        position.setAccessibleText("Playback position");
        position.setOnMouseReleased(event -> seek());
        position.setOnKeyReleased(event -> seek());
        position.valueChangingProperty().addListener((observable, wasChanging, changing) -> {
            if (!changing) {
                seek();
            }
        });
        time.getStyleClass().add("playback-time");
        time.setMinWidth(Region.USE_PREF_SIZE);
        ProgressBar played = new ProgressBar(0);
        played.progressProperty().bind(position.valueProperty().divide(position.maxProperty()));
        played.setMaxWidth(Double.MAX_VALUE);
        played.setMouseTransparent(true);
        played.getStyleClass().add("seek-progress");
        StackPane.setMargin(played, new Insets(0, 7, 0, 7));
        StackPane seekArea = new StackPane(played, position);
        seekArea.setMinWidth(40);
        HBox.setHgrow(seekArea, Priority.ALWAYS);
        HBox controls = new HBox(12, playPause, time, seekArea, mute, fullScreen);
        controls.setAlignment(Pos.CENTER_LEFT);
        controls.getStyleClass().add("player-controls");
        BorderPane videoSurface = new BorderPane(viewport);
        videoSurface.setMinSize(0, 0);
        videoSurface.getStyleClass().add("video-viewport");
        videoSurface.setBottom(controls);
        roundViewport(videoSurface);
        playback.setTop(heading);
        playback.setCenter(videoSurface);
        playback.setMinSize(0, 0);
    }

    private void startPlayback(VideoTile tile, boolean preview) {
        if (closed) {
            return;
        }
        stopPlayback();
        MediaView view = tile.view;
        tile.message.setGraphic(null);
        if (preview) {
            tile.message.setText("Loading preview…");
            tile.message.setGraphic(loadingIndicator());
            tile.message.setVisible(true);
            tile.indicator.setVisible(false);
            tile.button.pseudoClassStateChanged(PREVIEWING, true);
        } else {
            view = fullView;
            root.setCenter(playback);
            playbackTitle.setText(tile.path.getFileName().toString());
            playbackTitle.setTooltip(new Tooltip(tile.path.getFileName().toString()));
            playbackDuration.setText(tile.duration.getText());
            playbackMessage.setText("Loading video…");
            playbackMessage.setGraphic(loadingIndicator());
            playbackMessage.setVisible(true);
            setControlsDisabled(true);
            position.setValue(0);
            time.setText("00:00 / 00:00");
            setIcon(playPause, "play", "Play");
            setIcon(mute, "speaker", "Mute");
            root.requestFocus();
        }

        Playback session = new Playback(tile, view, preview);
        active = session;
        mediaLoader.execute(() -> loadMedia(session));
    }

    private void loadMedia(Playback session) {
        try {
            Media media = new Media(session.tile.path.toUri().toString());
            MediaPlayer player = new MediaPlayer(media);
            session.pendingPlayer.set(player);
            if (session.cancelled || closed) {
                disposePending(session);
                return;
            }
            Platform.runLater(() -> attachPlayer(session));
        } catch (RuntimeException error) {
            // Catch every runtime failure (MediaException, UnsupportedOperationException, ...)
            // so the tile shows "Preview unavailable" instead of loading forever.
            disposePending(session);
            if (!session.cancelled && !closed) {
                Platform.runLater(() -> playbackFailed(session, error));
            }
        }
    }

    private void attachPlayer(Playback session) {
        MediaPlayer player = session.pendingPlayer.getAndSet(null);
        if (player == null) {
            return;
        }
        if (active != session || closed) {
            player.dispose();
            return;
        }
        session.player = player;
        Media media = player.getMedia();
        session.view.setMediaPlayer(player);
        player.setMute(session.preview);
        player.setCycleCount(1);
        player.setOnReady(() -> ready(session));
        player.setOnError(() -> playbackFailed(session, player.getError()));
        media.setOnError(() -> playbackFailed(session, media.getError()));
        player.setOnHalted(() -> playbackFailed(session, player.getError()));
        player.setOnEndOfMedia(() -> {
            if (active != session) {
                return;
            }
            if (session.preview) {
                stopPlayback();
            } else {
                session.finished = true;
                setIcon(playPause, "play", "Replay");
            }
        });
        player.setOnPlaying(() -> {
            if (active == session && !session.preview) {
                setIcon(playPause, "pause", "Pause");
            }
        });
        player.setOnPaused(() -> {
            if (active == session && !session.preview) {
                setIcon(playPause, "play", "Play");
            }
        });
        player.currentTimeProperty().addListener((observable, previous, current) -> {
            if (active != session) {
                return;
            }
            if (session.preview) {
                session.tile.progress.setProgress(Math.min(1, current.toSeconds() / session.previewLimit));
            } else {
                if (!position.isValueChanging() && !position.isPressed()) {
                    position.setValue(current.toSeconds());
                }
                time.setText(formatTime(current) + " / " + formatTime(media.getDuration()));
            }
        });
        if (media.getError() != null || player.getError() != null) {
            playbackFailed(session, media.getError() != null ? media.getError() : player.getError());
        } else if (player.getStatus() == MediaPlayer.Status.READY) {
            ready(session);
        }
    }

    private void ready(Playback session) {
        // A prior hover may finish loading after the pointer has left or another clip has opened.
        if (active != session || closed || session.ready) {
            return;
        }
        session.ready = true;
        Duration duration = session.player.getMedia().getDuration();
        if (validDuration(duration)) {
            session.tile.duration.setText(formatTime(duration));
        }
        if (session.preview) {
            Duration limit = Duration.seconds(Config.PREVIEW_SECONDS);
            if (validDuration(duration) && duration.lessThan(limit)) {
                limit = duration;
            }
            session.player.setStopTime(limit);
            session.previewLimit = limit.toSeconds();
            session.tile.progress.setProgress(0);
            session.tile.progress.setVisible(true);
            session.tile.message.setGraphic(null);
            session.tile.message.setVisible(false);
        } else {
            playbackMessage.setGraphic(null);
            playbackMessage.setVisible(false);
            setControlsDisabled(false);
            position.setDisable(!validDuration(duration));
            if (validDuration(duration)) {
                position.setMax(duration.toSeconds());
            }
            playbackDuration.setText(formatTime(duration));
            time.setText("00:00 / " + formatTime(duration));
        }
        session.player.play();
    }

    private void playbackFailed(Playback session, Throwable error) {
        if (active == session) {
            stopPlayback();
            showPlaybackError(session.tile, session.preview, error);
        }
    }

    private void showPlaybackError(VideoTile tile, boolean preview, Throwable error) {
        tile.message.setText("Preview unavailable");
        tile.message.setGraphic(FluentIcons.create("unavailable"));
        tile.message.setVisible(true);
        tile.indicator.setVisible(false);
        if (!preview) {
            playbackMessage.setText("Unable to play this video.\nThe file may be unsupported, damaged, or unavailable.");
            playbackMessage.setGraphic(null);
            playbackMessage.setVisible(true);
            setControlsDisabled(true);
        }
        System.err.println("[PreviewWindow] Cannot play " + tile.path + ": " + error);
    }

    private void togglePlayback() {
        if (active == null || active.preview || active.player == null) {
            return;
        }
        if (active.player.getStatus() == MediaPlayer.Status.PLAYING) {
            active.player.pause();
        } else {
            if (active.finished) {
                active.player.seek(Duration.ZERO);
                active.finished = false;
            }
            active.player.play();
        }
    }

    private void seek() {
        if (active != null && active.player != null && !active.preview && !position.isDisabled()) {
            active.finished = false;
            active.player.seek(Duration.seconds(position.getValue()));
            if (active.player.getStatus() != MediaPlayer.Status.PLAYING) {
                setIcon(playPause, "play", "Play");
            }
        }
    }

    private void showGallery() {
        stage.setFullScreen(false);
        VideoTile previous = active == null ? null : active.tile;
        stopPlayback();
        root.setCenter(gallery);
        if (previous != null) {
            previous.button.requestFocus();
        }
    }

    private void stopPlayback() {
        Playback previous = active;
        active = null;
        mediaLoader.getQueue().clear();
        if (previous == null) {
            return;
        }
        previous.cancelled = true;
        disposePending(previous);
        previous.view.setMediaPlayer(null);
        if (previous.player != null) {
            previous.player.setOnReady(null);
            previous.player.setOnError(null);
            previous.player.getMedia().setOnError(null);
            previous.player.setOnHalted(null);
            previous.player.setOnEndOfMedia(null);
            previous.player.setOnPlaying(null);
            previous.player.setOnPaused(null);
            previous.player.dispose();
        }
        if (previous.preview) {
            previous.tile.message.setGraphic(null);
            previous.tile.message.setVisible(false);
            previous.tile.indicator.setVisible(true);
            previous.tile.progress.setVisible(false);
            previous.tile.button.pseudoClassStateChanged(PREVIEWING, false);
        }
    }

    private void resizeTiles(double availableWidth, double availableHeight) {
        double contentWidth = Math.max(260, availableWidth - tiles.getPadding().getLeft()
                - tiles.getPadding().getRight());
        int columns = Math.max(1, Math.min(3, (int) ((contentWidth + tiles.getHgap()) / 280)));
        // Leave one pixel for snapped control sizes so the last card stays on the row.
        tileWidth = Math.floor((contentWidth - (columns - 1) * tiles.getHgap()) / columns) - 1;
        if (columns == 1) {
            // Keep a complete card visible in short windows, including its filename.
            double heightLimitedWidth = Math.max(260, (availableHeight - 80) * 16 / 9);
            tileWidth = Math.min(tileWidth, heightLimitedWidth);
        }
        tiles.setAlignment(columns == 1 ? Pos.TOP_CENTER : Pos.TOP_LEFT);
        videos.values().forEach(tile -> tile.resize(tileWidth));
    }

    private static void disposePending(Playback session) {
        MediaPlayer player = session.pendingPlayer.getAndSet(null);
        if (player != null) {
            player.dispose();
        }
    }

    private static void setIcon(Button button, String icon, String description) {
        button.setGraphic(FluentIcons.create(icon));
        button.setAccessibleText(description);
        button.setTooltip(new Tooltip(description));
    }

    private static ProgressIndicator loadingIndicator() {
        ProgressIndicator indicator = new ProgressIndicator();
        indicator.getStyleClass().add("media-loading");
        indicator.setMaxSize(18, 18);
        indicator.setPrefSize(18, 18);
        return indicator;
    }

    private static void roundViewport(Region viewport) {
        Rectangle clip = new Rectangle();
        clip.widthProperty().bind(viewport.widthProperty());
        clip.heightProperty().bind(viewport.heightProperty());
        clip.setArcWidth(12);
        clip.setArcHeight(12);
        viewport.setClip(clip);
    }

    private void setControlsDisabled(boolean disabled) {
        playPause.setDisable(disabled);
        mute.setDisable(disabled);
        position.setDisable(disabled);
    }

    private static boolean validDuration(Duration duration) {
        return !duration.isUnknown() && !duration.isIndefinite() && duration.greaterThan(Duration.ZERO);
    }

    private static String formatTime(Duration duration) {
        if (!validDuration(duration)) {
            return "00:00";
        }
        long seconds = (long) duration.toSeconds();
        if (seconds >= 3600) {
            return String.format(Locale.ROOT, "%d:%02d:%02d", seconds / 3600, seconds / 60 % 60, seconds % 60);
        }
        return String.format(Locale.ROOT, "%02d:%02d", seconds / 60, seconds % 60);
    }

    private final class VideoTile {
        private final Path path;
        private final Button button = new Button();
        private final MediaView view = new MediaView();
        private final Label message = new Label();
        private final Label duration = new Label();
        private final Node indicator = FluentIcons.create("play");
        private final ProgressBar progress = new ProgressBar(0);
        private final StackPane viewport;
        private final VBox content;

        private VideoTile(Path path, int number) {
            this.path = path;
            view.setPreserveRatio(true);
            view.setId("preview-" + number);
            message.getStyleClass().add("video-message");
            message.setContentDisplay(ContentDisplay.TOP);
            message.setGraphicTextGap(10);
            message.setVisible(false);
            progress.getStyleClass().add("preview-progress");
            progress.setMaxWidth(Double.MAX_VALUE);
            progress.setMouseTransparent(true);
            progress.setVisible(false);
            StackPane.setAlignment(progress, Pos.BOTTOM_CENTER);
            viewport = new StackPane(view, indicator, message, progress);
            viewport.getStyleClass().add("video-viewport");
            viewport.getStyleClass().add("tile-viewport");
            roundViewport(viewport);
            Label filename = new Label(path.getFileName().toString());
            filename.getStyleClass().add("video-filename");
            HBox.setHgrow(filename, Priority.ALWAYS);
            filename.setMaxWidth(Double.MAX_VALUE);
            filename.setTooltip(new Tooltip(path.getFileName().toString()));
            duration.getStyleClass().add("video-duration");
            duration.setMinWidth(Region.USE_PREF_SIZE);
            duration.managedProperty().bind(duration.textProperty().isNotEmpty());
            duration.visibleProperty().bind(duration.managedProperty());
            HBox details = new HBox(12, filename, duration);
            details.setAlignment(Pos.CENTER_LEFT);
            VBox caption = new VBox(details);
            caption.setPadding(new Insets(12));
            content = new VBox(viewport, caption);
            content.setPadding(new Insets(8, 8, 0, 8));
            content.setMouseTransparent(true);
            button.setGraphic(content);
            button.getStyleClass().add("video-tile");
            button.setId("video-" + number);
            button.setAccessibleText("Play " + path.getFileName());
            button.setOnAction(event -> startPlayback(this, false));
            button.setOnMouseEntered(event -> {
                if (!closed && root.getCenter() == gallery) {
                    startPlayback(this, true);
                }
            });
            button.setOnMouseExited(event -> {
                if (active != null && active.tile == this && active.preview) {
                    stopPlayback();
                }
            });
        }

        private void resize(double width) {
            double videoWidth = width - 18;
            double videoHeight = videoWidth * 9 / 16;
            button.setMinWidth(width);
            button.setPrefWidth(width);
            button.setMaxWidth(width);
            content.setPrefWidth(width - 2);
            content.setMaxWidth(width - 2);
            viewport.setMinSize(videoWidth, videoHeight);
            viewport.setPrefSize(videoWidth, videoHeight);
            viewport.setMaxSize(videoWidth, videoHeight);
            view.setFitWidth(videoWidth);
            view.setFitHeight(videoHeight);
        }
    }

    private static final class Playback {
        // The loader publishes here until the JavaFX thread claims or cancels the player.
        private final AtomicReference<MediaPlayer> pendingPlayer = new AtomicReference<>();
        private volatile boolean cancelled;
        private MediaPlayer player;
        private final VideoTile tile;
        private final MediaView view;
        private final boolean preview;
        private boolean finished;
        private boolean ready;
        private double previewLimit = Config.PREVIEW_SECONDS;

        private Playback(VideoTile tile, MediaView view, boolean preview) {
            this.tile = tile;
            this.view = view;
            this.preview = preview;
        }
    }
}
