package upload.consumer;

import javafx.geometry.Pos;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.layout.StackPane;
import javafx.scene.shape.FillRule;
import javafx.scene.shape.SVGPath;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Loads bundled Microsoft Fluent UI System Icons as small JavaFX vector nodes. */
final class FluentIcons {
    private static final double ICON_SIZE = 20;
    private static final double SOURCE_SIZE = 24;
    private static final Map<String, String> FILES = Map.of(
            "back", "ic_fluent_arrow_left_24_regular.svg",
            "play", "ic_fluent_play_24_filled.svg",
            "pause", "ic_fluent_pause_24_filled.svg",
            "speaker", "ic_fluent_speaker_2_24_filled.svg",
            "muted", "ic_fluent_speaker_mute_24_filled.svg",
            "maximize", "ic_fluent_full_screen_maximize_24_regular.svg",
            "minimize", "ic_fluent_full_screen_minimize_24_regular.svg",
            "unavailable", "ic_fluent_video_off_24_regular.svg");
    private static final Map<String, List<PathData>> PATH_CACHE = new ConcurrentHashMap<>();

    private FluentIcons() { }

    static Node create(String name) {
        String file = FILES.get(Objects.requireNonNull(name, "name"));
        if (file == null) {
            throw new IllegalArgumentException("Unknown Fluent icon: " + name);
        }

        Group geometry = new Group();
        for (PathData data : PATH_CACHE.computeIfAbsent(file, FluentIcons::loadPathData)) {
            SVGPath path = new SVGPath();
            path.setContent(data.content());
            path.setFillRule(data.fillRule());
            path.getStyleClass().add("fluent-icon");
            geometry.getChildren().add(path);
        }
        geometry.setScaleX(ICON_SIZE / SOURCE_SIZE);
        geometry.setScaleY(ICON_SIZE / SOURCE_SIZE);

        StackPane icon = new StackPane(geometry);
        icon.setAlignment(Pos.CENTER);
        icon.setMinSize(ICON_SIZE, ICON_SIZE);
        icon.setPrefSize(ICON_SIZE, ICON_SIZE);
        icon.setMaxSize(ICON_SIZE, ICON_SIZE);
        icon.setMouseTransparent(true);
        return icon;
    }

    private static List<PathData> loadPathData(String file) {
        String resource = "/upload/consumer/icons/" + file;
        try (InputStream input = FluentIcons.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("Missing bundled icon: " + resource);
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

            Document document = factory.newDocumentBuilder().parse(input);
            NodeList paths = document.getElementsByTagNameNS("*", "path");
            java.util.ArrayList<PathData> result = new java.util.ArrayList<>(paths.getLength());
            for (int i = 0; i < paths.getLength(); i++) {
                Element element = (Element) paths.item(i);
                String content = element.getAttribute("d");
                if (content.isBlank()) {
                    continue;
                }
                FillRule rule = "evenodd".equalsIgnoreCase(element.getAttribute("fill-rule"))
                        ? FillRule.EVEN_ODD : FillRule.NON_ZERO;
                result.add(new PathData(content, rule));
            }
            if (result.isEmpty()) {
                throw new IllegalStateException("Bundled icon has no SVG paths: " + resource);
            }
            return List.copyOf(result);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to load bundled Fluent icon: " + resource, exception);
        }
    }

    private record PathData(String content, FillRule fillRule) { }
}
