# Networked Producer–Consumer Video Upload

A Java producer–consumer system where a **producer** uploads MP4 videos over **gRPC** to a **consumer** service. The consumer accepts multiple uploads at the same time, saves them, and shows a **10-second preview** of each video in a JavaFX window.

## Tech stack

| Part | Used for |
|---|---|
| Java 17+ | Threads, file I/O |
| gRPC + Protobuf | Producer ↔ consumer communication (client-streaming upload) |
| JavaFX | Consumer window and video previews |
| Maven | Build, dependencies, gRPC code generation |

## Prerequisites

You need **JDK 17 or newer** and **Maven 3.6 or newer**. Check if you already have them:

```bash
java -version
mvn -version
```

If both print a version, skip to [Build](#build).

### 1. Install a JDK

Download **Eclipse Temurin 17 or 21** from https://adoptium.net and run the installer.

- **Windows:** in the installer, enable **"Set JAVA_HOME variable"** and **"Add to PATH"**.
- **macOS:** `brew install --cask temurin`
- **Linux (Debian/Ubuntu):** `sudo apt install openjdk-17-jdk`

### 2. Install Maven

**Windows**
1. Download the **Binary zip archive** from https://maven.apache.org/download.cgi
2. Extract it somewhere permanent, e.g. `C:\Program Files\Maven\apache-maven-3.x.x`
3. Open **Edit the system environment variables → Environment Variables**
4. Under *System variables*, edit **Path** and add the `bin` folder, e.g. `C:\Program Files\Maven\apache-maven-3.x.x\bin`
5. Make sure **JAVA_HOME** exists and points to your JDK folder (not its `bin`), e.g. `C:\Program Files\Eclipse Adoptium\jdk-17...`
6. Open a **new** terminal and run `mvn -version`

**macOS**
```bash
brew install maven
```

**Linux (Debian/Ubuntu)**
```bash
sudo apt install maven
```

## Build

From the project folder:

```bash
mvn compile
```

The first build downloads all dependencies and the `protoc` compiler, so it takes a few minutes. It also generates the gRPC classes (`VideoUploadGrpc`, `UploadChunk`, `UploadReply`) from `src/main/proto/video_upload.proto` into `target/generated-sources/`. **Do not edit those files**, since they are regenerated on every build.

## Run

1. Put the MP4 files to upload in the `videos/` folder.
2. Start the **consumer** first (it runs the gRPC server and opens the window):
   ```bash
   mvn javafx:run
   ```
3. In a **second terminal**, start the **producer**:
   ```bash
   mvn exec:java -Dexec.mainClass=upload.producer.ProducerMain
   ```

Received videos are saved to `uploads/`.

## Project structure

```
video-upload/
├── pom.xml
├── videos/                      # producer reads MP4s from here
├── uploads/                     # consumer saves received MP4s here
└── src/main/
    ├── proto/
    │   └── video_upload.proto   # gRPC service + message definitions
    └── java/upload/
        ├── producer/
        │   └── ProducerMain.java
        └── consumer/
            ├── ConsumerMain.java
            ├── VideoUploadService.java
            └── PreviewWindow.java
```

## Troubleshooting

- **`mvn` is not recognized (Windows):** Maven's `bin` folder isn't on `Path`, or the terminal was opened before you changed it. Fix the variable and open a new terminal.
- **`JAVA_HOME is not defined correctly`:** `JAVA_HOME` must point to the JDK folder itself, not to its `bin` subfolder.
- **Video preview fails with a `MediaException`:** JavaFX only plays MP4s encoded as **H.264 video + AAC audio**. Some phone videos are H.265/HEVC and won't play; re-export them as H.264.
- **Warnings in `target/generated-sources` in VS Code:** these come from the generated gRPC/Protobuf code and are harmless. Hide them by typing `!**/generated-sources/**` into the Problems panel filter.
- **Producer can't connect:** make sure the consumer is already running before you start the producer.
