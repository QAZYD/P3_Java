# Networked Producer–Consumer Video Upload

University Java 17 project. A **producer** uploads MP4 videos to a **consumer** over
gRPC client streaming; the consumer saves them and shows a JavaFX window where each
video previews for its first 10 seconds.

## How it works

```text
ProducerMain (p threads, one folder each: videos/p1, videos/p2, ...)
    -> one gRPC VideoUpload.Upload stream per MP4:
       filename first, then 64 KB chunks (sent only when gRPC is ready), then complete
    -> ConsumerMain: gRPC server
    -> VideoUploadService: receives the bytes into uploads-incoming/
    -> UploadQueue (leaky bucket, max length q): full -> video dropped, producer told "Dropped: queue full"
    -> c consumer worker threads: move the video into uploads/
    -> PreviewWindow.addVideo: tile appears; hover = muted 10 s preview, click = full player
```

1. `ConsumerMain` asks for **c** (consumer threads) and **q** (max queue length), starts the
   gRPC server, the c workers, and the preview window.
2. `ProducerMain` asks for **p** (producer threads) and connects; thread *i* uploads every
   `.mp4` in `videos/p<i>`.
3. Each file is its own stream. The producer waits for gRPC to be ready before each chunk
   (flow control), so memory stays flat even for multi-GB files.
4. `VideoUploadService` keeps per-stream state and writes the incoming bytes to the staging
   folder `uploads-incoming/`. Path components in the filename are stripped. A broken or
   rejected upload deletes its partial file.
5. When the whole file has arrived it is offered to the queue. **Leaky bucket:** if q videos
   are already waiting, the new one is dropped (file deleted, reply `Dropped: queue full`).
   Otherwise the reply is `Received, queued for saving: <name>`.
6. The c workers take videos in FIFO order and move them into `uploads/` (repeated names
   become `name (1).mp4`, ...), then the tile is added to the window.
7. The producer exits when its folders are done; closing the window stops the server and the
   workers. Videos still waiting in the queue at shutdown are discarded.

### Inputs (p, c, q)

| Input | Asked by | Default (`Config`) | Skip the prompt with |
|---|---|---|---|
| p, producer threads | producer | `PRODUCER_THREADS` = 3 | `--p=N` |
| c, consumer threads | consumer | `CONSUMER_THREADS` = 2 | `--c=N` |
| q, max queue length | consumer | `QUEUE_CAPACITY` = 5 | `--q=N` |

Press Enter at a prompt to keep the default.

Saving a video is just a file move, so with the defaults the queue empties almost instantly
and drops are rare. To **demonstrate drops**, set `Config.PROCESSING_DELAY_MS` (e.g. `3000`,
extra time each worker spends per video), run the consumer with small values (`c=1`, `q=1`)
and upload several videos at once.

Shared constants are in `src/main/java/upload/common/Config.java`; the wire format is in
`src/main/proto/video_upload.proto` (`UploadChunk` = filename **or** chunk, `UploadReply`).

## Requirements

- JDK 17 or newer and Maven (no wrapper). Check with `java -version` and `mvn -version`.
- Dependencies (gRPC 1.68.1, Protobuf 3.25.5, JavaFX 21.0.5) download automatically.
  JavaFX native libraries must match the OS/CPU, so build on each machine; don't copy `target/`.
- Test videos must be **H.264 video (+ AAC audio)**. JavaFX cannot play H.265/HEVC; those
  upload fine but show "Preview unavailable". See [test-videos/README.md](test-videos/README.md)
  to generate synthetic H.264 clips with FFmpeg.

## Build and run

Run everything from the project root. **In PowerShell, quote every `-D...` argument**,
otherwise PowerShell splits it at the dot.

```powershell
mvn clean compile
mvn test                      # optional unit tests
```

Put MP4s in `videos\p1`, `videos\p2`, `videos\p3`.

Terminal 1, consumer: answer the c and q prompts, then wait for `[SERVER] Ready to receive uploads`:
```powershell
mvn javafx:run
```
If the prompts don't appear (some launchers don't pass console input; the defaults are then used),
give the values directly: `mvn javafx:run "-Djavafx.args=--c=2 --q=5"`.

Terminal 2, producer: answer the p prompt:
```powershell
mvn exec:java "-Dexec.mainClass=upload.producer.ProducerMain"
```
Without the prompt: `mvn exec:java "-Dexec.mainClass=upload.producer.ProducerMain" "-Dexec.args=--p=3"`.

Results are saved in `uploads\` (git-ignored). Uploads in progress are in `uploads-incoming\`.

### Consumer options

```text
ConsumerMain [--headless] [--c=N] [--q=N] [port] [grpcThreads] [uploadDirectory]
```

- `--headless` runs the server with no window (useful on a server-only VM):
  `mvn exec:java "-Dexec.mainClass=upload.consumer.ConsumerMain" "-Dexec.args=--headless"`
- With the GUI: `mvn javafx:run "-Djavafx.args=--c=2 --q=5 60051 4 uploads"`
- `grpcThreads` (default `Config.GRPC_THREADS` = 4) are the threads that receive chunks; they
  are separate from the c workers. Port defaults come from the network settings below.
  Invalid arguments print a message and usage instead of a stack trace.

## Network settings (same machine, VM, teammate's laptop, Docker)

The producer's target address and both sides' port are looked up in this order:

1. command-line argument (producer: host as first argument; consumer: port argument)
2. environment variable `CONSUMER_HOST` / `CONSUMER_PORT`
3. `network.properties` in the project root (git-ignored, one per machine)
4. `Config` defaults: `localhost`, `50051`

Both programs print which value they used and where it came from.

To configure a machine once, copy `network.properties.example` to `network.properties`:

```properties
consumer.host=192.168.56.1
consumer.port=60051
```

| Setup | Producer's `consumer.host` |
|---|---|
| Same machine | nothing (default `localhost`) |
| VirtualBox VM, NAT, consumer on host PC | `10.0.2.2` |
| VirtualBox host-only network | the host's host-only adapter IP, e.g. `192.168.56.1` |
| Teammate's laptop / bridged VM | the consumer machine's IPv4 from `ipconfig` |
| Docker container, consumer on host | `CONSUMER_HOST=host.docker.internal` |

One-off run against another machine:
```powershell
mvn exec:java "-Dexec.mainClass=upload.producer.ProducerMain" "-Dexec.args=192.168.1.40"
```

For two machines: both must be on the same network (campus Wi-Fi may block device-to-device
traffic; a phone hotspot works), allow Java through the consumer's firewall on private networks,
use the same port on both sides, and keep both copies of the code in sync (`git pull`).
The connection is plaintext with no authentication, so use it on a private network only.

## Testing

`mvn test` runs `ConsumerMainTest` (argument parsing, concurrent requests, callback limit,
repeated shutdown), `VideoUploadServiceTest` (successful upload, repeated names,
invalid sequences, cleanup) and `UploadQueueTest` (drops when full, c workers save and
rename duplicates, shutdown, "Dropped" reply). `PreviewWindowCheck` is a manual check with a `main` method;
it needs a desktop and real clips and is not run by `mvn test`.

End-to-end checklist:

- [ ] Each file prints `Accepted: true` and appears in `uploads\` and in the window.
- [ ] The same filename from two folders is saved as `name.mp4` and `name (1).mp4`.
- [ ] Hover preview is muted and stops at 10 seconds; click opens the full player.
- [ ] A large file (e.g. 1 GB) uploads with the producer limited to `MAVEN_OPTS=-Xmx256m`.
- [ ] Closing the consumer mid-upload: producer logs `Stream failed`, no partial file is left.
- [ ] Closing the window stops the server and frees the port.
- [ ] The prompts show the entered p, c, q in the startup log (`Consumer threads (c)`, `Queue capacity (q)`).
- [ ] Leaky bucket: with `PROCESSING_DELAY_MS = 3000`, `c=1`, `q=1` and several videos, the consumer logs
      `[QUEUE] Dropped ...`, the producer shows `Dropped: queue full`, and only the kept videos reach `uploads\`.

Create a 1 GB dummy file for the memory test (zeros, so it shows "Preview unavailable"):
```powershell
$f = [IO.File]::Create("$PWD\videos\p1\big.mp4"); $f.SetLength(1GB); $f.Close()
```

Verify an upload is byte-for-byte identical:
```powershell
Get-FileHash videos\p1\clip.mp4, uploads\clip.mp4 -Algorithm SHA256
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Unknown lifecycle phase ".mainClass=..."` | Quote the `-D...` argument in PowerShell. |
| `Failed to bind to address ...:50051` | A consumer is still running (close it, or find it with `Get-NetTCPConnection -LocalPort 50051 -State Listen`), or Windows reserved the port (`netsh interface ipv4 show excludedportrange protocol=tcp`); pick another port in `network.properties`. |
| `java.lang.Error: Unresolved compilation problem` | VS Code's compiler overwrote Maven's classes in `target/`. Run `mvn clean javafx:run`. |
| `Folder does not exist: videos\p1` | Run from the project root; create `videos\p1..p3`. |
| Producer can't connect from another machine | Wrong `consumer.host`, firewall, different networks, or port mismatch. Check the address/port the producer printed. |
| "Preview unavailable" | File is not H.264/AAC (or is the dummy test file). |
| `sun.misc.Unsafe` / `restricted method` warnings | Harmless warnings from Netty and JavaFX on newer JDKs. |

## Known limits

- The queue holds *finished* uploads: a video is fully received before it can be dropped,
  so a dropped video still used network bandwidth and temporary disk space.
- No limits on concurrent streams or file size, and no authentication or encryption.

## Ownership

| Part | Owner |
|---|---|
| `VideoUploadService` (receiving, validation, replies) | Person 1 |
| `UploadQueue`, `UploadSink` (leaky-bucket queue, c workers) | Person 1 / Person 2 |
| `ConsumerMain` (server, threads, lifecycle) | Person 2 |
| `ProducerMain` (threads, streaming, flow control) | Person 3 |
| `PreviewWindow`, `FluentIcons`, CSS (UI and playback) | Person 4 |
| `Config`, `UserInput`, `video_upload.proto` | Shared: change only after team agreement |
