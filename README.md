# Networked Producer–Consumer Video Upload

University Java 17 project using gRPC client streaming and Protocol Buffers.
Person 2 implements consumer server configuration, concurrency and lifecycle.
The upload service, producer and JavaFX window currently contain teammate TODOs;
this is a working server foundation, not yet a working media upload application.

## Current architecture and ownership

```text
ProducerMain (Person 3, TODO)
    -> gRPC VideoUpload.Upload client stream
    -> fixed gRPC callback executor (Person 2, implemented)
    -> VideoUploadService (Person 1, TODO)
    -> completion callback -> PreviewWindow.addVideo (Person 4, TODO)
```

`src/main/proto/video_upload.proto` generates `upload.grpc.VideoUploadGrpc`,
`UploadChunk` and `UploadReply`. The contract remains unchanged: filename first,
then 64 KB chunks, then stream completion and one reply.

The assignment's bounded FIFO queue and `c` consumer workers are **not implemented**.
The gRPC executor is separate from that future queue and does not cap open streams.

## Requirements and setup

Use JDK 17 and Maven (no Maven Wrapper is present). The POM compiles with
`--release 17`; dependencies include gRPC 1.68.1, Protobuf 3.25.5 and JavaFX 21.0.5.
Set `JAVA_HOME` to your JDK directory and add Maven's `bin` directory to `PATH`.
On Windows this can be done in Environment Variables; reopen your terminal.
On macOS, an installed JDK 17 can be selected with:

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Check the JVM Maven actually uses:

```text
java -version
mvn -version
```

Maven downloads dependencies and generates protocol classes. JavaFX native
libraries must match the OS and the **JVM** CPU architecture. Resolve dependencies
on each target machine; do not copy macOS native binaries onto Windows/Linux.
GUI operation additionally requires a graphical desktop and supported media codecs.
No Homebrew packages or unrelated system settings need to change for this work.

## Build and run

The following commands work on macOS and Windows CMD/PowerShell:

```text
mvn clean compile
mvn test
mvn exec:java "-Dexec.mainClass=upload.consumer.ConsumerMain" "-Dexec.args=--headless 50051 4 uploads"
```

The consumer starts JavaFX by default. With `--headless`, defaults are port
`50051`, four gRPC callback threads, and `uploads` under the working directory.

```text
ConsumerMain [--headless] [port] [grpcThreads] [uploadDirectory]
```

Trailing positional arguments may be omitted. Ports must be 1–65535 and thread
count must be positive. Malformed, extra and blank directory arguments are rejected
with a message and usage. Paths are normalized to absolute paths; missing directories
are created. A temporary create/delete probe verifies write access. Completed videos
are never deleted by server shutdown. Ordinary configuration errors return without
an uncaught stack trace (currently main returns normally, not a nonzero exit code).
For paths containing spaces, quote the directory inside Maven's argument string.

Stop with Ctrl+C. For a separate Java process, useful for testing JVM signal handling:

```text
mvn dependency:build-classpath "-Dmdep.outputFile=target/runtime-classpath.txt" "-DincludeScope=runtime"
```

macOS/Linux:
```sh
java -cp "target/classes:$(cat target/runtime-classpath.txt)" upload.consumer.ConsumerMain --headless 50051 4 uploads
```

Windows PowerShell:
```powershell
$consumerClasspath = "target/classes;" + (Get-Content target/runtime-classpath.txt -Raw).Trim()
java -cp $consumerClasspath upload.consumer.ConsumerMain --headless 50051 4 uploads
```

GUI lifecycle wiring is available via:

```text
mvn javafx:run "-Djavafx.args=50051 4 uploads"
```

**PreviewWindow is still a placeholder and does not show a window.** Use headless
mode until Person 4 completes it. GUI playback and window-close behavior have not
been visually tested. `mvn javafx:run` starts JavaFX. For server-only testing, use the `--headless` command above.

## Lifecycle and concurrency

`ConsumerMain` follows the team's original JavaFX skeleton: it extends `Application`
and overrides `start(Stage)` and `stop()`. Its fields hold the port, gRPC thread
count, upload directory, server and executor. `start(Stage)` creates the window on
the JavaFX thread and starts gRPC; `stop()` starts background cleanup. `main()`
launches JavaFX by default and accepts `--headless` for server-only testing while
the GUI is unfinished. A small `startServer()` helper accepts a test service, so the
server can be tested independently of Person 1's unfinished upload service.

The fixed executor limits executing gRPC callbacks, not upload streams or pending
videos. gRPC can interleave independent streams; Person 1 must retain state per stream.
There are no artificial production delays. The server stays alive in headless mode
via `awaitTermination()`.

Shutdown is synchronized and idempotent. A JVM hook or GUI close requests server
shutdown, allows five seconds for requests, then forces cancellation and waits up to
five more seconds. The executor gets the same graceful/forced sequence. Interrupted
shutdown restores the caller's interrupt flag. Java cannot forcibly terminate arbitrary
code that ignores interrupts; timeout warnings identify that condition. Startup failure
releases the server/executor. GUI shutdown waits run on a separate cleanup thread,
not the JavaFX Application Thread.

JavaFX constructs `PreviewWindow(Stage)` within `Application.start`. Upload completion
calls `window.addVideo(Path)` from a gRPC worker. Person 4's documented contract requires
`Platform.runLater()` inside that method; this placeholder does not yet update a UI.
The close handler exits JavaFX, and `Application.stop()` initiates the same server cleanup.

## Integration contracts

- **Person 1:** preserve `VideoUploadService(Path, Consumer<Path>)`. Implement stream
  validation, sanitized filenames, collision handling, writing/closing files, incomplete
  file cleanup and final replies. Invoke the callback only for successfully completed,
  published videos. Current placeholder streams never send replies; clients need deadlines.
- **Person 3:** implement streaming to a configurable host/port, `p` workers with distinct
  folders, response waiting, deadlines and network-error handling. `Config.HOST` is currently
  localhost; VM selection needs producer configuration rather than source changes.
- **Person 4:** preserve `PreviewWindow(Stage)` and thread-safe `addVideo(Path)`. Construct
  and show the stage in the constructor, schedule UI updates with `Platform.runLater`, and
  implement muted 10-second hover/full-click playback and media-error handling. Coordinate
  any replacement close handler with ConsumerMain's JavaFX exit lifecycle.

### Queue agreement needed

Agree with Person 1 on a `VideoJob` holding a closed staged path and response/completion
metadata, plus an admission method returning accepted/full/stopping. Submit after a stream
has staged successfully but **before** sending an accepted reply. A `Consumer<Path>` alone
cannot return queue rejection; add an agreed admission interface rather than silently
changing this constructor or the protocol.

Use a separate `ArrayBlockingQueue<VideoJob>(q)` and `c` worker threads. Admission uses
`offer`, never `put`: full means reject the incoming job, retain older queued jobs, clean
its staging file and reply `accepted=false, message="QUEUE_FULL"` using the existing
reply fields after team agreement. Queue order is FIFO; parallel workers may finish out
of order. Jobs taken by workers no longer count toward `q`. Workers publish into shared
storage and notify the GUI. Agree whether an accepted reply means queued or fully processed,
and how processing errors and shutdown draining/cancellation are reported.

Staging before admission does not bound concurrent uploads or temporary disk usage.
Coordinate separate limits for active streams, per-file bytes and total staging storage,
with rejection/cleanup and deadlines. Keep these limits distinct from `q`. Add separate
CLI parameters for `c` and `q` only when the team implements this boundary.

## Tests and media fixtures

`mvn test` runs `ConsumerMainTest` using a **test-only** gRPC service, real local TCP,
and the unchanged generated upload messages. It checks invalid arguments, six
successful concurrent requests, a measured peak of four callbacks, and repeated
shutdown. It does not test actual uploads, queue capacity, or GUI rendering.

Check listening sockets while the consumer runs:

macOS:
```sh
lsof -nP -iTCP:50051 -sTCP:LISTEN
```

Windows CMD:
```cmd
netstat -ano | findstr :50051
```

See [test-videos/README.md](test-videos/README.md) for reproducible synthetic H.264/yuv420p
3-, 10- and 25-second fixture commands and SHA-256 comparisons. FFmpeg was unavailable;
fixtures have not been generated. MP4s are already ignored by Git.

## Two-VM deployment

Run the consumer on a reachable VM and have the producer connect to that VM's IP or DNS
name, not localhost. The server binds the configured port on available interfaces. Configure
VM bridged/host-only networking or NAT forwarding as appropriate and permit inbound TCP
50051 (or the chosen port) from the producer in the consumer firewall. Check connectivity
before uploading. Use a writable storage path and enough disk space. This development
server is plaintext without authentication; keep deployment on the intended private lab
network. GUI mode needs a desktop; headless mode supports a server-only VM.

Windows, JavaFX media playback, real uploads and two-VM operation remain unverified.

## Verification record

Run `mvn clean test` on Java 17. The current skeleton-shaped implementation was
compiled and tested locally; see the latest test output for the actual result.
Real uploads, GUI playback, Windows and two-VM operation remain unverified.
