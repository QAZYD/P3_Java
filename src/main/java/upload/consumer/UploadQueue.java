package upload.consumer;

import upload.common.Config;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Leaky-bucket queue between the upload streams and the c consumer workers.
 *
 *   finished upload (staged file) --offer--> [ queue, capacity q ] --take--> c worker threads
 *                                    |                                          |
 *                          full: drop the video                  save into the uploads folder,
 *                                                                then show it in the window
 *
 * offer() never waits: if all q slots are taken the new video is dropped (leaky bucket).
 * Videos already in the queue are kept and saved in FIFO order (c workers may finish out of order).
 */
public final class UploadQueue implements UploadSink {

    private record Job(Path stagedFile, String filename) {}

    private final int capacity;
    private final int workerCount;
    private final Path uploadDir;
    private final Consumer<Path> onSaved;
    private final BlockingQueue<Job> queue;
    private final List<Thread> workers = new ArrayList<>();
    private volatile boolean stopped;

    /**
     * @param capacity    q, max number of videos waiting
     * @param workerCount c, number of consumer worker threads
     * @param uploadDir   the single folder finished videos are saved to
     * @param onSaved     called with the saved path (ConsumerMain forwards it to the window)
     */
    public UploadQueue(int capacity, int workerCount, Path uploadDir, Consumer<Path> onSaved) {
        if (capacity < 1 || workerCount < 1) {
            throw new IllegalArgumentException("Queue capacity and worker count must be at least 1.");
        }
        this.capacity = capacity;
        this.workerCount = workerCount;
        this.uploadDir = uploadDir;
        this.onSaved = onSaved;
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /** Starts the c worker threads. */
    public synchronized void start() {
        if (!workers.isEmpty() || stopped) {
            throw new IllegalStateException("Queue already started or stopped.");
        }
        for (int i = 1; i <= workerCount; i++) {
            int id = i;
            Thread worker = new Thread(() -> workerLoop(id), "consumer-worker-" + id);
            worker.setDaemon(true); // never keeps the JVM alive on its own
            workers.add(worker);
            worker.start();
        }
    }

    /** Called by VideoUploadService when an upload has fully arrived. Never blocks. */
    @Override
    public Result submit(Path stagedFile, String filename) {
        if (stopped) {
            return Result.STOPPED;
        }
        if (queue.offer(new Job(stagedFile, filename))) {
            System.out.printf("[QUEUE] Queued %s (%d/%d)%n", filename, queue.size(), capacity);
            return Result.ACCEPTED;
        }
        System.out.printf("[QUEUE] Dropped %s: queue full (%d/%d)%n", filename, capacity, capacity);
        return Result.QUEUE_FULL;
    }

    public int capacity() {
        return capacity;
    }

    /** Number of videos currently waiting (not counting ones a worker is saving). */
    public int size() {
        return queue.size();
    }

    private void workerLoop(int id) {
        while (true) {
            Job job;
            try {
                job = queue.take();
            } catch (InterruptedException e) {
                return; // shutdown
            }
            save(id, job);
        }
    }

    private void save(int id, Job job) {
        if (Config.PROCESSING_DELAY_MS > 0) {
            try {
                Thread.sleep(Config.PROCESSING_DELAY_MS);
            } catch (InterruptedException e) {
                // Shutting down: finish saving this video, then the loop exits on the next take().
                Thread.currentThread().interrupt();
            }
        }
        Path target;
        try {
            target = moveToFreeName(job.stagedFile(), job.filename());
        } catch (IOException e) {
            System.err.printf("[WORKER %d] Could not save %s: %s%n", id, job.filename(), e.getMessage());
            deleteQuietly(job.stagedFile());
            return;
        }
        System.out.printf("[WORKER %d] Saved %s (queue %d/%d)%n", id, target.getFileName(), queue.size(), capacity);
        try {
            onSaved.accept(target);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    /** Held while a worker picks a free name and moves its file there, so two workers never pick the same name. */
    private final Object nameLock = new Object();

    /**
     * Moves the staged file to "name.mp4", or "name (1).mp4", "name (2).mp4", ... if taken.
     * Choosing the name and moving happen under one lock, and the move never replaces an
     * existing file. (Reserving a name with an empty placeholder and then moving over it with
     * REPLACE_EXISTING is not safe on Windows: Java deletes the target first, which briefly
     * frees the name for another worker.) Same-drive moves are renames, so the lock is brief.
     */
    private Path moveToFreeName(Path staged, String filename) throws IOException {
        int dot = filename.lastIndexOf('.');
        String base = dot > 0 ? filename.substring(0, dot) : filename;
        String extension = dot > 0 ? filename.substring(dot) : "";
        synchronized (nameLock) {
            Files.createDirectories(uploadDir);
            for (int n = 0; ; n++) {
                Path candidate = uploadDir.resolve(n == 0 ? filename : base + " (" + n + ")" + extension);
                if (Files.exists(candidate)) {
                    continue;
                }
                try {
                    return Files.move(staged, candidate);
                } catch (FileAlreadyExistsException taken) {
                    // Created by something outside this queue since the check; try the next number.
                }
            }
        }
    }

    /**
     * Stops accepting videos, lets workers finish the video they are saving (up to the timeout),
     * and deletes the staged files of videos still waiting in the queue.
     */
    public void shutdown(long timeout, TimeUnit unit) {
        List<Thread> toJoin;
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            toJoin = new ArrayList<>(workers);
        }
        toJoin.forEach(Thread::interrupt);
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        boolean interrupted = false;
        for (Thread worker : toJoin) {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMs <= 0) {
                break;
            }
            try {
                worker.join(remainingMs);
            } catch (InterruptedException e) {
                interrupted = true;
                break;
            }
        }
        List<Job> leftover = new ArrayList<>();
        queue.drainTo(leftover);
        for (Job job : leftover) {
            deleteQuietly(job.stagedFile());
        }
        if (!leftover.isEmpty()) {
            System.out.printf("[QUEUE] Discarded %d unsaved video(s) at shutdown%n", leftover.size());
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteQuietly(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            System.err.println("[QUEUE] Could not delete " + file + ": " + e.getMessage());
        }
    }
}
