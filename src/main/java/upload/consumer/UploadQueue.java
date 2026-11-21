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


public final class UploadQueue implements UploadSink {

    private record Job(Path stagedFile, String filename) {}

    private final int capacity;
    private final int workerCount;
    private final Path uploadDir;
    private final Consumer<Path> onSaved;
    private final BlockingQueue<Job> queue;
    private final List<Thread> workers = new ArrayList<>();
    private volatile boolean stopped;

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

    public synchronized void start() {
        if (!workers.isEmpty() || stopped) {
            throw new IllegalStateException("Queue already started or stopped.");
        }
        for (int i = 1; i <= workerCount; i++) {
            int id = i;
            Thread worker = new Thread(() -> workerLoop(id), "consumer-worker-" + id);
            worker.setDaemon(true); 
            workers.add(worker);
            worker.start();
        }
    }

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

    private final Object nameLock = new Object();

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
                }
            }
        }
    }

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
