package com.example.gsb.sc;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handle for one submitted subtask. Tracks execution, receives cancel signals
 * and exposes the terminal {@link TaskResult}.
 */
public final class Subtask<T> {

    private final int index;
    private final String name;
    private final Callable<T> body;
    final Duration timeout;
    final long startNanos = System.nanoTime();

    private final CountDownLatch terminated = new CountDownLatch(1);
    private volatile boolean started;
    private volatile T value;
    private volatile Throwable error;
    private volatile boolean cancelRequested;
    private volatile boolean timedOut;
    volatile Future<?> future;
    volatile ScheduledFuture<?> timeoutFuture;

    Subtask(int index, String name, Callable<T> body, Duration timeout) {
        this.index = index;
        this.name = name;
        this.body = body;
        this.timeout = timeout;
    }

    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    void run() {
        started = true;
        try {
            value = body.call();
        } catch (Throwable t) {
            error = t;
        } finally {
            terminated.countDown();
        }
    }

    /** Sends the cancel signal (interrupt). Idempotent; ignored if already terminated. */
    void requestCancel(boolean dueToTimeout) {
        if (isTerminated()) {
            return;
        }
        if (dueToTimeout) {
            timedOut = true;
        } else {
            cancelRequested = true;
        }
        Future<?> f = future;
        if (f != null && f.cancel(true) && !started) {
            // Cancelled before it ever ran: the runner will never count the latch down.
            terminated.countDown();
        }
    }

    boolean awaitTermination(long nanos) throws InterruptedException {
        return terminated.await(nanos, TimeUnit.NANOSECONDS);
    }

    boolean isTerminated() {
        return terminated.getCount() == 0;
    }

    TaskResult<T> toResult() {
        if (!isTerminated()) {
            String reason = timedOut
                    ? "did not stop after its own timeout"
                    : "did not respond to the cancellation interrupt";
            return TaskResult.failure(index, name, TaskState.UNRESPONSIVE,
                    new UnresponsiveTaskException("subtask '" + name + "' " + reason));
        }
        if (timedOut) {
            return TaskResult.failure(index, name, TaskState.TIMEOUT,
                    new TaskTimeoutException("subtask '" + name + "' exceeded its timeout of " + timeout));
        }
        if (error != null) {
            if (cancelRequested && isInterruptLike(error)) {
                return TaskResult.failure(index, name, TaskState.CANCELLED,
                        new CancellationException("subtask '" + name + "' was cancelled"));
            }
            return TaskResult.failure(index, name, TaskState.FAILED, error);
        }
        if (cancelRequested) {
            return TaskResult.failure(index, name, TaskState.CANCELLED,
                    new CancellationException("subtask '" + name + "' was cancelled"));
        }
        return TaskResult.success(index, name, value);
    }

    private static boolean isInterruptLike(Throwable t) {
        return t instanceof InterruptedException || t instanceof CancellationException;
    }
}
