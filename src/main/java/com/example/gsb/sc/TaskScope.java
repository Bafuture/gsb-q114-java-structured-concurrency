package com.example.gsb.sc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A structured-concurrency scope: subtasks forked inside the scope run concurrently,
 * {@link #join()} waits for all of them, and closing the scope guarantees that no
 * subtask is still being waited on (cancellation is propagated first).
 *
 * <pre>{@code
 * try (TaskScope scope = TaskScope.open(ScopeConfig.builder().scopeTimeout(Duration.ofSeconds(2)).build())) {
 *     SubtaskHandle<Foo> a = scope.fork("a", serviceA::call);
 *     SubtaskHandle<Bar> b = scope.fork("b", serviceB::call);
 *     List<Object> results = scope.join(); // in submission order
 * }
 * }</pre>
 *
 * <p>All worker and scheduler threads are daemons. A subtask that ignores
 * interruption is marked {@link SubtaskState#UNRESPONSIVE} after the configured
 * grace period and the scope stops blocking on it, so {@code join()} can never
 * hang forever.
 */
public final class TaskScope implements AutoCloseable {

    enum CancelReason { NONE, EXPLICIT, FAILURE, TIMEOUT }

    private final ScopeConfig config;
    private final ScheduledExecutorService scheduler;
    private final List<Subtask<?>> subtasks = new ArrayList<>();
    private final AtomicInteger sequencer = new AtomicInteger();

    private volatile CancelReason cancelReason = CancelReason.NONE;
    private boolean joining;
    private boolean joined;
    private boolean closed;

    private TaskScope(ScopeConfig config) {
        this.config = config;
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "taskscope-scheduler");
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
        if (config.scopeTimeout() != null) {
            scheduler.schedule(this::onScopeTimeout, config.scopeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** Opens a scope with default configuration (no timeouts, wait for all). */
    public static TaskScope open() {
        return open(ScopeConfig.defaults());
    }

    /** Opens a scope with the given configuration. */
    public static TaskScope open(ScopeConfig config) {
        return new TaskScope(config);
    }

    /** Forks a subtask with an auto-generated name and the configured default timeout. */
    public <T> SubtaskHandle<T> fork(Callable<T> task) {
        return fork("subtask-" + sequencer.get(), task, null);
    }

    /** Forks a named subtask with the configured default timeout. */
    public <T> SubtaskHandle<T> fork(String name, Callable<T> task) {
        return fork(name, task, null);
    }

    /**
     * Forks a named subtask with its own timeout. {@code null} falls back to
     * {@link ScopeConfig#subtaskTimeout()}. The subtask timeout and the scope timeout
     * are independent; whichever fires first wins.
     */
    public synchronized <T> SubtaskHandle<T> fork(String name, Callable<T> task, Duration timeout) {
        if (closed) {
            throw new IllegalStateException("cannot fork into a closed scope");
        }
        if (joining || joined) {
            throw new IllegalStateException("cannot fork after join() has started");
        }
        Subtask<T> subtask = new Subtask<>(sequencer.getAndIncrement(), name, task, scheduler, config, this);
        subtasks.add(subtask);
        subtask.start();
        Duration effectiveTimeout = timeout != null ? timeout : config.subtaskTimeout();
        if (effectiveTimeout != null) {
            scheduler.schedule(subtask::onSubtaskTimeout, effectiveTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        if (cancelReason != CancelReason.NONE) {
            subtask.requestCancel();
        }
        return subtask;
    }

    /** Cancels the scope: every still-running subtask is interrupted immediately. */
    public void cancel() {
        initiateCancel(CancelReason.EXPLICIT);
    }

    /**
     * Waits for every subtask to end (success, failure, cancellation or timeout).
     *
     * @return results in submission order
     * @throws ScopeTimeoutException if the scope-level timeout fired
     * @throws ScopeFailedException  if any subtask failed, timed out, was cancelled or stayed unresponsive
     */
    public List<Object> join() {
        List<Subtask<?>> snapshot;
        synchronized (this) {
            if (joined) {
                throw new IllegalStateException("join() may only be called once");
            }
            joining = true;
            snapshot = new ArrayList<>(subtasks);
        }
        for (Subtask<?> subtask : snapshot) {
            subtask.awaitTerminal();
        }
        synchronized (this) {
            joining = false;
            joined = true;
        }
        scheduler.shutdownNow();

        List<SubtaskFailure> failures = new ArrayList<>();
        for (Subtask<?> subtask : snapshot) {
            SubtaskFailure failure = subtask.toFailure(cancelReason);
            if (failure != null) {
                failures.add(failure);
            }
        }
        if (!failures.isEmpty()) {
            if (cancelReason == CancelReason.TIMEOUT) {
                throw new ScopeTimeoutException(failures);
            }
            throw new ScopeFailedException(failures);
        }
        List<Object> results = new ArrayList<>(snapshot.size());
        for (Subtask<?> subtask : snapshot) {
            results.add(subtask.valueIfSuccess());
        }
        return results;
    }

    /**
     * Closes the scope. If {@link #join()} was never called, the scope is cancelled
     * and every subtask is awaited (unresponsive ones abandoned after the grace
     * period). Scheduler threads are shut down; worker threads are daemons.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (!joined) {
            if (cancelReason == CancelReason.NONE) {
                initiateCancel(CancelReason.EXPLICIT);
            }
            try {
                join();
            } catch (ScopeFailedException ignored) {
                // close() is a best-effort cleanup boundary; failures must not escape it
            }
        }
        scheduler.shutdownNow();
    }

    private void onScopeTimeout() {
        initiateCancel(CancelReason.TIMEOUT);
    }

    void onSubtaskBusinessFailure() {
        if (config.shutdownPolicy() == ShutdownPolicy.SHUTDOWN_ON_FAILURE) {
            initiateCancel(CancelReason.FAILURE);
        }
    }

    private void initiateCancel(CancelReason reason) {
        List<Subtask<?>> toCancel;
        synchronized (this) {
            if (cancelReason == CancelReason.NONE) {
                cancelReason = reason;
            }
            toCancel = new ArrayList<>(subtasks);
        }
        for (Subtask<?> subtask : toCancel) {
            subtask.requestCancel();
        }
    }
}
