package com.example.gsb.sc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A structured-concurrency scope: subtasks are {@link #submit(String, Callable) submitted}
 * into the scope, {@link #join()} waits for all of them and {@link #close()} guarantees
 * that no subtask is still being waited on when the scope exits.
 *
 * <p>Semantics:
 * <ul>
 *   <li>Fail-fast: the first business failure or per-task timeout cancels all
 *       remaining subtasks (cancel signal = thread interrupt).</li>
 *   <li>Scope timeout ({@link Builder#scopeTimeout}) and per-task timeouts
 *       ({@link #submit(String, Duration, Callable)}) are independent; whichever
 *       fires first wins.</li>
 *   <li>A subtask that ignores interrupts is waited for only up to the
 *       {@link Builder#abandonGrace abandon grace}, then reported as
 *       {@link TaskState#UNRESPONSIVE} and abandoned (its daemon thread is left
 *       to die on its own; the scope never blocks on it).</li>
 *   <li>{@link #join()} returns results in submission order, or throws
 *       {@link AggregateTaskException} aggregating every failure.</li>
 * </ul>
 *
 * <p>Typical usage:
 * <pre>{@code
 * try (TaskScope scope = TaskScope.builder()
 *         .scopeTimeout(Duration.ofSeconds(2))
 *         .taskTimeout(Duration.ofMillis(500))
 *         .build()) {
 *     scope.submit("user", () -> loadUser(id));
 *     scope.submit("orders", () -> loadOrders(id));
 *     ScopeResult result = scope.join();
 *     return result.values();
 * }
 * }</pre>
 */
public final class TaskScope implements AutoCloseable {

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final Duration scopeTimeout;
    private final Duration defaultTaskTimeout;
    private final Duration abandonGrace;

    private final List<Subtask<?>> subtasks = new ArrayList<>();
    private volatile boolean cancelRequested;
    private boolean joined;
    private boolean closed;

    private TaskScope(Builder builder) {
        this.scopeTimeout = builder.scopeTimeout;
        this.defaultTaskTimeout = builder.taskTimeout;
        this.abandonGrace = builder.abandonGrace;
        this.executor = Executors.newCachedThreadPool(newDaemonFactory("task-scope-worker"));
        this.scheduler = Executors.newSingleThreadScheduledExecutor(newDaemonFactory("task-scope-timer"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TaskScope create() {
        return builder().build();
    }

    /** Submits a subtask using the scope's default per-task timeout (if any). */
    public <T> Subtask<T> submit(String name, Callable<T> body) {
        return submit(name, defaultTaskTimeout, body);
    }

    /**
     * Submits a subtask with an explicit per-task timeout
     * ({@code null} means no per-task timeout).
     */
    public synchronized <T> Subtask<T> submit(String name, Duration taskTimeout, Callable<T> body) {
        Objects.requireNonNull(body, "body");
        if (closed || joined) {
            throw new IllegalStateException("scope is already joined/closed, cannot submit '" + name + "'");
        }
        Subtask<T> subtask = new Subtask<>(subtasks.size(), name, body, taskTimeout);
        subtasks.add(subtask);
        subtask.future = executor.submit(subtask::run);
        if (taskTimeout != null) {
            subtask.timeoutFuture = scheduler.schedule(
                    () -> subtask.requestCancel(true), taskTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        if (cancelRequested) {
            subtask.requestCancel(false);
        }
        return subtask;
    }

    /**
     * Waits until every subtask has terminated (success, failure, cancellation,
     * or abandoned as unresponsive). Any failure triggers cancellation of all
     * remaining subtasks (fail-fast).
     *
     * @return ordered results when every subtask succeeded
     * @throws AggregateTaskException if at least one subtask did not succeed;
     *         the exception aggregates all failures and the full ordered report
     * @throws InterruptedException if the joining thread is interrupted;
     *         all subtasks are cancelled before it is rethrown
     */
    public ScopeResult join() throws InterruptedException {
        final List<Subtask<?>> tasks;
        synchronized (this) {
            if (joined) {
                throw new IllegalStateException("join() already called");
            }
            joined = true;
            tasks = new ArrayList<>(subtasks);
        }

        final long graceNanos = abandonGrace.toNanos();
        final long scopeDeadline = scopeTimeout == null
                ? Long.MAX_VALUE
                : System.nanoTime() + scopeTimeout.toNanos();
        boolean cancelFired = cancelRequested;
        long cancelFiredAt = cancelFired ? System.nanoTime() : 0;

        try {
            for (Subtask<?> subtask : tasks) {
                long deadline = scopeDeadline;
                if (subtask.timeout != null) {
                    deadline = Math.min(deadline,
                            subtask.startNanos + subtask.timeout.toNanos() + graceNanos);
                }
                if (cancelFired) {
                    deadline = Math.min(deadline, cancelFiredAt + graceNanos);
                }
                long remaining = deadline - System.nanoTime();
                boolean terminated = remaining > 0 && subtask.awaitTermination(remaining);
                if (!terminated) {
                    boolean scopeExpired = System.nanoTime() >= scopeDeadline;
                    if (!cancelFired) {
                        cancelFired = true;
                        cancelFiredAt = System.nanoTime();
                        cancelAll();
                    }
                    if (scopeExpired) {
                        // Scope deadline reached: cancel everyone, then give this
                        // subtask one last abandon grace before giving up on it.
                        subtask.awaitTermination(graceNanos);
                    }
                    // Otherwise the subtask blew its own timeout + grace: it stays
                    // unterminated and will be reported UNRESPONSIVE.
                } else if (!cancelFired && isFailure(subtask)) {
                    // Fail-fast: a business failure or per-task timeout cancels the rest.
                    cancelFired = true;
                    cancelFiredAt = System.nanoTime();
                    cancelAll();
                }
            }
        } catch (InterruptedException interrupted) {
            cancelAll();
            long deadline = System.nanoTime() + graceNanos;
            for (Subtask<?> subtask : tasks) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    subtask.awaitTermination(remaining);
                } catch (InterruptedException ignored) {
                    // Keep cancelling/waiting best-effort; restore the flag below.
                }
            }
            Thread.currentThread().interrupt();
            throw interrupted;
        } finally {
            for (Subtask<?> subtask : tasks) {
                ScheduledFuture<?> timeoutFuture = subtask.timeoutFuture;
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }
            }
        }

        List<TaskResult<?>> results = new ArrayList<>(tasks.size());
        for (Subtask<?> subtask : tasks) {
            results.add(subtask.toResult());
        }
        ScopeResult result = new ScopeResult(results);
        if (!result.isSuccess()) {
            throw new AggregateTaskException(result);
        }
        return result;
    }

    private static boolean isFailure(Subtask<?> subtask) {
        TaskState state = subtask.toResult().state();
        return state == TaskState.FAILED || state == TaskState.TIMEOUT;
    }

    /** Cancels all unfinished subtasks; they will observe an interrupt. */
    public void cancel() {
        cancelRequested = true;
        cancelAll();
    }

    private void cancelAll() {
        List<Subtask<?>> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(subtasks);
        }
        for (Subtask<?> subtask : snapshot) {
            subtask.requestCancel(false);
        }
    }

    /**
     * Ensures every subtask has terminated (joining if necessary) and releases
     * the scope's threads. Never blocks indefinitely: unresponsive subtasks are
     * abandoned after the abandon grace.
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
            cancel();
            try {
                join();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (AggregateTaskException ignored) {
                // Cancellation/failure report is discarded on close().
            }
        }
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    private static ThreadFactory newDaemonFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /** Scope configuration. All timeouts are optional; {@code null} disables them. */
    public static final class Builder {

        private Duration scopeTimeout;
        private Duration taskTimeout;
        private Duration abandonGrace = Duration.ofSeconds(1);

        private Builder() {
        }

        /** Overall deadline for {@link #join()}; on expiry all subtasks are cancelled. */
        public Builder scopeTimeout(Duration scopeTimeout) {
            this.scopeTimeout = scopeTimeout;
            return this;
        }

        /** Default per-task timeout applied by {@link #submit(String, Callable)}. */
        public Builder taskTimeout(Duration taskTimeout) {
            this.taskTimeout = taskTimeout;
            return this;
        }

        /**
         * How long to wait for a subtask to react to the cancel signal before
         * abandoning it and reporting {@link TaskState#UNRESPONSIVE}.
         */
        public Builder abandonGrace(Duration abandonGrace) {
            this.abandonGrace = Objects.requireNonNull(abandonGrace, "abandonGrace");
            return this;
        }

        public TaskScope build() {
            return new TaskScope(this);
        }
    }
}
