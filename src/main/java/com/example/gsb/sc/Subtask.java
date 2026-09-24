package com.example.gsb.sc;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * One forked subtask: owns its worker thread, its state machine and the logic for
 * timeout / cancellation / unresponsiveness handling.
 */
final class Subtask<T> implements SubtaskHandle<T> {

    private final int index;
    private final String name;
    private final Callable<T> callable;
    private final ScheduledExecutorService scheduler;
    private final ScopeConfig config;
    private final TaskScope scope;

    private final Object lock = new Object();
    private final CountDownLatch terminal = new CountDownLatch(1);

    private Thread runner;
    private boolean runnerFinished;
    private SubtaskState state = SubtaskState.RUNNING;
    private T value;
    private Throwable error;

    Subtask(int index,
            String name,
            Callable<T> callable,
            ScheduledExecutorService scheduler,
            ScopeConfig config,
            TaskScope scope) {
        this.index = index;
        this.name = name;
        this.callable = callable;
        this.scheduler = scheduler;
        this.config = config;
        this.scope = scope;
    }

    void start() {
        Thread thread = new Thread(this::run, "taskscope-task-" + index + "-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        synchronized (lock) {
            runner = Thread.currentThread();
            if (state != SubtaskState.RUNNING) {
                // Cancelled/timed out before the worker actually started.
                runnerFinished = true;
                terminal.countDown();
                return;
            }
        }
        T result = null;
        Throwable failure = null;
        try {
            result = callable.call();
        } catch (Throwable t) {
            failure = t;
        }
        boolean businessFailure = false;
        synchronized (lock) {
            runnerFinished = true;
            if (state == SubtaskState.RUNNING) {
                if (failure == null) {
                    state = SubtaskState.SUCCESS;
                    value = result;
                } else {
                    state = SubtaskState.FAILED;
                    error = failure;
                    businessFailure = true;
                }
            }
            terminal.countDown();
        }
        if (businessFailure) {
            scope.onSubtaskBusinessFailure();
        }
    }

    /** Scheduled per-subtask timeout. No-op if the subtask already reached a terminal state. */
    void onSubtaskTimeout() {
        Thread toInterrupt = null;
        synchronized (lock) {
            if (state != SubtaskState.RUNNING) {
                return;
            }
            state = SubtaskState.TIMED_OUT;
            toInterrupt = runner;
        }
        if (toInterrupt != null) {
            toInterrupt.interrupt();
        }
        scheduleUnresponsiveCheck();
    }

    /** Scope cancellation (explicit, scope timeout or shutdown-on-failure). */
    void requestCancel() {
        Thread toInterrupt = null;
        synchronized (lock) {
            if (state != SubtaskState.RUNNING) {
                return;
            }
            state = SubtaskState.CANCELLED;
            toInterrupt = runner;
        }
        if (toInterrupt != null) {
            toInterrupt.interrupt();
        }
        scheduleUnresponsiveCheck();
    }

    private void scheduleUnresponsiveCheck() {
        try {
            scheduler.schedule(this::markUnresponsive,
                    config.interruptGracePeriod().toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException ignored) {
            // Scheduler already shut down: the terminal latch still reaches zero once the
            // worker exits, or the scope abandons it on close().
        }
    }

    private void markUnresponsive() {
        synchronized (lock) {
            if (runnerFinished) {
                return;
            }
            if (state == SubtaskState.CANCELLED || state == SubtaskState.TIMED_OUT) {
                state = SubtaskState.UNRESPONSIVE;
                // Stop waiting: the worker is a daemon thread and will not block JVM shutdown.
                terminal.countDown();
            }
        }
    }

    /** Waits until the subtask ends or is abandoned as unresponsive. Never blocks indefinitely. */
    void awaitTerminal() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    terminal.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    scope.cancel();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    SubtaskFailure toFailure(TaskScope.CancelReason scopeCancelReason) {
        synchronized (lock) {
            return switch (state) {
                case FAILED -> new SubtaskFailure(index, name, FailureKind.BUSINESS_FAILURE, error);
                case TIMED_OUT -> new SubtaskFailure(index, name, FailureKind.SUBTASK_TIMEOUT, null);
                case UNRESPONSIVE -> new SubtaskFailure(index, name, FailureKind.UNRESPONSIVE, null);
                case CANCELLED -> new SubtaskFailure(index, name,
                        scopeCancelReason == TaskScope.CancelReason.TIMEOUT
                                ? FailureKind.SCOPE_TIMEOUT
                                : FailureKind.CANCELLED,
                        null);
                case RUNNING, SUCCESS -> null;
            };
        }
    }

    T valueIfSuccess() {
        synchronized (lock) {
            if (state != SubtaskState.SUCCESS) {
                throw new IllegalStateException("subtask '" + name + "' did not succeed: " + state);
            }
            return value;
        }
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SubtaskState state() {
        synchronized (lock) {
            return state;
        }
    }

    @Override
    public T result() {
        return valueIfSuccess();
    }

    @Override
    public String toString() {
        return "Subtask[" + index + " '" + name + "' " + state() + "]";
    }
}
