package com.example.gsb.sc;

import java.util.Objects;

/**
 * Outcome of a single subtask, located by its submission {@link #index()} and {@link #name()}.
 *
 * @param <T> result type of the subtask
 */
public final class TaskResult<T> {

    private final int index;
    private final String name;
    private final TaskState state;
    private final T value;
    private final Throwable error;

    private TaskResult(int index, String name, TaskState state, T value, Throwable error) {
        this.index = index;
        this.name = name;
        this.state = state;
        this.value = value;
        this.error = error;
    }

    static <T> TaskResult<T> success(int index, String name, T value) {
        return new TaskResult<>(index, name, TaskState.SUCCESS, value, null);
    }

    static <T> TaskResult<T> failure(int index, String name, TaskState state, Throwable error) {
        Objects.requireNonNull(error, "error");
        return new TaskResult<>(index, name, state, null, error);
    }

    /** Position in submission order. */
    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    public TaskState state() {
        return state;
    }

    public boolean isSuccess() {
        return state == TaskState.SUCCESS;
    }

    /** The produced value. Only valid when {@link #isSuccess()}. */
    public T value() {
        if (!isSuccess()) {
            throw new IllegalStateException(
                    "subtask [" + index + "] '" + name + "' did not succeed (state=" + state + ")", error);
        }
        return value;
    }

    /** The failure/cancellation cause, or {@code null} on success. */
    public Throwable error() {
        return error;
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return "[" + index + "] '" + name + "' SUCCESS";
        }
        String cause = error == null ? "" : " (" + error.getClass().getSimpleName()
                + (error.getMessage() == null ? "" : ": " + error.getMessage()) + ")";
        return "[" + index + "] '" + name + "' " + state + cause;
    }
}
