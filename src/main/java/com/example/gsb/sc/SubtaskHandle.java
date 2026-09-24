package com.example.gsb.sc;

/**
 * Public handle of a forked subtask.
 *
 * @param <T> result type
 */
public interface SubtaskHandle<T> {

    /** Submission-order index inside the scope (0-based). */
    int index();

    /** Human-readable name. */
    String name();

    /** Current lifecycle state. */
    SubtaskState state();

    /**
     * Returns the subtask result. Call this after {@link TaskScope#join()} returned successfully.
     *
     * @throws IllegalStateException if the subtask did not succeed
     */
    T result();
}
