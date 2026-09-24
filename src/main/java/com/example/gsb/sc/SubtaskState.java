package com.example.gsb.sc;

/** Lifecycle states of a single subtask. */
public enum SubtaskState {
    /** The subtask is still running. */
    RUNNING,
    /** The subtask completed normally with a result. */
    SUCCESS,
    /** The subtask threw a business exception. */
    FAILED,
    /** The subtask was cancelled (scope cancelled / timed out) before it finished. */
    CANCELLED,
    /** The subtask's own timeout fired before it finished. */
    TIMED_OUT,
    /** The subtask did not respond to interruption within the grace period; the scope stopped waiting for it. */
    UNRESPONSIVE
}
