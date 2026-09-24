package com.example.gsb.sc;

/** Classification of a subtask failure, so callers can tell business failures from cancellations. */
public enum FailureKind {
    /** The subtask itself threw an exception. */
    BUSINESS_FAILURE,
    /** The subtask's own timeout fired. */
    SUBTASK_TIMEOUT,
    /** The scope-level timeout fired and the subtask was cancelled because of it. */
    SCOPE_TIMEOUT,
    /** The scope was cancelled explicitly (or by the shutdown-on-failure policy). */
    CANCELLED,
    /** The subtask ignored interruption and did not stop within the grace period. */
    UNRESPONSIVE
}
