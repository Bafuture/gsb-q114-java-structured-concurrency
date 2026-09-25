package com.example.gsb.sc;

/** Terminal state of a subtask. */
public enum TaskState {
    /** The subtask completed normally and produced a value. */
    SUCCESS,
    /** The subtask threw a business exception. */
    FAILED,
    /** The subtask was cancelled (scope cancelled / scope timeout / fail-fast) and stopped. */
    CANCELLED,
    /** The subtask exceeded its own timeout and stopped after the cancel signal. */
    TIMEOUT,
    /** The subtask ignored the interrupt and did not stop within the abandon grace period. */
    UNRESPONSIVE
}
