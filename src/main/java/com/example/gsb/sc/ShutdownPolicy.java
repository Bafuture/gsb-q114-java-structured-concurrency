package com.example.gsb.sc;

/** What the scope should do when one subtask fails. */
public enum ShutdownPolicy {
    /** Wait for every subtask to finish, then aggregate all failures. */
    AWAIT_ALL,
    /** Cancel all remaining subtasks as soon as one subtask fails. */
    SHUTDOWN_ON_FAILURE
}
