package com.example.gsb.sc;

/** A subtask exceeded its own per-task timeout. */
public class TaskTimeoutException extends RuntimeException {

    public TaskTimeoutException(String message) {
        super(message);
    }
}
