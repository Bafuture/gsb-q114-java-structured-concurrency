package com.example.gsb.sc;

/**
 * A subtask did not respond to the cancellation interrupt within the
 * abandon grace period, so the scope stopped waiting for it.
 */
public class UnresponsiveTaskException extends RuntimeException {

    public UnresponsiveTaskException(String message) {
        super(message);
    }
}
