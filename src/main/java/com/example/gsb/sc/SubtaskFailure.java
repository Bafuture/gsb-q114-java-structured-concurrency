package com.example.gsb.sc;

import java.util.Objects;
import java.util.Optional;

/**
 * Describes one failed/cancelled subtask. The {@link #index()} is the submission-order
 * position of the subtask inside its scope, so failures can be mapped back to the
 * exact subtask that produced them.
 */
public record SubtaskFailure(int index, String name, FailureKind kind, Throwable cause) {

    public SubtaskFailure {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
    }

    /** True when the subtask failed with a business exception (as opposed to being cancelled/timed out). */
    public boolean isBusinessFailure() {
        return kind == FailureKind.BUSINESS_FAILURE;
    }

    /** True when the subtask did not fail by itself but was cancelled or timed out. */
    public boolean isCancellation() {
        return kind == FailureKind.CANCELLED
                || kind == FailureKind.SCOPE_TIMEOUT
                || kind == FailureKind.SUBTASK_TIMEOUT
                || kind == FailureKind.UNRESPONSIVE;
    }

    public Optional<Throwable> causeAsOptional() {
        return Optional.ofNullable(cause);
    }

    @Override
    public String toString() {
        String base = "[" + index + "] '" + name + "' " + kind;
        return cause == null ? base : base + ": " + cause;
    }
}
