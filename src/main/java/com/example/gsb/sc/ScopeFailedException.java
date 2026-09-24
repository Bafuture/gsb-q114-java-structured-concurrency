package com.example.gsb.sc;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown by {@link TaskScope#join()} when one or more subtasks did not succeed.
 * Carries one {@link SubtaskFailure} per failed/cancelled subtask, in submission order.
 */
public class ScopeFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<SubtaskFailure> failures;

    public ScopeFailedException(List<SubtaskFailure> failures) {
        this(failures, "scope failed: " + describe(failures));
    }

    protected ScopeFailedException(List<SubtaskFailure> failures, String message) {
        super(message);
        this.failures = List.copyOf(failures);
        for (SubtaskFailure failure : this.failures) {
            if (failure.cause() != null) {
                addSuppressed(failure.cause());
            }
        }
    }

    /** Every failed/cancelled subtask, ordered by submission index. */
    public List<SubtaskFailure> failures() {
        return failures;
    }

    /** Only failures where the subtask itself threw an exception. */
    public List<SubtaskFailure> businessFailures() {
        return failures.stream()
                .filter(SubtaskFailure::isBusinessFailure)
                .toList();
    }

    /** Only failures caused by cancellation, timeout or an unresponsive subtask. */
    public List<SubtaskFailure> cancellations() {
        return failures.stream()
                .filter(SubtaskFailure::isCancellation)
                .toList();
    }

    private static String describe(List<SubtaskFailure> failures) {
        String detail = failures.stream()
                .map(SubtaskFailure::toString)
                .collect(Collectors.joining("; "));
        return failures.size() + " subtask failure(s) [" + detail + "]";
    }
}
