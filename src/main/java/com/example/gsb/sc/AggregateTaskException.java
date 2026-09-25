package com.example.gsb.sc;

import java.util.List;

/**
 * Thrown by {@link TaskScope#join()} when at least one subtask did not complete
 * successfully. Aggregates <em>all</em> failures (business failures, cancellations,
 * timeouts, unresponsive tasks); inspect {@link #failures()} to distinguish
 * {@link TaskState#FAILED} (business) from {@link TaskState#CANCELLED} and friends.
 */
public class AggregateTaskException extends RuntimeException {

    private final ScopeResult scopeResult;

    public AggregateTaskException(ScopeResult scopeResult) {
        super(buildMessage(scopeResult));
        this.scopeResult = scopeResult;
    }

    private static String buildMessage(ScopeResult result) {
        StringBuilder sb = new StringBuilder()
                .append(result.failures().size())
                .append(" of ")
                .append(result.size())
                .append(" subtask(s) did not complete successfully: ");
        List<TaskResult<?>> failures = result.failures();
        for (int i = 0; i < failures.size(); i++) {
            if (i > 0) {
                sb.append("; ");
            }
            sb.append(failures.get(i));
        }
        return sb.toString();
    }

    /** Every non-successful subtask outcome, in submission order. */
    public List<TaskResult<?>> failures() {
        return scopeResult.failures();
    }

    /** Full ordered report, including successful subtasks. */
    public ScopeResult scopeResult() {
        return scopeResult;
    }
}
