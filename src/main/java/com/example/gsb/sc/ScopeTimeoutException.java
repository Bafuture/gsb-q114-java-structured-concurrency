package com.example.gsb.sc;

import java.util.List;

/**
 * Thrown by {@link TaskScope#join()} when the scope-level timeout fired and at least
 * one unfinished subtask had to be cancelled because of it. Extends
 * {@link ScopeFailedException}, so callers may handle both with one catch clause.
 */
public class ScopeTimeoutException extends ScopeFailedException {

    private static final long serialVersionUID = 1L;

    public ScopeTimeoutException(List<SubtaskFailure> failures) {
        super(failures, "scope timed out: " + failures.size() + " subtask(es) cancelled/failed");
    }
}
