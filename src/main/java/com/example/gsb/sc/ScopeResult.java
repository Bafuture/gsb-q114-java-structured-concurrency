package com.example.gsb.sc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Ordered outcomes of every subtask submitted to a {@link TaskScope}.
 * Results are indexed by submission order, so a failure can be located
 * to the exact subtask via {@link TaskResult#index()} / {@link TaskResult#name()}.
 */
public final class ScopeResult {

    private final List<TaskResult<?>> results;

    ScopeResult(List<TaskResult<?>> results) {
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
    }

    /** All outcomes, in submission order. */
    public List<TaskResult<?>> results() {
        return results;
    }

    /** Outcome of the subtask submitted at position {@code index}. */
    public TaskResult<?> result(int index) {
        return results.get(index);
    }

    public int size() {
        return results.size();
    }

    /** True when every subtask completed successfully. */
    public boolean isSuccess() {
        return results.stream().allMatch(TaskResult::isSuccess);
    }

    /** All non-successful outcomes (FAILED / CANCELLED / TIMEOUT / UNRESPONSIVE), in submission order. */
    public List<TaskResult<?>> failures() {
        List<TaskResult<?>> failed = new ArrayList<>();
        for (TaskResult<?> r : results) {
            if (!r.isSuccess()) {
                failed.add(r);
            }
        }
        return Collections.unmodifiableList(failed);
    }

    /** Values of all subtasks in submission order. Only valid when {@link #isSuccess()}. */
    public List<Object> values() {
        List<Object> values = new ArrayList<>(results.size());
        for (TaskResult<?> r : results) {
            values.add(r.value());
        }
        return values;
    }
}
