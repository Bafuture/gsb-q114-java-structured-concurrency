package com.example.gsb.sc;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration of a {@link TaskScope}.
 *
 * @param scopeTimeout         overall deadline for the whole scope; when it fires, every
 *                             unfinished subtask is interrupted. {@code null} means no
 *                             scope-level timeout.
 * @param subtaskTimeout       default per-subtask timeout; may be overridden per fork.
 *                             {@code null} means subtasks do not time out individually.
 * @param interruptGracePeriod how long the scope waits for a subtask to stop after it has
 *                             been interrupted before the subtask is reported as
 *                             {@link SubtaskState#UNRESPONSIVE} and the scope gives up
 *                             waiting for it.
 * @param shutdownPolicy       whether one failing subtask cancels the rest
 *                             ({@link ShutdownPolicy#SHUTDOWN_ON_FAILURE}) or the scope
 *                             waits for all and aggregates ({@link ShutdownPolicy#AWAIT_ALL}).
 */
public record ScopeConfig(
        Duration scopeTimeout,
        Duration subtaskTimeout,
        Duration interruptGracePeriod,
        ShutdownPolicy shutdownPolicy) {

    private static final Duration DEFAULT_GRACE_PERIOD = Duration.ofMillis(500);

    public ScopeConfig {
        if (scopeTimeout != null) requireNonNegative(scopeTimeout, "scopeTimeout");
        if (subtaskTimeout != null) requireNonNegative(subtaskTimeout, "subtaskTimeout");
        if (interruptGracePeriod == null) {
            interruptGracePeriod = DEFAULT_GRACE_PERIOD;
        } else {
            requireNonNegative(interruptGracePeriod, "interruptGracePeriod");
        }
        if (shutdownPolicy == null) {
            shutdownPolicy = ShutdownPolicy.AWAIT_ALL;
        }
    }

    public static ScopeConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Duration requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative: " + value);
        }
        return value;
    }

    public static final class Builder {
        private Duration scopeTimeout;
        private Duration subtaskTimeout;
        private Duration interruptGracePeriod = DEFAULT_GRACE_PERIOD;
        private ShutdownPolicy shutdownPolicy = ShutdownPolicy.AWAIT_ALL;

        /** Overall deadline of the scope; all unfinished subtasks are interrupted when it fires. */
        public Builder scopeTimeout(Duration scopeTimeout) {
            this.scopeTimeout = scopeTimeout;
            return this;
        }

        /** Default deadline applied to every forked subtask unless overridden. */
        public Builder subtaskTimeout(Duration subtaskTimeout) {
            this.subtaskTimeout = subtaskTimeout;
            return this;
        }

        /** Time to wait for an interrupted subtask to stop before reporting it as unresponsive. */
        public Builder interruptGracePeriod(Duration interruptGracePeriod) {
            this.interruptGracePeriod = interruptGracePeriod;
            return this;
        }

        public Builder shutdownPolicy(ShutdownPolicy shutdownPolicy) {
            this.shutdownPolicy = shutdownPolicy;
            return this;
        }

        public ScopeConfig build() {
            return new ScopeConfig(scopeTimeout, subtaskTimeout, interruptGracePeriod, shutdownPolicy);
        }
    }
}
