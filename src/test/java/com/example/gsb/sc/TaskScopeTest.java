package com.example.gsb.sc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class TaskScopeTest {

    private static Callable<String> sleepThenReturn(long millis, String value) {
        return () -> {
            Thread.sleep(millis);
            return value;
        };
    }

    private static Callable<Void> sleepInterruptibly(long millis, AtomicBoolean interrupted) {
        return () -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
            return null;
        };
    }

    @Test
    void allSuccess_returnsResultsInSubmissionOrder() throws Exception {
        ScopeResult result;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .build()) {
            scope.submit("slow", sleepThenReturn(150, "first"));
            scope.submit("fast", sleepThenReturn(10, "second"));
            scope.submit("mid", sleepThenReturn(60, "third"));
            result = scope.join();
        }

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.values()).containsExactly("first", "second", "third");
        assertThat(result.result(0).name()).isEqualTo("slow");
        assertThat(result.result(2).value()).isEqualTo("third");
    }

    @Test
    void partialFailure_aggregatesAllFailuresAndCancelsRest() {
        AtomicBoolean sleeperInterrupted = new AtomicBoolean();

        AggregateTaskException thrown = null;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .abandonGrace(Duration.ofMillis(500))
                .build()) {
            scope.submit("ok", sleepThenReturn(10, "fine"));
            scope.submit("boom-1", () -> {
                throw new IllegalArgumentException("business failure one");
            });
            // Converts the cancel interrupt into its own business failure:
            // proves the failure is aggregated even after fail-fast cancellation.
            scope.submit("boom-2", () -> {
                try {
                    Thread.sleep(30_000);
                    return null;
                } catch (InterruptedException e) {
                    throw new IllegalStateException("business failure two");
                }
            });
            scope.submit("long-sleeper", sleepInterruptibly(30_000, sleeperInterrupted));
            try {
                scope.join();
            } catch (AggregateTaskException e) {
                thrown = e;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        assertThat(thrown).isNotNull();
        ScopeResult report = thrown.scopeResult();
        assertThat(report.result(0).state()).isEqualTo(TaskState.SUCCESS);
        assertThat(report.result(1).state()).isEqualTo(TaskState.FAILED);
        assertThat(report.result(1).error()).hasMessageContaining("business failure one");
        assertThat(report.result(2).state()).isEqualTo(TaskState.FAILED);
        assertThat(report.result(2).error()).hasMessageContaining("business failure two");
        assertThat(report.result(3).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(sleeperInterrupted).isTrue();

        // Failures are locatable by index and name, business vs cancelled distinguishable.
        assertThat(thrown.failures())
                .extracting(TaskResult::index)
                .containsExactly(1, 2, 3);
        assertThat(thrown.failures())
                .extracting(TaskResult::name)
                .containsExactly("boom-1", "boom-2", "long-sleeper");
        assertThat(thrown.getMessage()).contains("boom-1", "boom-2", "long-sleeper");
    }

    @Test
    void scopeTimeout_cancelsAllUnfinishedSubtasks() {
        AtomicBoolean interrupted1 = new AtomicBoolean();
        AtomicBoolean interrupted2 = new AtomicBoolean();

        long start = System.nanoTime();
        AggregateTaskException thrown = null;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofMillis(300))
                .abandonGrace(Duration.ofMillis(500))
                .build()) {
            scope.submit("a", sleepInterruptibly(30_000, interrupted1));
            scope.submit("b", sleepInterruptibly(30_000, interrupted2));
            try {
                scope.join();
            } catch (AggregateTaskException e) {
                thrown = e;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(thrown).isNotNull();
        assertThat(thrown.scopeResult().results())
                .extracting(TaskResult::state)
                .containsExactly(TaskState.CANCELLED, TaskState.CANCELLED);
        assertThat(interrupted1).isTrue();
        assertThat(interrupted2).isTrue();
        assertThat(elapsedMillis).isLessThan(5_000);
    }

    @Test
    void taskTimeout_marksResponsiveTaskTimeoutAndCancelsOthers() {
        AtomicBoolean otherInterrupted = new AtomicBoolean();

        AggregateTaskException thrown = null;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .abandonGrace(Duration.ofMillis(500))
                .build()) {
            // Per-task timeout of 200ms; the task stops on interrupt -> TIMEOUT.
            scope.submit("timed-out", Duration.ofMillis(200), sleepInterruptibly(30_000, new AtomicBoolean()));
            scope.submit("bystander", sleepInterruptibly(30_000, otherInterrupted));
            try {
                scope.join();
            } catch (AggregateTaskException e) {
                thrown = e;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        assertThat(thrown).isNotNull();
        ScopeResult report = thrown.scopeResult();
        assertThat(report.result(0).state()).isEqualTo(TaskState.TIMEOUT);
        assertThat(report.result(0).error()).isInstanceOf(TaskTimeoutException.class);
        assertThat(report.result(1).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(otherInterrupted).isTrue();
    }

    @Test
    void unresponsiveSubtask_isAbandonedAndMarkedUnresponsive() {
        AtomicBoolean bystanderInterrupted = new AtomicBoolean();
        CountDownLatch stubbornStarted = new CountDownLatch(1);

        long start = System.nanoTime();
        AggregateTaskException thrown = null;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .abandonGrace(Duration.ofMillis(300))
                .build()) {
            // Ignores interrupts completely; runs for ~30s if not abandoned.
            scope.submit("stubborn", Duration.ofMillis(200), () -> {
                stubbornStarted.countDown();
                long end = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < end) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ignored) {
                        // Deliberately unresponsive.
                    }
                }
                return null;
            });
            scope.submit("bystander", sleepInterruptibly(30_000, bystanderInterrupted));
            try {
                scope.join();
            } catch (AggregateTaskException e) {
                thrown = e;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(thrown).isNotNull();
        ScopeResult report = thrown.scopeResult();
        assertThat(report.result(0).state()).isEqualTo(TaskState.UNRESPONSIVE);
        assertThat(report.result(0).error()).isInstanceOf(UnresponsiveTaskException.class);
        assertThat(report.result(1).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(bystanderInterrupted).isTrue();
        // Did not wait for the 30s stubborn task: timeout 200ms + grace 300ms + slack.
        assertThat(elapsedMillis).isLessThan(5_000);
    }

    @Test
    void closeWithoutJoin_cancelsEverythingAndDoesNotBlock() throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(1);

        long start = System.nanoTime();
        try (TaskScope scope = TaskScope.builder()
                .abandonGrace(Duration.ofMillis(300))
                .build()) {
            scope.submit("worker", () -> {
                started.countDown();
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    throw e;
                }
                return null;
            });
            // Make sure the subtask is actually running before the scope exits.
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            // No join(): exiting the try-block must cancel and reap the subtask.
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(interrupted).isTrue();
        assertThat(elapsedMillis).isLessThan(5_000);
    }

    @Test
    void externalCancel_cancelsAllSubtasks() throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(1);

        AggregateTaskException thrown = null;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .abandonGrace(Duration.ofMillis(300))
                .build()) {
            scope.submit("worker", () -> {
                started.countDown();
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    throw e;
                }
                return null;
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            scope.cancel();
            try {
                scope.join();
            } catch (AggregateTaskException e) {
                thrown = e;
            }
        }

        assertThat(thrown).isNotNull();
        assertThat(thrown.scopeResult().result(0).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(interrupted).isTrue();
    }

    @Test
    void submitAfterJoin_isRejected() {
        try (TaskScope scope = TaskScope.create()) {
            scope.submit("one", () -> "ok");
            try {
                scope.join();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            assertThatThrownBy(() -> scope.submit("two", () -> "late"))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void emptyScope_joinsSuccessfully() throws Exception {
        try (TaskScope scope = TaskScope.create()) {
            ScopeResult result = scope.join();
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.size()).isZero();
        }
    }

    @Test
    void mixedResults_valuesAreOnlyReadFromSuccessfulTasks() {
        List<TaskResult<?>> results;
        try (TaskScope scope = TaskScope.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .abandonGrace(Duration.ofMillis(200))
                .build()) {
            scope.submit("good", () -> "value");
            scope.submit("bad", () -> {
                throw new RuntimeException("nope");
            });
            try {
                scope.join();
                throw new AssertionError("expected AggregateTaskException");
            } catch (AggregateTaskException e) {
                results = e.scopeResult().results();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        assertThat(results.get(0).value()).isEqualTo("value");
        assertThatThrownBy(results.get(1)::value)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bad");
    }
}
