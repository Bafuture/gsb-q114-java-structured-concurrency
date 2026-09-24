package com.example.gsb.sc;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskScopeTest {

    @Test
    void allSuccess_returnsResultsInSubmissionOrder() {
        try (TaskScope scope = TaskScope.open()) {
            SubtaskHandle<Integer> first = scope.fork("first", () -> 1);
            SubtaskHandle<String> second = scope.fork("second", () -> {
                Thread.sleep(60);
                return "two";
            });
            SubtaskHandle<Integer> third = scope.fork("third", () -> 3);

            List<Object> results = scope.join();

            assertThat(results).containsExactly(1, "two", 3);
            assertThat(first.result()).isEqualTo(1);
            assertThat(second.result()).isEqualTo("two");
            assertThat(third.state()).isEqualTo(SubtaskState.SUCCESS);
        }
    }

    @Test
    void partialFailure_aggregatesEveryBusinessFailureWithIndexAndName() {
        try (TaskScope scope = TaskScope.open()) {
            SubtaskHandle<String> ok = scope.fork("ok", () -> "fine");
            scope.fork("boom-1", () -> {
                throw new IllegalStateException("bad-1");
            });
            scope.fork("boom-2", () -> {
                Thread.sleep(40);
                throw new IllegalArgumentException("bad-2");
            });

            assertThatThrownBy(scope::join)
                    .isInstanceOfSatisfying(ScopeFailedException.class, ex -> {
                        assertThat(ex).isNotInstanceOf(ScopeTimeoutException.class);
                        assertThat(ex.failures()).hasSize(2);
                        assertThat(ex.businessFailures()).hasSize(2);
                        assertThat(ex.cancellations()).isEmpty();
                        assertThat(ex.failures()).extracting(SubtaskFailure::index)
                                .containsExactly(1, 2);
                        assertThat(ex.failures()).extracting(SubtaskFailure::name)
                                .containsExactly("boom-1", "boom-2");
                        assertThat(ex.failures()).extracting(SubtaskFailure::kind)
                                .containsOnly(FailureKind.BUSINESS_FAILURE);
                        assertThat(ex.getSuppressed()).hasSize(2);
                        assertThat(ex.failures().get(0).cause()).isInstanceOf(IllegalStateException.class)
                                .hasMessage("bad-1");
                        assertThat(ex.failures().get(1).cause()).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("bad-2");
                    });

            assertThat(ok.result()).isEqualTo("fine");
        }
    }

    @Test
    void scopeTimeout_cancelsAllUnfinishedSubtasks() {
        ScopeConfig config = ScopeConfig.builder()
                .scopeTimeout(Duration.ofMillis(200))
                .interruptGracePeriod(Duration.ofMillis(300))
                .build();
        try (TaskScope scope = TaskScope.open(config)) {
            SubtaskHandle<Integer> a = scope.fork("long-a", () -> {
                Thread.sleep(10_000);
                return 1;
            });
            SubtaskHandle<Integer> b = scope.fork("long-b", () -> {
                Thread.sleep(10_000);
                return 2;
            });

            long start = System.nanoTime();
            assertThatThrownBy(scope::join)
                    .isInstanceOf(ScopeTimeoutException.class)
                    .isInstanceOfSatisfying(ScopeFailedException.class, ex -> {
                        assertThat(ex.failures()).hasSize(2);
                        assertThat(ex.cancellations()).hasSize(2);
                        assertThat(ex.businessFailures()).isEmpty();
                        assertThat(ex.failures()).extracting(SubtaskFailure::index)
                                .containsExactly(0, 1);
                        assertThat(ex.failures()).extracting(SubtaskFailure::kind)
                                .containsOnly(FailureKind.SCOPE_TIMEOUT);
                    });
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(elapsedMs).isLessThan(3_000L);
            assertThat(a.state()).isEqualTo(SubtaskState.CANCELLED);
            assertThat(b.state()).isEqualTo(SubtaskState.CANCELLED);
        }
    }

    @Test
    void subtaskTimeout_firesBeforeTheScopeDeadlineAndInterruptsOnlyTheSlowTask() {
        ScopeConfig config = ScopeConfig.builder()
                .scopeTimeout(Duration.ofSeconds(10))
                .subtaskTimeout(Duration.ofMillis(150))
                .interruptGracePeriod(Duration.ofMillis(300))
                .build();
        try (TaskScope scope = TaskScope.open(config)) {
            SubtaskHandle<String> fast = scope.fork("fast", () -> "quick");
            SubtaskHandle<String> slow = scope.fork("slow", () -> {
                Thread.sleep(10_000);
                return "never";
            });

            long start = System.nanoTime();
            assertThatThrownBy(scope::join)
                    .isInstanceOfSatisfying(ScopeFailedException.class, ex -> {
                        assertThat(ex).isNotInstanceOf(ScopeTimeoutException.class);
                        assertThat(ex.failures()).hasSize(1);
                        SubtaskFailure failure = ex.failures().get(0);
                        assertThat(failure.index()).isEqualTo(1);
                        assertThat(failure.name()).isEqualTo("slow");
                        assertThat(failure.kind()).isEqualTo(FailureKind.SUBTASK_TIMEOUT);
                        assertThat(failure.isBusinessFailure()).isFalse();
                    });
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(elapsedMs).isLessThan(3_000L);
            assertThat(fast.result()).isEqualTo("quick");
            assertThat(slow.state()).isEqualTo(SubtaskState.TIMED_OUT);
        }
    }

    @Test
    void unresponsiveSubtask_isAbandonedAfterGracePeriodAndMarkedUnresponsive() throws Exception {
        ScopeConfig config = ScopeConfig.builder()
                .subtaskTimeout(Duration.ofMillis(100))
                .interruptGracePeriod(Duration.ofMillis(200))
                .build();
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean stillRunningAfterJoin = new AtomicBoolean(false);
        try (TaskScope scope = TaskScope.open(config)) {
            SubtaskHandle<String> stubborn = scope.fork("stubborn", () -> {
                started.countDown();
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                        // Deliberately swallows interruption: simulates an unresponsive task.
                    }
                }
                stillRunningAfterJoin.set(true);
                return "done";
            });

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            assertThatThrownBy(scope::join)
                    .isInstanceOfSatisfying(ScopeFailedException.class, ex -> {
                        assertThat(ex.failures()).hasSize(1);
                        SubtaskFailure failure = ex.failures().get(0);
                        assertThat(failure.index()).isZero();
                        assertThat(failure.name()).isEqualTo("stubborn");
                        assertThat(failure.kind()).isEqualTo(FailureKind.UNRESPONSIVE);
                    });
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            // 100 ms timeout + 200 ms grace, with ample scheduling slack: join must not block
            // anywhere near the 5 s the task keeps running.
            assertThat(elapsedMs).isLessThan(2_000L);
            assertThat(stubborn.state()).isEqualTo(SubtaskState.UNRESPONSIVE);
        }
        // The abandoned worker is a daemon thread; give it a moment and verify the scope did
        // not wait for it (it finishes its own 5 s loop well after close() returned).
        assertThat(stillRunningAfterJoin.get()).isFalse();
    }

    @Test
    void closeWithoutJoin_cancelsAllSubtasks() {
        SubtaskHandle<Integer> handle;
        TaskScope scope = TaskScope.open(ScopeConfig.builder()
                .interruptGracePeriod(Duration.ofMillis(300))
                .build());
        try {
            handle = scope.fork("orphan", () -> {
                Thread.sleep(10_000);
                return 1;
            });
        } finally {
            scope.close();
        }
        assertThat(handle.state()).isEqualTo(SubtaskState.CANCELLED);
    }

    @Test
    void explicitCancel_isReportedSeparatelyFromBusinessFailure() throws Exception {
        ScopeConfig config = ScopeConfig.builder()
                .interruptGracePeriod(Duration.ofMillis(300))
                .build();
        try (TaskScope scope = TaskScope.open(config)) {
            CountDownLatch started = new CountDownLatch(1);
            SubtaskHandle<Integer> longTask = scope.fork("long", () -> {
                started.countDown();
                Thread.sleep(10_000);
                return 1;
            });
            scope.fork("fails", () -> {
                throw new IllegalStateException("boom");
            });

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            scope.cancel();

            assertThatThrownBy(scope::join)
                    .isInstanceOfSatisfying(ScopeFailedException.class, ex -> {
                        assertThat(ex.businessFailures()).hasSize(1);
                        assertThat(ex.businessFailures().get(0).name()).isEqualTo("fails");
                        assertThat(ex.cancellations()).hasSize(1);
                        assertThat(ex.cancellations().get(0).name()).isEqualTo("long");
                        assertThat(ex.cancellations().get(0).kind()).isEqualTo(FailureKind.CANCELLED);
                    });
            assertThat(longTask.state()).isEqualTo(SubtaskState.CANCELLED);
        }
    }
}
