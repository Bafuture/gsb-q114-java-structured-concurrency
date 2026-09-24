# 结构化并发任务框架

Pair-wise GSB 标注任务仓库（第 11 批 / 114）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们要在一个请求里并发调用多个子任务，任何一步超时或失败都要把整组任务取消掉，避免线程泄漏。请从零实现一个结构化并发任务框架。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ）。要求：1) 支持在一个作用域内提交多个子任务并等待全部结束，作用域退出时必须保证所有子任务已结束（成功、失败或已取消）；2) 取消传播：作用域被取消或超时时，所有未完成子任务必须收到取消信号并停止工作；3) 子任务自身超时与作用域整体超时分别可配，先到者生效；4) 异常聚合：多个子任务失败时要抛出包含全部失败的聚合异常，并能区分业务失败与被取消；5) 结果收集：按提交顺序返回各子任务结果，失败位置要能定位到具体子任务；6) 中断处理：子任务不响应中断时，超时后必须能放弃等待并在报告中标记为未响应，不得无限阻塞；7) 测试覆盖全部成功、部分失败聚合、作用域超时取消全部、单任务超时与不响应中断的处理；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。

## 实现说明

结构化并发框架位于 `com.example.gsb.sc` 包，核心类型：

- `TaskScope`：try-with-resources 作用域，`fork(...)` 提交子任务，`join()` 等待全部结束并按提交顺序返回结果；作用域退出时保证取消并结束/放弃所有子任务。
- `ScopeConfig`：可分别配置作用域整体超时（`scopeTimeout`）、单任务默认超时（`subtaskTimeout`，也可在 `fork` 时单独覆盖，先到者生效）、中断宽限期（`interruptGracePeriod`）和失败策略（`ShutdownPolicy.AWAIT_ALL` / `SHUTDOWN_ON_FAILURE`）。
- `ScopeFailedException`：聚合异常，`failures()` 按提交顺序返回 `SubtaskFailure(index, name, FailureKind, cause)`；`businessFailures()` 与 `cancellations()` 可区分业务失败与取消；作用域整体超时抛其子类 `ScopeTimeoutException`。
- 子任务不响应中断时，宽限期过后被标记为 `SubtaskState.UNRESPONSIVE`，框架放弃等待（工作线程为 daemon，不会阻止 JVM 退出），`join()` 不会无限阻塞。

```java
try (TaskScope scope = TaskScope.open(
        ScopeConfig.builder()
                .scopeTimeout(Duration.ofSeconds(2))
                .subtaskTimeout(Duration.ofSeconds(1))
                .build())) {
    SubtaskHandle<Foo> a = scope.fork("a", serviceA::call);
    SubtaskHandle<Bar> b = scope.fork("b", serviceB::call);
    List<Object> results = scope.join(); // 按提交顺序
}
```
