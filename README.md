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

---

## 实现说明（分支 A）

源码位于 `src/main/java/com/example/gsb/sc/`：

| 类 | 职责 |
|----|------|
| `TaskScope` | 结构化并发作用域：提交子任务、`join()` 等待全部结束、`cancel()` 取消、`close()` 保证退出时无遗留等待 |
| `Subtask` | 子任务句柄：接收取消信号（中断）、记录原始结果 |
| `TaskResult` / `TaskState` | 单任务结果：`SUCCESS / FAILED / CANCELLED / TIMEOUT / UNRESPONSIVE` |
| `ScopeResult` | 按提交顺序排列的全量结果报告，可按下标/名称定位失败 |
| `AggregateTaskException` | 聚合异常，包含全部失败（区分业务失败与被取消） |
| `TaskTimeoutException` / `UnresponsiveTaskException` | 子任务超时、不响应中断的标记异常 |

### 用法示例

```java
try (TaskScope scope = TaskScope.builder()
        .scopeTimeout(Duration.ofSeconds(2))      // 作用域整体超时
        .taskTimeout(Duration.ofMillis(500))      // 默认单任务超时
        .abandonGrace(Duration.ofMillis(300))     // 不响应中断时的放弃等待宽限
        .build()) {
    scope.submit("user", () -> loadUser(id));
    scope.submit("orders", Duration.ofMillis(800), () -> loadOrders(id)); // 单独超时
    ScopeResult result = scope.join();            // 任一失败 -> 取消其余并抛 AggregateTaskException
    return result.values();                       // 按提交顺序
}
```

### 语义要点

- **取消传播**：任一子任务业务失败/超时，或作用域被取消/整体超时，所有未完成子任务立即收到中断信号。
- **先到先生效**：单任务超时与作用域整体超时独立计时，谁先触发谁生效。
- **不响应中断**：超时后等待一个放弃宽限（abandon grace），仍不停止则标记 `UNRESPONSIVE` 并放弃等待，绝不无限阻塞（工作线程为守护线程，不会拖住 JVM 退出）。
- **异常聚合**：`AggregateTaskException#failures()` 含全部失败，`TaskResult#state()` 区分 `FAILED`（业务）与 `CANCELLED`/`TIMEOUT`/`UNRESPONSIVE`。

### 测试

`src/test/java/com/example/gsb/sc/TaskScopeTest.java` 覆盖：全部成功按序返回、部分失败聚合+fail-fast 取消、作用域超时取消全部、单任务超时、不响应中断标记放弃、未 join 直接 close 的取消回收、外部 cancel、重复提交拒绝等 10 个用例。

```bash
./mvnw -q verify   # 或 mvn -q verify
```
