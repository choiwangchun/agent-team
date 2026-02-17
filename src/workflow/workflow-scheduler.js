class WorkflowScheduler {
  constructor({
    repository,
    taskExecutor = null,
    pollIntervalMs = 1000,
    maxTasksPerTick = 3,
    taskDelayMs = 120,
  }) {
    this.repository = repository;
    this.taskExecutor = typeof taskExecutor === "function" ? taskExecutor : null;
    this.pollIntervalMs = Math.max(100, Number(pollIntervalMs) || 1000);
    this.maxTasksPerTick = Math.max(1, Number(maxTasksPerTick) || 3);
    this.taskDelayMs = Math.max(0, Number(taskDelayMs) || 0);
    this.timer = null;
    this.running = false;
  }

  start() {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      this.processTick().catch((error) => {
        console.error("Workflow scheduler tick failed:", error);
      });
    }, this.pollIntervalMs);

    this.timer.unref?.();

    this.processTick().catch((error) => {
      console.error("Initial workflow scheduler tick failed:", error);
    });
  }

  stop() {
    if (!this.timer) {
      return;
    }
    clearInterval(this.timer);
    this.timer = null;
  }

  async processTick() {
    if (this.running) {
      return;
    }

    this.running = true;
    try {
      const tasks = await this.claimTasksForTick(this.maxTasksPerTick);
      if (tasks.length === 0) {
        return;
      }
      await Promise.allSettled(
        tasks.map((task) => this.processTask(task))
      );
    } finally {
      this.running = false;
    }
  }

  async claimTasksForTick(limit) {
    const safeLimit = Math.max(1, Number(limit) || 1);
    const attempts = await Promise.all(
      Array.from({ length: safeLimit }, () =>
        this.repository.claimPendingWorkflowTask()
      )
    );

    const claimed = [];
    const seen = new Set();
    for (const task of attempts) {
      const taskId = String(task?.id || "").trim();
      if (!taskId || seen.has(taskId)) {
        continue;
      }
      seen.add(taskId);
      claimed.push(task);
    }
    return claimed;
  }

  async processTask(task) {
    const workflowId = String(task?.workflowId || "").trim();
    if (!workflowId) {
      return;
    }

    const taskTitle = String(task?.title || task?.taskKey || "Task").trim() || "Task";
    const taskKind = String(task?.kind || "general").trim() || "general";
    let agentLabel = "Lead Agent";
    let agent = null;

    if (task?.agentId) {
      try {
        agent = await this.repository.getAgent(task.agentId);
        if (agent?.name) {
          agentLabel = String(agent.name).trim() || agentLabel;
        } else {
          agentLabel = `agent:${String(task.agentId).slice(0, 8)}`;
        }
      } catch {
        agentLabel = `agent:${String(task.agentId).slice(0, 8)}`;
      }
    }

    try {
      const configuredModel = String(agent?.modelTier || "Balanced (default)")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 120);
      await this.repository.appendWorkflowEvent({
        workflowId,
        taskId: task.id,
        role: "agent",
        message: `${agentLabel} 시작: ${taskTitle} [configured:${configuredModel}]`,
        meta: {
          taskKey: task.taskKey,
          kind: taskKind,
          configuredModel,
        },
      });

      if (this.taskDelayMs > 0) {
        await new Promise((resolve) => {
          setTimeout(resolve, this.taskDelayMs);
        });
      }

      const rawOutput = this.taskExecutor
        ? await this.taskExecutor({
            task,
            workflowId,
            taskTitle,
            taskKind,
            agent,
            agentLabel,
          })
        : null;
      const output = this.normalizeTaskOutput({
        task,
        taskKind,
        taskTitle,
        agentLabel,
        rawOutput,
      });

      const modelUsed = String(output?.model || configuredModel || "local/fallback")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 120);
      const outputStatus = String(output?.status || "completed")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
      const commandStepCount = Array.isArray(output?.commandRuns)
        ? output.commandRuns.length
        : 0;
      const messageDispatchCount = Math.max(
        0,
        Number(output?.messageDispatchCount) || 0
      );
      await this.repository.completeWorkflowTask(task.id, output);
      await this.repository.appendWorkflowEvent({
        workflowId,
        taskId: task.id,
        role: "agent",
        message: `${agentLabel} 완료: ${taskTitle} [model:${modelUsed}]${commandStepCount > 0 ? ` [cmd:${commandStepCount}]` : ""}${messageDispatchCount > 0 ? ` [msg:${messageDispatchCount}]` : ""}${output.summary ? ` · ${output.summary}` : ""}`,
        meta: {
          taskKey: task.taskKey,
          kind: taskKind,
          model: modelUsed,
          status: outputStatus || "completed",
          commandStepCount,
          messageDispatchCount,
        },
      });
    } catch (error) {
      const message = String(error?.message || "workflow_task_failed").trim();
      await this.repository.failWorkflowTask(task.id, message);
      await this.repository.appendWorkflowEvent({
        workflowId,
        taskId: task.id,
        role: "system",
        message: `${agentLabel} 실패: ${taskTitle} (${message})`,
        meta: {
          taskKey: task.taskKey,
          kind: taskKind,
          error: message,
        },
      });
    } finally {
      await this.repository.reconcileWorkflowStatus(workflowId);
    }
  }

  normalizeTaskOutput({
    task,
    taskKind,
    taskTitle,
    agentLabel,
    rawOutput,
  }) {
    const output =
      rawOutput && typeof rawOutput === "object" && !Array.isArray(rawOutput)
        ? { ...rawOutput }
        : {};
    if (!output.taskKey) {
      output.taskKey = task?.taskKey || "";
    }
    if (!output.kind) {
      output.kind = taskKind;
    }
    if (!output.agent) {
      output.agent = agentLabel;
    }
    if (!output.summary) {
      output.summary = `${agentLabel}가 '${taskTitle}' 작업을 처리했습니다.`;
    }
    output.summary = String(output.summary || "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 280);
    if (!output.completedAt) {
      output.completedAt = new Date().toISOString();
    }
    return output;
  }
}

module.exports = {
  WorkflowScheduler,
};
