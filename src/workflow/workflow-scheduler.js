class WorkflowScheduler {
  constructor({
    repository,
    pollIntervalMs = 1000,
    maxTasksPerTick = 3,
    taskDelayMs = 120,
  }) {
    this.repository = repository;
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
      for (let index = 0; index < this.maxTasksPerTick; index += 1) {
        const task = await this.repository.claimPendingWorkflowTask();
        if (!task) {
          break;
        }
        await this.processTask(task);
      }
    } finally {
      this.running = false;
    }
  }

  async processTask(task) {
    const workflowId = String(task?.workflowId || "").trim();
    if (!workflowId) {
      return;
    }

    const taskTitle = String(task?.title || task?.taskKey || "Task").trim() || "Task";
    const taskKind = String(task?.kind || "general").trim() || "general";
    let agentLabel = "Lead Agent";

    if (task?.agentId) {
      try {
        const agent = await this.repository.getAgent(task.agentId);
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
      await this.repository.appendWorkflowEvent({
        workflowId,
        taskId: task.id,
        role: "agent",
        message: `${agentLabel} 시작: ${taskTitle}`,
        meta: {
          taskKey: task.taskKey,
          kind: taskKind,
        },
      });

      if (this.taskDelayMs > 0) {
        await new Promise((resolve) => {
          setTimeout(resolve, this.taskDelayMs);
        });
      }

      const output = {
        taskKey: task.taskKey,
        kind: taskKind,
        agent: agentLabel,
        summary: `${agentLabel}가 '${taskTitle}' 작업을 처리했습니다.`,
        completedAt: new Date().toISOString(),
      };

      await this.repository.completeWorkflowTask(task.id, output);
      await this.repository.appendWorkflowEvent({
        workflowId,
        taskId: task.id,
        role: "agent",
        message: `${agentLabel} 완료: ${taskTitle}`,
        meta: {
          taskKey: task.taskKey,
          kind: taskKind,
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
}

module.exports = {
  WorkflowScheduler,
};
