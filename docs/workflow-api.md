# Workflow API (Phase 1)

멀티 에이전트 오케스트레이션 1차 백엔드 API입니다.

## Create Workflow

`POST /api/workflows`

```json
{
  "goal": "Q3 수출금융 데이터를 정리하고 인사이트 리포트를 만들어줘",
  "datasetId": "optional-dataset-id",
  "selectedFeatures": ["Fiscal Year", "Deal Cancelled"],
  "nodes": [
    { "id": "node-1", "agentId": "agent-id-1", "name": "Planner" },
    { "id": "node-2", "agentId": "agent-id-2", "name": "Analyst" }
  ],
  "edges": [
    { "from": "node-1", "to": "node-2" }
  ]
}
```

또는 `tasks`를 직접 지정할 수 있습니다.

```json
{
  "goal": "데이터 품질 점검",
  "tasks": [
    {
      "taskKey": "plan",
      "title": "작업 계획",
      "kind": "planning",
      "agentId": "agent-id-1",
      "dependsOnTaskKeys": []
    },
    {
      "taskKey": "analyze",
      "title": "데이터 분석",
      "kind": "analysis",
      "agentId": "agent-id-2",
      "dependsOnTaskKeys": ["plan"]
    }
  ]
}
```

## Read Workflow

- `GET /api/workflows`
- `GET /api/workflows/:workflowId`
- `GET /api/workflows/:workflowId/tasks`
- `GET /api/workflows/:workflowId/events`

## Manual Tick

`POST /api/workflows/:workflowId/tick`

- 스케줄러가 pending task를 실행합니다.
- serverless 환경에서는 주요 조회 API에서도 자동 tick이 수행됩니다.

## Notes

- 1차 스케줄러는 기본 실행기입니다.
- task dependency(`dependsOnTaskKeys`)가 완료된 순서대로 실행됩니다.
- task 실패 시 workflow 상태는 `failed`로 전환됩니다.
