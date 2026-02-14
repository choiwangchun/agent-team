# Agent Ops API

## 목적

`agent 생성`과 `agent 배치` 탭에서 실제 생성/배치/스케일링을 수행하기 위한 백엔드 API입니다.

## Endpoints

### Agent

- `GET /api/agents?limit=50`
- `GET /api/agents/:agentId`
- `POST /api/agents`

예시:

```json
{
  "name": "Frontend Design Agent",
  "modelTier": "Balanced (default)",
  "systemPrompt": "Design production-ready UI with accessibility.",
  "tools": ["Figma", "Web Search"]
}
```

### Deployment

- `GET /api/deployments?limit=100`
- `POST /api/deployments`
- `PATCH /api/deployments/:deploymentId/scale`

배치 생성 예시:

```json
{
  "agentId": "agent-id",
  "queueName": "design-queue",
  "environment": "staging",
  "desiredReplicas": 2
}
```

스케일 변경 예시:

```json
{
  "desiredReplicas": 4
}
```

## Storage

- `DATABASE_URL` 설정 시 PostgreSQL 영속 저장
- 미설정 시 in-memory 저장

## UI 연결 포인트

- `views/dashboard.ejs`의 `agent-creation` 탭:
  - 생성 폼 submit → `POST /api/agents`
  - 최근 Agent 목록 → `GET /api/agents`
- `views/dashboard.ejs`의 `agent-deployment` 탭:
  - 배치 생성 → `POST /api/deployments`
  - 배치 목록/메트릭 → `GET /api/deployments`
  - 스케일 버튼 → `PATCH /api/deployments/:id/scale`
