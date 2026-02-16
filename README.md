# Agent Workspace

데이터를 온톨로지 기반으로 정규화하고, 생성한 에이전트를 노드 플로우로 배치해 실행 전략까지 설계할 수 있는 AI Operations 대시보드입니다.

## 한 줄 가치 제안

서로 다른 회사의 제각각 데이터 포맷을 업로드해 같은 의미로 통합하고, 그 결과를 에이전트 운영 플로우에 바로 연결합니다.

## 어떤 문제를 해결하나요?

- 조직마다 다른 컬럼명/값 표현으로 데이터가 합쳐지지 않는 문제
- 에이전트를 만들었지만 실제 운영 관점(배치, 연결, 역할 분담)으로 확장하기 어려운 문제
- 데이터 처리와 에이전트 운영이 분리되어 의사결정 속도가 느려지는 문제

## 핵심 기능

- 데이터 처리 (Ontology Mapping)
  - CSV/JSON/XLSX 업로드
  - 컬럼 의미 정규화 (예: `male/female`, `boy/girl` -> `gender`)
  - 추출 feature 확인 + 데이터 프리뷰
  - 수동 컬럼 매핑 override 지원
- Agent 생성
  - 이름/시스템 인스트럭션/스킬/아바타 기반 에이전트 생성
  - 최근 생성 에이전트 확인
- Agent 배치
  - 노드 드래그 앤 드롭
  - 노드 간 방향성 연결
  - 노드별 feature 할당
  - 명령창 기반 운영 상태 확인
- 운영 API
  - Agent/Deployment CRUD
  - Ingestion Job / Dataset 조회
  - Merge API

## 아키텍처 요약

- Backend: `Node.js + Express + EJS`
- Data: `PostgreSQL`(권장) 또는 In-Memory fallback
- Styling: `Tailwind CSS`
- Deploy: Vercel Serverless entrypoint (`api/index.js`)

## 빠른 시작 (Local)

### 1) 설치

```bash
npm install
```

### 2) 실행

```bash
npm run dev
```

- 기본 주소: `http://localhost:3000`

### 3) 프로덕션 실행

```bash
npm run start
```

## 환경 변수

`.env` 또는 배포 환경변수로 설정:

- `DATABASE_URL` (선택, 권장)
  - 설정 시 PostgreSQL 영속 저장
  - 미설정 시 in-memory
- `AUTH_DISABLED` (기본 `true`)
  - `true`면 로그인/JWT를 우회하고 단일 퍼블릭 세션으로 동작
  - 빠른 공개 데모/초기 배포용, 운영 보안 모드에서는 `false`로 전환
- `MEMORY_REPO_PERSIST` (기본 `true`, `DATABASE_URL` 미설정 시)
  - 로컬 재시작 후에도 Provider Auth 연결 상태를 파일에 유지
- `MEMORY_REPO_STATE_FILE` (기본 `.cache/memory-repository.json`)
  - in-memory 모드 로컬 상태 파일 경로
- `NODE_ENV`
- `MAX_UPLOAD_SIZE_MB` (기본 20)
- `JOB_POLL_INTERVAL_MS` (기본 750)
- `JOB_BATCH_SIZE` (기본 5)
- `WORKFLOW_POLL_INTERVAL_MS` (기본 900)
- `WORKFLOW_BATCH_SIZE` (기본 3)
- `WORKFLOW_TASK_DELAY_MS` (기본 100ms, 기본 스케줄러의 task 실행 딜레이)
- `DATA_AI_MAX_ROWS` (기본 5000)
- `DATA_AI_MAX_COLUMNS` (기본 120)
- `DATA_AI_CELL_MAX_LENGTH` (기본 2048)
- `DATA_AI_SAMPLE_ROWS` (기본 40)
- `DATA_AI_MODEL_TIMEOUT_MS` (기본 45000ms)
- `DATA_AI_RUN_TTL_SEC` (기본 1800초)
- `DATA_AI_PYTHON_TOOL_ENABLED` (기본 로컬 `true`, 서버리스 `false`)
- `DATA_AI_PYTHON_BIN` (기본 `python3`)
- `DATA_AI_PYTHON_TIMEOUT_MS` (기본 20000ms)
- `DATA_AI_PYTHON_SCRIPT` (기본 `scripts/data_transformer.py`)
- `MICROCACHE_TTL_MS` (선택)
- `JWT_ACCESS_SECRET` (운영 필수)
- `JWT_REFRESH_SECRET` (운영 필수)
- `ACCESS_TOKEN_TTL_SEC` (기본 900초)
- `REFRESH_TOKEN_TTL_SEC` (기본 1209600초, 14일)
- `AUTH_COOKIE_DOMAIN` (선택)
- `AUTH_BOOTSTRAP_EMAIL` (기본 `admin@agent.local`)
- `AUTH_BOOTSTRAP_PASSWORD` (개발 기본 `admin1234!`, 운영은 직접 지정 권장)
- `AUTH_BOOTSTRAP_NAME` (기본 `Owner`)
- `AUTH_BOOTSTRAP_ROLE` (기본 `owner`)
- `LOGIN_WINDOW_MS` (기본 60000)
- `LOGIN_MAX_ATTEMPTS` (기본 5)
- `SUPABASE_URL` (아바타 Storage 사용 시)
- `SUPABASE_SERVICE_ROLE_KEY` (아바타 Storage 사용 시)
- `SUPABASE_AVATAR_BUCKET` (기본 `avatars`)
- `SUPABASE_AVATAR_PREFIX` (선택, 기본 루트)
- `SUPABASE_AVATAR_PUBLIC` (기본 `true`)
- `SUPABASE_AVATAR_SIGNED_URL_EXPIRES_SEC` (기본 3600, private bucket용)
- `SUPABASE_AVATAR_MAX_FILES` (기본 5000, 랜덤 카탈로그 스캔 상한)

참고:

- 데이터 처리 명령은 기본적으로 모델이 변경안을 제안하고, 사용자가 `apply`로 승인해야 현재 작업 데이터에 반영됩니다.
- `DATA_AI_PYTHON_TOOL_ENABLED=true`인 로컬 환경에서는 변환 적용을 Python 툴로 실행하고, 실패 시 JS fallback을 사용합니다.

### 아바타 저장소 전환 (Supabase Storage)

- `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY`가 설정되면 `/api/avatars/random`은 Supabase bucket을 우선 사용합니다.
- Supabase에 아바타를 업로드하려면:

```bash
npm run sync:avatars
```

- 기본 업로드 소스 폴더는 `./avatars`이고, 다른 경로를 쓰려면:

```bash
npm run sync:avatars -- ./my-avatar-dir
```

- 무료 플랜(1GB)처럼 용량 제한이 필요하면, 먼저 서브셋을 만든 뒤 업로드:

```bash
AVATAR_SUBSET_MAX_MB=900 npm run sync:avatars:subset
```

- 서브셋 생성만 먼저 확인하려면:

```bash
AVATAR_SUBSET_MAX_MB=900 npm run prepare:avatars:subset
```

## Vercel 배포 가이드 (현재 구조 기준)

이 저장소는 서버리스 진입점이 이미 포함되어 있습니다.

- `api/index.js` -> Express app 라우팅
- `vercel.json` -> 모든 경로를 서버리스 함수로 전달

### Vercel 설정

- Framework Preset: `Other`
- Install Command: `npm install`
- Build Command: `npm run build:css`
- Output Directory: 비워두기 (설정하지 않음)

주의:

- `Output Directory=public`로 지정하면 정적 사이트 모드가 되어 `404 NOT_FOUND`가 발생할 수 있습니다.
- 운영 환경에서는 `DATABASE_URL`을 반드시 설정하세요.

## API 빠른 보기

### System

- `GET /healthz`
- `GET /ops/metrics`
- `GET /api/system/storage`

### Auth

- `POST /api/auth/login`
- `POST /api/auth/refresh`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/auth/users` (admin/owner)
- `POST /api/auth/users` (admin/owner)
- `PATCH /api/auth/users/:userId` (admin/owner)

### Ontology

- `GET /api/ontology/fields`
- `POST /api/ontology/fields`
- `GET /api/ontology/overrides`
- `POST /api/ontology/overrides`

### Data

- `POST /api/data/upload`
- `POST /api/data/table`
- `GET /api/jobs`
- `GET /api/jobs/:jobId`
- `GET /api/data/datasets`
- `GET /api/data/datasets/:datasetId`
- `POST /api/data/merge`

### Agent / Deployment

- `GET /api/agents`
- `GET /api/agents/:agentId`
- `POST /api/agents` (operator/admin/owner)
- `GET /api/deployments`
- `POST /api/deployments` (operator/admin/owner)
- `PATCH /api/deployments/:deploymentId/scale` (operator/admin/owner)

### Workflow (Agent Team 1차)

- `GET /api/workflows`
- `POST /api/workflows` (operator/admin/owner)
- `GET /api/workflows/:workflowId`
- `GET /api/workflows/:workflowId/tasks`
- `GET /api/workflows/:workflowId/events`
- `POST /api/workflows/:workflowId/tick` (operator/admin/owner)

자세한 스펙은 아래 문서를 참고하세요.

- `docs/data-processing-api.md`
- `docs/agent-ops-api.md`
- `docs/workflow-api.md`

## 프로젝트 구조

```text
api/                  # Vercel serverless entry
agents/               # 에이전트 정의 문서
docs/                 # API 문서
public/css/           # Tailwind 빌드 산출물
src/data/             # Repository 계층 (memory/postgres)
src/ontology/         # Ontology 서비스
src/pipeline/         # Ingestion job queue
views/                # EJS UI 템플릿
server.js             # Express app + bootstrap
vercel.json           # Vercel 라우팅/함수 설정
```

## 왜 이 프로젝트가 매력적인가?

- 실무에서 바로 부딪히는 데이터 불일치 문제를 제품 기능으로 직접 해결
- 데이터 처리 -> 에이전트 생성 -> 배치까지 한 화면 흐름으로 연결
- 로컬 개발과 서버리스 배포를 모두 고려한 구조

## 로드맵

- Sub-agent 계층 구조 저장 (`parentAgentId` 등)
- 데이터 feature 선택 기반 자동 라우팅 규칙
- 배치 플로우 실행 이력/리플레이
- 팀 협업 기능 (권한, 감사 로그)
- 배포/스케일 정책 고도화

## License

`LICENSE` 파일을 참고하세요.
