# Agent Set for kairo Agent

This directory contains reusable role-specific agent prompts.

## Included Agents

- `frontend-design-agent.md`
- `frontend-architecture-agent.md`
- `backend-engineer-agent.md`
- `ceo-agent.md`
- `cto-agent.md`
- `marketer-agent.md`
- `nodejs-optimization-agent.md`

## How to Use

1. Pick one role file based on the decision you need.
2. Paste the full file content as the system prompt for that role.
3. Provide a compact task brief:
   - product context
   - constraints
   - timeline
   - success metric
4. Run roles in parallel for independent outputs, then merge.

## Suggested Collaboration Order

1. `ceo-agent.md` for business priority and scope.
2. `cto-agent.md` for technical direction and tradeoffs.
3. `frontend-architecture-agent.md` and `backend-engineer-agent.md` for system design.
4. `frontend-design-agent.md` for UX and visual details.
5. `nodejs-optimization-agent.md` for performance tuning.
6. `marketer-agent.md` for launch and growth execution.

## File Index

See `registry.json` for machine-readable names and paths.
