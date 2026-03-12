#!/usr/bin/env bash
RUN_ID="$1"
TOKEN=$(cat /tmp/cognus_token.txt)
for i in $(seq 1 360); do
  OP=$(curl -sS -X POST http://localhost:8080/api/v1/runbooks/operate -H "Authorization: c $TOKEN" -H 'Content-Type: application/json' -d "{\"run_id\":\"$RUN_ID\",\"action\":\"advance\"}")
  ST=$(echo "$OP" | jq -r '.data.status // .data.run.status // empty')
  STAGE=$(echo "$OP" | jq -r '.data.current_stage // .data.run.current_stage // empty')
  CKPT=$(echo "$OP" | jq -r '.data.current_checkpoint // .data.run.current_checkpoint // empty')
  if [ -z "$ST" ]; then
    ST_JSON=$(curl -sS -H "Authorization: c $TOKEN" "http://localhost:8080/api/v1/runbooks/$RUN_ID/status")
    ST=$(echo "$ST_JSON" | jq -r '.data.status // .data.run.status // empty')
    STAGE=$(echo "$ST_JSON" | jq -r '.data.current_stage // .data.run.current_stage // empty')
    CKPT=$(echo "$ST_JSON" | jq -r '.data.current_checkpoint // .data.run.current_checkpoint // empty')
  fi
  echo "tick=$i status=$ST stage=$STAGE checkpoint=$CKPT"
  if [ "$ST" = "completed" ] || [ "$ST" = "failed" ]; then
    break
  fi
  sleep 3
done
curl -sS -H "Authorization: c $TOKEN" "http://localhost:8080/api/v1/runbooks/$RUN_ID/status" >/tmp/runbook_status_latest_patch.json
jq '{run_id:(.data.run_id // .data.run.run_id),status:(.data.status // .data.run.status),current_stage:(.data.current_stage // .data.run.current_stage),current_checkpoint:(.data.current_checkpoint // .data.run.current_checkpoint),failed_steps:(.data.failed_steps // .data.run.failed_steps)}' /tmp/runbook_status_latest_patch.json
