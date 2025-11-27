#!/bin/bash

curl -s -X PUT "http://127.0.0.1:8080/api/interpreter/setting/${FLINK_INTERPRETER_ID}" \
  -H "Content-Type: application/json" \
  --data @- <<'EOF'
{
  "name": "flink",
  "group": "flink",
  "properties": {
    "flink.execution.mode": { "name": "flink.execution.mode", "value": "remote", "type": "string" },
    "flink.webui.address": { "name": "flink.webui.address", "value": "jobmanager:8081", "type": "string" },
    "flink.execution.remote.host": { "name": "flink.execution.remote.host", "value": "jobmanager", "type": "string" },
    "flink.execution.remote.port": { "name": "flink.execution.remote.port", "value": "6123", "type": "string" },
    "flink.execution.uri": { "name": "flink.execution.uri", "value": "jobmanager:8081", "type": "string" },
    "flink.sql.gateway.url": { "name": "flink.sql.gateway.url", "value": "http://sql-gateway:8083", "type": "string" }
  }
}
EOF

echo "Attempted to update Flink interpreter settings (ID: flink)."

exit 0