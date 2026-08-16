-- Operational recovery must not scan every archived task/run on each scheduler tick.
-- SQLite's JSON functions are deterministic and may participate in a partial index.
CREATE INDEX orch_runs_pending_workspace_commit
ON orch_runs(status, id)
WHERE status = 'succeeded'
  AND json_extract(output_json, '$.workspace_commit.status') = 'pending';

CREATE INDEX orch_tasks_parent_status
ON orch_tasks(parent_task_id, status, created_at);
