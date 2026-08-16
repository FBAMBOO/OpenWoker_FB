-- Blob downloads are addressed by digest; resolve authorization metadata without
-- scanning every task and then every evidence row.
CREATE INDEX orch_evidence_blob_lookup
ON orch_evidence(content_hash, blob_uri);
