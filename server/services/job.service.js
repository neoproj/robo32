export async function getActiveJob(auditPool) {
  const rows = await auditPool.execute(
    `SELECT id, filename, uploaded_by, total_rows, status, created_at, finished_at
     FROM AUDIT_JOB
     WHERE status = 'PROCESSING'
     ORDER BY created_at DESC
     LIMIT 1`
  );
  return rows?.[0] || null;
}

export async function getRecentJobs(auditPool, limit = 5) {
  const rows = await auditPool.execute(
    `SELECT id, filename, uploaded_by, total_rows, status, created_at, finished_at
     FROM AUDIT_JOB
     ORDER BY created_at DESC
     LIMIT ?`,
    [limit]
  );
  return rows || [];
}

export async function createJob(auditPool, { filename, uploadedBy, totalRows }) {
  const result = await auditPool.execute(
    `INSERT INTO AUDIT_JOB (filename, uploaded_by, total_rows, status)
     VALUES (?, ?, ?, 'PROCESSING')`,
    [filename, uploadedBy, totalRows]
  );
  return Number(result?.insertId);
}

export async function updateJobStatus(auditPool, jobId, status) {
  await auditPool.execute(
    `UPDATE AUDIT_JOB
     SET status = ?, finished_at = CASE WHEN ? = 'COMPLETED' OR ? = 'FAILED' THEN NOW() ELSE finished_at END
     WHERE id = ?`,
    [status, status, status, jobId]
  );
}

export async function getJobSummary(auditPool, jobId) {
  const rows = await auditPool.execute(
    `SELECT id, total_rows
     FROM AUDIT_JOB
     WHERE id = ?`,
    [jobId]
  );
  const job = rows?.[0];

  if (!job) {
    return null;
  }

  const countsRows = await auditPool.execute(
    `SELECT
        COUNT(*) AS processed,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success,
        SUM(CASE WHEN status <> 'SUCCESS' THEN 1 ELSE 0 END) AS errors
     FROM AUDIT_PRODUTO
     WHERE job_id = ?`,
    [jobId]
  );
  const counts = countsRows?.[0] || {};

  const processed = Number(counts?.processed || 0);
  const total = Number(job.total_rows || 0);
  const remaining = Math.max(total - processed, 0);

  return {
    total,
    processed,
    success: Number(counts?.success || 0),
    errors: Number(counts?.errors || 0),
    remaining
  };
}

export async function getJobErrors(auditPool, jobId) {
  const rows = await auditPool.execute(
    `SELECT row_number, cd_produto_antecessor, status, error_message, cd_especie_nova, cd_classe_nova, cd_sub_cla_nova
     FROM AUDIT_PRODUTO
     WHERE job_id = ? AND status <> 'SUCCESS'
     ORDER BY row_number ASC`,
    [jobId]
  );

  return rows || [];
}

export async function cancelJob(auditPool, jobId) {
  await auditPool.execute(
    `UPDATE AUDIT_JOB
     SET status = 'FAILED', finished_at = NOW()
     WHERE id = ? AND status = 'PROCESSING'`,
    [jobId]
  );
}

export async function getAllProcessingJobs(auditPool) {
  const rows = await auditPool.execute(
    `SELECT id, filename, uploaded_by, total_rows, status, created_at
     FROM AUDIT_JOB
     WHERE status = 'PROCESSING'
     ORDER BY created_at DESC`
  );
  return rows || [];
}
