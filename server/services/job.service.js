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

/**
 * Jobs que podem ser desfeitos: finalizados (COMPLETED ou FAILED) e que
 * tenham pelo menos um produto criado com sucesso (cd_produto_novo preenchido).
 */
export async function getJobsEligibleForRollback(auditPool, limit = 50) {
  const rows = await auditPool.execute(
    `SELECT j.id, j.filename, j.uploaded_by, j.total_rows, j.status, j.created_at, j.finished_at,
            COUNT(p.cd_produto_novo) AS produtos_criados
       FROM AUDIT_JOB j
       INNER JOIN AUDIT_PRODUTO p ON p.job_id = j.id
         AND p.status = 'SUCCESS'
         AND p.cd_produto_novo IS NOT NULL
      WHERE j.status IN ('COMPLETED', 'FAILED')
      GROUP BY j.id, j.filename, j.uploaded_by, j.total_rows, j.status, j.created_at, j.finished_at
      ORDER BY j.created_at DESC
      LIMIT ?`,
    [limit]
  );
  return rows || [];
}

/**
 * Retorna os CD_PRODUTO_NOVO (produtos criados pelo job) para rollback.
 * Rollback deve atuar apenas nos novos produtos, nunca no cd_produto_antecessor.
 */
export async function getJobNewProductIds(auditPool, jobId) {
  const rows = await auditPool.execute(
    `SELECT cd_produto_novo
       FROM AUDIT_PRODUTO
      WHERE job_id = ? AND status = 'SUCCESS' AND cd_produto_novo IS NOT NULL
      ORDER BY row_number ASC`,
    [jobId]
  );
  return (rows || []).map((r) => Number(r.cd_produto_novo)).filter(Number.isFinite);
}

/**
 * Registra na auditoria um novo registro de rollback (mantém histórico).
 * Insere em AUDIT_ROLLBACK: job_id, solicitante e quando.
 */
export async function insertRollbackAudit(auditPool, { jobId, requestedBy, produtosRemovidos }) {
  await auditPool.execute(
    `INSERT INTO AUDIT_ROLLBACK (job_id, requested_by, requested_at, produtos_removidos)
     VALUES (?, ?, NOW(), ?)`,
    [jobId, requestedBy || null, produtosRemovidos ?? 0]
  );
}

/**
 * Libera success_antecessor do job que teve rollback, para não bloquear
 * reprocessamento do mesmo antecessor (evita violar uk_audit_produto_success_antecessor).
 */
export async function clearSuccessAntecessorForJob(auditPool, jobId) {
  await auditPool.execute(
    `UPDATE AUDIT_PRODUTO SET success_antecessor = NULL WHERE job_id = ? AND status = 'SUCCESS'`,
    [jobId]
  );
}

/**
 * Libera success_antecessor de linhas que pertencem a jobs com rollback.
 * Usado quando o INSERT falha por duplicate key (antecessor já existe em job desfeito).
 */
export async function clearSuccessAntecessorFromRolledBackJobs(auditPool, antecessor) {
  await auditPool.execute(
    `UPDATE AUDIT_PRODUTO p
       INNER JOIN AUDIT_ROLLBACK r ON r.job_id = p.job_id
       SET p.success_antecessor = NULL
     WHERE p.success_antecessor = ?`,
    [antecessor]
  );
}
