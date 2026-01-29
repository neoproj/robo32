-- Auditoria de rollback: novo registro por desfazimento (histórico preservado).
-- Executar no banco de auditoria (MariaDB) que contém a tabela AUDIT_JOB.
-- Não altera AUDIT_JOB; cada rollback gera uma linha em AUDIT_ROLLBACK.

CREATE TABLE IF NOT EXISTS AUDIT_ROLLBACK (
  id                INT          NOT NULL AUTO_INCREMENT,
  job_id            INT          NOT NULL,
  requested_by      VARCHAR(255) NULL,
  requested_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  produtos_removidos INT         NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY idx_rollback_job (job_id)
);
