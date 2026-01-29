-- ============================================================
-- Verificar se o rollback do job foi aplicado
-- Execute no MariaDB (banco de auditoria).
-- Substitua 57 pelo ID do job que você desfez.
-- ============================================================

SET @job_id = 57;

-- 1) Registro de rollback (deve existir se você clicou em "Confirmar desfazer")
SELECT
  id,
  job_id,
  requested_by    AS solicitante,
  requested_at    AS quando,
  produtos_removidos
FROM AUDIT_ROLLBACK
WHERE job_id = @job_id
ORDER BY requested_at DESC;

-- 2) Produtos que o job 57 tinha criado (lista dos cd_produto_novo que foram removidos do Oracle)
SELECT
  row_number,
  cd_produto_antecessor,
  cd_produto_novo,
  status
FROM AUDIT_PRODUTO
WHERE job_id = @job_id
  AND status = 'SUCCESS'
  AND cd_produto_novo IS NOT NULL
ORDER BY row_number;

-- 3) Resumo: quantos produtos o job criou x quantos o rollback disse que removeu
SELECT
  (SELECT COUNT(*) FROM AUDIT_PRODUTO WHERE job_id = @job_id AND status = 'SUCCESS' AND cd_produto_novo IS NOT NULL) AS produtos_criados_pelo_job,
  (SELECT COALESCE(SUM(produtos_removidos), 0) FROM AUDIT_ROLLBACK WHERE job_id = @job_id) AS produtos_removidos_no_rollback;

-- ============================================================
-- No Oracle: conferir se os produtos foram removidos
-- Use os cd_produto_novo da query (2) acima. Exemplo para job 57:
--   SELECT COUNT(*) FROM dbamv.PRODUTO WHERE cd_produto IN (id1, id2, ...);
-- Se retornar 0, o rollback foi aplicado no Oracle.
-- ============================================================
