-- ============================================================
-- Corrige success_antecessor de jobs que tiveram rollback
-- ANTES da melhoria que limpa automaticamente no desfazer.
--
-- Execute no MariaDB (banco de auditoria).
-- Use quando ainda aparecer "Duplicate entry" para uk_audit_produto_success_antecessor
-- após processar um antecessor que só existia em job desfeito (ex.: job 57).
-- ============================================================

-- Opção 1: Apenas o JOB 57
UPDATE AUDIT_PRODUTO
   SET success_antecessor = NULL
 WHERE job_id = 57
   AND status = 'SUCCESS';

-- Opção 2: TODOS os jobs que têm registro em AUDIT_ROLLBACK (recomendado)
-- Descomente e execute se quiser corrigir todos de uma vez:
/*
UPDATE AUDIT_PRODUTO p
  INNER JOIN AUDIT_ROLLBACK r ON r.job_id = p.job_id
  SET p.success_antecessor = NULL
WHERE p.status = 'SUCCESS';
*/

-- Conferir: linhas do job 57 não devem mais ter success_antecessor preenchido
-- SELECT job_id, row_number, cd_produto_antecessor, success_antecessor, status
--   FROM AUDIT_PRODUTO WHERE job_id = 57 LIMIT 10;
