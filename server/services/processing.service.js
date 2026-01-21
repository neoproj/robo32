import { getOracleConnection } from '../config/database.js';
import { cloneProdutoOracle } from './oracle-clone.service.js';

export async function processarPlanilha({ jobId, rows, mapping, user, oraclePool, auditPool }) {
  let oracleConn;

  try {
    // Obtém conexão do pool (sessionCallback já garante empresa 4)
    try {
      oracleConn = await getOracleConnection(oraclePool);
    } catch (err) {
      await registrarFalhaConexao(auditPool, {
        jobId,
        rows,
        mapping,
        user,
        error: err?.message ?? 'ERROR_ORACLE_CONNECTION'
      });
      throw err;
    }

    const totalRows = rows.length;
    console.log(`\n[PROCESSAMENTO] Iniciando processamento de ${totalRows} produto(s)...\n`);

    for (const [index, row] of rows.entries()) {
      const antecessor = row[mapping.cd_produto_antecessor];
      const novaEsp = row[mapping.cd_especie];
      const novaCla = row[mapping.cd_classe];
      const novaSub = row[mapping.cd_sub_cla];

      const progresso = `[${index + 1}/${totalRows}]`;
      console.log(`\n${progresso} Processando produto ${antecessor}...`);

      try {
        // Duplicidade histórica (auditoria)
        const hist = await auditPool.execute(
          `SELECT id
             FROM AUDIT_PRODUTO
            WHERE success_antecessor = ?
              AND status = 'SUCCESS'
            LIMIT 1`,
          [antecessor]
        );

        if (hist?.[0]) {
          throw new Error('DUPLICADO_HISTORICO');
        }

        // Clone (sem commits internos)
        const novoCd = await cloneProdutoOracle(oracleConn, {
          cdProdutoAntecessor: antecessor,
          cdEspecieNova: novaEsp,
          cdClasseNova: novaCla,
          cdSubClaNova: novaSub,
          somenteEmpresa4: true
        });

        await oracleConn.commit();

        await registrarAuditoria(auditPool, {
          job_id: jobId,
          row: index + 1,
          antecessor,
          novo: novoCd,
          status: 'SUCCESS',
          error: null,
          esp: novaEsp,
          cla: novaCla,
          sub: novaSub,
          user
        });

        console.log(`${progresso} ✓ OK -> novo produto: ${novoCd}`);
      } catch (err) {
        try {
          await oracleConn.rollback();
        } catch (_) {}

        const message = err?.message ?? 'ERROR_ORACLE';

        await registrarAuditoria(auditPool, {
          job_id: jobId,
          row: index + 1,
          antecessor,
          novo: null,
          status: message === 'DUPLICADO_HISTORICO' ? 'DUPLICADO_HISTORICO' : 'ERROR_ORACLE',
          error: message,
          esp: novaEsp,
          cla: novaCla,
          sub: novaSub,
          user
        });

        console.error(`${progresso} ✗ ERRO produto ${antecessor}: ${message}`);
      }
    }
  } finally {
    if (oracleConn) {
      try {
        await oracleConn.close();
      } catch (_) {}
    }
  }
}

function toNumberOrZero(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

async function registrarFalhaConexao(auditPool, { jobId, rows, mapping, user, error }) {
  for (const [index, row] of rows.entries()) {
    await registrarAuditoria(auditPool, {
      job_id: jobId,
      row: index + 1,
      antecessor: toNumberOrZero(row[mapping.cd_produto_antecessor]),
      novo: null,
      status: 'ERROR_ORACLE',
      error,
      esp: toNumberOrZero(row[mapping.cd_especie]),
      cla: toNumberOrZero(row[mapping.cd_classe]),
      sub: toNumberOrZero(row[mapping.cd_sub_cla]),
      user
    });
  }
}

async function registrarAuditoria(auditPool, d) {
  const sql = `INSERT INTO AUDIT_PRODUTO
    (job_id, row_number, cd_produto_antecessor, cd_produto_novo, status, error_message,
     cd_especie_nova, cd_classe_nova, cd_sub_cla_nova, success_antecessor, executed_at,
     created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), NOW())`;

  await auditPool.execute(sql, [
    d.job_id,
    d.row,
    d.antecessor,
    d.novo || null,
    d.status,
    d.error || null,
    d.esp,
    d.cla,
    d.sub,
    d.status === 'SUCCESS' ? d.antecessor : null
  ]);
}
