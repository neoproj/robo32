import { getOracleConnection } from '../config/database.js';
import { cloneProdutoOracle } from './oracle-clone.service.js';
import { clearSuccessAntecessorFromRolledBackJobs } from './job.service.js';

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

    const antecessoresProcessadosNesteJob = new Set();

    function parseNumber(value) {
    if (value === null || value === undefined || value === '') return NaN;
    const n = Number(String(value).trim().replace(',', '.'));
    return Number.isFinite(n) ? n : NaN;
  }

    for (const [index, row] of rows.entries()) {
      const rawAntecessor = row[mapping.cd_produto_antecessor];
      const rawNovaEsp = row[mapping.cd_especie];
      const rawNovaCla = row[mapping.cd_classe];
      const rawNovaSub = row[mapping.cd_sub_cla];

      const antecessor = parseNumber(rawAntecessor, 'antecessor');
      const novaEsp = parseNumber(rawNovaEsp, 'especie');
      const novaCla = parseNumber(rawNovaCla, 'classe');
      const novaSub = parseNumber(rawNovaSub, 'subclasse');

      const progresso = `[${index + 1}/${totalRows}]`;
      console.log(`\n${progresso} Processando produto ${rawAntecessor}...`);

      try {
        if (!Number.isFinite(antecessor) || antecessor <= 0) {
          throw new Error(`Linha ${index + 1}: cd_produto_antecessor inválido (valor: "${rawAntecessor}"). Deve ser um número.`);
        }
        if (!Number.isFinite(novaEsp) || !Number.isFinite(novaCla) || !Number.isFinite(novaSub)) {
          throw new Error(`Linha ${index + 1}: espécie/classe/subclasse inválidos. Valores: esp=${rawNovaEsp}, cla=${rawNovaCla}, sub=${rawNovaSub}. Devem ser números.`);
        }

        if (antecessoresProcessadosNesteJob.has(antecessor)) {
          throw new Error(`Duplicado na mesma planilha: antecessor ${antecessor} já processado em linha anterior.`);
        }

        // Duplicidade histórica: só bloqueia se já foi processado com sucesso em um job que NÃO teve rollback
        const hist = await auditPool.execute(
          `SELECT p.job_id, j.filename
             FROM AUDIT_PRODUTO p
             INNER JOIN AUDIT_JOB j ON j.id = p.job_id
            WHERE p.success_antecessor = ?
              AND p.status = 'SUCCESS'
              AND NOT EXISTS (
                SELECT 1 FROM AUDIT_ROLLBACK r WHERE r.job_id = p.job_id
              )
            LIMIT 1`,
          [antecessor]
        );

        if (hist?.[0]) {
          const { job_id, filename } = hist[0];
          const msg = `Duplicado: já processado no JOB-${job_id} (arquivo: ${filename || '—'})`;
          throw new Error(msg);
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

        let auditOk = false;
        try {
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
          auditOk = true;
        } catch (auditErr) {
          const errMsg = String(auditErr?.message ?? '');
            const isDupKey =
              auditErr?.errno === 1062 ||
              auditErr?.code === 'ER_DUP_ENTRY' ||
              errMsg.includes('uk_audit_produto_success_antecessor') ||
              errMsg.includes("Duplicate entry") ||
              errMsg.includes('1062');
          if (isDupKey) {
            await clearSuccessAntecessorFromRolledBackJobs(auditPool, antecessor);
            try {
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
              auditOk = true;
            } catch (retryErr) {
              await registrarAuditoria(auditPool, {
                job_id: jobId,
                row: index + 1,
                antecessor,
                novo: novoCd,
                status: 'SUCCESS',
                error: 'Antecessor já utilizado em outro job (registro sem success_antecessor).',
                esp: novaEsp,
                cla: novaCla,
                sub: novaSub,
                user,
                skipSuccessAntecessor: true
              });
              auditOk = true;
            }
          }
          if (!auditOk) throw auditErr;
        }

        antecessoresProcessadosNesteJob.add(antecessor);
        console.log(`${progresso} ✓ OK -> novo produto: ${novoCd}`);
      } catch (err) {
        try {
          await oracleConn.rollback();
        } catch (_) {}

        const message = err?.message ?? 'ERROR_ORACLE';
        const isDuplicado =
          message === 'DUPLICADO_HISTORICO' ||
          message.startsWith('Duplicado:') ||
          message.startsWith('Duplicado na mesma planilha:');

        await registrarAuditoria(auditPool, {
          job_id: jobId,
          row: index + 1,
          antecessor,
          novo: null,
          status: isDuplicado ? 'DUPLICADO_HISTORICO' : 'ERROR_ORACLE',
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
    d.skipSuccessAntecessor ? null : (d.status === 'SUCCESS' ? d.antecessor : null)
  ]);
}
