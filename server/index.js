import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import multer from 'multer';
import oracledb from 'oracledb';
import xlsx from 'xlsx';
import { unlink } from 'node:fs/promises';

import {
  assertEnv,
  createMariaPool,
  createOraclePool,
  getOracleConnection,
  initOracle
} from './config/database.js';
import {
  getActiveJob,
  getJobErrors,
  getJobSummary,
  getRecentJobs,
  createJob,
  updateJobStatus,
  cancelJob,
  getAllProcessingJobs,
  getJobsEligibleForRollback,
  getJobNewProductIds,
  insertRollbackAudit,
  clearSuccessAntecessorForJob
} from './services/job.service.js';
import { processarPlanilha } from './services/processing.service.js';
import { executeRollbackOracle } from './services/rollback.service.js';
import { getConfigForEdit, updateEnvFile } from './services/config.service.js';
import { validateLayout } from './services/upload-validation.service.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const jsonReplacer = (key, value) => (typeof value === 'bigint' ? value.toString() : value);
app.set('json replacer', jsonReplacer);

const upload = multer({ dest: 'uploads' });

// Rota de saúde (para confirmar que é este servidor e que a API está ativa)
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, config: true });
});

// reforço (não atrapalha)
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

const REQUIRED_ENV = [
  'DB_AUDIT_HOST',
  'DB_AUDIT_USER',
  'DB_AUDIT_NAME',
  'ORA_USER',
  'ORA_PASS',
  'ORA_CONN_STR'
];
// DB_AUDIT_PASS é opcional (permite senha vazia para XAMPP)

const IDENTIFIER_REGEX = /^[A-Za-z0-9_]+$/;
const REQUIRED_MAPPING_FIELDS = ['cd_produto_antecessor', 'cd_especie', 'cd_classe', 'cd_sub_cla'];

function safeIdentifier(value, label) {
  if (!value || !IDENTIFIER_REGEX.test(value)) {
    throw new Error(`Identificador invalido para ${label}.`);
  }
  return value;
}

function jsonSafe(res, data) {
  return res.json(JSON.parse(JSON.stringify(data, jsonReplacer)));
}

function normalizeMapping(mapping) {
  if (!mapping) return null;

  const hasFieldKeys = REQUIRED_MAPPING_FIELDS.every((field) =>
    Object.prototype.hasOwnProperty.call(mapping, field)
  );
  if (hasFieldKeys) return mapping;

  const inverted = {};
  for (const [column, field] of Object.entries(mapping)) {
    if (field) inverted[field] = column;
  }
  return inverted;
}

async function markValidationError(mariaPool, auditId) {
  const table = safeIdentifier(process.env.DB_AUDIT_TABLE || 'AUDIT_PRODUTO', 'DB_AUDIT_TABLE');
  const idColumn = safeIdentifier(process.env.DB_AUDIT_ID_COLUMN || 'id', 'DB_AUDIT_ID_COLUMN');
  const statusColumn = safeIdentifier(
    process.env.DB_AUDIT_STATUS_COLUMN || 'status',
    'DB_AUDIT_STATUS_COLUMN'
  );

  const sql = `UPDATE ${table} SET ${statusColumn} = ? WHERE ${idColumn} = ?`;
  await mariaPool.execute(sql, ['ERROR_VALIDACAO', auditId]);
}

app.post('/api/validate-subclas', async (req, res) => {
  const { esp, cla, sub, auditId } = req.body || {};

  if (!esp || !cla || !sub || !auditId) {
    return res.status(400).json({ error: 'esp, cla, sub e auditId sao obrigatorios.' });
  }

  let oracleConn;
  try {
    const oraclePool = app.locals.oraclePool;
    const mariaPool = app.locals.mariaPool;

    oracleConn = await getOracleConnection(oraclePool);

    const result = await oracleConn.execute(
      `SELECT COUNT(*) AS CNT
         FROM dbamv.sub_clas
        WHERE CD_ESPECIE = :esp
          AND CD_CLASSE  = :cla
          AND CD_SUB_CLA = :sub`,
      { esp, cla, sub }
    );

    const count = Number(result.rows?.[0]?.CNT || 0);

    if (count === 0) {
      await markValidationError(mariaPool, auditId);
      return jsonSafe(res, { valid: false, status: 'ERROR_VALIDACAO' });
    }

    return jsonSafe(res, { valid: true });
  } catch (err) {
    console.error('Erro ao validar subclasse:', err);
    return res.status(500).json({ error: 'Falha ao validar subclasse.' });
  } finally {
    if (oracleConn) {
      try {
        await oracleConn.close();
      } catch (_) {}
    }
  }
});

app.get('/api/jobs/active', async (req, res) => {
  try {
    const mariaPool = app.locals.mariaPool;
    const recentLimit = Number(req.query.recentLimit || 5);
    const limit = Number.isFinite(recentLimit) && recentLimit > 0 ? recentLimit : 5;

    const [activeJob, recentJobs] = await Promise.all([
      getActiveJob(mariaPool),
      getRecentJobs(mariaPool, limit)
    ]);

    return jsonSafe(res, { activeJob, recentJobs });
  } catch (err) {
    console.error('Erro ao buscar jobs:', err);
    return res.status(500).json({ error: 'Falha ao buscar jobs.' });
  }
});

app.get('/api/jobs/:jobId/summary', async (req, res) => {
  try {
    const mariaPool = app.locals.mariaPool;
    const jobId = Number(req.params.jobId);
    if (!Number.isFinite(jobId) || jobId <= 0) {
      return res.status(400).json({ error: 'jobId invalido.' });
    }

    const summary = await getJobSummary(mariaPool, jobId);
    if (!summary) {
      return res.status(404).json({ error: 'Job nao encontrado.' });
    }
    return jsonSafe(res, summary);
  } catch (err) {
    console.error('Erro ao buscar resumo do job:', err);
    return res.status(500).json({ error: 'Falha ao buscar resumo do job.' });
  }
});

app.get('/api/jobs/:jobId/details', async (req, res) => {
  try {
    const mariaPool = app.locals.mariaPool;
    const jobId = Number(req.params.jobId);
    if (!Number.isFinite(jobId) || jobId <= 0) {
      return res.status(400).json({ error: 'jobId invalido.' });
    }

    const rows = await getJobErrors(mariaPool, jobId);
    return jsonSafe(res, { rows });
  } catch (err) {
    console.error('Erro ao buscar detalhes do job:', err);
    return res.status(500).json({ error: 'Falha ao buscar detalhes do job.' });
  }
});

function requireConfigPassword(req, res) {
  const expected = process.env.CONFIG_PASSWORD;
  if (!expected || expected === '') {
    return res.status(503).json({
      error: 'Configuração desabilitada. Defina CONFIG_PASSWORD no arquivo .env.'
    });
  }
  const password = req.body?.password ?? req.headers['x-config-password'] ?? '';
  if (password !== expected) {
    return res.status(401).json({ error: 'Senha incorreta.' });
  }
  return null;
}

/** Verifica senha de administrador para acessar a aba de configuração. */
app.post('/api/config/verify', (req, res) => {
  const err = requireConfigPassword(req, res);
  if (err) return err;
  return jsonSafe(res, { ok: true });
});

/** Retorna configuração atual (valores sensíveis mascarados). Requer senha no body. */
app.post('/api/config', (req, res) => {
  const err = requireConfigPassword(req, res);
  if (err) return err;
  const config = getConfigForEdit();
  return jsonSafe(res, { config });
});

/** Atualiza .env com os valores enviados. Requer senha no body. */
app.put('/api/config', (req, res) => {
  const err = requireConfigPassword(req, res);
  if (err) return err;
  const config = req.body?.config;
  if (!config || typeof config !== 'object') {
    return res.status(400).json({ error: 'Envie { config: { ... } }.' });
  }
  try {
    updateEnvFile(config);
    return jsonSafe(res, { ok: true, message: 'Arquivo .env atualizado. Reinicie o servidor para aplicar.' });
  } catch (e) {
    console.error('Erro ao atualizar .env:', e);
    return res.status(500).json({ error: e?.message ?? 'Falha ao gravar .env.' });
  }
});

/** Lista jobs que podem ser desfeitos (têm produtos criados com sucesso). */
app.get('/api/jobs/rollback-candidates', async (req, res) => {
  try {
    const mariaPool = app.locals.mariaPool;
    const limit = Math.min(Number(req.query.limit) || 50, 100);
    const jobs = await getJobsEligibleForRollback(mariaPool, limit);
    return jsonSafe(res, { jobs });
  } catch (err) {
    console.error('Erro ao buscar jobs para rollback:', err);
    return res.status(500).json({ error: 'Falha ao buscar jobs para desfazer.' });
  }
});

/** Desfaz um job: remove no Oracle EMPRESA_PRODUTO, UNI_PRO e PRODUTO dos produtos criados. */
app.post('/api/jobs/:jobId/rollback', async (req, res) => {
  const jobId = Number(req.params.jobId);
  if (!Number.isFinite(jobId) || jobId <= 0) {
    return res.status(400).json({ error: 'jobId invalido.' });
  }

  const requestedBy = req.body?.requestedBy ?? null;

  let oracleConn;
  try {
    const mariaPool = app.locals.mariaPool;
    const oraclePool = app.locals.oraclePool;

    const productIds = await getJobNewProductIds(mariaPool, jobId);
    if (productIds.length === 0) {
      return res.status(400).json({
        error: 'Nenhum produto criado com sucesso neste job para desfazer.'
      });
    }

    oracleConn = await getOracleConnection(oraclePool);
    const result = await executeRollbackOracle(oracleConn, productIds);
    await oracleConn.commit();

    let auditRollbackOk = true;
    try {
      await insertRollbackAudit(mariaPool, {
        jobId,
        requestedBy,
        produtosRemovidos: productIds.length
      });
      await clearSuccessAntecessorForJob(mariaPool, jobId);
    } catch (auditErr) {
      console.error('Auditoria de rollback (AUDIT_ROLLBACK):', auditErr?.message);
      auditRollbackOk = false;
    }

    return jsonSafe(res, {
      ok: true,
      jobId,
      produtosRemovidos: productIds.length,
      registrosDeletados: result.deleted,
      erros: result.errors,
      auditRollbackRegistrado: auditRollbackOk
    });
  } catch (err) {
    if (oracleConn) {
      try {
        await oracleConn.rollback();
      } catch (_) {}
    }
    console.error('Erro ao desfazer job:', err);
    return res.status(500).json({
      error: err?.message ?? 'Falha ao executar rollback no Oracle.'
    });
  } finally {
    if (oracleConn) {
      try {
        await oracleConn.close();
      } catch (_) {}
    }
  }
});

function parseSpreadsheet(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  return xlsx.utils.sheet_to_json(sheet, { defval: null });
}

app.post('/api/jobs/upload', upload.single('file'), async (req, res) => {
  const { mapping, uploadedBy } = req.body || {};
  const file = req.file;

  if (!file) {
    return res.status(400).json({ error: 'Arquivo nao enviado.' });
  }

  try {
    const mariaPool = app.locals.mariaPool;
    const oraclePool = app.locals.oraclePool;

    const activeJob = await getActiveJob(mariaPool);
    if (activeJob) {
      await unlink(file.path);
      return res.status(409).json({ error: 'Ja existe um job em processamento.' });
    }

    let parsedMapping = null;
    try {
      parsedMapping = mapping ? JSON.parse(mapping) : null;
    } catch (err) {
      await unlink(file.path);
      return res.status(400).json({ error: 'Mapeamento em formato invalido.' });
    }

    if (!parsedMapping) {
      await unlink(file.path);
      return res.status(400).json({ error: 'Mapeamento nao informado.' });
    }

    const normalizedMapping = normalizeMapping(parsedMapping);
    if (!normalizedMapping) {
      await unlink(file.path);
      return res.status(400).json({ error: 'Mapeamento nao informado.' });
    }

    const missing = REQUIRED_MAPPING_FIELDS.filter((field) => !normalizedMapping[field]);
    if (missing.length) {
      await unlink(file.path);
      return res.status(400).json({
        error: `Mapeamento incompleto. Campos obrigatorios: ${missing.join(', ')}`
      });
    }

    const rows = parseSpreadsheet(file.path);
    if (!rows.length) {
      await unlink(file.path);
      return res.status(400).json({ error: 'Planilha sem dados.' });
    }

    const layoutResult = validateLayout(rows, normalizedMapping);
    if (!layoutResult.valid) {
      await unlink(file.path);
      return res.status(400).json({
        error: 'Layout da planilha inválido.',
        validationErrors: layoutResult.errors
      });
    }

    const jobId = await createJob(mariaPool, {
      filename: file.originalname,
      uploadedBy: uploadedBy || 'desconhecido',
      totalRows: rows.length
    });

    jsonSafe(res.status(202), { jobId });

    setImmediate(async () => {
      try {
        await processarPlanilha({
          jobId,
          rows,
          mapping: normalizedMapping,
          user: uploadedBy || 'desconhecido',
          oraclePool,
          auditPool: mariaPool
        });
        await updateJobStatus(mariaPool, jobId, 'COMPLETED');
      } catch (err) {
        console.error('Erro ao processar job:', err);
        await updateJobStatus(mariaPool, jobId, 'FAILED');
      } finally {
        await unlink(file.path);
      }
    });
  } catch (err) {
    console.error('Erro no upload:', err);
    if (file?.path) {
      await unlink(file.path);
    }
    return res.status(500).json({ error: 'Falha ao iniciar processamento.' });
  }
});

/**
 * Boot da aplicação
 */
async function bootstrap() {
  assertEnv(REQUIRED_ENV);

  initOracle();

  app.locals.mariaPool = createMariaPool();
  app.locals.oraclePool = await createOraclePool();

  const portBase = Number(process.env.PORT || 3001);
  const portMax = portBase + 10;

  function tryListen(port) {
    if (port > portMax) {
      console.error(`Nenhuma porta livre entre ${portBase} e ${portMax}. Libere uma porta ou altere PORT no .env`);
      process.exit(1);
    }
    const server = app.listen(port, () => {
      console.log(`API rodando na porta ${port}`);
      console.log(`  Rotas ativas: GET /api/health, POST /api/config/verify, etc.`);
      if (port !== portBase) {
        console.log(`  (Porta ${portBase} estava ocupada.) Se o frontend não conectar, defina VITE_API_BASE=http://localhost:${port} no .env`);
      }
    });
    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        console.log(`Porta ${port} ocupada, tentando ${port + 1}...`);
        tryListen(port + 1);
      } else {
        console.error(err);
        process.exit(1);
      }
    });
  }

  tryListen(portBase);
}

bootstrap().catch((err) => {
  console.error('Falha no bootstrap:', err);
  process.exit(1);
});
