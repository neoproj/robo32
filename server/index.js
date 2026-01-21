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
  getAllProcessingJobs
} from './services/job.service.js';
import { processarPlanilha } from './services/processing.service.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const jsonReplacer = (key, value) => (typeof value === 'bigint' ? value.toString() : value);
app.set('json replacer', jsonReplacer);

const upload = multer({ dest: 'uploads' });

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

  const port = Number(process.env.PORT || 3001);
  app.listen(port, () => console.log(`API rodando na porta ${port}`));
}

bootstrap().catch((err) => {
  console.error('Falha no bootstrap:', err);
  process.exit(1);
});
