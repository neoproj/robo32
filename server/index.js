import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';
import oracledb from 'oracledb';

import { assertEnv, createMariaPool, createOraclePool, initOracle } from './config/database.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

const REQUIRED_ENV = [
  'DB_AUDIT_HOST',
  'DB_AUDIT_USER',
  'DB_AUDIT_PASS',
  'DB_AUDIT_NAME',
  'ORA_USER',
  'ORA_PASS',
  'ORA_CONN_STR'
];

const IDENTIFIER_REGEX = /^[A-Za-z0-9_]+$/;

function safeIdentifier(value, label) {
  if (!value || !IDENTIFIER_REGEX.test(value)) {
    throw new Error(`Identificador invalido para ${label}.`);
  }

  return value;
}

async function markValidationError(mariaPool, auditId) {
  const table = safeIdentifier(process.env.DB_AUDIT_TABLE || 'audit_rows', 'DB_AUDIT_TABLE');
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

  try {
    const oraclePool = app.locals.oraclePool;
    const mariaPool = app.locals.mariaPool;
    const oracleConn = await oraclePool.getConnection();
    const result = await oracleConn.execute(
      `SELECT COUNT(*) AS CNT
       FROM SUB_CLAS
       WHERE CD_ESPECIE = :esp
         AND CD_CLASSE = :cla
         AND CD_SUB_CLA = :sub`,
      { esp, cla, sub }
    );
    await oracleConn.close();

    const count = Number(result.rows?.[0]?.CNT || 0);

    if (count === 0) {
      await markValidationError(mariaPool, auditId);
      return res.json({ valid: false, status: 'ERROR_VALIDACAO' });
    }

    return res.json({ valid: true });
  } catch (err) {
    console.error('Erro ao validar subclasse:', err);
    return res.status(500).json({ error: 'Falha ao validar subclasse.' });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

async function startServer() {
  try {
    assertEnv(REQUIRED_ENV);
    initOracle();
    app.locals.oraclePool = await createOraclePool();
    app.locals.mariaPool = createMariaPool();

    const port = Number(process.env.PORT || 3000);
    app.listen(port, () => {
      console.log(`API rodando na porta ${port}`);
    });
  } catch (err) {
    console.error('Erro ao iniciar API:', err);
    process.exit(1);
  }
}

startServer();
