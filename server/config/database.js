import oracledb from 'oracledb';
import mariadb from 'mariadb';

const DEFAULT_ORA_LIB_DIR = 'C:\\oracle\\instantclient_19_8';

export function initOracle() {
  try {
    oracledb.initOracleClient({
      libDir: process.env.ORA_LIB_DIR || DEFAULT_ORA_LIB_DIR
    });
  } catch (err) {
    console.error('Erro ao iniciar Oracle Client:', err);
    process.exit(1);
  }
}

export async function createOraclePool() {
  return oracledb.createPool({
    user: process.env.ORA_USER,
    password: process.env.ORA_PASS,
    connectString: process.env.ORA_CONN_STR,
    poolMin: 1,
    poolMax: 5,
    poolIncrement: 1
  });
}

export function createMariaPool() {
  return mariadb.createPool({
    host: process.env.DB_AUDIT_HOST,
    user: process.env.DB_AUDIT_USER,
    password: process.env.DB_AUDIT_PASS,
    database: process.env.DB_AUDIT_NAME,
    connectionLimit: 5
  });
}

export function assertEnv(vars) {
  const missing = vars.filter((key) => !process.env[key]);
  if (missing.length) {
    throw new Error(`Variaveis ausentes no .env: ${missing.join(', ')}`);
  }
}
