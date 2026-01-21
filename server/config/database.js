import oracledb from 'oracledb';
import mariadb from 'mariadb';

const DEFAULT_ORA_LIB_DIR = 'C:\\oracle\\instantclient_19_8';
const EMPRESA_FIXA = 4;

// Saída padrão em objeto
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.fetchArraySize = 200;

export function assertEnv(vars) {
  const missing = vars.filter((key) => process.env[key] === undefined || process.env[key] === '');
  if (missing.length) {
    throw new Error(`Variaveis ausentes no .env: ${missing.join(', ')}`);
  }
}

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

/**
 * Força contexto MV na sessão do pool:
 * - set_empresa(4)
 * - pkt_configest.inicializa
 * - valida le_empresa = 4
 * - valida existência de configest para empresa 4
 */
async function prepararSessaoMV(conn, cdEmpresa = EMPRESA_FIXA) {
  if (process.env.ORA_SKIP_SET_EMPRESA === '1') {
    console.warn(
      '[Oracle] ORA_SKIP_SET_EMPRESA=1 ativo: pulando set_empresa/configest (modo teste).'
    );
    return;
  }
  // Diagnóstico: empresa precisa existir no CONFIGEST
  const cfg = await conn.execute(
    `SELECT COUNT(*) AS QTD
       FROM dbamv.configest
      WHERE cd_multi_empresa = :emp`,
    { emp: cdEmpresa }
  );

  const qtd = Number(cfg.rows?.[0]?.QTD ?? 0);
  if (qtd < 1) {
    throw new Error(
      `Empresa ${cdEmpresa} sem registro em DBAMV.CONFIGEST (necessário para PKT_CONFIGEST/TRG_PRODUTO).`
    );
  }

  // Rotas comuns para setar empresa MV
  const setters = [
    { name: 'DBAMV.PKG_MV_CONFIG', sql: `BEGIN dbamv.pkg_mv_config.set_empresa(:emp); END;` },
    { name: 'DBAMV.MS_SET_CONFIG', sql: `BEGIN dbamv.ms_set_config.set_empresa(:emp); END;` },
    { name: 'PKG_MV_CONFIG', sql: `BEGIN pkg_mv_config.set_empresa(:emp); END;` },
    { name: 'MS_SET_CONFIG', sql: `BEGIN ms_set_config.set_empresa(:emp); END;` }
  ];

  let ok = false;
  const errors = [];

  for (const setter of setters) {
    try {
      await conn.execute(setter.sql, { emp: cdEmpresa });
      ok = true;
      break;
    } catch (e) {
      errors.push(`${setter.name}: ${e?.message ?? 'N/A'}`);
    }
  }

  if (!ok) {
    throw new Error(
      `Falha ao executar set_empresa(${cdEmpresa}) na sessão. Tentativas:\n- ${errors.join('\n- ')}`
    );
  }

  // Inicializa configest (evita ORA-20002 em triggers)
  await conn.execute(`BEGIN dbamv.pkt_configest.inicializa; END;`);

  // Valida empresa efetiva
  const r = await conn.execute(`SELECT dbamv.pkg_mv2000.le_empresa AS EMP FROM dual`);
  const empSessao = r.rows?.[0]?.EMP ?? null;

  if (Number(empSessao) !== Number(cdEmpresa)) {
    throw new Error(`Contexto MV divergente. Esperado=${cdEmpresa}, Sessão=${empSessao}`);
  }
}

export async function createOraclePool() {
  assertEnv(['ORA_USER', 'ORA_PASS', 'ORA_CONN_STR']);

  if (process.env.ORA_USE_POOL === '0') {
    console.warn('[Oracle] ORA_USE_POOL=0 ativo: pool desabilitado.');
    return null;
  }

  const poolMax = Number(process.env.ORA_POOL_MAX || 10);
  const poolMin = Number(process.env.ORA_POOL_MIN || 1);
  const poolIncrement = Number(process.env.ORA_POOL_INCREMENT || 1);
  const queueTimeout = Number(process.env.ORA_QUEUE_TIMEOUT || 300000);

  return oracledb.createPool({
    user: process.env.ORA_USER,
    password: process.env.ORA_PASS,
    connectString: process.env.ORA_CONN_STR,
    poolMin,
    poolMax,
    poolIncrement,
    queueTimeout,

    /**
     * CRÍTICO:
     * Toda conexão devolvida pelo pool (nova ou reutilizada) passa aqui.
     * Assim LE_EMPRESA sempre fica = 4 antes de qualquer DML.
     */
    sessionCallback: async (conn) => {
      await prepararSessaoMV(conn, EMPRESA_FIXA);
    }
  });
}

export async function getOracleConnection(oraclePool) {
  const usePool = process.env.ORA_USE_POOL !== '0';
  const debug = process.env.ORA_DEBUG === '1';

  if (usePool && oraclePool) {
    if (debug) {
      console.log('[Oracle] Tentando obter conexao via pool...');
    }
    try {
      const conn = await oraclePool.getConnection();
      if (debug) {
        console.log('[Oracle] Conexao obtida via pool.');
      }
      return conn;
    } catch (err) {
      console.warn(
        '[Oracle] Falha ao obter conexao do pool, tentando conexao direta:',
        err?.message ?? err
      );
    }
  }

  if (debug) {
    console.log('[Oracle] Usando conexao direta (sem pool).');
  }
  return oracledb.getConnection({
    user: process.env.ORA_USER,
    password: process.env.ORA_PASS,
    connectString: process.env.ORA_CONN_STR
  });
}

export function createMariaPool() {
  assertEnv(['DB_AUDIT_HOST', 'DB_AUDIT_USER', 'DB_AUDIT_NAME']);
  // DB_AUDIT_PASS pode ser vazio (XAMPP sem senha)

  return mariadb.createPool({
    host: process.env.DB_AUDIT_HOST,
    user: process.env.DB_AUDIT_USER,
    password: process.env.DB_AUDIT_PASS || '',
    database: process.env.DB_AUDIT_NAME,
    connectionLimit: 5
  });
}
