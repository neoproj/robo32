import oracledb from 'oracledb';

const EMPRESA_FIXA = 4;

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.fetchArraySize = 200;

function sanitizeIdentifier(name) {
  const up = String(name || '').toUpperCase();
  if (!/^[A-Z0-9_]+$/.test(up)) throw new Error(`Identificador inválido: ${name}`);
  return up;
}

/**
 * Reforço de contexto (opcional).
 * O pool já faz isso via sessionCallback, mas este helper protege chamadas fora do pool.
 */
export async function prepararSessaoMV(conn, cdEmpresa = EMPRESA_FIXA) {
  if (process.env.ORA_SKIP_SET_EMPRESA === '1') {
    console.warn(
      '[Oracle] ORA_SKIP_SET_EMPRESA=1 ativo: pulando set_empresa/configest (modo teste).'
    );
    return;
  }
  const cfg = await conn.execute(
    `SELECT COUNT(*) AS QTD
       FROM dbamv.configest
      WHERE cd_multi_empresa = :p_emp`,
    { p_emp: cdEmpresa }
  );
  const qtd = Number(cfg.rows?.[0]?.QTD ?? 0);
  if (qtd < 1) {
    throw new Error(`Empresa ${cdEmpresa} sem registro em DBAMV.CONFIGEST.`);
  }

  const setters = [
    { name: 'DBAMV.PKG_MV_CONFIG', sql: `BEGIN dbamv.pkg_mv_config.set_empresa(:p_emp); END;` },
    { name: 'DBAMV.MS_SET_CONFIG', sql: `BEGIN dbamv.ms_set_config.set_empresa(:p_emp); END;` },
    { name: 'PKG_MV_CONFIG', sql: `BEGIN pkg_mv_config.set_empresa(:p_emp); END;` },
    { name: 'MS_SET_CONFIG', sql: `BEGIN ms_set_config.set_empresa(:p_emp); END;` }
  ];

  let ok = false;
  const errors = [];

  for (const setter of setters) {
    try {
      await conn.execute(setter.sql, { p_emp: cdEmpresa });
      ok = true;
      break;
    } catch (e) {
      errors.push(`${setter.name}: ${e?.message ?? 'N/A'}`);
    }
  }

  if (!ok) {
    throw new Error(`Falha ao executar set_empresa(${cdEmpresa}). Tentativas:\n- ${errors.join('\n- ')}`);
  }

  await conn.execute(`BEGIN dbamv.pkt_configest.inicializa; END;`);

  const r = await conn.execute(`SELECT dbamv.pkg_mv2000.le_empresa AS EMP FROM dual`);
  const empSessao = r.rows?.[0]?.EMP ?? null;

  if (Number(empSessao) !== Number(cdEmpresa)) {
    throw new Error(`Contexto MV divergente. Esperado=${cdEmpresa}, Sessão=${empSessao}`);
  }
}

async function getTableColumnsMeta(conn, tableName, owner = 'DBAMV') {
  const t = sanitizeIdentifier(tableName);
  const o = sanitizeIdentifier(owner);

  const r = await conn.execute(
    `
    SELECT column_name, nullable, column_id
      FROM all_tab_columns
     WHERE owner = :p_owner
       AND table_name = :p_table
     ORDER BY column_id
    `,
    { p_owner: o, p_table: t }
  );

  if (!r.rows || r.rows.length === 0) {
    throw new Error(`Tabela não encontrada ou sem acesso: ${o}.${t}`);
  }
  return r.rows;
}

async function getCloneColumnList(conn, tableName, excluir = [], owner = 'DBAMV') {
  const meta = await getTableColumnsMeta(conn, tableName, owner);
  const exclude = new Set(excluir.map((c) => sanitizeIdentifier(c)));

  const cols = meta.map((m) => m.COLUMN_NAME).filter((c) => !exclude.has(sanitizeIdentifier(c)));

  if (!cols.length) {
    throw new Error(`Nenhuma coluna disponível para clonagem em ${owner}.${tableName} após exclusões.`);
  }
  return cols.join(', ');
}

async function validarSubClas(conn, cdEspecie, cdClasse, cdSubCla) {
  const r = await conn.execute(
    `
    SELECT 1 AS OK
      FROM dbamv.sub_clas
     WHERE cd_especie = :p_especie
       AND cd_classe  = :p_classe
       AND cd_sub_cla = :p_sub_cla
    `,
    { p_especie: cdEspecie, p_classe: cdClasse, p_sub_cla: cdSubCla }
  );

  if (!r.rows || r.rows.length === 0) {
    throw new Error(`Classificação inexistente em SUB_CLAS: especie=${cdEspecie}, classe=${cdClasse}, sub=${cdSubCla}`);
  }
}

/**
 * CLONE do produto:
 * - NÃO faz COMMIT/ROLLBACK
 * - NÃO desabilita trigger
 * - Empresa é fixa = 4 (pelo pool e reforço)
 *
 * Retorna o novo CD_PRODUTO.
 */
export async function cloneProdutoOracle(conn, {
  cdProdutoAntecessor,
  cdEspecieNova,
  cdClasseNova,
  cdSubClaNova,
  somenteEmpresa4 = true
}) {
  // Defesa extra (mesmo com pool)
  await prepararSessaoMV(conn, EMPRESA_FIXA);

  // Diagnóstico útil (pula se ORA_SKIP_SET_EMPRESA estiver ativo)
  if (somenteEmpresa4 && process.env.ORA_SKIP_SET_EMPRESA !== '1') {
    const diag = await conn.execute(`SELECT dbamv.pkg_mv2000.le_empresa AS EMP FROM dual`);
    const empSessao = diag.rows?.[0]?.EMP ?? null;
    if (Number(empSessao) !== EMPRESA_FIXA) {
      throw new Error(`Sessão não está na empresa 4. LE_EMPRESA=${empSessao}`);
    }
  }

  await validarSubClas(conn, cdEspecieNova, cdClasseNova, cdSubClaNova);

  const resSeq = await conn.execute(`SELECT dbamv.seq_produto.NEXTVAL AS NEXTID FROM dual`);
  const novoCdProduto = resSeq.rows?.[0]?.NEXTID ?? null;
  if (!novoCdProduto) throw new Error('Falha ao obter SEQ_PRODUTO.NEXTVAL.');

  // PRODUTO (exclui chave/classificação/dt e auditoria preenchida pelo trigger)
  const excluirProduto = [
    'CD_PRODUTO', 'CD_ESPECIE', 'CD_CLASSE', 'CD_SUB_CLA', 'DT_CADASTRO',
    'CD_USUARIO_INC', 'DT_INC_USUARIO', 'CD_USUARIO_ALT', 'DT_ALT_USUARIO'
  ];
  const colsProd = await getCloneColumnList(conn, 'PRODUTO', excluirProduto, 'DBAMV');

  const rInsProd = await conn.execute(
    `
    INSERT INTO dbamv.produto (CD_PRODUTO, CD_ESPECIE, CD_CLASSE, CD_SUB_CLA, DT_CADASTRO, ${colsProd})
    SELECT :p_novo_id, :p_especie, :p_classe, :p_sub_cla, SYSDATE, ${colsProd}
      FROM dbamv.produto
     WHERE cd_produto = :p_antecessor
    `,
    {
      p_novo_id: novoCdProduto,
      p_especie: cdEspecieNova,
      p_classe: cdClasseNova,
      p_sub_cla: cdSubClaNova,
      p_antecessor: cdProdutoAntecessor
    }
  );

  if (rInsProd.rowsAffected !== 1) {
    throw new Error(`Produto antecessor não encontrado: CD_PRODUTO=${cdProdutoAntecessor}`);
  }

  // UNI_PRO (barcode NULL conforme regra)
  const excluirUni = ['CD_UNI_PRO', 'CD_PRODUTO', 'CD_CODIGO_DE_BARRAS'];
  const colsUni = await getCloneColumnList(conn, 'UNI_PRO', excluirUni, 'DBAMV');

  await conn.execute(
    `
    INSERT INTO dbamv.uni_pro (CD_UNI_PRO, CD_PRODUTO, CD_CODIGO_DE_BARRAS, ${colsUni})
    SELECT dbamv.seq_uni_pro.NEXTVAL, :p_novo_id, NULL, ${colsUni}
      FROM dbamv.uni_pro
     WHERE cd_produto = :p_antecessor
    `,
    { p_novo_id: novoCdProduto, p_antecessor: cdProdutoAntecessor }
  );

  // EMPRESA_PRODUTO
  const excluirEmp = ['CD_PRODUTO'];
  const colsEmp = await getCloneColumnList(conn, 'EMPRESA_PRODUTO', excluirEmp, 'DBAMV');

  if (somenteEmpresa4) {
    await conn.execute(
      `
      INSERT INTO dbamv.empresa_produto (CD_PRODUTO, ${colsEmp})
      SELECT :p_novo_id, ${colsEmp}
        FROM dbamv.empresa_produto
       WHERE cd_produto = :p_antecessor
         AND cd_multi_empresa = :p_emp
      `,
      { p_novo_id: novoCdProduto, p_antecessor: cdProdutoAntecessor, p_emp: EMPRESA_FIXA }
    );
  } else {
    await conn.execute(
      `
      INSERT INTO dbamv.empresa_produto (CD_PRODUTO, ${colsEmp})
      SELECT :p_novo_id, ${colsEmp}
        FROM dbamv.empresa_produto
       WHERE cd_produto = :p_antecessor
      `,
      { p_novo_id: novoCdProduto, p_antecessor: cdProdutoAntecessor }
    );
  }

  // Reforça contexto imediatamente antes do UPDATE que dispara trigger
  await prepararSessaoMV(conn, EMPRESA_FIXA);

  // Desativar antecessor
  await conn.execute(
    `
    UPDATE dbamv.produto
       SET sn_movimentacao = 'N',
           sn_bloqueio_de_compra = 'S'
     WHERE cd_produto = :p_antecessor
    `,
    { p_antecessor: cdProdutoAntecessor }
  );

  await conn.execute(
    `
    UPDATE dbamv.empresa_produto
       SET sn_movimentacao = 'N',
           sn_bloqueio_de_compra = 'S'
     WHERE cd_produto = :p_antecessor
       AND cd_multi_empresa = :p_emp
    `,
    { p_antecessor: cdProdutoAntecessor, p_emp: EMPRESA_FIXA }
  );

  return Number(novoCdProduto);
}
