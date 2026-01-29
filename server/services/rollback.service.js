/**
 * Rollback de um job: remove no Oracle os produtos CRIADOS pelo job (cd_produto_novo).
 * Nunca remove o cd_produto_antecessor.
 * Ordem: EMPRESA_PRODUTO → UNI_PRO → PRODUTO (respeitando FKs).
 */
export async function executeRollbackOracle(connection, productIds) {
  if (!Array.isArray(productIds) || productIds.length === 0) {
    return { deleted: 0, errors: [] };
  }

  const ids = productIds.filter((id) => Number.isFinite(Number(id)) && Number(id) > 0);
  const errors = [];
  let deleted = 0;

  for (const cdProduto of ids) {
    // cdProduto = cd_produto_novo (produto criado pelo clone), não o antecessor
    try {
      // 1. EMPRESA_PRODUTO (depende de PRODUTO)
      const r1 = await connection.execute(
        `DELETE FROM dbamv.EMPRESA_PRODUTO WHERE cd_produto = :cd`,
        { cd: cdProduto }
      );
      if (r1.rowsAffected > 0) deleted += Number(r1.rowsAffected);

      // 2. UNI_PRO (depende de PRODUTO)
      const r2 = await connection.execute(
        `DELETE FROM dbamv.UNI_PRO WHERE cd_produto = :cd`,
        { cd: cdProduto }
      );
      if (r2.rowsAffected > 0) deleted += Number(r2.rowsAffected);

      // 3. PRODUTO
      const r3 = await connection.execute(
        `DELETE FROM dbamv.PRODUTO WHERE cd_produto = :cd`,
        { cd: cdProduto }
      );
      if (r3.rowsAffected > 0) deleted += Number(r3.rowsAffected);
    } catch (err) {
      errors.push({ cd_produto: cdProduto, message: err?.message ?? String(err) });
    }
  }

  return { deleted, errors };
}
