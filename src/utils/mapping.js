export function suggestMapping(columnName) {
  if (!columnName) {
    return '';
  }

  const normalized = columnName.toUpperCase();

  if (normalized.includes('PROD') || normalized.includes('ANT')) {
    return 'cd_produto_antecessor';
  }

  if (normalized.includes('ESP')) {
    return 'cd_especie';
  }

  if (normalized.includes('SUB')) {
    return 'cd_sub_cla';
  }

  if (normalized.includes('CLA') && !normalized.includes('SUB')) {
    return 'cd_classe';
  }

  return '';
}
