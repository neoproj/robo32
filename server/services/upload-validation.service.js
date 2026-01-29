/**
 * Validação do layout da planilha no upload.
 * Garante colunas obrigatórias e valores numéricos válidos antes de criar o job.
 */

const REQUIRED_FIELDS = ['cd_produto_antecessor', 'cd_especie', 'cd_classe', 'cd_sub_cla'];

const MAX_ROWS = 50000;

function parseNumber(value) {
  if (value === null || value === undefined || value === '') return NaN;
  const s = String(value).trim().replace(',', '.');
  if (s === '') return NaN;
  const n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}

/**
 * Valida o layout e os dados da planilha.
 * @param {Array<Object>} rows - Linhas da planilha (objetos com chaves = cabeçalhos).
 * @param {Object} mapping - Mapeamento campo -> nome da coluna (ex.: { cd_produto_antecessor: 'Código' }).
 * @returns {{ valid: boolean, errors: Array<{ row: number, field: string, value: string, message: string }> }}
 */
export function validateLayout(rows, mapping) {
  const errors = [];

  if (!rows || rows.length === 0) {
    return { valid: false, errors: [{ row: 0, field: '', value: '', message: 'Planilha sem linhas de dados.' }] };
  }

  if (rows.length > MAX_ROWS) {
    return {
      valid: false,
      errors: [{ row: 0, field: '', value: '', message: `Planilha com mais de ${MAX_ROWS.toLocaleString('pt-BR')} linhas. Reduza o arquivo.` }]
    };
  }

  const firstRow = rows[0];
  const headers = Object.keys(firstRow || {});

  for (const field of REQUIRED_FIELDS) {
    const columnName = mapping[field];
    if (!columnName || typeof columnName !== 'string') {
      errors.push({ row: 0, field, value: '', message: `Mapeamento obrigatório ausente: ${field}.` });
      continue;
    }
    if (!headers.includes(columnName)) {
      errors.push({
        row: 1,
        field,
        value: columnName,
        message: `Coluna "${columnName}" (${field}) não encontrada na planilha. Cabeçalhos: ${headers.slice(0, 10).join(', ')}${headers.length > 10 ? '...' : ''}.`
      });
    }
  }

  if (errors.length > 0) {
    return { valid: false, errors };
  }

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const rowNum = i + 1;

    const rawAntecessor = row[mapping.cd_produto_antecessor];
    const rawEsp = row[mapping.cd_especie];
    const rawCla = row[mapping.cd_classe];
    const rawSub = row[mapping.cd_sub_cla];

    const antecessor = parseNumber(rawAntecessor);
    const esp = parseNumber(rawEsp);
    const cla = parseNumber(rawCla);
    const sub = parseNumber(rawSub);

    if (!Number.isFinite(antecessor) || antecessor <= 0) {
      errors.push({
        row: rowNum,
        field: 'cd_produto_antecessor',
        value: String(rawAntecessor ?? ''),
        message: `Deve ser um número maior que zero. Valor: "${rawAntecessor ?? ''}".`
      });
    }
    if (!Number.isFinite(esp)) {
      errors.push({
        row: rowNum,
        field: 'cd_especie',
        value: String(rawEsp ?? ''),
        message: `Deve ser um número. Valor: "${rawEsp ?? ''}".`
      });
    }
    if (!Number.isFinite(cla)) {
      errors.push({
        row: rowNum,
        field: 'cd_classe',
        value: String(rawCla ?? ''),
        message: `Deve ser um número. Valor: "${rawCla ?? ''}".`
      });
    }
    if (!Number.isFinite(sub)) {
      errors.push({
        row: rowNum,
        field: 'cd_sub_cla',
        value: String(rawSub ?? ''),
        message: `Deve ser um número. Valor: "${rawSub ?? ''}".`
      });
    }

    if (errors.length >= 50) {
      errors.push({ row: rowNum, field: '', value: '', message: `... Validação interrompida. Corrija os erros acima e envie novamente.` });
      break;
    }
  }

  return {
    valid: errors.length === 0,
    errors
  };
}
