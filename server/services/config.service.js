import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const CONFIG_KEYS = [
  'DB_AUDIT_HOST',
  'DB_AUDIT_USER',
  'DB_AUDIT_PASS',
  'DB_AUDIT_NAME',
  'PORT',
  'ORA_USER',
  'ORA_PASS',
  'ORA_CONN_STR',
  'ORA_LIB_DIR'
];

const MASKED_KEYS = ['DB_AUDIT_PASS', 'ORA_PASS'];

/**
 * Retorna configuração para edição: valores mascarados para senhas.
 */
export function getConfigForEdit(env = process.env) {
  const out = {};
  for (const key of CONFIG_KEYS) {
    const value = env[key];
    if (MASKED_KEYS.includes(key) && value) {
      out[key] = '[MASKED]';
    } else {
      out[key] = value ?? '';
    }
  }
  return out;
}

/**
 * Lê arquivo .env e retorna objeto chave -> valor (apenas linhas KEY=value).
 */
function parseEnvFile(content) {
  const obj = {};
  const lines = (content || '').split(/\r?\n/);
  for (const line of lines) {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (m) {
      const key = m[1];
      let value = m[2];
      if (/^["']/.test(value) && value.endsWith(value[0])) {
        value = value.slice(1, -1);
      }
      obj[key] = value;
    }
  }
  return obj;
}

function escapeEnvValue(v) {
  const needsQuote = /\s|#|"|'/.test(v);
  return needsQuote ? `"${v.replace(/"/g, '\\"')}"` : v;
}

/**
 * Atualiza arquivo .env com as chaves fornecidas (não altera CONFIG_PASSWORD).
 * Preserva outras chaves já existentes no arquivo (ex.: DB_AUDIT_TABLE).
 */
export function updateEnvFile(updates, envPath = null) {
  const baseDir = process.cwd();
  const path = envPath || join(baseDir, '.env');

  let current = {};
  try {
    const raw = readFileSync(path, 'utf8');
    current = parseEnvFile(raw);
  } catch (err) {
    if (err?.code !== 'ENOENT') throw err;
  }

  for (const [key, value] of Object.entries(updates)) {
    if (key === 'CONFIG_PASSWORD') continue;
    if (!CONFIG_KEYS.includes(key)) continue;
    if (value === '[MASKED]' || (MASKED_KEYS.includes(key) && (value === '' || value == null))) {
      continue;
    }
    current[key] = String(value ?? '').trim();
  }

  const lines = [];
  for (const key of CONFIG_KEYS) {
    if (current[key] !== undefined && current[key] !== '') {
      lines.push(`${key}=${escapeEnvValue(current[key])}`);
    }
  }
  if (current.CONFIG_PASSWORD !== undefined && current.CONFIG_PASSWORD !== '') {
    lines.push(`CONFIG_PASSWORD=${escapeEnvValue(current.CONFIG_PASSWORD)}`);
  }
  for (const [key, value] of Object.entries(current)) {
    if (CONFIG_KEYS.includes(key) || key === 'CONFIG_PASSWORD') continue;
    if (value !== undefined && value !== '') {
      lines.push(`${key}=${escapeEnvValue(value)}`);
    }
  }

  writeFileSync(path, lines.join('\n') + '\n', 'utf8');
}
