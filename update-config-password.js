#!/usr/bin/env node
/**
 * Script para atualizar CONFIG_PASSWORD no arquivo .env.
 * Uso:
 *   node update-config-password.js "nova_senha"   (define a senha via argumento)
 *   node update-config-password.js                 (pede a senha no terminal)
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';

const ENV_PATH = join(process.cwd(), '.env');
const KEY = 'CONFIG_PASSWORD';

function readEnvLines() {
  try {
    return readFileSync(ENV_PATH, 'utf8').split(/\r?\n/);
  } catch (err) {
    if (err?.code === 'ENOENT') {
      console.error('Arquivo .env não encontrado em:', ENV_PATH);
      process.exit(1);
    }
    throw err;
  }
}

function writeEnvWithPassword(lines, newPassword) {
  let found = false;
  const out = lines.map((line) => {
    if (line.startsWith(KEY + '=')) {
      found = true;
      const needsQuote = /[\s#"']/.test(newPassword);
      const value = needsQuote ? `"${newPassword.replace(/"/g, '\\"')}"` : newPassword;
      return `${KEY}=${value}`;
    }
    return line;
  });
  if (!found) {
    out.push(`${KEY}=${newPassword}`);
  }
  writeFileSync(ENV_PATH, out.join('\n') + (out[out.length - 1] === '' ? '' : '\n'), 'utf8');
}

function prompt(question) {
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer);
    });
  });
}

async function main() {
  const argPassword = process.argv[2];

  let newPassword;
  if (argPassword !== undefined) {
    newPassword = String(argPassword);
    if (newPassword === '') {
      console.error('Senha não pode ser vazia quando passada por argumento.');
      process.exit(1);
    }
  } else {
    const p1 = await prompt('Nova senha (CONFIG_PASSWORD): ');
    const p2 = await prompt('Confirme a senha: ');
    if (p1 !== p2) {
      console.error('As senhas não coincidem.');
      process.exit(1);
    }
    newPassword = p1;
    if (!newPassword.trim()) {
      console.error('Senha não pode ser vazia.');
      process.exit(1);
    }
    newPassword = newPassword.trim();
  }

  const lines = readEnvLines();
  writeEnvWithPassword(lines, newPassword);
  console.log('CONFIG_PASSWORD atualizado no .env com sucesso.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
