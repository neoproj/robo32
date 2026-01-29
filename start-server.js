#!/usr/bin/env node
/**
 * Inicia o servidor Robo32. No Windows, tenta liberar a porta 3001 antes.
 * Uso: node start-server.js   (execute na pasta do projeto: C:\robo32\robo32)
 */
import { spawn } from 'node:child_process';
import { platform } from 'node:os';

const PORT = process.env.PORT || '3001';

function startServer() {
  console.log('Iniciando servidor Robo32...');
  const child = spawn('node', ['server/index.js'], {
    stdio: 'inherit',
    shell: true,
    cwd: process.cwd()
  });
  child.on('error', (err) => {
    console.error(err);
    process.exit(1);
  });
  child.on('exit', (code) => process.exit(code ?? 0));
}

if (platform() === 'win32') {
  const netstat = spawn('netstat', ['-ano'], { stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '';
  netstat.stdout?.on('data', (d) => { out += d.toString(); });
  netstat.stderr?.on('data', () => {});
  netstat.on('close', () => {
    const lines = out.split(/\r?\n/).filter((l) => l.includes(`:${PORT}`) && l.includes('LISTENING'));
    const pids = [...new Set(lines.map((l) => l.trim().split(/\s+/).pop()).filter((p) => /^\d+$/.test(p)))];
    if (pids.length > 0) {
      console.log(`Porta ${PORT} em uso. Encerrando processo(s): ${pids.join(', ')}`);
      let n = 0;
      const go = () => {
        n++;
        if (n === pids.length) {
          setTimeout(startServer, 500);
        }
      };
      pids.forEach((pid) => {
        spawn('taskkill', ['/PID', pid, '/F'], { stdio: 'ignore' }).on('close', go);
      });
    } else {
      startServer();
    }
  });
} else {
  startServer();
}
