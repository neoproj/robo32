import dotenv from 'dotenv';
import mariadb from 'mariadb';

dotenv.config();

async function cancelTrappedJobs() {
  let pool;
  
  try {
    pool = mariadb.createPool({
      host: process.env.DB_AUDIT_HOST,
      user: process.env.DB_AUDIT_USER,
      password: process.env.DB_AUDIT_PASS || '',
      database: process.env.DB_AUDIT_NAME,
      connectionLimit: 5
    });

    console.log('Buscando jobs travados em processamento...\n');
    
    const jobs = await pool.execute(
      `SELECT id, filename, uploaded_by, total_rows, created_at
       FROM AUDIT_JOB
       WHERE status = 'PROCESSING'
       ORDER BY created_at DESC`
    );

    if (jobs.length === 0) {
      console.log('✓ Nenhum job travado encontrado.');
      return;
    }

    console.log(`Encontrados ${jobs.length} job(s) travado(s):\n`);
    jobs.forEach((job, index) => {
      const created = new Date(job.created_at).toLocaleString('pt-BR');
      console.log(`${index + 1}. Job ID: ${job.id}`);
      console.log(`   Arquivo: ${job.filename}`);
      console.log(`   Upload por: ${job.uploaded_by}`);
      console.log(`   Total de linhas: ${job.total_rows}`);
      console.log(`   Criado em: ${created}\n`);
    });

    // Cancela todos os jobs travados
    for (const job of jobs) {
      await pool.execute(
        `UPDATE AUDIT_JOB
         SET status = 'FAILED', finished_at = NOW()
         WHERE id = ?`,
        [job.id]
      );
      console.log(`✓ Job ${job.id} cancelado.`);
    }

    console.log(`\n✓ Todos os jobs travados foram cancelados com sucesso!`);
    console.log('Agora você pode iniciar um novo job.');

  } catch (err) {
    console.error('Erro ao cancelar jobs:', err);
    process.exit(1);
  } finally {
    if (pool) await pool.end();
  }
}

cancelTrappedJobs();
