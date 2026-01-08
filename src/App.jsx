import { useMemo, useState } from 'react';
import { suggestMapping } from './utils/mapping.js';

const sampleJobs = [
  { id: 'JOB-0912', user: 'Carla M.', status: 'Progresso', uploadedAt: 'Hoje 09:12' },
  { id: 'JOB-0833', user: 'Andre', status: 'Finalizado', uploadedAt: 'Hoje 08:33' },
  { id: 'JOB-0704', user: 'Dani S.', status: 'Erro', uploadedAt: 'Hoje 07:04' }
];

const sampleColumns = [
  'PROD_ANT',
  'ESP_BOV',
  'CLA_PRINCIPAL',
  'SUB_CLASSE',
  'DESCRICAO'
];

const mappingOptions = [
  { value: '', label: 'Sem mapeamento' },
  { value: 'cd_produto_antecessor', label: 'cd_produto_antecessor' },
  { value: 'cd_especie', label: 'cd_especie' },
  { value: 'cd_classe', label: 'cd_classe' },
  { value: 'cd_sub_cla', label: 'cd_sub_cla' }
];

export default function App() {
  const initialMappings = useMemo(
    () =>
      sampleColumns.reduce((acc, column) => {
        acc[column] = suggestMapping(column);
        return acc;
      }, {}),
    []
  );

  const [mappings, setMappings] = useState(initialMappings);

  const handleMappingChange = (column, value) => {
    setMappings((prev) => ({ ...prev, [column]: value }));
  };

  return (
    <div className="page">
      <header className="hero">
        <div>
          <p className="eyebrow">Robo32</p>
          <h1>Central de Controle de Uploads</h1>
          <p className="subtitle">
            Guie o consultor no fluxo correto e reduza erros no mapeamento.
          </p>
        </div>
        <div className="hero-card">
          <h2>Auditoria em tempo real</h2>
          <div className="audit-grid">
            <div>
              <p className="label">Processados</p>
              <p className="value ok">150</p>
            </div>
            <div>
              <p className="label">Erros</p>
              <p className="value warn">2</p>
            </div>
            <div>
              <p className="label">Restantes</p>
              <p className="value neutral">48</p>
            </div>
          </div>
          <p className="hint">Atualiza a cada 5s enquanto o backend processa.</p>
        </div>
      </header>

      <section className="grid">
        <div className="panel">
          <div className="panel-header">
            <h2>Dashboard de Jobs</h2>
            <button className="ghost">Ver histórico completo</button>
          </div>
          <div className="job-list">
            {sampleJobs.map((job) => (
              <div className="job-row" key={job.id}>
                <div>
                  <p className="job-id">{job.id}</p>
                  <p className="job-meta">Upload por {job.user}</p>
                </div>
                <div>
                  <p className={`status ${job.status.toLowerCase()}`}>{job.status}</p>
                  <p className="job-meta">{job.uploadedAt}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>Upload & Mapping</h2>
            <button className="primary">Novo upload</button>
          </div>
          <div className="upload-box">
            <div>
              <p className="upload-title">Solte a planilha aqui</p>
              <p className="upload-subtitle">.xlsx, .csv — até 20MB</p>
            </div>
            <button className="ghost">Selecionar arquivo</button>
          </div>
          <div className="mapping-table">
            <div className="mapping-head">
              <span>Coluna Detectada</span>
              <span>Mapeamento sugerido</span>
            </div>
            {sampleColumns.map((column) => (
              <div className="mapping-row" key={column}>
                <span className="column-name">{column}</span>
                <div className="mapping-controls">
                  <select
                    value={mappings[column] || ''}
                    onChange={(event) => handleMappingChange(column, event.target.value)}
                  >
                    {mappingOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                  <span className="mapping-hint">
                    {mappings[column] ? 'Sugestão ativa' : 'Sem sugestão'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="panel wide">
        <div className="panel-header">
          <h2>Checklist do Processamento</h2>
          <button className="ghost">Exportar relatório</button>
        </div>
        <div className="checklist">
          <div>
            <p className="check-title">1. Validar Subclasse</p>
            <p className="check-desc">
              Verifica no Oracle a combinação de espécie, classe e subclasse antes
              de inserir.
            </p>
          </div>
          <div>
            <p className="check-title">2. Registrar Auditoria</p>
            <p className="check-desc">
              Em erro, marca como <span className="tag">ERROR_VALIDACAO</span> no
              MariaDB.
            </p>
          </div>
          <div>
            <p className="check-title">3. Inserção Operacional</p>
            <p className="check-desc">
              Apenas registros validados seguem para o Oracle 12g.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
