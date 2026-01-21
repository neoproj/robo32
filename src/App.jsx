import { useEffect, useMemo, useState } from 'react';
import * as XLSX from 'xlsx';
import { suggestMapping } from './utils/mapping.js';

const mappingOptions = [
  { value: '', label: 'Sem mapeamento' },
  { value: 'cd_produto_antecessor', label: 'cd_produto_antecessor' },
  { value: 'cd_especie', label: 'cd_especie' },
  { value: 'cd_classe', label: 'cd_classe' },
  { value: 'cd_sub_cla', label: 'cd_sub_cla' }
];

export default function App() {
  const apiBase = import.meta.env.VITE_API_BASE || '';
  const [columns, setColumns] = useState([]);
  const [mappings, setMappings] = useState({});
  const [recentJobs, setRecentJobs] = useState([]);
  const [activeJob, setActiveJob] = useState(null);
  const [detailsJob, setDetailsJob] = useState(null);
  const [summary, setSummary] = useState({ processed: 0, errors: 0, remaining: 0, total: 0 });
  const [errorRows, setErrorRows] = useState([]);
  const [file, setFile] = useState(null);
  const [uploadedBy, setUploadedBy] = useState('');
  const [statusMessage, setStatusMessage] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [historyExpanded, setHistoryExpanded] = useState(false);

  const initialMappings = useMemo(
    () =>
      columns.reduce((acc, column) => {
        acc[column] = suggestMapping(column);
        return acc;
      }, {}),
    [columns]
  );

  useEffect(() => {
    setMappings(initialMappings);
  }, [initialMappings]);

  useEffect(() => {
    let isMounted = true;
    const recentLimit = historyExpanded ? 200 : 5;

    const fetchJobs = async () => {
      try {
        const response = await fetch(`${apiBase}/api/jobs/active?recentLimit=${recentLimit}`);
        const data = await response.json();
        if (!isMounted) return;

        setActiveJob(data.activeJob);
        const recentList = Array.isArray(data.recentJobs)
          ? data.recentJobs
          : data.recentJobs
            ? [data.recentJobs]
            : [];
        setRecentJobs(recentList);

        const jobForDetails = data.activeJob?.id ? data.activeJob : recentList[0];
        setDetailsJob(jobForDetails || null);

        if (jobForDetails?.id) {
          const [summaryRes, detailsRes] = await Promise.all([
            fetch(`${apiBase}/api/jobs/${jobForDetails.id}/summary`),
            fetch(`${apiBase}/api/jobs/${jobForDetails.id}/details`)
          ]);

          const summaryData = await summaryRes.json();
          const detailsData = await detailsRes.json();
          if (!isMounted) return;
          setSummary(summaryData);
          setErrorRows(detailsData.rows || []);
        } else {
          setDetailsJob(null);
          setSummary({ processed: 0, errors: 0, remaining: 0, total: 0 });
          setErrorRows([]);
        }
      } catch (err) {
        if (!isMounted) return;
        setStatusMessage('Falha ao carregar dados do backend.');
      }
    };

    fetchJobs();
    const interval = setInterval(fetchJobs, 5000);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [apiBase, historyExpanded]);

  const handleMappingChange = (column, value) => {
    setMappings((prev) => ({ ...prev, [column]: value }));
  };

  const handleFileChange = async (event) => {
    const selected = event.target.files?.[0];
    if (!selected) return;

    setFile(selected);
    setStatusMessage('');

    const data = await selected.arrayBuffer();
    const workbook = XLSX.read(data, { type: 'array' });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    const headers = rows.length ? Object.keys(rows[0]) : [];
    setColumns(headers);
  };

  const handleUpload = async () => {
    if (!file) {
      setStatusMessage('Selecione um arquivo para iniciar.');
      return;
    }

    if (activeJob?.id) {
      setStatusMessage('Ja existe um job em processamento.');
      return;
    }

    try {
      setIsUploading(true);
      setStatusMessage('');
      const formData = new FormData();
      formData.append('file', file);
      formData.append('mapping', JSON.stringify(mappings));
      formData.append('uploadedBy', uploadedBy || 'desconhecido');

      const response = await fetch(`${apiBase}/api/jobs/upload`, {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error?.error || 'Falha ao iniciar job.');
      }

      const data = await response.json();
      setStatusMessage(`Job ${data.jobId} iniciado. Aguarde o processamento.`);
      setFile(null);
    } catch (err) {
      setStatusMessage(err.message);
    } finally {
      setIsUploading(false);
    }
  };

  const formatStatus = (status) => {
    if (status === 'PROCESSING') return 'Progresso';
    if (status === 'COMPLETED') return 'Finalizado';
    if (status === 'FAILED') return 'Erro';
    return status || '---';
  };

  const statusClass = (status) => {
    if (status === 'PROCESSING') return 'progresso';
    if (status === 'COMPLETED') return 'finalizado';
    if (status === 'FAILED') return 'erro';
    return '';
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
              <p className="value ok">{summary.processed}</p>
            </div>
            <div>
              <p className="label">Erros</p>
              <p className="value warn">{summary.errors}</p>
            </div>
            <div>
              <p className="label">Restantes</p>
              <p className="value neutral">{summary.remaining}</p>
            </div>
          </div>
          <p className="hint">
            {activeJob
              ? `Job ativo: ${activeJob.filename}`
              : detailsJob
                ? `Ultimo job: ${detailsJob.filename}`
                : 'Nenhum job em processamento no momento.'}
          </p>
        </div>
      </header>

      <section className="grid">
        <div className="panel">
          <div className="panel-header">
            <h2>Dashboard de Jobs</h2>
            <button className="ghost" onClick={() => setHistoryExpanded((prev) => !prev)}>
              {historyExpanded ? 'Ver menos' : 'Ver historico completo'}
            </button>
          </div>
          <div className="job-list">
            {recentJobs.length === 0 && <p className="job-meta">Nenhum upload registrado.</p>}
            {recentJobs.map((job) => (
              <div className="job-row" key={job.id}>
                <div>
                  <p className="job-id">JOB-{job.id}</p>
                  <p className="job-meta">Upload por {job.uploaded_by}</p>
                </div>
                <div>
                  <p className={`status ${statusClass(job.status)}`}>{formatStatus(job.status)}</p>
                  <p className="job-meta">
                    {new Date(job.created_at).toLocaleString('pt-BR')}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>Upload & Mapping</h2>
            <button className="primary" onClick={handleUpload} disabled={isUploading}>
              {isUploading ? 'Enviando...' : 'Iniciar upload'}
            </button>
          </div>
          <div className="upload-box">
            <div>
              <p className="upload-title">Solte a planilha aqui</p>
              <p className="upload-subtitle">.xlsx, .csv — até 20MB</p>
            </div>
            <label className="ghost">
              Selecionar arquivo
              <input type="file" onChange={handleFileChange} />
            </label>
          </div>
          <div className="upload-meta">
            <label>
              Responsavel pelo upload
              <input
                type="text"
                value={uploadedBy}
                onChange={(event) => setUploadedBy(event.target.value)}
                placeholder="Nome do usuario"
              />
            </label>
            {statusMessage && <p className="status-message">{statusMessage}</p>}
          </div>
          <div className="mapping-table">
            <div className="mapping-head">
              <span>Coluna Detectada</span>
              <span>Mapeamento sugerido</span>
            </div>
            {columns.length === 0 && (
              <p className="job-meta">Selecione um arquivo para mapear as colunas.</p>
            )}
            {columns.map((column) => (
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
          <h2>Falhas do Job</h2>
          <p className="job-meta">
            {detailsJob ? `Job ${detailsJob.id}` : 'Nenhum job ativo'}
          </p>
        </div>
        <div className="error-table">
          {errorRows.length === 0 && (
            <p className="job-meta">Sem erros registrados ate o momento.</p>
          )}
          {errorRows.map((row) => (
            <div className="error-row" key={`${row.row_number}-${row.cd_produto_antecessor}`}>
              <div>
                <p className="job-id">Linha {row.row_number}</p>
                <p className="job-meta">Antecessor: {row.cd_produto_antecessor}</p>
              </div>
              <div>
                <p className="status erro">{row.status}</p>
                <p className="job-meta">{row.error_message}</p>
              </div>
            </div>
          ))}
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
