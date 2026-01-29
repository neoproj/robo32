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
  // Em desenvolvimento, se VITE_API_BASE não estiver definido, usa a porta do backend (evita depender do proxy)
  const apiBase =
    import.meta.env.VITE_API_BASE ||
    (typeof window !== 'undefined' && window.location?.port === '5173' ? 'http://localhost:3001' : '');
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
  const [showModalDesfazer, setShowModalDesfazer] = useState(false);
  const [rollbackCandidates, setRollbackCandidates] = useState([]);
  const [rollbackSelectedJobId, setRollbackSelectedJobId] = useState('');
  const [rollbackLoading, setRollbackLoading] = useState(false);
  const [rollbackSubmitting, setRollbackSubmitting] = useState(false);
  const [rollbackMessage, setRollbackMessage] = useState('');
  const [rollbackRequestedBy, setRollbackRequestedBy] = useState('');
  const [activeTab, setActiveTab] = useState('central');
  const [showConfigPasswordModal, setShowConfigPasswordModal] = useState(false);
  const [configPassword, setConfigPassword] = useState('');
  const [configForm, setConfigForm] = useState({});
  const [configSaveMessage, setConfigSaveMessage] = useState('');
  const [configLoading, setConfigLoading] = useState(false);
  const [configSaving, setConfigSaving] = useState(false);
  const [configPasswordError, setConfigPasswordError] = useState('');
  const [configPasswordSession, setConfigPasswordSession] = useState('');
  const [uploadValidationErrors, setUploadValidationErrors] = useState([]);
  const CONFIG_KEYS = [
    { key: 'DB_AUDIT_HOST', label: 'MariaDB — Host' },
    { key: 'DB_AUDIT_USER', label: 'MariaDB — Usuário' },
    { key: 'DB_AUDIT_PASS', label: 'MariaDB — Senha', mask: true },
    { key: 'DB_AUDIT_NAME', label: 'MariaDB — Nome do banco' },
    { key: 'PORT', label: 'Porta da API' },
    { key: 'ORA_USER', label: 'Oracle — Usuário' },
    { key: 'ORA_PASS', label: 'Oracle — Senha', mask: true },
    { key: 'ORA_CONN_STR', label: 'Oracle — String de conexão' },
    { key: 'ORA_LIB_DIR', label: 'Oracle — Pasta do Instant Client' }
  ];

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
    if (!showModalDesfazer) return;
    setRollbackMessage('');
    const fetchCandidates = async () => {
      try {
        setRollbackLoading(true);
        const res = await fetch(`${apiBase}/api/jobs/rollback-candidates?limit=50`);
        const data = await res.json();
        if (res.ok && Array.isArray(data.jobs)) {
          setRollbackCandidates(data.jobs);
          if (data.jobs.length > 0) {
            setRollbackSelectedJobId(String(data.jobs[0].id));
          }
        } else {
          setRollbackCandidates([]);
        }
      } catch (_) {
        setRollbackCandidates([]);
      } finally {
        setRollbackLoading(false);
      }
    };
    fetchCandidates();
  }, [showModalDesfazer, apiBase]);

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
        const data = await response.json();
        const msg = data?.error || 'Falha ao iniciar job.';
        if (Array.isArray(data?.validationErrors) && data.validationErrors.length > 0) {
          setUploadValidationErrors(data.validationErrors);
          const first = data.validationErrors.slice(0, 5);
          const more = data.validationErrors.length > 5 ? ` ... e mais ${data.validationErrors.length - 5} erro(s).` : '';
          setStatusMessage(
            `${msg} (${data.validationErrors.length} erro(s)). Ex.: ${first.map((e) => `Linha ${e.row}${e.field ? `, ${e.field}` : ''}: ${e.message}`).join('; ')}${more}`
          );
        } else {
          setUploadValidationErrors([]);
          setStatusMessage(msg);
        }
        return;
      }

      const data = await response.json();
      setUploadValidationErrors([]);
      setStatusMessage(`Job ${data.jobId} iniciado. Aguarde o processamento.`);
      setFile(null);
    } catch (err) {
      setUploadValidationErrors([]);
      setStatusMessage(err?.message || 'Erro ao enviar.');
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

  const handleConfirmRollback = async () => {
    const jobId = rollbackSelectedJobId ? Number(rollbackSelectedJobId) : 0;
    if (!jobId) {
      setRollbackMessage('Selecione um job.');
      return;
    }
    try {
      setRollbackSubmitting(true);
      setRollbackMessage('');
      const res = await fetch(`${apiBase}/api/jobs/${jobId}/rollback`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ requestedBy: rollbackRequestedBy || null })
      });
      const data = await res.json();
      if (res.ok && data.ok) {
        setRollbackMessage(
          `Desfeito: ${data.produtosRemovidos} produto(s) removido(s), ${data.registrosDeletados} registro(s) no Oracle.`
        );
        setTimeout(() => {
          setShowModalDesfazer(false);
        }, 2000);
      } else {
        setRollbackMessage(data?.error || 'Falha ao desfazer.');
      }
    } catch (err) {
      setRollbackMessage(err?.message || 'Erro ao desfazer.');
    } finally {
      setRollbackSubmitting(false);
    }
  };

  const openConfigTab = () => {
    setConfigPasswordError('');
    setConfigPassword('');
    setShowConfigPasswordModal(true);
  };

  const handleConfigPasswordSubmit = async (e) => {
    e?.preventDefault();
    const pwd = configPassword.trim();
    if (!pwd) {
      setConfigPasswordError('Digite a senha.');
      return;
    }
    try {
      setConfigLoading(true);
      setConfigPasswordError('');
      const res = await fetch(`${apiBase}/api/config/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: pwd })
      });
      let data;
      try {
        data = await res.json();
      } catch (_) {
        setConfigPasswordError(
          res.status === 404
            ? 'API não encontrada. Verifique se o backend está rodando (npm run dev:server) na porta 3001.'
            : 'Resposta inválida do servidor. Verifique se o backend está rodando.'
        );
        return;
      }
      if (!res.ok) {
        setConfigPasswordError(data?.error || 'Senha incorreta.');
        return;
      }
      const configRes = await fetch(`${apiBase}/api/config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: pwd })
      });
      let configData;
      try {
        configData = await configRes.json();
      } catch (_) {
        setConfigPasswordError('Resposta inválida ao carregar configuração. Verifique o backend.');
        return;
      }
      if (!configRes.ok) {
        setConfigPasswordError(configData?.error || 'Falha ao carregar configuração.');
        return;
      }
      setConfigForm(configData.config || {});
      setConfigPasswordSession(pwd);
      setShowConfigPasswordModal(false);
      setActiveTab('config');
    } catch (err) {
      setConfigPasswordError(err?.message || 'Erro de conexão. Verifique se o backend está rodando.');
    } finally {
      setConfigLoading(false);
    }
  };

  const handleConfigFormChange = (key, value) => {
    setConfigForm((prev) => ({ ...prev, [key]: value }));
  };

  const handleConfigSave = async (e) => {
    e?.preventDefault();
    const pwd = configPasswordSession;
    if (!pwd) {
      setConfigSaveMessage('Sessão expirada. Abra Configuração novamente e digite a senha.');
      return;
    }
    try {
      setConfigSaving(true);
      setConfigSaveMessage('');
      const res = await fetch(`${apiBase}/api/config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: pwd, config: configForm })
      });
      const data = await res.json();
      if (res.ok) {
        setConfigSaveMessage(data?.message || 'Configuração salva. Reinicie o servidor para aplicar.');
      } else {
        setConfigSaveMessage(data?.error || 'Falha ao salvar.');
      }
    } catch (err) {
      setConfigSaveMessage(err?.message || 'Erro ao salvar.');
    } finally {
      setConfigSaving(false);
    }
  };

  return (
    <div className="page">
      <nav className="tabs-nav">
        <button
          type="button"
          className={`tab ${activeTab === 'central' ? 'active' : ''}`}
          onClick={() => { setActiveTab('central'); setConfigPasswordSession(''); }}
        >
          Central
        </button>
        <button
          type="button"
          className={`tab ${activeTab === 'config' ? 'active' : ''}`}
          onClick={openConfigTab}
          title="Configuração (exige senha)"
          aria-label="Configuração"
        >
          <span className="tab-icon" aria-hidden>⚙</span>
          Configuração
        </button>
      </nav>

      {activeTab === 'config' ? (
        <section className="panel wide config-panel">
          <div className="panel-header">
            <h2>Conexão com os bancos</h2>
            <button
              type="button"
              className="ghost"
              onClick={() => { setActiveTab('central'); setConfigPasswordSession(''); }}
            >
              Voltar à Central
            </button>
          </div>
          <p className="config-hint">
            Altere os valores e clique em Salvar para atualizar o arquivo .env. Reinicie o servidor para aplicar.
          </p>
          <form className="config-form" onSubmit={handleConfigSave}>
            {CONFIG_KEYS.map(({ key, label, mask }) => (
              <div className="config-row" key={key}>
                <label htmlFor={`config-${key}`}>{label}</label>
                <input
                  id={`config-${key}`}
                  type={mask ? 'password' : 'text'}
                  value={configForm[key] === '[MASKED]' ? '' : (configForm[key] ?? '')}
                  onChange={(e) => handleConfigFormChange(key, e.target.value)}
                  placeholder={configForm[key] === '[MASKED]' ? 'Deixe em branco para manter' : ''}
                  autoComplete={mask ? 'off' : 'on'}
                />
              </div>
            ))}
            <div className="config-actions">
              <button type="submit" className="primary" disabled={configSaving}>
                {configSaving ? 'Salvando...' : 'Salvar'}
              </button>
            </div>
            {configSaveMessage && (
              <p className={`config-save-msg ${configSaveMessage.includes('Reinicie') ? 'ok' : 'err'}`}>
                {configSaveMessage}
              </p>
            )}
          </form>
        </section>
      ) : (
        <>
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
            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
              <button className="ghost" onClick={() => setHistoryExpanded((prev) => !prev)}>
                {historyExpanded ? 'Ver menos' : 'Ver historico completo'}
              </button>
              <button
                type="button"
                className="ghost"
                onClick={() => setShowModalDesfazer(true)}
              >
                Desfazer job
              </button>
            </div>
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
            {uploadValidationErrors.length > 0 && (
              <ul className="upload-validation-errors" aria-live="polite">
                {uploadValidationErrors.slice(0, 15).map((e, i) => (
                  <li key={i}>
                    Linha {e.row}
                    {e.field ? `, ${e.field}` : ''}: {e.message}
                    {e.value ? ` (valor: "${e.value}")` : ''}
                  </li>
                ))}
                {uploadValidationErrors.length > 15 && (
                  <li className="job-meta">... e mais {uploadValidationErrors.length - 15} erro(s). Corrija a planilha e envie novamente.</li>
                )}
              </ul>
            )}
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
        </>
      )}

      {showModalDesfazer && (
        <div
          className="modal-overlay"
          onClick={(e) => e.target === e.currentTarget && setShowModalDesfazer(false)}
          role="dialog"
          aria-modal="true"
          aria-labelledby="modal-desfazer-title"
        >
          <div className="modal-box">
            <h3 id="modal-desfazer-title">Desfazer job</h3>
            <p>Escolha o job que deseja desfazer. Serão removidos no Oracle os produtos criados (EMPRESA_PRODUTO, UNI_PRO, PRODUTO).</p>
            {rollbackLoading ? (
              <p className="job-meta">Carregando jobs...</p>
            ) : rollbackCandidates.length === 0 ? (
              <p className="job-meta">Nenhum job com produtos criados para desfazer.</p>
            ) : (
              <>
                <label htmlFor="rollback-job-select">Job</label>
                <select
                  id="rollback-job-select"
                  value={rollbackSelectedJobId}
                  onChange={(e) => setRollbackSelectedJobId(e.target.value)}
                  disabled={rollbackSubmitting}
                >
                  {rollbackCandidates.map((job) => (
                    <option key={job.id} value={job.id}>
                      JOB-{job.id} — {job.filename} ({Number(job.produtos_criados ?? 0)} produto(s))
                    </option>
                  ))}
                </select>
                <label htmlFor="rollback-requested-by">Solicitante da remoção</label>
                <input
                  id="rollback-requested-by"
                  type="text"
                  value={rollbackRequestedBy}
                  onChange={(e) => setRollbackRequestedBy(e.target.value)}
                  placeholder="Nome de quem está solicitando a remoção"
                  disabled={rollbackSubmitting}
                  style={{ width: '100%', marginBottom: '16px', padding: '10px 14px', borderRadius: '12px', border: '1px solid rgba(0,0,0,0.2)' }}
                />
                <div className="modal-actions">
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => setShowModalDesfazer(false)}
                    disabled={rollbackSubmitting}
                  >
                    Cancelar
                  </button>
                  <button
                    type="button"
                    className="danger"
                    onClick={handleConfirmRollback}
                    disabled={rollbackSubmitting}
                  >
                    {rollbackSubmitting ? 'Desfazendo...' : 'Confirmar desfazer'}
                  </button>
                </div>
              </>
            )}
            {rollbackMessage && (
              <p
                className={`modal-rollback-message ${
                  rollbackMessage.startsWith('Desfeito') ? 'ok' : 'err'
                }`}
              >
                {rollbackMessage}
              </p>
            )}
          </div>
        </div>
      )}

      {showConfigPasswordModal && (
        <div
          className="modal-overlay"
          onClick={(e) => e.target === e.currentTarget && setShowConfigPasswordModal(false)}
          role="dialog"
          aria-modal="true"
          aria-labelledby="modal-config-password-title"
        >
          <div className="modal-box">
            <h3 id="modal-config-password-title">Acesso à Configuração</h3>
            <p>Digite a senha de administrador para editar a conexão com os bancos e atualizar o .env.</p>
            <form onSubmit={handleConfigPasswordSubmit}>
              <label htmlFor="config-password-input">Senha</label>
              <input
                id="config-password-input"
                type="password"
                value={configPassword}
                onChange={(e) => { setConfigPassword(e.target.value); setConfigPasswordError(''); }}
                placeholder="Senha de administrador"
                disabled={configLoading}
                autoFocus
                className="config-password-input"
              />
              {configPasswordError && (
                <p className="modal-rollback-message err">{configPasswordError}</p>
              )}
              <div className="modal-actions">
                <button
                  type="button"
                  className="ghost"
                  onClick={() => setShowConfigPasswordModal(false)}
                  disabled={configLoading}
                >
                  Cancelar
                </button>
                <button type="submit" className="primary" disabled={configLoading}>
                  {configLoading ? 'Verificando...' : 'Entrar'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
