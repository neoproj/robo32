# Documento de Visão — Robo32 (Reclassificação de Produtos)

## Objetivo
Automatizar a reclassificação de produtos no Oracle (DBAMV), reduzindo erro manual e garantindo rastreabilidade por auditoria antes da execução em produção. O processo prevê validação pelas áreas responsáveis e autorização formal do Gerente de TI antes da aplicação em produção.

## Escopo do Robô
**Inclui:**
- Upload de planilha (.xlsx/.csv)
- Mapeamento de colunas
- Validação de classificação no Oracle
- Clonagem do produto no Oracle
- Auditoria detalhada no MariaDB
- Dashboard de acompanhamento

**Não inclui:**
- Alteração de preços ou estoque
- Dados clínicos ou de pacientes
- Qualquer rotina fora do cadastro de produtos

## Arquitetura e Integrações
- **Frontend:** React (interface de upload, mapping e acompanhamento)
- **Backend:** Node.js/Express (API de upload e processamento)
- **Banco Oracle:** schema DBAMV (operações de clonagem e atualização)
- **Banco MariaDB:** auditoria (AUDIT_JOB e AUDIT_PRODUTO)
- **Oracle Client:** Instant Client configurado via variável de ambiente

## Como o Robô Funciona (Fluxo Resumido)
1. Recebe planilha com colunas de:
   - `cd_produto_antecessor`
   - `cd_especie`
   - `cd_classe`
   - `cd_sub_cla`
2. Sugere o mapeamento automático e permite ajuste manual.
3. Impede múltiplos jobs simultâneos (apenas 1 processamento ativo).
4. Para cada linha:
   - Valida a combinação espécie/classe/subclasse no Oracle.
   - Clona o produto e gera novo `CD_PRODUTO` via `SEQ_PRODUTO`.
   - Copia registros de `PRODUTO`, `UNI_PRO` e `EMPRESA_PRODUTO`.
   - Desativa o produto antecessor (bloqueio de compra e movimentação).
   - Registra auditoria detalhada no MariaDB.
5. Finaliza o job com status **COMPLETED** ou **FAILED**.

## Regras de Negócio Implementadas
- **Validação de classificação:** espécie, classe e subclasse devem existir em `SUB_CLAS`.
- **Proteção contra duplicidade:** impede reprocessar um antecessor já clonado com sucesso.
- **Contexto MV fixo:** todas as operações são executadas com `le_empresa = 4`.
- **Transação por linha:** cada erro gera rollback apenas da linha corrente.

## Auditoria e Rastreabilidade
- **AUDIT_JOB:** arquivo, usuário, total de linhas, status e timestamps.
- **AUDIT_PRODUTO:** resultado por linha (`SUCCESS`, `ERROR_ORACLE`, `DUPLICADO_HISTORICO`, `ERROR_VALIDACAO`) + mensagem de erro.
- Dashboard exibe processados, erros e pendentes em tempo real.

## Controles de Segurança Existentes
- **Credenciais fora do código** via `.env`.
- **Validação de identificadores SQL** para evitar uso de nomes inválidos.
- **Queries parametrizadas** para dados de entrada.
- **Arquivos temporários apagados** após processamento.
- **Contexto Oracle validado** antes de operações críticas.

## Pontos de Atenção e Reforços Recomendados para Produção
- **Autenticação e autorização:** hoje não há login; recomenda-se AD/SSO ou controle via VPN/Proxy.
- **Restrição de CORS e firewall:** limitar acesso somente a redes internas.
- **TLS obrigatório:** publicar API apenas via HTTPS.
- **Limite de upload no backend:** front menciona 20MB, mas o servidor não impõe limite explícito.
- **Segregação de contas:** usuário Oracle com menor privilégio possível.
- **Monitoramento:** logs centralizados e alertas de falha/tempo de job.
- **Plano de rollback:** estratégia de reversão validada pela TI e área usuária.

## Governança e Processo de Autorização
- **Homologação:** rodar em ambiente de teste e validar resultados com as áreas.
- **Gate de aprovação:** autorização formal do Gerente de TI.
- **Execução em produção:** janela definida e acompanhamento.
- **Pós-processo:** checagem da auditoria e checklist de validação.

## Critérios para Liberação em Produção (propostos)
- Acesso e autenticação definidos.
- Credenciais segregadas e ambiente produtivo configurado.
- Plano de rollback aprovado pela TI.
- Checklists assinados pelas áreas responsáveis.
- Janela de execução e contingência aprovadas.

## Endpoints Principais (para TI)
- `POST /api/jobs/upload` — upload e início do processamento
- `GET /api/jobs/active` — job ativo + histórico
- `GET /api/jobs/:jobId/summary` — resumo do job
- `GET /api/jobs/:jobId/details` — linhas com erro
