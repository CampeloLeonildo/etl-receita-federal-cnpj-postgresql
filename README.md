# ETL Receita Federal CNPJ com Python e PostgreSQL

Pipeline ETL desenvolvido em Python para baixar, extrair, processar e carregar os dados públicos de CNPJ da Receita Federal em um banco PostgreSQL.

O projeto foi criado com foco em **processamento de grandes volumes de dados**, utilizando carga em partes/chunks, controle de checkpoint, logs de execução, tabelas snapshot por data, views atuais e criação de índices para performance.

---

## Dados Públicos de CNPJ

A Receita Federal do Brasil disponibiliza bases públicas com informações do Cadastro Nacional da Pessoa Jurídica (CNPJ).

Fonte oficial dos dados:

- Portal de dados abertos: [Cadastro Nacional da Pessoa Jurídica - CNPJ](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- Layout dos arquivos: [Metadados CNPJ Receita Federal](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)
- Diretório de arquivos públicos: `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/`

De forma geral, essas bases contêm informações cadastrais de empresas, estabelecimentos, sócios, Simples Nacional, CNAEs, naturezas jurídicas, municípios, países, qualificações e motivos de situação cadastral.

Esses dados podem ser utilizados para análises econômicas, mercadológicas, estudos regionais, enriquecimento de bases, inteligência comercial e projetos de dados.

---

## Objetivo do projeto

Este repositório contém um processo de ETL para:

1. Listar os arquivos públicos disponíveis no link da Receita Federal.
2. Baixar automaticamente os arquivos `.zip`.
3. Verificar se o arquivo local já existe e se possui o mesmo tamanho do arquivo remoto.
4. Descompactar os arquivos baixados.
5. Ler os arquivos CSV em partes/chunks.
6. Tratar e padronizar os dados.
7. Inserir os dados em tabelas PostgreSQL.
8. Criar tabelas snapshot por data.
9. Criar views atuais apontando para o snapshot mais recente.
10. Criar índices para melhorar performance de consulta.
11. Registrar logs de execução.
12. Registrar logs por arquivo e parte processada.
13. Controlar checkpoint para permitir retomada da execução.
14. Remover snapshots antigos, mantendo apenas os últimos configurados.

---

## Principais recursos

- Download automático dos arquivos públicos da Receita Federal
- Extração automática dos arquivos ZIP
- Carga em PostgreSQL
- Processamento em chunks para arquivos grandes
- Checkpoint por arquivo e parte
- Modo de retomada da execução
- Logs de execução geral
- Logs por arquivo e parte processada
- Tabelas snapshot por data
- Views atuais com nomes fixos
- Criação automática de índices
- Controle de snapshots antigos
- Configuração via `.env`
- Estrutura preparada para portfólio e evolução do projeto

---

## Tecnologias utilizadas

- Python
- PostgreSQL
- Pandas
- SQLAlchemy
- Psycopg2
- Requests
- Wget
- Python Dotenv
- WebDAV
- Git e GitHub

---

## Infraestrutura necessária

Recomendado:

- Python 3.10 ou superior
- PostgreSQL 14 ou superior
- Git
- Ambiente virtual Python

Dependências Python:

```bash
pip install -r requirements.txt

Estrutura sugerida do projeto
etl-receita-federal-cnpj-postgresql/
├── main.py
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── Dados_RFB_ERD.png
├── ERD_Dados_RFB.pgerd
└── Consultas/
    └── consultas_exemplo.sql
	
	
Configuração do ambiente

Crie um arquivo .env com base no arquivo .env.example.	
	
# =============================================================================
# CONFIGURAÇÕES DE DIRETÓRIOS
# =============================================================================

OUTPUT_FILES_PATH=D:\RFB\Dados_RFB\OUTPUT_FILES
EXTRACTED_FILES_PATH=D:\RFB\Dados_RFB\EXTRACTED_FILES


# =============================================================================
# CONFIGURAÇÕES DO POSTGRESQL
# =============================================================================

DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=sua_senha_aqui
DB_NAME=RFB


# =============================================================================
# ORIGEM DOS DADOS
# =============================================================================

PUBLIC_SHARE_URL=https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9?dir=/2026-04


# =============================================================================
# MODO DE EXECUÇÃO
# =============================================================================
# FULL   = inicia uma carga nova do zero
# RESUME = continua de onde parou usando checkpoint
# RESET  = limpa execução da data atual e reinicia

RUN_MODE=FULL


# =============================================================================
# TESTE E CONTROLE DE SNAPSHOTS
# =============================================================================

# 0 = carga completa
# Exemplo para teste: LIMIT_ROWS=10000
LIMIT_ROWS=0

# Mantém o snapshot atual e o anterior
KEEP_LAST_SNAPSHOTS=2	


Variáveis de ambiente

| Variável               | Descrição                                             |
| ---------------------- | ----------------------------------------------------- |
| `OUTPUT_FILES_PATH`    | Diretório onde os arquivos ZIP serão baixados         |
| `EXTRACTED_FILES_PATH` | Diretório onde os arquivos serão extraídos            |
| `DB_HOST`              | Host do PostgreSQL                                    |
| `DB_PORT`              | Porta do PostgreSQL                                   |
| `DB_USER`              | Usuário do banco                                      |
| `DB_PASSWORD`          | Senha do banco                                        |
| `DB_NAME`              | Nome do banco de dados                                |
| `PUBLIC_SHARE_URL`     | Link público dos arquivos da Receita Federal          |
| `RUN_MODE`             | Define o modo de execução do ETL                      |
| `LIMIT_ROWS`           | Limita linhas para teste. Use `0` para carga completa |
| `KEEP_LAST_SNAPSHOTS`  | Quantidade de snapshots mantidos por tabela           |



Modos de execução

O projeto possui três modos de execução controlados pela variável RUN_MODE.

FULL

Inicia uma carga nova do zero para a data atual.
RUN_MODE=FULL

Esse modo:

remove as tabelas snapshot da data atual;
remove checkpoints da data atual;
processa todos os arquivos novamente;
recria as tabelas, índices e views.


Continua uma execução interrompida.
RUN_MODE=RESUME

Esse modo:

não apaga as tabelas snapshot;
consulta a tabela etl_checkpoint;
pula partes já processadas com sucesso;
reprocessa apenas partes pendentes ou com erro.

Esse modo é útil quando o processo é interrompido por queda de energia, erro de conexão, 
parada manual ou falha temporária.


RESET

Limpa a execução atual e reinicia.
RUN_MODE=RESET

Esse modo:

remove tabelas snapshot da data atual;
remove checkpoints da data atual;
remove logs de arquivos da data atual;
inicia uma carga nova.

Como executar
Clone o repositório:
git clone https://github.com/CampeloLeonildo/etl-receita-federal-cnpj-postgresql.git
Acesse a pasta do projeto:
cd etl-receita-federal-cnpj-postgresql
Crie um ambiente virtual:
python -m venv .venv
Ative o ambiente virtual:

No Windows:

.venv\Scripts\activate
Instale as dependências:
pip install -r requirements.txt
Crie o arquivo .env com base no .env.example.
Execute o ETL:
python main.py


Para testar sem carregar todos os dados, configure:

LIMIT_ROWS=10000
RUN_MODE=FULL

Assim o processo carrega apenas uma quantidade limitada de linhas por arquivo/parte.

Para carga completa:

LIMIT_ROWS=0
RUN_MODE=FULL
Tabelas geradas

O projeto cria tabelas com sufixo de data no padrão:

<tabela>_A<AAAAMMDD>

Exemplos:

empresa_A20260429
estabelecimento_A20260429
socios_A20260429
simples_A20260429
cnae_A20260429
moti_A20260429
munic_A20260429
natju_A20260429
pais_A20260429
quals_A20260429

Esse padrão permite manter histórico de cargas por data.

Views atuais

Além das tabelas snapshot, o projeto cria views com nomes fixos apontando para o snapshot atual:

vw_empresa_atual
vw_estabelecimento_atual
vw_socios_atual
vw_simples_atual
vw_cnae_atual
vw_moti_atual
vw_munic_atual
vw_natju_atual
vw_pais_atual
vw_quals_atual

Assim, consultas e aplicações podem sempre usar as views, sem precisar saber a data do último snapshot.

Exemplo:

SELECT *
FROM public.vw_empresa_atual
LIMIT 100;
Tabelas de controle do ETL

O projeto cria tabelas auxiliares para controle, auditoria e retomada.

etl_execucao_log

Registra o resultado geral da execução.

Campos principais:

snapshot_date
run_mode
status
mensagem
tempo_total_segundos
tempo_total_formatado

Consulta exemplo:

SELECT *
FROM public.etl_execucao_log
ORDER BY id DESC;
etl_arquivo_log

Registra tempo e volume processado por arquivo e parte.

Campos principais:

snapshot_date
grupo
arquivo
parte
status
qtd_linhas
tempo_segundos
tempo_formatado
mensagem

Consulta exemplo:

SELECT 
    snapshot_date,
    grupo,
    arquivo,
    parte,
    status,
    qtd_linhas,
    tempo_formatado
FROM public.etl_arquivo_log
ORDER BY id DESC;
etl_checkpoint

Controla a retomada da execução.

Campos principais:

snapshot_date
grupo
arquivo
parte
status
qtd_linhas
mensagem

Consulta exemplo:

SELECT *
FROM public.etl_checkpoint
ORDER BY data_atualizacao DESC;

Ver partes concluídas:

SELECT 
    grupo,
    arquivo,
    COUNT(*) AS partes_concluidas,
    SUM(qtd_linhas) AS total_linhas
FROM public.etl_checkpoint
WHERE status = 'SUCESSO'
GROUP BY grupo, arquivo
ORDER BY grupo, arquivo;

Ver partes com erro:

SELECT *
FROM public.etl_checkpoint
WHERE status = 'ERRO'
ORDER BY data_atualizacao DESC;
Tabelas principais

Para maiores informações, consulte o layout oficial dos dados da Receita Federal.

Tabela	Descrição
empresa	Dados cadastrais da empresa em nível de matriz
estabelecimento	Dados por unidade/estabelecimento, incluindo endereço, telefone, CNAE e situação cadastral
socios	Dados cadastrais dos sócios
simples	Dados de Simples Nacional e MEI
cnae	Código e descrição dos CNAEs
quals	Qualificação de sócios, responsáveis e representantes legais
natju	Naturezas jurídicas
moti	Motivos de situação cadastral
pais	Países
munic	Municípios
Índices criados

O projeto cria índices para melhorar consultas e relacionamentos entre tabelas.

Principais índices:

cnpj_basico em empresa
cnpj_basico em estabelecimento
cnpj_basico em socios
cnpj_basico em simples
índice composto de CNPJ completo em estabelecimento
uf em estabelecimento
municipio em estabelecimento
cnae_fiscal_principal em estabelecimento
índice técnico em _etl_arquivo e _etl_parte
Colunas técnicas adicionadas

Durante a carga, o ETL adiciona colunas técnicas nas tabelas carregadas:

Coluna	Descrição
_etl_snapshot_date	Data do snapshot
_etl_grupo	Grupo do arquivo processado
_etl_arquivo	Nome do arquivo de origem
_etl_parte	Número da parte/chunk processado
_etl_data_carga	Data e hora da carga

Essas colunas são usadas para auditoria e para permitir reprocessamento seguro de partes em caso de falha.

Observações importantes
Os arquivos da Receita Federal são grandes.
A carga completa pode levar várias horas, dependendo da máquina e do banco.
Recomenda-se testar primeiro com LIMIT_ROWS=10000.
Não suba o arquivo .env para o GitHub.
Não suba arquivos .zip, .csv, logs ou pastas de dados.
Use sempre .env.example como referência pública.

Possíveis melhorias futuras
Criar Docker Compose com PostgreSQL
Criar dashboard com Streamlit ou Power BI
Criar camada de validação de volume por tabela
Criar testes automatizados
Adicionar carga incremental
Criar documentação técnica da arquitetura
Criar consultas analíticas de exemplo
Publicar imagens do modelo de dados
Adicionar suporte a execução agendada
Autor

Desenvolvido por Leonildo Campêlo.

Perfil profissional:

DBA SQL Server
Analista de Sistemas
Database Developer
ETL Developer
Data Engineer em evolução