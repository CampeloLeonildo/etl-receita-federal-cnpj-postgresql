import datetime
import gc
import logging
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote
import xml.etree.ElementTree as ET

import pandas as pd
import psycopg2
import requests
import wget
from dotenv import load_dotenv
from sqlalchemy import create_engine


# =============================================================================
# ETL Receita Federal CNPJ -> PostgreSQL
# =============================================================================
# Objetivo do projeto:
#   1. Ler arquivos públicos de CNPJ da Receita Federal.
#   2. Baixar arquivos ZIP automaticamente.
#   3. Extrair os arquivos.
#   4. Carregar os dados no PostgreSQL em tabelas snapshot por data.
#      Exemplo: empresa_A20260429
#   5. Processar arquivos grandes em partes/chunks para evitar excesso de memória.
#   6. Registrar checkpoint por arquivo e parte para permitir retomada da carga.
#   7. Evitar duplicidade ao retomar uma parte que falhou no meio.
#   8. Registrar logs de execução, tempo por etapa, tempo por arquivo e linhas carregadas.
#   9. Criar índices e views finais apontando para o snapshot atual.
#  10. Remover snapshots antigos, mantendo apenas os últimos N.
#
# Modos de execução:
#   RUN_MODE=FULL
#       Inicia uma carga nova do zero para a data atual.
#       Apaga as tabelas snapshot da data atual e remove checkpoints da data atual.
#
#   RUN_MODE=RESUME
#       Continua uma carga interrompida.
#       Não apaga as tabelas snapshot.
#       Pula partes já concluídas com status SUCESSO na tabela etl_checkpoint.
#
#   RUN_MODE=RESET
#       Limpa tabelas snapshot, checkpoints e logs de arquivo da data atual.
#       Depois executa novamente como uma carga nova.
# =============================================================================


# =============================================================================
# Funções utilitárias gerais
# =============================================================================

def makedirs(path):
    """Cria um diretório caso ele ainda não exista."""
    if path and not os.path.exists(path):
        os.makedirs(path)


def clear_directory(path):
    """
    Limpa todos os arquivos e subpastas de um diretório.

    O processo usa isso antes da extração dos ZIPs para evitar mistura entre
    arquivos de cargas anteriores e arquivos da carga atual.
    """
    makedirs(path)

    for nome in os.listdir(path):
        full_path = os.path.join(path, nome)

        try:
            if os.path.isfile(full_path) or os.path.islink(full_path):
                os.remove(full_path)
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)
        except Exception as e:
            print(f'Erro ao remover {full_path}: {e}')
            logging.exception(f'Erro ao remover {full_path}')


def setup_logging(log_file='etl_receita_federal.log'):
    """Configura o arquivo de log do ETL."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )


def format_duration(seconds):
    """Formata segundos no padrão HH:MM:SS."""
    seconds = int(round(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f'{hours:02d}:{minutes:02d}:{secs:02d}'


def print_and_log(message):
    """Imprime uma mensagem no terminal e também grava no log."""
    print(message)
    logging.info(message)


def print_and_log_elapsed(label, start_time):
    """Calcula, imprime e grava no log o tempo gasto em uma etapa."""
    elapsed = time.time() - start_time
    message = f'{label}: {format_duration(elapsed)} ({round(elapsed)} segundos)'
    print_and_log(message)
    return elapsed


# =============================================================================
# Funções de configuração
# =============================================================================

def get_env(key: str, default=None):
    """Lê uma variável de ambiente e gera erro se ela não existir."""
    value = os.getenv(key, default)
    if value is None:
        raise KeyError(f"Variável '{key}' não encontrada no .env")
    return value


def get_env_int(key: str, default: int) -> int:
    """Lê uma variável de ambiente que deve ser número inteiro."""
    value = os.getenv(key)
    if value is None or str(value).strip() == '':
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Variável '{key}' precisa ser um número inteiro. Valor atual: {value}")


def get_run_mode():
    """Lê e valida o modo de execução: FULL, RESUME ou RESET."""
    run_mode = os.getenv('RUN_MODE', 'FULL').upper().strip()
    if run_mode not in ['FULL', 'RESUME', 'RESET']:
        raise ValueError(f'RUN_MODE inválido: {run_mode}. Use FULL, RESUME ou RESET.')
    return run_mode


# =============================================================================
# Funções para link público/WebDAV da Receita Federal
# =============================================================================

def parse_public_share_url(share_url: str):
    """Quebra o link público da Receita Federal em base_url, token e pasta."""
    parsed = urlparse(share_url)
    token = parsed.path.rstrip('/').split('/')[-1]
    query = parse_qs(parsed.query)
    folder = unquote(query.get('dir', ['/'])[0]).strip('/')
    base_url = f'{parsed.scheme}://{parsed.netloc}'
    return base_url, token, folder


def build_public_dav_folder_url(share_url: str) -> str:
    """Monta a URL WebDAV da pasta pública."""
    base_url, token, folder = parse_public_share_url(share_url)
    dav_url = f'{base_url}/public.php/dav/files/{token}'
    if folder:
        dav_url += '/' + '/'.join(quote(part, safe='') for part in folder.split('/'))
    return dav_url


def build_public_download_url(share_url: str, file_name: str) -> str:
    """Monta a URL de download de um arquivo específico."""
    base_url, token, folder = parse_public_share_url(share_url)
    parts = [f'{base_url}/public.php/dav/files/{token}']
    if folder:
        parts.extend(quote(part, safe='') for part in folder.split('/'))
    parts.append(quote(file_name, safe=''))
    return '/'.join(parts)


def list_public_share_files(share_url: str, extension: str = '.zip') -> list[str]:
    """Lista os arquivos de uma pasta pública WebDAV."""
    dav_folder_url = build_public_dav_folder_url(share_url)
    headers = {
        'Depth': '1',
        'Content-Type': 'application/xml; charset="utf-8"',
        'X-Requested-With': 'XMLHttpRequest',
    }
    body = """<?xml version="1.0" encoding="UTF-8"?>
    <d:propfind xmlns:d="DAV:">
      <d:prop>
        <d:resourcetype />
        <d:getcontentlength />
      </d:prop>
    </d:propfind>"""

    response = requests.request('PROPFIND', dav_folder_url, headers=headers, data=body, timeout=120)
    response.raise_for_status()

    ns = {'d': 'DAV:'}
    root = ET.fromstring(response.content)
    arquivos = []

    for item in root.findall('d:response', ns):
        href = item.findtext('d:href', default='', namespaces=ns)
        resourcetype = item.find('d:propstat/d:prop/d:resourcetype', ns)
        if resourcetype is not None and resourcetype.find('d:collection', ns) is not None:
            continue
        nome_arquivo = unquote(href.rstrip('/').split('/')[-1])
        if nome_arquivo.lower().endswith(extension.lower()):
            arquivos.append(nome_arquivo)

    return sorted(set(arquivos))


def check_diff(url, file_name):
    """Verifica se o arquivo remoto é diferente do arquivo local."""
    if not os.path.isfile(file_name):
        return True

    response = requests.head(url, allow_redirects=True, timeout=120)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)

    if new_size != old_size:
        os.remove(file_name)
        return True
    return False


def bar_progress(current, total, width=80):
    """Barra de progresso usada pelo wget.download."""
    progress_message = 'Downloading: %d%% [%d / %d] bytes - ' % (
        current / total * 100 if total else 0,
        current,
        total
    )
    sys.stdout.write('\r' + progress_message)
    sys.stdout.flush()


# =============================================================================
# Funções de nomes de tabela
# =============================================================================

def build_snapshot_table_name(base_name: str, snapshot_date: str) -> str:
    """Gera o nome da tabela snapshot. Exemplo: empresa_A20260429."""
    return f'{base_name}_A{snapshot_date}'


def build_backup_table_name(base_name: str, snapshot_date: str) -> str:
    """Gera o nome de backup usado pela lógica original."""
    return f'{base_name}_bkp_{snapshot_date}'


# =============================================================================
# Funções de banco de dados: existência, logs, checkpoint e limpeza
# =============================================================================

def table_exists(cur, table_name: str, schema: str = 'public') -> bool:
    """Verifica se uma tabela existe no schema informado."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        );
    """, (schema, table_name))
    return cur.fetchone()[0]


def rename_table_if_exists(cur, conn, old_name: str, new_name: str, schema: str = 'public'):
    """Renomeia uma tabela, se ela existir. Mantido por compatibilidade."""
    if table_exists(cur, new_name, schema):
        print_and_log(f'Removendo tabela já existente para rename: {schema}.{new_name}')
        cur.execute(f'DROP TABLE "{schema}"."{new_name}";')
        conn.commit()

    if table_exists(cur, old_name, schema):
        print_and_log(f'Renomeando {schema}.{old_name} -> {schema}.{new_name}')
        cur.execute(f'ALTER TABLE "{schema}"."{old_name}" RENAME TO "{new_name}";')
        conn.commit()
    else:
        print_and_log(f'Tabela não encontrada para renomear: {schema}.{old_name}')


def backup_existing_tables(cur, conn, base_tables, backup_tables, schema='public'):
    """Executa backup das tabelas base, se elas existirem."""
    print_and_log('\nIniciando backup das tabelas atuais...')
    for base_name in base_tables:
        backup_name = backup_tables[base_name]
        rename_table_if_exists(cur, conn, base_name, backup_name, schema=schema)
    print_and_log('Backup das tabelas atuais concluído.\n')


def create_execution_log_table(cur, conn, schema='public'):
    """Cria a tabela de log geral da execução."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."etl_execucao_log" (
            id SERIAL PRIMARY KEY,
            data_execucao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            snapshot_date VARCHAR(20),
            run_mode VARCHAR(20),
            status VARCHAR(50),
            mensagem TEXT,
            tempo_total_segundos INTEGER,
            tempo_total_formatado VARCHAR(20)
        );
    """)
    conn.commit()


def registrar_execucao(cur, conn, snapshot_date, run_mode, status, mensagem, tempo_total=None, schema='public'):
    """Registra uma execução na tabela etl_execucao_log."""
    create_execution_log_table(cur, conn, schema=schema)
    cur.execute(f"""
        INSERT INTO "{schema}"."etl_execucao_log"
        (snapshot_date, run_mode, status, mensagem, tempo_total_segundos, tempo_total_formatado)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (
        snapshot_date,
        run_mode,
        status,
        mensagem,
        int(round(tempo_total)) if tempo_total is not None else None,
        format_duration(tempo_total) if tempo_total is not None else None
    ))
    conn.commit()


def create_file_execution_log_table(cur, conn, schema='public'):
    """Cria a tabela de log detalhado por arquivo/parte."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."etl_arquivo_log" (
            id SERIAL PRIMARY KEY,
            data_execucao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            snapshot_date VARCHAR(20),
            grupo VARCHAR(80),
            arquivo TEXT,
            parte INTEGER NULL,
            status VARCHAR(50),
            qtd_linhas BIGINT,
            tempo_segundos INTEGER,
            tempo_formatado VARCHAR(20),
            mensagem TEXT
        );
    """)
    conn.commit()


def registrar_arquivo_execucao(
    cur, conn, snapshot_date, grupo, arquivo, status, qtd_linhas,
    tempo_segundos, mensagem, parte=None, schema='public'
):
    """Registra tempo e volume de linhas processadas por arquivo/parte."""
    create_file_execution_log_table(cur, conn, schema=schema)
    cur.execute(f"""
        INSERT INTO "{schema}"."etl_arquivo_log"
        (snapshot_date, grupo, arquivo, parte, status, qtd_linhas, tempo_segundos, tempo_formatado, mensagem)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        snapshot_date,
        grupo,
        arquivo,
        parte,
        status,
        qtd_linhas,
        int(round(tempo_segundos)) if tempo_segundos is not None else None,
        format_duration(tempo_segundos) if tempo_segundos is not None else None,
        mensagem
    ))
    conn.commit()


def create_checkpoint_table(cur, conn, schema='public'):
    """Cria a tabela de checkpoint, base do modo RESUME."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."etl_checkpoint" (
            id SERIAL PRIMARY KEY,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            snapshot_date VARCHAR(20),
            grupo VARCHAR(80),
            arquivo TEXT,
            parte INTEGER DEFAULT 0,
            status VARCHAR(50),
            qtd_linhas BIGINT DEFAULT 0,
            mensagem TEXT,
            UNIQUE (snapshot_date, grupo, arquivo, parte)
        );
    """)
    conn.commit()


def checkpoint_success_exists(cur, snapshot_date, grupo, arquivo, parte, schema='public'):
    """Verifica se uma parte já foi concluída com sucesso."""
    cur.execute(f"""
        SELECT EXISTS (
            SELECT 1
            FROM "{schema}"."etl_checkpoint"
            WHERE snapshot_date = %s
              AND grupo = %s
              AND arquivo = %s
              AND parte = %s
              AND status = 'SUCESSO'
        );
    """, (snapshot_date, grupo, arquivo, parte))
    return cur.fetchone()[0]


def registrar_checkpoint(cur, conn, snapshot_date, grupo, arquivo, parte, status, qtd_linhas, mensagem=None, schema='public'):
    """Registra ou atualiza o checkpoint de uma parte."""
    create_checkpoint_table(cur, conn, schema=schema)
    cur.execute(f"""
        INSERT INTO "{schema}"."etl_checkpoint"
        (snapshot_date, grupo, arquivo, parte, status, qtd_linhas, data_atualizacao, mensagem)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
        ON CONFLICT (snapshot_date, grupo, arquivo, parte)
        DO UPDATE SET
            status = EXCLUDED.status,
            qtd_linhas = EXCLUDED.qtd_linhas,
            data_atualizacao = CURRENT_TIMESTAMP,
            mensagem = EXCLUDED.mensagem;
    """, (snapshot_date, grupo, arquivo, parte, status, qtd_linhas, mensagem))
    conn.commit()


def delete_snapshot_part(cur, conn, table_name, arquivo, parte, schema='public'):
    """
    Remove linhas de uma parte específica antes de reprocessá-la.

    Isso evita duplicidade caso o processo tenha caído no meio de uma inserção.
    """
    if not table_exists(cur, table_name, schema):
        return

    cur.execute(f"""
        DELETE FROM "{schema}"."{table_name}"
        WHERE _etl_arquivo = %s
          AND _etl_parte = %s;
    """, (arquivo, parte))
    conn.commit()


def prepare_run_mode(cur, conn, run_mode, snapshot_date, tables, schema='public'):
    """Prepara o ambiente conforme RUN_MODE."""
    create_checkpoint_table(cur, conn, schema=schema)
    create_file_execution_log_table(cur, conn, schema=schema)

    if run_mode in ['FULL', 'RESET']:
        print_and_log(f'Modo {run_mode}: limpando tabelas snapshot da data atual...')
        for table_name in tables.values():
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE;')

        cur.execute(f'DELETE FROM "{schema}"."etl_checkpoint" WHERE snapshot_date = %s;', (snapshot_date,))

        if run_mode == 'RESET':
            cur.execute(f'DELETE FROM "{schema}"."etl_arquivo_log" WHERE snapshot_date = %s;', (snapshot_date,))

        conn.commit()

    elif run_mode == 'RESUME':
        print_and_log('Modo RESUME ativado. As tabelas snapshot não serão apagadas.')


# =============================================================================
# Views, índices e limpeza de snapshots antigos
# =============================================================================

def create_current_views(cur, conn, tables, schema='public'):
    """Cria views fixas apontando para as tabelas snapshot atuais."""
    print_and_log('\nCriando views atuais...')
    for base_name, snapshot_table in tables.items():
        view_name = f'vw_{base_name}_atual'
        print_and_log(f'Criando/atualizando view {schema}.{view_name} -> {schema}.{snapshot_table}')
        cur.execute(f"""
            CREATE OR REPLACE VIEW "{schema}"."{view_name}" AS
            SELECT *
            FROM "{schema}"."{snapshot_table}";
        """)
    conn.commit()
    print_and_log('Views atuais criadas/atualizadas com sucesso.')


def cleanup_old_snapshots(cur, conn, base_tables, keep_last=2, schema='public'):
    """Remove snapshots antigos, mantendo apenas os últimos N."""
    print_and_log('\nIniciando limpeza de snapshots antigos...')
    for base_name in base_tables:
        pattern = f'{base_name}_A%'
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name LIKE %s
            ORDER BY table_name DESC;
        """, (schema, pattern))

        snapshot_tables = [row[0] for row in cur.fetchall()]
        tables_to_keep = snapshot_tables[:keep_last]
        tables_to_drop = snapshot_tables[keep_last:]

        print_and_log(f'\nTabela base: {base_name}')
        print_and_log(f'Mantendo snapshots: {tables_to_keep}')

        for table_name in tables_to_drop:
            print_and_log(f'Removendo snapshot antigo: {schema}.{table_name}')
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE;')

    conn.commit()
    print_and_log('\nLimpeza de snapshots antigos concluída.')


def create_indexes(cur, conn, tables, schema='public'):
    """Cria índices nas tabelas snapshot."""
    print_and_log('\nCriando índices nas tabelas snapshot...')

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS "{tables['empresa']}_cnpj"
        ON "{schema}"."{tables['empresa']}"(cnpj_basico);

        CREATE INDEX IF NOT EXISTS "{tables['estabelecimento']}_cnpj"
        ON "{schema}"."{tables['estabelecimento']}"(cnpj_basico);

        CREATE INDEX IF NOT EXISTS "{tables['socios']}_cnpj"
        ON "{schema}"."{tables['socios']}"(cnpj_basico);

        CREATE INDEX IF NOT EXISTS "{tables['simples']}_cnpj"
        ON "{schema}"."{tables['simples']}"(cnpj_basico);

        CREATE INDEX IF NOT EXISTS "{tables['estabelecimento']}_cnpj_completo"
        ON "{schema}"."{tables['estabelecimento']}"(cnpj_basico, cnpj_ordem, cnpj_dv);

        CREATE INDEX IF NOT EXISTS "{tables['estabelecimento']}_uf"
        ON "{schema}"."{tables['estabelecimento']}"(uf);

        CREATE INDEX IF NOT EXISTS "{tables['estabelecimento']}_municipio"
        ON "{schema}"."{tables['estabelecimento']}"(municipio);

        CREATE INDEX IF NOT EXISTS "{tables['estabelecimento']}_cnae"
        ON "{schema}"."{tables['estabelecimento']}"(cnae_fiscal_principal);
    """)

    for table_name in tables.values():
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS "{table_name}_etl_arquivo_parte"
            ON "{schema}"."{table_name}"(_etl_arquivo, _etl_parte);
        """)

    conn.commit()

    print_and_log('\nÍndices criados nas tabelas snapshot:')
    for key in ['empresa', 'estabelecimento', 'socios', 'simples']:
        print_and_log(f'- {tables[key]}')


# =============================================================================
# Funções de carga e transformação
# =============================================================================

def to_sql(dataframe, **kwargs):
    """Insere um DataFrame no PostgreSQL em partes menores."""
    size = 4096
    total = len(dataframe)
    name = kwargs.get('name')

    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))

    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(**kwargs)
        index = min((i + 1) * size, total)
        percent = (index * 100) / total if total else 100
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')

    sys.stdout.write('\n')


def apply_limit_rows(default_nrows=None, limit_rows=0):
    """Aplica limite de linhas para modo teste."""
    if limit_rows and limit_rows > 0:
        if default_nrows is None:
            return limit_rows
        return min(default_nrows, limit_rows)
    return default_nrows


def add_etl_metadata(df, snapshot_date, grupo, arquivo, parte):
    """Adiciona colunas técnicas para auditoria, checkpoint e retomada segura."""
    df['_etl_snapshot_date'] = snapshot_date
    df['_etl_grupo'] = grupo
    df['_etl_arquivo'] = arquivo
    df['_etl_parte'] = parte
    df['_etl_data_carga'] = datetime.datetime.now()
    return df


def transform_empresa(df):
    """Transforma o arquivo EMPRESA."""
    df = df.reset_index(drop=True)
    df.columns = [
        'cnpj_basico',
        'razao_social',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social',
        'porte_empresa',
        'ente_federativo_responsavel'
    ]
    df['capital_social'] = df['capital_social'].astype(str).str.replace(',', '.', regex=False)
    df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce')
    return df


def transform_estabelecimento(df):
    """Transforma o arquivo ESTABELECIMENTO."""
    df = df.reset_index(drop=True)
    df.columns = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
        'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
        'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2',
        'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico',
        'situacao_especial', 'data_situacao_especial'
    ]
    return df


def transform_socios(df):
    """Transforma o arquivo SOCIOS."""
    df = df.reset_index(drop=True)
    df.columns = [
        'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social',
        'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade',
        'pais', 'representante_legal', 'nome_do_representante',
        'qualificacao_representante_legal', 'faixa_etaria'
    ]
    return df


def transform_simples(df):
    """Transforma o arquivo SIMPLES NACIONAL."""
    df = df.reset_index(drop=True)
    df.columns = [
        'cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples',
        'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei',
        'data_exclusao_mei'
    ]
    return df


def transform_tabela_codigo_descricao(df):
    """Transforma tabelas auxiliares no padrão codigo/descricao."""
    df = df.reset_index(drop=True)
    df.columns = ['codigo', 'descricao']
    return df


def process_csv_group(
    cur, conn, engine, snapshot_date, grupo, arquivos, table_name,
    extracted_files, dtypes, transform_func, chunk_size_default,
    limit_rows, schema='public'
):
    """
    Processa um grupo de arquivos CSV em chunks com checkpoint por parte.

    Este é o coração do modo RESUME.
    """
    group_start = time.time()

    print_and_log('\n###############################')
    print_and_log(f'## Arquivos de {grupo}')
    print_and_log('###############################')

    for arquivo in arquivos:
        arquivo_start = time.time()
        linhas_processadas_execucao = 0
        partes_processadas_execucao = 0
        part = 0

        print_and_log(f'\nTrabalhando no arquivo: {arquivo} [...]')
        extracted_file_path = os.path.join(extracted_files, arquivo)
        chunk_size = apply_limit_rows(chunk_size_default, limit_rows)

        try:
            reader = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                chunksize=chunk_size,
                header=None,
                dtype=dtypes,
                encoding='latin-1',
            )

            for df in reader:
                if df.empty:
                    break

                if checkpoint_success_exists(cur, snapshot_date, grupo, arquivo, part, schema=schema):
                    print_and_log(f'{grupo} | {arquivo} | parte {part} já processada. Pulando...')
                    part += 1
                    continue

                parte_start = time.time()
                linhas_parte = len(df)

                try:
                    registrar_checkpoint(
                        cur, conn, snapshot_date, grupo, arquivo, part,
                        'PROCESSANDO', linhas_parte, 'Parte em processamento', schema=schema
                    )

                    delete_snapshot_part(cur, conn, table_name, arquivo, part, schema=schema)

                    df = transform_func(df)
                    df = add_etl_metadata(df, snapshot_date, grupo, arquivo, part)

                    print_and_log(
                        f'Inserindo {grupo} | arquivo: {arquivo} | parte: {part} | linhas: {linhas_parte}'
                    )

                    to_sql(df, name=table_name, con=engine, if_exists='append', index=False)

                    parte_elapsed = time.time() - parte_start

                    registrar_checkpoint(
                        cur, conn, snapshot_date, grupo, arquivo, part,
                        'SUCESSO', linhas_parte,
                        f'Parte processada com sucesso em {format_duration(parte_elapsed)}',
                        schema=schema
                    )

                    registrar_arquivo_execucao(
                        cur, conn, snapshot_date, grupo, arquivo, 'SUCESSO',
                        linhas_parte, parte_elapsed, 'Parte carregada com sucesso',
                        parte=part, schema=schema
                    )

                    linhas_processadas_execucao += linhas_parte
                    partes_processadas_execucao += 1

                    print_and_log(
                        f'{grupo} | {arquivo} | parte {part} concluída | '
                        f'tempo: {format_duration(parte_elapsed)} | linhas: {linhas_parte}'
                    )

                    part += 1

                    if limit_rows and limit_rows > 0:
                        break

                except Exception as e:
                    parte_elapsed = time.time() - parte_start

                    registrar_checkpoint(
                        cur, conn, snapshot_date, grupo, arquivo, part,
                        'ERRO', linhas_parte, str(e), schema=schema
                    )

                    registrar_arquivo_execucao(
                        cur, conn, snapshot_date, grupo, arquivo, 'ERRO',
                        linhas_parte, parte_elapsed, str(e), parte=part, schema=schema
                    )

                    logging.exception(f'Erro ao processar {grupo} | {arquivo} | parte {part}')
                    raise

                finally:
                    try:
                        del df
                        gc.collect()
                    except Exception:
                        pass

            arquivo_elapsed = time.time() - arquivo_start

            registrar_arquivo_execucao(
                cur, conn, snapshot_date, grupo, arquivo, 'ARQUIVO_CONCLUIDO',
                linhas_processadas_execucao, arquivo_elapsed,
                f'Arquivo finalizado nesta execução. Partes processadas: {partes_processadas_execucao}',
                parte=None, schema=schema
            )

            print_and_log(
                f'Tempo total do arquivo {arquivo}: '
                f'{format_duration(arquivo_elapsed)} ({round(arquivo_elapsed)} segundos) | '
                f'linhas carregadas nesta execução: {linhas_processadas_execucao} | '
                f'partes carregadas nesta execução: {partes_processadas_execucao}'
            )

        except Exception as e:
            arquivo_elapsed = time.time() - arquivo_start
            registrar_arquivo_execucao(
                cur, conn, snapshot_date, grupo, arquivo, 'ERRO_ARQUIVO',
                linhas_processadas_execucao, arquivo_elapsed, str(e), parte=None, schema=schema
            )
            logging.exception(f'Erro geral no arquivo {arquivo} do grupo {grupo}')
            raise

    print_and_log_elapsed(f'Tempo de execução do grupo {grupo}', group_start)


# =============================================================================
# Organização dos arquivos extraídos
# =============================================================================

def group_extracted_files(extracted_files):
    """Classifica os arquivos extraídos nos grupos esperados."""
    items = [
        name for name in os.listdir(extracted_files)
        if os.path.isfile(os.path.join(extracted_files, name))
    ]

    grupos = {
        'empresa': [],
        'estabelecimento': [],
        'socios': [],
        'simples': [],
        'cnae': [],
        'moti': [],
        'munic': [],
        'natju': [],
        'pais': [],
        'quals': [],
    }

    for item in items:
        nome_upper = item.upper()
        if 'EMPRE' in nome_upper:
            grupos['empresa'].append(item)
        elif 'ESTABELE' in nome_upper:
            grupos['estabelecimento'].append(item)
        elif 'SOCIO' in nome_upper:
            grupos['socios'].append(item)
        elif 'SIMPLES' in nome_upper:
            grupos['simples'].append(item)
        elif 'CNAE' in nome_upper:
            grupos['cnae'].append(item)
        elif 'MOTI' in nome_upper:
            grupos['moti'].append(item)
        elif 'MUNIC' in nome_upper:
            grupos['munic'].append(item)
        elif 'NATJU' in nome_upper:
            grupos['natju'].append(item)
        elif 'PAIS' in nome_upper:
            grupos['pais'].append(item)
        elif 'QUALS' in nome_upper:
            grupos['quals'].append(item)

    for key in grupos:
        grupos[key] = sorted(grupos[key])

    return grupos


def print_file_summary(grupos):
    """Exibe a quantidade de arquivos encontrados por grupo."""
    print_and_log('\nResumo dos arquivos extraídos:')
    for nome, arquivos in grupos.items():
        print_and_log(f'{nome}: {len(arquivos)}')


def validar_arquivos_obrigatorios(grupos):
    """Valida se todos os grupos obrigatórios foram encontrados."""
    obrigatorios = [
        'empresa', 'estabelecimento', 'socios', 'simples', 'cnae',
        'moti', 'munic', 'natju', 'pais', 'quals'
    ]
    for nome in obrigatorios:
        if len(grupos.get(nome, [])) == 0:
            raise RuntimeError(f'Nenhum arquivo encontrado para o grupo: {nome}')


# =============================================================================
# Processo principal
# =============================================================================

def main():
    """Função principal do ETL."""
    setup_logging()

    process_start = time.time()
    cur = None
    conn = None
    snapshot_date = datetime.datetime.now().strftime('%Y%m%d')
    run_mode = 'FULL'

    try:
        logging.info('Processo iniciado')

        # ------------------------------------------------------------------
        # 1. Carregamento do .env
        # ------------------------------------------------------------------
        dotenv_path = Path(os.getenv('ENV_FILE_PATH', r'D:\RFB\code\.env'))
        if not dotenv_path.is_file():
            raise FileNotFoundError(f'.env não encontrado em: {dotenv_path}')

        print_and_log(f'Usando .env em: {dotenv_path}')
        load_dotenv(dotenv_path=dotenv_path, override=True)

        run_mode = get_run_mode()
        public_share_url = os.getenv(
            'PUBLIC_SHARE_URL',
            'https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9?dir=/2026-04'
        )
        limit_rows = get_env_int('LIMIT_ROWS', 0)
        keep_last_snapshots = get_env_int('KEEP_LAST_SNAPSHOTS', 2)

        print_and_log(f'RUN_MODE: {run_mode}')
        print_and_log(f'LIMIT_ROWS: {limit_rows}')
        print_and_log(f'KEEP_LAST_SNAPSHOTS: {keep_last_snapshots}')

        # ------------------------------------------------------------------
        # 2. Tabelas base e nomes snapshot
        # ------------------------------------------------------------------
        base_tables = [
            'empresa', 'estabelecimento', 'socios', 'simples', 'cnae',
            'moti', 'munic', 'natju', 'pais', 'quals'
        ]
        tables = {t: build_snapshot_table_name(t, snapshot_date) for t in base_tables}
        backup_tables = {t: build_backup_table_name(t, snapshot_date) for t in base_tables}

        # ------------------------------------------------------------------
        # 3. Diretórios
        # ------------------------------------------------------------------
        output_files = get_env('OUTPUT_FILES_PATH')
        extracted_files = get_env('EXTRACTED_FILES_PATH')
        makedirs(output_files)
        makedirs(extracted_files)

        print_and_log('Diretórios definidos:')
        print_and_log(f'output_files: {output_files}')
        print_and_log(f'extracted_files: {extracted_files}')

        # ------------------------------------------------------------------
        # 4. Conexão PostgreSQL
        # ------------------------------------------------------------------
        user = get_env('DB_USER')
        passw = get_env('DB_PASSWORD')
        host = get_env('DB_HOST')
        port = get_env('DB_PORT')
        database = get_env('DB_NAME')

        engine = create_engine(f'postgresql://{user}:{passw}@{host}:{port}/{database}')
        conn = psycopg2.connect(dbname=database, user=user, host=host, port=port, password=passw)
        cur = conn.cursor()

        create_execution_log_table(cur, conn, schema='public')
        create_file_execution_log_table(cur, conn, schema='public')
        create_checkpoint_table(cur, conn, schema='public')

        print_and_log(f'\nData do snapshot: {snapshot_date}')
        print_and_log(f'Link de origem: {public_share_url}')

        # ------------------------------------------------------------------
        # 5. Lista arquivos ZIP no link público
        # ------------------------------------------------------------------
        list_start = time.time()
        files = list_public_share_files(public_share_url, extension='.zip')
        if not files:
            raise RuntimeError('Nenhum arquivo .zip foi encontrado no link informado.')

        print_and_log('\nArquivos encontrados no link:')
        for i, f in enumerate(files, start=1):
            print_and_log(f'{i} - {f}')
        print_and_log_elapsed('Tempo para listar arquivos', list_start)

        # ------------------------------------------------------------------
        # 6. Limpa pasta de extração
        # ------------------------------------------------------------------
        clear_start = time.time()
        print_and_log('\nLimpando pasta de extração...')
        clear_directory(extracted_files)
        print_and_log_elapsed('Tempo para limpar pasta de extração', clear_start)

        # ------------------------------------------------------------------
        # 7. Download dos arquivos
        # ------------------------------------------------------------------
        download_start = time.time()
        print_and_log('\nIniciando download dos arquivos...')

        for i, file_name in enumerate(files, start=1):
            arquivo_start = time.time()
            print_and_log(f'Baixando arquivo {i}/{len(files)}: {file_name}')
            url = build_public_download_url(public_share_url, file_name)
            local_file = os.path.join(output_files, file_name)

            if check_diff(url, local_file):
                wget.download(url, out=local_file, bar=bar_progress)
                sys.stdout.write('\n')
                mensagem_download = 'Arquivo baixado com sucesso'
            else:
                mensagem_download = 'Arquivo já existia localmente com o mesmo tamanho'
                print_and_log(mensagem_download)

            arquivo_elapsed = time.time() - arquivo_start
            registrar_arquivo_execucao(
                cur, conn, snapshot_date, 'DOWNLOAD', file_name, 'SUCESSO',
                0, arquivo_elapsed, mensagem_download, parte=None, schema='public'
            )
            print_and_log(
                f'Tempo do download de {file_name}: {format_duration(arquivo_elapsed)} '
                f'({round(arquivo_elapsed)} segundos)'
            )

        print_and_log_elapsed('Tempo total de download', download_start)

        # ------------------------------------------------------------------
        # 8. Backup das tabelas base, se existirem
        # ------------------------------------------------------------------
        backup_existing_tables(cur, conn, base_tables, backup_tables, schema='public')

        # ------------------------------------------------------------------
        # 9. Descompactação dos arquivos
        # ------------------------------------------------------------------
        unzip_start = time.time()
        print_and_log('\nDescompactando arquivos...')

        for i, file_name in enumerate(files, start=1):
            arquivo_start = time.time()
            full_path = os.path.join(output_files, file_name)
            print_and_log(f'Descompactando {i}/{len(files)}: {file_name}')

            try:
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_files)

                arquivo_elapsed = time.time() - arquivo_start
                registrar_arquivo_execucao(
                    cur, conn, snapshot_date, 'DESCOMPACTACAO', file_name,
                    'SUCESSO', 0, arquivo_elapsed, 'Arquivo descompactado com sucesso',
                    parte=None, schema='public'
                )
                print_and_log(
                    f'Tempo de descompactação de {file_name}: {format_duration(arquivo_elapsed)} '
                    f'({round(arquivo_elapsed)} segundos)'
                )
            except Exception as e:
                arquivo_elapsed = time.time() - arquivo_start
                registrar_arquivo_execucao(
                    cur, conn, snapshot_date, 'DESCOMPACTACAO', file_name,
                    'ERRO', 0, arquivo_elapsed, str(e), parte=None, schema='public'
                )
                logging.exception(f'Erro ao descompactar {file_name}')
                raise

        print_and_log_elapsed('Tempo total de descompactação', unzip_start)

        # ------------------------------------------------------------------
        # 10. Agrupa arquivos extraídos e valida
        # ------------------------------------------------------------------
        grupos = group_extracted_files(extracted_files)
        print_file_summary(grupos)
        validar_arquivos_obrigatorios(grupos)

        # ------------------------------------------------------------------
        # 11. Prepara modo de execução
        # ------------------------------------------------------------------
        prepare_run_mode(cur, conn, run_mode, snapshot_date, tables, schema='public')

        # ------------------------------------------------------------------
        # 12. Configuração dos grupos de carga
        # ------------------------------------------------------------------
        empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
        estabelecimento_dtypes = {
            0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32',
            6: 'Int32', 7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32',
            12: object, 13: object, 14: object, 15: object, 16: object, 17: object,
            18: object, 19: object, 20: 'Int32', 21: object, 22: object, 23: object,
            24: object, 25: object, 26: object, 27: object, 28: object, 29: 'Int32'
        }
        socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32', 7: object, 8: object, 9: 'Int32', 10: 'Int32'}
        simples_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'}

        carga_grupos = [
            {'grupo': 'EMPRESA', 'arquivos': grupos['empresa'], 'table_name': tables['empresa'], 'dtypes': empresa_dtypes, 'transform_func': transform_empresa, 'chunk_size_default': 1000000},
            {'grupo': 'ESTABELECIMENTO', 'arquivos': grupos['estabelecimento'], 'table_name': tables['estabelecimento'], 'dtypes': estabelecimento_dtypes, 'transform_func': transform_estabelecimento, 'chunk_size_default': 1000000},
            {'grupo': 'SOCIOS', 'arquivos': grupos['socios'], 'table_name': tables['socios'], 'dtypes': socios_dtypes, 'transform_func': transform_socios, 'chunk_size_default': 1000000},
            {'grupo': 'SIMPLES', 'arquivos': grupos['simples'], 'table_name': tables['simples'], 'dtypes': simples_dtypes, 'transform_func': transform_simples, 'chunk_size_default': 1000000},
            {'grupo': 'CNAE', 'arquivos': grupos['cnae'], 'table_name': tables['cnae'], 'dtypes': 'object', 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
            {'grupo': 'MOTI', 'arquivos': grupos['moti'], 'table_name': tables['moti'], 'dtypes': {0: 'Int32', 1: object}, 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
            {'grupo': 'MUNIC', 'arquivos': grupos['munic'], 'table_name': tables['munic'], 'dtypes': {0: 'Int32', 1: object}, 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
            {'grupo': 'NATJU', 'arquivos': grupos['natju'], 'table_name': tables['natju'], 'dtypes': {0: 'Int32', 1: object}, 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
            {'grupo': 'PAIS', 'arquivos': grupos['pais'], 'table_name': tables['pais'], 'dtypes': {0: 'Int32', 1: object}, 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
            {'grupo': 'QUALS', 'arquivos': grupos['quals'], 'table_name': tables['quals'], 'dtypes': {0: 'Int32', 1: object}, 'transform_func': transform_tabela_codigo_descricao, 'chunk_size_default': 500000},
        ]

        # ------------------------------------------------------------------
        # 13. Carga dos arquivos com checkpoint
        # ------------------------------------------------------------------
        carga_start = time.time()
        for cfg in carga_grupos:
            process_csv_group(
                cur=cur,
                conn=conn,
                engine=engine,
                snapshot_date=snapshot_date,
                grupo=cfg['grupo'],
                arquivos=cfg['arquivos'],
                table_name=cfg['table_name'],
                extracted_files=extracted_files,
                dtypes=cfg['dtypes'],
                transform_func=cfg['transform_func'],
                chunk_size_default=cfg['chunk_size_default'],
                limit_rows=limit_rows,
                schema='public'
            )
        print_and_log_elapsed('Tempo total de carga dos dados', carga_start)

        # ------------------------------------------------------------------
        # 14. Índices, views e limpeza
        # ------------------------------------------------------------------
        index_start = time.time()
        create_indexes(cur, conn, tables, schema='public')
        print_and_log_elapsed('Tempo de criação dos índices', index_start)

        view_start = time.time()
        create_current_views(cur, conn, tables, schema='public')
        print_and_log_elapsed('Tempo de criação das views atuais', view_start)

        cleanup_start = time.time()
        cleanup_old_snapshots(cur, conn, base_tables, keep_last=keep_last_snapshots, schema='public')
        print_and_log_elapsed('Tempo de limpeza de snapshots antigos', cleanup_start)

        # ------------------------------------------------------------------
        # 15. Registro de sucesso
        # ------------------------------------------------------------------
        total_time = time.time() - process_start
        registrar_execucao(
            cur, conn, snapshot_date, run_mode, 'SUCESSO',
            'Carga finalizada com sucesso', total_time, schema='public'
        )

        print_and_log('\nProcesso 100% finalizado! Crédito: Leo Campêlo')
        print_and_log(f'Tempo total do processamento: {format_duration(total_time)} ({round(total_time)} segundos)')
        print_and_log(f'Novas tabelas foram criadas com nomes no padrão *_A{snapshot_date}')
        print_and_log('Views atuais foram criadas no padrão vw_<tabela>_atual')
        print_and_log(f'Foram mantidos os últimos {keep_last_snapshots} snapshots por tabela base')
        print_and_log('Checkpoint disponível na tabela public.etl_checkpoint')
        print_and_log('Log de arquivos disponível na tabela public.etl_arquivo_log')

    except Exception as e:
        total_time = time.time() - process_start
        print(f'\nErro no processo: {e}')
        logging.exception('Erro no processo principal')

        if cur is not None and conn is not None:
            try:
                registrar_execucao(
                    cur, conn, snapshot_date, run_mode, 'ERRO',
                    str(e), total_time, schema='public'
                )
            except Exception:
                logging.exception('Erro ao registrar falha na tabela de log')

        raise

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        logging.info('Conexão com banco encerrada')


if __name__ == '__main__':
    main()
