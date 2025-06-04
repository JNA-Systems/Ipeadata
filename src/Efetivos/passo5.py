from google.cloud import bigquery

# Função para gerar os campos de ano dinamicamente (1974 a 2023)
def anos_schema():
    return [bigquery.SchemaField(str(ano), "FLOAT") for ano in range(1974, 2024)]

# Campos comuns antes dos anos (sem acento e sem letra maiúscula)
schemas = {
    "brasil": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        *anos_schema(),
    ],
    "regioes": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        bigquery.SchemaField("codigo_regiao", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        *anos_schema(),
    ],
    "estados": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        bigquery.SchemaField("codigo_regiao", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        *anos_schema(),
    ],
    "mesorregioes": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        bigquery.SchemaField("codigo_regiao", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("codigo_mesorregiao", "STRING"),
        bigquery.SchemaField("mesorregiao", "STRING"),
        *anos_schema(),
    ],
    "microrregioes": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        bigquery.SchemaField("codigo_regiao", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("codigo_mesorregiao", "STRING"),
        bigquery.SchemaField("mesorregiao", "STRING"),
        bigquery.SchemaField("codigo_microrregiao", "STRING"),
        bigquery.SchemaField("microrregiao", "STRING"),
        *anos_schema(),
    ],
    "efetivo": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("codigo_brasil", "STRING"),
        bigquery.SchemaField("brasil", "STRING"),
        bigquery.SchemaField("codigo_regiao", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("codigo_mesorregiao", "STRING"),
        bigquery.SchemaField("mesorregiao", "STRING"),
        bigquery.SchemaField("codigo_microrregiao", "STRING"),
        bigquery.SchemaField("microrregiao", "STRING"),
        bigquery.SchemaField("codigo_municipio", "STRING"),
        bigquery.SchemaField("municipio", "STRING"),
        *anos_schema(),
    ],
}

# Lista de arquivos CSV e tabelas de destino
arquivos_e_tabelas = [
    {"csv": "data/Efetivos/passo3/efetivo_animais_municipios.csv",              "tabela": "Ipeadata.efetivo_animais_municipios",      "tipo": "efetivo"},
    {"csv": "data/Efetivos/passo4/efetivos_animais_brasil.csv",                 "tabela": "Ipeadata.efetivo_animais_brasil",          "tipo": "brasil"},
    {"csv": "data/Efetivos/passo4/efetivos_animais_regiao.csv",                 "tabela": "Ipeadata.efetivo_animais_regioes",         "tipo": "regioes"},
    {"csv": "data/Efetivos/passo4/efetivos_animais_estado.csv",                 "tabela": "Ipeadata.efetivo_animais_estados",         "tipo": "estados"},
    {"csv": "data/Efetivos/passo4/efetivos_animais_meso.csv",                   "tabela": "Ipeadata.efetivo_animais_mesorregioes",    "tipo": "mesorregioes"},
    {"csv": "data/Efetivos/passo4/efetivos_animais_micro.csv",                  "tabela": "Ipeadata.efetivo_animais_microrregioes",   "tipo": "microrregioes"},
]

# Instancia o client do BigQuery
client = bigquery.Client()
projeto = "site-ds3x"

# Processa cada item (tabela + CSV)
for item in arquivos_e_tabelas:
    tabela_id = f"{projeto}.{item['tabela']}"
    print(f"\n Processando {item['csv']} -> {tabela_id}")

    # Remove a tabela anterior, se existir
    client.delete_table(tabela_id, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schemas[item["tipo"]]
    )

    try:
        with open(item["csv"], "rb") as arquivo:
            job = client.load_table_from_file(arquivo, tabela_id, job_config=job_config)
        job.result()
        print(f" Upload concluído! {job.output_rows} linhas inseridas na tabela '{item['tabela']}'")
    except Exception as e:
        print(f" Erro ao processar {item['csv']}: {e}")
