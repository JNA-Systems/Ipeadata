from google.cloud import bigquery

# Função para gerar dinamicamente as colunas de anos (1974–2023)
def anos_schema():
    return [bigquery.SchemaField(str(ano), "FLOAT") for ano in range(1974, 2024)]

# Schemas para os arquivos de despesas
schemas = {
    "despesas_estado": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("tipo_unidade", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        *anos_schema(),
    ],
    "despesas_municipio": [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("fonte", "STRING"),
        bigquery.SchemaField("unidade", "STRING"),
        bigquery.SchemaField("tipo_unidade", "STRING"),
        bigquery.SchemaField("codigo_estado", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("codigo_municipio", "STRING"),
        bigquery.SchemaField("municipio", "STRING"),
        *anos_schema(),
    ],
}

# Lista com os arquivos e tabelas destino
arquivos = [
    {
        "csv": r"C:\Users\sjalves\Desktop\teste\data\despesas\passo1\despesa_estado.csv",
        "tabela": "Ipeadata.despesa_estado",
        "tipo": "despesas_estado"
    },
    {
        "csv": r"C:\Users\sjalves\Desktop\teste\data\despesas\passo1\despesa_municipio.csv",
        "tabela": "Ipeadata.despesa_municipio",
        "tipo": "despesas_municipio"
    },
]

# Nome do projeto no BigQuery
projeto = "site-ds3x"
client = bigquery.Client()

# Loop para processar os arquivos
for item in arquivos:
    tabela_id = f"{projeto}.{item['tabela']}"
    print(f"\nProcessando {item['csv']} -> {tabela_id}")

    client.delete_table(tabela_id, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schemas[item["tipo"]],
    )

    try:
        with open(item["csv"], "rb") as arquivo:
            job = client.load_table_from_file(arquivo, tabela_id, job_config=job_config)
        job.result()
        print(f"Upload concluído! {job.output_rows} linhas inseridas na tabela '{item['tabela']}'")
    except Exception as e:
        print(f"Erro ao processar {item['csv']}: {e}")
