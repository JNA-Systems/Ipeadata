import csv
import os
from google.cloud import bigquery

# Função para gerar os campos de ano dinamicamente
def anos_schema():
    return [bigquery.SchemaField(str(ano), "FLOAT") for ano in range(1974, 2025)]

# Schemas organizados por tipo
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
    "municipio": [
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

# Corrigir CSV automaticamente
def corrigir_csv_estrutura(caminho_entrada, caminho_saida, schema):
    with open(caminho_entrada, encoding="utf-8") as f_in:
        leitor = list(csv.reader(f_in))

    cabecalho = leitor[0]
    colunas_esperadas = len(schema)

    with open(caminho_saida, "w", newline='', encoding="utf-8") as f_out:
        escritor = csv.writer(f_out)
        escritor.writerow(cabecalho)

        for linha in leitor[1:]:
            diff = colunas_esperadas - len(linha)
            if diff > 0:
                linha += [""] * diff
            elif diff < 0:
                linha = linha[:colunas_esperadas]
            escritor.writerow(linha)

# Arquivos e tabelas
arquivos_e_tabelas = [
    #Quantidade
    {"csv": "data/produção/passo1/quantidade_produçao_alimenticio.csv",    "tabela": "Ipeadata.quant_prod_municipios",      "tipo": "municipio"},
    {"csv": "data/produção/passo2/quantidade_produçao_brasil.csv",         "tabela": "Ipeadata.quant_prod_brasil",          "tipo": "brasil"},
    {"csv": "data/produção/passo2/quantidade_produçao_regiao.csv",        "tabela": "Ipeadata.quant_prod_regioes",         "tipo": "regioes"},
    {"csv": "data/produção/passo2/quantidade_produçao_estado.csv",        "tabela": "Ipeadata.quant_prod_estados",         "tipo": "estados"},
    {"csv": "data/produção/passo2/quantidade_produçao_meso.csv",          "tabela": "Ipeadata.quant_prod_mesorregioes",    "tipo": "mesorregioes"},
    {"csv": "data/produção/passo2/quantidade_produçao_micro.csv",         "tabela": "Ipeadata.quant_prod_microrregioes",   "tipo": "microrregioes"},
    #Valor
    {"csv": "data/produção/passo3/valor_produçao_municipios.csv",                   "tabela": "Ipeadata.valor_prod_municipios",      "tipo": "municipio"},
    {"csv": "data/produção/passo3/valor_produçao_brasil.csv",                       "tabela": "Ipeadata.valor_prod_brasil",          "tipo": "brasil"},
    {"csv": "data/produção/passo3/valor_produçao_regiao.csv",                       "tabela": "Ipeadata.valor_prod_regioes",         "tipo": "regioes"},
    {"csv": "data/produção/passo3/valor_produçao_estado.csv",                       "tabela": "Ipeadata.valor_prod_estados",         "tipo": "estados"},
    {"csv": "data/produção/passo3/valor_produçao_meso.csv",                         "tabela": "Ipeadata.valor_prod_mesorregioes",    "tipo": "mesorregioes"},
    {"csv": "data/produção/passo3/valor_produçao_micro.csv",                        "tabela": "Ipeadata.valor_prod_microrregioes",   "tipo": "microrregioes"},
]

client = bigquery.Client()
projeto = "site-ds3x"

for item in arquivos_e_tabelas:
    tipo = item["tipo"]
    tabela_id = f"{projeto}.{item['tabela']}"
    caminho_original = item["csv"]

    print(f"\n🔧 Corrigindo {caminho_original}...")
    corrigir_csv_estrutura(caminho_original, caminho_original, schemas[tipo])  # sobrescreve o arquivo

    print(f"📤 Carregando {caminho_original} -> {tabela_id}")
    client.delete_table(tabela_id, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schemas[tipo]
    )

    try:
        with open(caminho_original, "rb") as arquivo:
            job = client.load_table_from_file(arquivo, tabela_id, job_config=job_config)
        job.result()
        print(f"✅ Upload concluído! {job.output_rows} linhas inseridas na tabela '{item['tabela']}'")
    except Exception as e:
        print(f"❌ Erro ao processar {item['csv']}: {e}")


