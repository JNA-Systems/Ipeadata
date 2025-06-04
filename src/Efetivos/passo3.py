import requests
import pandas as pd

# Base da API
BASE_URL = "http://www.ipeadata.gov.br/api/odata4/"

# Lista de tuplas: (código da série, nome para título e arquivo)
SERIES_CODIGOS = [
    ("QUANTBOVINOS", "bovinos"),
    ("QUANTEQUINOS", "equinos"),
    ("QUANTSUINOS", "suínos"),
    ("QUANTOVINOS", "ovinos"),
    ("QUANTCAPRINOS", "caprinos")
]

def obter_valores_serie(serie_codigo):
    url = f"{BASE_URL}ValoresSerie(SERCODIGO='{serie_codigo}')?$top=100000"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['value']
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
        return []

def montar_nome_fonte_unidade(nome_serie):
    nome_serie_formatado = nome_serie.capitalize()
    nome = f"Efetivo - {nome_serie_formatado} - quantidade"
    fonte = "Instituto Brasileiro de Geografia e Estatística"
    unidade = "Cabeca"
    return nome, fonte, unidade

def main():
    hierarquia = pd.read_csv('data/Efetivos/passo2/hierarquia_ibge.csv', dtype=str)
    municipios = hierarquia[hierarquia['Tipo'] == 'Município'][['Codigo', 'Nome', 'Codigo_Pai']]
    microrregioes = hierarquia[hierarquia['Tipo'] == 'Microrregião'][['Codigo', 'Nome', 'Codigo_Pai']]
    mesorregioes = hierarquia[hierarquia['Tipo'] == 'Mesorregião'][['Codigo', 'Nome', 'Codigo_Pai']]
    estados = hierarquia[hierarquia['Tipo'] == 'Estado'][['Codigo', 'Nome', 'Codigo_Pai']]
    regioes = hierarquia[hierarquia['Tipo'] == 'Região'][['Codigo', 'Nome']]

    dfs = []

    for serie_codigo, nome_serie in SERIES_CODIGOS:
        print(f"Consultando série {serie_codigo}...")

        dados_serie = obter_valores_serie(serie_codigo)
        if not dados_serie:
            print(f"Nenhum dado encontrado para {serie_codigo}.")
            continue

        nome, fonte, unidade = montar_nome_fonte_unidade(nome_serie)

        registros = []
        for item in dados_serie:
            if item.get("VALDATA") and item.get("TERCODIGO"):
                registros.append({
                    "sercodigo": item.get("SERCODIGO"),
                    "valdata": item.get("VALDATA"),
                    "tercodigo": str(item.get("TERCODIGO")),
                    "valvalor": item.get("VALVALOR")
                })
        df_valores = pd.DataFrame(registros)

        if df_valores.empty:
            print(f"Nenhum valor válido encontrado para {serie_codigo}.")
            continue

        df_valores['ano'] = pd.to_datetime(df_valores['valdata'], utc=True, errors='coerce').dt.year
        df_valores = df_valores.dropna(subset=['ano'])
        df_valores['ano'] = df_valores['ano'].astype(int)
        df_valores['tercodigo'] = df_valores['tercodigo'].astype(str)

        df = df_valores.merge(
            municipios.rename(columns={
                'Codigo': 'codigo_municipio',
                'Nome': 'municipio',
                'Codigo_Pai': 'codigo_microrregiao'
            }), left_on='tercodigo', right_on='codigo_municipio', how='left'
        )

        df = df.merge(
            microrregioes.rename(columns={
                'Codigo': 'codigo_microrregiao',
                'Nome': 'microrregiao',
                'Codigo_Pai': 'codigo_mesorregiao'
            }), on='codigo_microrregiao', how='left'
        )

        df = df.merge(
            mesorregioes.rename(columns={
                'Codigo': 'codigo_mesorregiao',
                'Nome': 'mesorregiao',
                'Codigo_Pai': 'codigo_estado'
            }), on='codigo_mesorregiao', how='left'
        )

        df = df.merge(
            estados.rename(columns={
                'Codigo': 'codigo_estado',
                'Nome': 'estado',
                'Codigo_Pai': 'codigo_regiao'
            }), on='codigo_estado', how='left'
        )

        df = df.merge(
            regioes.rename(columns={
                'Codigo': 'codigo_regiao',
                'Nome': 'regiao'
            }), on='codigo_regiao', how='left'
        )

        df['codigo_brasil'] = '0'
        df['brasil'] = 'brasil'

        df = df.dropna(subset=[
            'codigo_municipio', 'municipio',
            'codigo_microrregiao', 'microrregiao',
            'codigo_mesorregiao', 'mesorregiao',
            'codigo_estado', 'estado',
            'codigo_regiao', 'regiao'
        ])

        tabela_pivot = df.pivot_table(
            index=[
                'codigo_brasil', 'brasil',
                'codigo_regiao', 'regiao',
                'codigo_estado', 'estado',
                'codigo_mesorregiao', 'mesorregiao',
                'codigo_microrregiao', 'microrregiao',
                'codigo_municipio', 'municipio'
            ],
            columns='ano',
            values='valvalor',
            aggfunc='sum'
        ).reset_index()

        tabela_pivot.insert(0, 'unidade', unidade)
        tabela_pivot.insert(0, 'fonte', fonte)
        tabela_pivot.insert(0, 'nome', nome)

        anos_finais = [ano for ano in range(1974, 2024)]
        colunas_anos_existentes = [col for col in tabela_pivot.columns if isinstance(col, int) and col in anos_finais]
        for ano in anos_finais:
            if ano not in colunas_anos_existentes:
                tabela_pivot[ano] = None

        colunas_final = [
            'nome', 'fonte', 'unidade',
            'codigo_brasil', 'brasil',
            'codigo_regiao', 'regiao',
            'codigo_estado', 'estado',
            'codigo_mesorregiao', 'mesorregiao',
            'codigo_microrregiao', 'microrregiao',
            'codigo_municipio', 'municipio'
        ] + [ano for ano in anos_finais]

        tabela_pivot = tabela_pivot[colunas_final]
        dfs.append(tabela_pivot)

    if dfs:
        tabela_final = pd.concat(dfs, ignore_index=True)
        path_saida = "data/Efetivos/passo3/efetivo_animais_municipios.csv"
        tabela_final.to_csv(path_saida, index=False, encoding='utf-8-sig')
        print(f"Arquivo único '{path_saida}' gerado com sucesso!")
    else:
        print("Nenhuma série disponível para exportar.")

if __name__ == "__main__":
    main()
