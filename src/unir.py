import pandas as pd
import os

# Caminhos dos arquivos CSV
# Certifique-se de que esses arquivos estejam no mesmo diretório onde o script está sendo executado,
# ou forneça o caminho completo para eles (ex: "C:/Users/Jonas/Downloads/meus_dados/efetivo_animais_municipios.csv")
caminhos = {
    # "IBGE - Efetivo de Animais": "data/Efetivos/passo3/efetivo_animais_municipios.csv",
    "IBGE - Quantidade Produzida": "data/produção/passo1/quantidade_produçao_alimenticio.csv"
}

# Lista para armazenar os DataFrames processados individualmente
dataframes = []

for origem, caminho in caminhos.items():
    print(f"Lendo: {caminho}")
    try:
        # Tenta ler o arquivo CSV
        df = pd.read_csv(caminho)
    except FileNotFoundError:
        # Se o arquivo não for encontrado, imprime um erro e pula para o próximo
        print(f"Erro: O arquivo '{caminho}' não foi encontrado. Por favor, verifique o caminho e se o arquivo existe.")
        continue # Continua para o próximo item no loop

    # Padroniza todos os nomes das colunas para minúsculas
    # Isso evita problemas com 'NOME' vs 'nome', 'UNIDADE_MEDIDA' vs 'unidade_medida', etc.
    df.columns = df.columns.str.lower()

    # Adiciona a coluna 'origem_dados' para identificar a fonte de cada linha
    df["origem_dados"] = origem

    # Lógica para criar a coluna 'nome_produto' e padronizar 'unidade'
    if origem == "IBGE - Quantidade Produzida":
        # Para dados de produção, cria 'nome_produto' usando 'nome' e 'unidade'
        if "nome" in df.columns and "unidade" in df.columns:
            df["nome_produto"] = "Produção - " + df["nome"].astype(str) + " - " + df["unidade"].astype(str)
        else:
            print(f"Aviso: Colunas 'nome' ou 'unidade' não encontradas em '{origem}'. 'nome_produto' pode estar incompleto.")
            df["nome_produto"] = "Produção - N/A" # Fallback para caso as colunas esperadas não existam

    elif origem == "IBGE - Efetivo de Animais":
        # Para dados de efetivo de animais, cria 'nome_produto' e renomeia a coluna de unidade
        if "nome" in df.columns and "unidade_medida" in df.columns:
            df["nome_produto"] = "Efetivo - " + df["nome"].astype(str) + " - " + df["unidade_medida"].astype(str)
        else:
            print(f"Aviso: Colunas 'nome' ou 'unidade_medida' não encontradas em '{origem}'. 'nome_produto' pode estar incompleto.")
            df["nome_produto"] = "Efetivo - N/A" # Fallback

        # Renomeia a coluna 'unidade_medida' para 'unidade' para que todas as unidades fiquem na mesma coluna final
        if "unidade_medida" in df.columns:
            df.rename(columns={"unidade_medida": "unidade"}, inplace=True)

    # Adiciona o DataFrame processado à lista
    dataframes.append(df)

# Verifica se há DataFrames para concatenar
if not dataframes:
    print("Nenhum arquivo CSV válido foi processado. O arquivo unificado não será gerado.")
else:
    # Concatena todos os DataFrames na lista em um único DataFrame
    df_unificado = pd.concat(dataframes, ignore_index=True)

    # Remove linhas duplicadas do DataFrame unificado
    df_unificado = df_unificado.drop_duplicates()

    # Define a ordem das colunas principais que o usuário deseja no CSV final
    colunas_principais_desejadas = ["origem_dados", "nome_produto", "fonte", "unidade", "codigo_brasil", "brasil"]

    # Pega todas as colunas que realmente existem no DataFrame unificado
    todas_as_colunas_unificadas = df_unificado.columns.tolist()

    # Define quais colunas originais (e.g., 'nome', 'unidade_medida') não devem aparecer no output final,
    # pois seus dados já foram usados para criar 'nome_produto' ou 'unidade'.
    colunas_a_excluir_do_resto = ["nome", "unidade_medida"] # Inclua aqui qualquer outra coluna temporária que não queira no final

    # Constrói a ordem final das colunas:
    # 1. As colunas principais desejadas, na ordem especificada.
    # 2. Todas as outras colunas que não são as principais e não estão na lista de exclusão.
    final_columns_order = []
    for col in colunas_principais_desejadas:
        if col in todas_as_colunas_unificadas:
            final_columns_order.append(col)

    for col in todas_as_colunas_unificadas:
        if col not in final_columns_order and col not in colunas_a_excluir_do_resto:
            final_columns_order.append(col)

    # Aplica a nova ordem das colunas ao DataFrame unificado
    df_unificado = df_unificado[final_columns_order]

    # Define o caminho de saída para o arquivo CSV unificado
    saida = "dashboard/Unificado/arquivo_unificado.csv"

    # Garante que o diretório de saída exista; se não, ele é criado
    os.makedirs(os.path.dirname(saida), exist_ok=True)

    # Salva o DataFrame unificado em um arquivo CSV, usando ponto e vírgula como separador
    df_unificado.to_csv(saida, index=False, sep=';')

    print(f"\n✅ Arquivo final salvo em: {saida}")
