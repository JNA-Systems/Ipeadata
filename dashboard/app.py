import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Dashboard IPEAdata", layout="wide")

@st.cache_data
def load_data():
    dataframes_dict = {}

    paths = {
        # CORREÇÃO AQUI: Adicionado '..' no início do caminho para subir um nível
        "IBGE - Efetivo de Animais": os.path.join("..", "data", "Efetivos", "passo3", "efetivo_animais_municipios.csv"),
        "IBGE - Quantidade Produzida": os.path.join("..", "data", "produção", "passo1", "quantidade_produçao_alimenticio.csv")
    }

    for origin_name, file_path in paths.items():
        if not os.path.exists(file_path):
            st.error(f"Erro: O arquivo '{file_path}' NÃO foi encontrado no local esperado.")
            st.warning("Verifique se o caminho relativo está correto e se o arquivo realmente está lá.")
            continue

        try:
            df = pd.read_csv(file_path) # Se seus arquivos usam ponto e vírgula, adicione sep=';'
            df.columns = df.columns.str.lower()
            df['origem_dados'] = origin_name
            df['fonte'] = "IBGE"

            if origin_name == "IBGE - Efetivo de Animais":
                if 'nome' in df.columns and 'unidade_medida' in df.columns:
                    df['display_name'] = "Efetivo - " + df['nome'].astype(str) + " - " + df['unidade_medida'].astype(str)
                    df['unidade_final'] = df['unidade_medida']
                else:
                    st.warning(f"Colunas 'nome' ou 'unidade_medida' não encontradas para '{origin_name}'. Nomes de exibição podem estar incompletos.")
                    df['display_name'] = "Efetivo - N/A"
                    df['unidade_final'] = "N/A"
            elif origin_name == "IBGE - Quantidade Produzida":
                if 'nome' in df.columns and 'unidade' in df.columns:
                    df['display_name'] = "Produção - " + df['nome'].astype(str) + " - " + df['unidade'].astype(str)
                    df['unidade_final'] = df['unidade']
                else:
                    st.warning(f"Colunas 'nome' ou 'unidade' não encontradas para '{origin_name}'. Nomes de exibição podem estar incompletos.")
                    df['display_name'] = "Produção - N/A"
                    df['unidade_final'] = "N/A"
            
            dataframes_dict[origin_name] = df

        except Exception as e:
            st.error(f"Erro ao carregar ou processar o arquivo '{file_path}': {e}")
            
    return dataframes_dict

# ... (o restante do seu código Streamlit permanece o mesmo) ...

all_dfs = load_data()

st.title("📊 Dashboard de Dados - IPEAdata")

available_origins = list(all_dfs.keys())

if not available_origins:
    st.error("Nenhum dado foi carregado. Por favor, verifique os caminhos dos arquivos e os arquivos CSV.")
    st.stop()

origem_dado_selecionada = st.selectbox(
    "Selecione a origem dos dados (origem_dados):",
    available_origins
)

df_selected_origin = all_dfs.get(origem_dado_selecionada)

if df_selected_origin is None or df_selected_origin.empty:
    st.warning("⚠️ Nenhum dado disponível para a origem selecionada.")
    st.stop()

nomes_disponiveis = df_selected_origin['display_name'].dropna().unique()
if len(nomes_disponiveis) > 0:
    nome_selecionado = st.selectbox(
        "Selecione o tipo específico (animal/grão):",
        nomes_disponiveis
    )
    df_nome = df_selected_origin[df_selected_origin['display_name'] == nome_selecionado]
else:
    st.warning("⚠️ Nenhum tipo específico encontrado para a origem de dados selecionada. Por favor, ajuste os filtros.")
    df_nome = pd.DataFrame()

if not df_nome.empty:
    estados_disponiveis = df_nome['estado'].dropna().unique()
    if len(estados_disponiveis) > 0:
        estado_selecionado = st.selectbox(
            "Selecione o estado:",
            estados_disponiveis
        )
        df_estado = df_nome[df_nome['estado'] == estado_selecionado]

        municipios_disponiveis = df_estado['municipio'].dropna().unique()

        if len(municipios_disponiveis) == 0:
            st.warning("⚠️ Nenhum município encontrado para essa combinação. Por favor, ajuste os filtros.")
        else:
            municipio_selecionado = st.selectbox(
                "Selecione o município:",
                municipios_disponiveis
            )
            df_filtrado = df_estado[df_estado['municipio'] == municipio_selecionado]

            if df_filtrado.empty:
                st.warning("⚠️ Nenhum dado encontrado para o município selecionado. Por favor, ajuste os filtros.")
            else:
                anos_colunas = [col for col in df_filtrado.columns if str(col).isdigit() and int(col) > 1900]
                
                valid_anos = [
                    col for col in anos_colunas
                    if pd.to_numeric(df_filtrado[col], errors='coerce').notna().any()
                ]

                if not valid_anos:
                    st.warning("⚠️ Não foram encontrados dados numéricos para os anos na seleção atual. Por favor, ajuste os filtros.")
                else:
                    df_series = df_filtrado[valid_anos].T
                    df_series.columns = [nome_selecionado]
                    df_series.index = df_series.index.astype(int)

                    st.subheader(f"📈 Evolução temporal de {nome_selecionado} - {municipio_selecionado}, {estado_selecionado}")
                    st.line_chart(df_series)

                    st.subheader("📌 Indicadores Chave")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("�� Total", f"{df_series.sum().iloc[0]:,.2f}")
                    with col2:
                        st.metric("Média", f"{df_series.mean().iloc[0]:,.2f}")
                    with col3:
                        st.metric("🕒 Valor mais recente", f"{df_series.iloc[-1].iloc[0]:,.2f}")

                    st.subheader("Tabela de Dados")
                    display_cols = [
                        'origem_dados', 'display_name', 'fonte', 'unidade_final',
                        'codigo_brasil', 'brasil', 'estado', 'municipio'
                    ]
                    existing_display_cols = [col for col in display_cols if col in df_filtrado.columns]
                    
                    final_table_cols = existing_display_cols + valid_anos

                    st.dataframe(df_filtrado[final_table_cols].reset_index(drop=True))
    else:
        st.warning("⚠️ Nenhum estado encontrado para essa combinação de origem e tipo específico. Por favor, ajuste os filtros.")
else:
    st.info("Por favor, selecione uma 'origem dos dados' e um 'tipo específico' para continuar.")