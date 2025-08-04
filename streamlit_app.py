# -*- coding: utf-8 -*-
# SCRIPT 3: DASHBOARD STREAMLIT (VISUALIZADOR)
# Este script recebe dados via MQTT, salva em um banco de dados SQLite,
# e visualiza os dados do banco em tempo real.

import streamlit as st
import pandas as pd
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import queue
import sqlite3
import altair as alt # Importa a biblioteca Altair

# --- Configurações ---
BROKER_ADDRESS = "broker.hivemq.com"
TOPIC = "bess/leituras/simulador"
DB_NAME = "bess_dados.db"
AUTOR = "Marcus Vinícius de Medeiros"
EMAIL = "marcus.vinicius.medeiros@ee.ufcg.edu.br"
SENHA_ADMIN = "debora"

# --- Funções do Banco de Dados (SQLite) ---

def criar_tabela():
    """Garante que a tabela para armazenar os dados exista no banco."""
    # check_same_thread=False é necessário para o ambiente multithread do Streamlit
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_bess TEXT NOT NULL,
            tensao REAL,
            corrente REAL,
            potencia REAL,
            timestamp DATETIME NOT NULL
        )
        """)
        conn.commit()
        print("Tabela 'medicoes' verificada/criada com sucesso.")

def inserir_dados(dados):
    """Insere uma nova leitura no banco de dados."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO medicoes (id_bess, tensao, corrente, potencia, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (dados['id_bess'], dados['tensao'], dados['corrente'], dados['potencia'], dados['timestamp']))
        conn.commit()

def carregar_dados_do_db():
    """Carrega todos os dados da tabela 'medicoes' para um DataFrame."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        df = pd.read_sql_query("SELECT * FROM medicoes", conn, parse_dates=['timestamp'])
        return df

def limpar_banco_de_dados():
    """Apaga todos os registros da tabela medicoes."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM medicoes")
        conn.commit()
        print("Banco de dados limpo.")

# --- Funções de Inicialização e MQTT ---

def on_connect(client, userdata, flags, rc):
    """Callback executado quando o cliente se conecta ao broker."""
    if rc == 0:
        print("Conectado ao Broker MQTT!")
        client.subscribe(TOPIC)
    else:
        print(f"Falha na conexão, código de retorno: {rc}\n")

def on_message(client, userdata, msg):
    """
    Callback executado na thread do MQTT.
    'userdata' é a nossa fila (queue). Coloca a mensagem diretamente na fila.
    """
    try:
        dados = json.loads(msg.payload.decode())
        userdata.put(dados)
    except Exception as e:
        print(f"Erro ao colocar mensagem na fila: {e}")

def inicializar_estado_sessao():
    """Inicializa tudo que precisa persistir no st.session_state."""
    if 'data_queue' not in st.session_state:
        st.session_state.data_queue = queue.Queue()
    
    if 'mqtt_client' not in st.session_state:
        print("Criando uma nova instância do cliente MQTT e conectando...")
        # Passamos a fila como 'userdata' para que ela esteja disponível no on_message
        client = mqtt.Client(userdata=st.session_state.data_queue)
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start()
            st.session_state.mqtt_client = client
        except Exception as e:
            st.error(f"Não foi possível conectar ao broker MQTT: {e}")

# --- Interface Gráfica do Streamlit ---

st.set_page_config(page_title="BESS - Monitoramento", layout="wide")

# --- Barra Lateral ---
st.sidebar.title("Opções de Visualização")
selected_bess = st.sidebar.selectbox(
    "Selecione o BESS:",
    options=['BESS001', 'BESS002']
)
max_points = st.sidebar.number_input(
    "Pontos a exibir nos gráficos (janela):",
    min_value=10, max_value=1000, value=50, step=10,
    help="Define o número de leituras mais recentes a serem exibidas nos gráficos."
)
st.sidebar.markdown("---")
st.sidebar.markdown("---")

password = st.sidebar.text_input("Digite a senha de administrador para confirmar:", type="password")
    
if st.sidebar.button("Limpar Histórico de Dados"):
    if password == SENHA_ADMIN:
        limpar_banco_de_dados()
        st.success("Histórico de dados limpo com sucesso!")
    elif not password:
        st.warning("Por favor, digite a senha.")
    else:
        st.error("Senha incorreta. Ação não permitida.")

# --- Página Principal ---
st.title(":zap: BESS - Battery Energy Storage System")
st.markdown(f"**Autor:** `{AUTOR}` | **Email:** `{EMAIL}`")
st.markdown("---")

# Garante que a tabela exista e o estado da sessão seja inicializado
criar_tabela()
inicializar_estado_sessao()

placeholder = st.empty()

# Loop principal da aplicação Streamlit
while True:
    # Processa mensagens da fila e insere no banco de dados
    while not st.session_state.data_queue.empty():
        dados = st.session_state.data_queue.get()
        dados['timestamp'] = datetime.now()
        inserir_dados(dados)

    # Carrega os dados mais recentes do banco
    all_data = carregar_dados_do_db()

    with placeholder.container():
        if not all_data.empty:
            last_row = all_data.iloc[-1]
            last_message_str = f"ID: {last_row['id_bess']} | Tensão: {last_row['tensao']}V | Corrente: {last_row['corrente']}A | Potência: {last_row['potencia']}kW (às {last_row['timestamp'].strftime('%H:%M:%S')})"
            st.info(f"Última Leitura Salva: {last_message_str}")
        else:
            st.info("Aguardando a primeira mensagem...")

        # Filtra o DataFrame com base na seleção do usuário
        data_to_display = all_data[all_data['id_bess'] == selected_bess]
        chart_data = data_to_display.tail(max_points)

        # Verifica se há dados para exibir antes de tentar plotar
        if not chart_data.empty:
            st.subheader(f"Gráficos em Tempo Real para: {selected_bess}")
            
            # Layout dos gráficos: 2 colunas em cima, 1 em baixo
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("##### Tensão (V)")
                tensao_chart = alt.Chart(chart_data).mark_line(color="#0072B2").encode(
                    x=alt.X('timestamp:T', title=None),
                    y=alt.Y('tensao:Q', title="Tensão (V)", scale=alt.Scale(zero=False)),
                    tooltip=['timestamp', 'tensao']
                ).interactive()
                st.altair_chart(tensao_chart, use_container_width=True)

            with col2:
                st.markdown("##### Corrente (A)")
                corrente_chart = alt.Chart(chart_data).mark_line(color="#D55E00").encode(
                    x=alt.X('timestamp:T', title=None),
                    y=alt.Y('corrente:Q', title="Corrente (A)", scale=alt.Scale(zero=False)),
                    tooltip=['timestamp', 'corrente']
                ).interactive()
                st.altair_chart(corrente_chart, use_container_width=True)
            
            st.markdown("##### Potência (kW)")
            potencia_chart = alt.Chart(chart_data).mark_line(color="#009E73").encode(
                x=alt.X('timestamp:T', title=None),
                y=alt.Y('potencia:Q', title="Potência (kW)", scale=alt.Scale(zero=False)),
                tooltip=['timestamp', 'potencia']
            ).interactive()
            st.altair_chart(potencia_chart, use_container_width=True)
        
        st.subheader(f"Histórico de Leituras para: {selected_bess}")
        
        if not data_to_display.empty:
            df_display = data_to_display.copy()
            df_display['timestamp'] = df_display['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(df_display.sort_index(ascending=False), use_container_width=True)
        else:
            st.warning(f"Nenhum dado recebido ainda para {selected_bess}.")

    time.sleep(1)
