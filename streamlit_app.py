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

# --- Configurações ---
BROKER_ADDRESS = "test.mosquitto.org"
TOPIC = "bess/leituras/simulador"
DB_NAME = "bess_dados.db"

# --- Funções do Banco de Dados (SQLite) ---

def criar_tabela():
    """Garante que a tabela para armazenar os dados exista no banco."""
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
    """Coloca a mensagem na fila que está no st.session_state."""
    try:
        dados = json.loads(msg.payload.decode())
        st.session_state.data_queue.put(dados)
    except Exception as e:
        print(f"Erro ao colocar mensagem na fila: {e}")

def inicializar_estado_sessao():
    """Inicializa tudo que precisa persistir no st.session_state."""
    if 'data_queue' not in st.session_state:
        st.session_state.data_queue = queue.Queue()
    
    if 'mqtt_client' not in st.session_state:
        print("Criando uma nova instância do cliente MQTT e conectando...")
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start()
            st.session_state.mqtt_client = client
        except Exception as e:
            st.error(f"Não foi possível conectar ao broker MQTT: {e}")

# --- Interface Gráfica do Streamlit ---

st.set_page_config(page_title="Monitor MQTT em Tempo Real", layout="wide")
st.title("📊 Monitor de Dados BESS em Tempo Real")
st.markdown(f"Recebendo dados do tópico `{TOPIC}` e salvando em `{DB_NAME}`.")

# Garante que a tabela exista e o estado da sessão seja inicializado
criar_tabela()
inicializar_estado_sessao()

# Adiciona a caixa de seleção para filtrar por BESS
selected_bess = st.selectbox(
    "Selecione o BESS para visualizar:",
    options=['BESS001', 'BESS002'] # Opção 'Todos' removida
)

if st.button("Limpar Dados do Histórico"):
    limpar_banco_de_dados()
    st.toast("Histórico de dados limpo!")

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

        # Verifica se há dados para exibir antes de tentar plotar
        if not data_to_display.empty:
            st.subheader(f"Gráficos em Tempo Real para: {selected_bess}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown("##### Tensão (V)")
                st.line_chart(data_to_display.set_index('timestamp')['tensao'], use_container_width=True)
            with col2:
                st.markdown("##### Corrente (A)")
                st.line_chart(data_to_display.set_index('timestamp')['corrente'], use_container_width=True)
            with col3:
                st.markdown("##### Potência (kW)")
                st.line_chart(data_to_display.set_index('timestamp')['potencia'], use_container_width=True)
        
        st.subheader(f"Histórico de Leituras para: {selected_bess}")
        
        if not data_to_display.empty:
            df_display = data_to_display.copy()
            df_display['timestamp'] = df_display['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(df_display.sort_index(ascending=False), use_container_width=True)
        else:
            st.warning(f"Nenhum dado recebido ainda para {selected_bess}.")

    time.sleep(1)
