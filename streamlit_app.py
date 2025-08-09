# -*- coding: utf-8 -*-
# SCRIPT 3: DASHBOARD STREAMLIT (VISUALIZADOR)
# Este script recebe dados e alarmes via MQTT, salva em um banco de dados SQLite,
# e visualiza as informações em tempo real com notificações de alarme.

import streamlit as st
import pandas as pd
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import queue
import sqlite3
import altair as alt # Importa a biblioteca Altair
from streamlit_option_menu import option_menu # Importa o novo menu

# --- Configurações ---
BROKER_ADDRESS = "broker.emqx.io"
TOPIC_LEITURAS = "bess/leituras/simulador"
TOPIC_ALARMES = "bess/alarmes/simulador"
DB_NAME = "bess_dados.db"
AUTOR = "Marcus Vinícius de Medeiros"
EMAIL = "marcus.vinicius.medeiros@ee.ufcg.edu.br"
SENHA_ADMIN = "debora"

# --- Funções do Banco de Dados (SQLite) ---

def criar_tabelas():
    """Garante que as tabelas para dados e alarmes existam no banco."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        # Tabela para medições contínuas
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
        # Tabela para registro de alarmes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS alarmes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_bess TEXT NOT NULL,
            tipo_alarme TEXT NOT NULL,
            mensagem TEXT,
            timestamp DATETIME NOT NULL
        )
        """)
        conn.commit()
        print("Tabelas 'medicoes' e 'alarmes' verificadas/criadas com sucesso.")

def inserir_dados(dados):
    """Insere uma nova leitura no banco de dados."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO medicoes (id_bess, tensao, corrente, potencia, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (dados['id_bess'], dados['tensao'], dados['corrente'], dados['potencia'], dados['timestamp']))
        conn.commit()

def inserir_alarme(alarme):
    """Insere um novo alarme no banco de dados."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alarmes (id_bess, tipo_alarme, mensagem, timestamp)
            VALUES (?, ?, ?, ?)
        """, (alarme['id_bess'], alarme['tipo_alarme'], alarme['mensagem'], alarme['timestamp']))
        conn.commit()

def carregar_dados_do_db(tabela='medicoes'):
    """Carrega todos os dados de uma tabela específica para um DataFrame."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {tabela}", conn, parse_dates=['timestamp'])
        return df

def limpar_banco_de_dados(tabela='medicoes'):
    """Apaga todos os registros de uma tabela específica."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {tabela}")
        conn.commit()
        print(f"Tabela '{tabela}' limpa.")

# --- Funções de Inicialização e MQTT ---

def on_connect(client, userdata, flags, rc):
    """Callback executado quando o cliente se conecta ao broker."""
    if rc == 0:
        print("Conectado ao Broker MQTT!")
        client.subscribe([(TOPIC_LEITURAS, 0), (TOPIC_ALARMES, 0)])
    else:
        print(f"Falha na conexão, código de retorno: {rc}\n")

def on_message(client, userdata, msg):
    """'userdata' é a nossa fila (queue). Coloca a mensagem e o tópico na fila."""
    try:
        userdata.put((msg.topic, msg.payload.decode()))
    except Exception as e:
        print(f"Erro ao colocar mensagem na fila: {e}")

def inicializar_estado_sessao():
    """Inicializa tudo que precisa persistir no st.session_state."""
    if 'msg_queue' not in st.session_state:
        st.session_state.msg_queue = queue.Queue()

    if 'mqtt_client' not in st.session_state:
        print("Criando uma nova instância do cliente MQTT e conectando...")
        client = mqtt.Client(userdata=st.session_state.msg_queue)
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start()
            st.session_state.mqtt_client = client
        except Exception as e:
            st.error(f"Não foi possível conectar ao broker MQTT: {e}")

# --- Interface Gráfica do Streamlit ---

st.set_page_config(page_title="BESS - MVM", layout="wide")

# Garante que as tabelas existam e o estado da sessão seja inicializado
criar_tabelas()
inicializar_estado_sessao()

# --- Barra Lateral com o novo menu ---
with st.sidebar:
    st.image("Logo-MVM.png", width=100)
    selected = option_menu(
        menu_title="Menu Principal",
        options=["Gráficos", "Alarmes", "Configurações"],
        icons=['graph-up-arrow', 'bell-fill', 'gear-fill'],
        menu_icon="cloud",
        default_index=0
    )
    # Lógica da notificação de alarme
    if 'new_alarm_timestamp' in st.session_state:
        if time.time() - st.session_state.new_alarm_timestamp < 5:
            with st.chat_message("alert", avatar="🚨"):
                st.write("Novo alarme recebido!")
        else:
            # Limpa o estado após 5 segundos para a mensagem desaparecer
            del st.session_state.new_alarm_timestamp

# --- Processamento de dados MQTT (executa em toda atualização de página) ---
while not st.session_state.msg_queue.empty():
    topic, payload_str = st.session_state.msg_queue.get()
    dados = json.loads(payload_str)
    dados['timestamp'] = datetime.now()

    if topic == TOPIC_LEITURAS:
        inserir_dados(dados)
    elif topic == TOPIC_ALARMES:
        inserir_alarme(dados)
        # Define o timestamp do alarme para acionar a notificação
        st.session_state.new_alarm_timestamp = time.time()

# --- Página Principal (Título e Autor) ---
st.title(":zap: BESS - Battery Energy Storage System")
st.markdown(f"**Autor:** `{AUTOR}` | **Email:** `{EMAIL}`")
st.markdown("---")

# --- Lógica de Renderização das Páginas ---

# --- Página de Gráficos ---
if selected == "Gráficos":
    selected_bess_grafico = st.selectbox(
        "Selecione o BESS para visualizar:",
        options=['BESS001', 'BESS002'],
        key='grafico_select'
    )

    all_data = carregar_dados_do_db('medicoes')

    if not all_data.empty:
        last_row = all_data.iloc[-1]
        last_message_str = f"ID: {last_row['id_bess']} | Tensão: {last_row['tensao']}V | Corrente: {last_row['corrente']}A | Potência: {last_row['potencia']}kW (às {last_row['timestamp'].strftime('%H:%M:%S')})"
        st.info(f"Última Leitura Salva: {last_message_str}")
    else:
        st.info("Aguardando a primeira mensagem...")

    data_to_display = all_data[all_data['id_bess'] == selected_bess_grafico]

    max_points_grafico = st.session_state.get('max_points', 50)
    chart_data = data_to_display.tail(max_points_grafico)

    if not chart_data.empty:
        st.subheader(f"Gráficos para: {selected_bess_grafico}")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### Tensão (V)")
            # Gráfico base da tensão
            tensao_chart = alt.Chart(chart_data).mark_line(color="#0072B2").encode(
                x=alt.X('timestamp:T', title=None),
                y=alt.Y('tensao:Q', title="Tensão (V)", scale=alt.Scale(zero=False)),
                tooltip=['timestamp', 'tensao']
            ).interactive()

            # Linhas de limite
            limite_superior = st.session_state.get('limite_superior_tensao', 500.0)
            limite_inferior = st.session_state.get('limite_inferior_tensao', 470.0)

            rule_sup = alt.Chart(pd.DataFrame({'y': [limite_superior]})).mark_rule(color="red", strokeDash=[5,5]).encode(y='y')
            rule_inf = alt.Chart(pd.DataFrame({'y': [limite_inferior]})).mark_rule(color="red", strokeDash=[5,5]).encode(y='y')

            # Combina o gráfico da linha com as regras de limite
            st.altair_chart(tensao_chart + rule_sup + rule_inf, use_container_width=True)

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

    st.subheader(f"Histórico de Leituras para: {selected_bess_grafico}")
    if not data_to_display.empty:
        df_display = data_to_display.copy()
        df_display['timestamp'] = df_display['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(df_display.sort_index(ascending=False), use_container_width=True)
    else:
        st.warning(f"Nenhum dado recebido ainda para {selected_bess_grafico}.")

# --- Página de Alarmes ---
elif selected == "Alarmes":
    st.title(":bell: Histórico de Alarmes")
    st.markdown("---")

    alarm_data = carregar_dados_do_db('alarmes')

    if not alarm_data.empty:
        st.dataframe(alarm_data.sort_index(ascending=False), use_container_width=True)
    else:
        st.info("Nenhum alarme registrado até o momento.")

# --- Página de Configurações ---
elif selected == "Configurações":
    st.title(":gear: Configurações Gerais")
    st.markdown("---")

    st.subheader("Parâmetros dos Gráficos")
    st.session_state.max_points = st.number_input(
        "Pontos a exibir nos gráficos (janela de tempo):",
        min_value=10, max_value=1000, 
        value=st.session_state.get('max_points', 50),
        step=10,
        help="Define o número de leituras mais recentes a serem exibidas na página de Gráficos."
    )

    # Novos campos para configurar os limites de tensão
    st.subheader("Limites de Tensão para Gráfico")
    st.session_state.limite_superior_tensao = st.number_input(
        "Limite Superior de Tensão (V)",
        value=st.session_state.get('limite_superior_tensao', 495.0),
        format="%.2f"
    )
    st.session_state.limite_inferior_tensao = st.number_input(
        "Limite Inferior de Tensão (V)",
        value=st.session_state.get('limite_inferior_tensao', 475.0),
        format="%.2f"
    )

    st.markdown("---")

    st.subheader("Gerenciamento do Banco de Dados")
    st.warning("Atenção: As ações a seguir são irreversíveis.")

    password = st.text_input("Digite a senha de administrador para confirmar:", type="password")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Limpar Histórico de Leituras"):
            if password == SENHA_ADMIN:
                limpar_banco_de_dados('medicoes')
                st.success("Histórico de leituras limpo com sucesso!")
            else:
                st.error("Senha incorreta.")
    with col2:
        if st.button("Limpar Histórico de Alarmes"):
            if password == SENHA_ADMIN:
                limpar_banco_de_dados('alarmes')
                st.success("Histórico de alarmes limpo com sucesso!")
            else:
                st.error("Senha incorreta.")

# --- Mecanismo de Atualização Automática ---
time.sleep(1)
try:
    st.rerun()
except st.errors.StreamlitAPIException as e:
    if "RerunData" not in str(e):
        raise
