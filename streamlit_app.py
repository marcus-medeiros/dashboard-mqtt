# -*- coding: utf-8 -*-
# SCRIPT 3: DASHBOARD STREAMLIT (VISUALIZADOR)
# Este script cria uma aplicação web com Streamlit para visualizar
# em tempo real os dados recebidos via MQTT.

import streamlit as st
import pandas as pd
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import queue # Importa a biblioteca de fila thread-safe

# --- Configurações ---
BROKER_ADDRESS = "broker.hivemq.com"
TOPIC = "bess/leituras/simulador"

# --- Fila para comunicação entre a thread MQTT e a thread principal do Streamlit ---
# Esta fila é segura para ser usada por múltiplas threads.
DATA_QUEUE = queue.Queue()

# --- Funções de Inicialização e MQTT ---

def inicializar_dados():
    """Inicializa o DataFrame e a última mensagem no estado da sessão."""
    if 'data' not in st.session_state:
        st.session_state.data = pd.DataFrame(columns=['id_bess', 'tensao', 'corrente', 'potencia', 'timestamp'])
    if 'last_message' not in st.session_state:
        st.session_state.last_message = "Aguardando a primeira mensagem..."

def on_connect(client, userdata, flags, rc):
    """Callback executado quando o cliente se conecta ao broker."""
    if rc == 0:
        print("Conectado ao Broker MQTT!")
        client.subscribe(TOPIC)
        print(f"Escutando o tópico: {TOPIC}")
    else:
        print(f"Falha na conexão, código de retorno: {rc}\n")

def on_message(client, userdata, msg):
    """
    Callback executado na thread do MQTT.
    NÃO PODE ter código do Streamlit aqui.
    Apenas coloca a mensagem recebida na fila.
    """
    try:
        dados = json.loads(msg.payload.decode())
        print(f"Mensagem colocada na fila: {dados}")
        DATA_QUEUE.put(dados) # Coloca o dicionário de dados na fila
    except Exception as e:
        print(f"Erro ao colocar mensagem na fila: {e}")

def conectar_mqtt():
    """Cria e gerencia a conexão do cliente MQTT."""
    if 'mqtt_client_connected' not in st.session_state:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start()
            st.session_state.mqtt_client_connected = True
            print("Cliente MQTT conectado e loop iniciado.")
        except Exception as e:
            st.error(f"Não foi possível conectar ao broker MQTT: {e}")
            st.session_state.mqtt_client_connected = False

# --- Interface Gráfica do Streamlit (Executada na Thread Principal) ---

st.set_page_config(page_title="Monitor MQTT em Tempo Real", layout="wide")
st.title("📊 Monitor de Dados BESS em Tempo Real")
st.markdown(f"Recebendo dados do tópico `{TOPIC}`.")

# Inicializa os dados e a conexão na primeira execução
inicializar_dados()
conectar_mqtt()

if st.session_state.mqtt_client_connected:
    st.success("✅ Conectado ao broker MQTT e recebendo dados.")
else:
    st.error("❌ Desconectado do broker MQTT. Verifique a conexão e o broker.")

if st.button("Limpar Dados da Tabela"):
    st.session_state.data = pd.DataFrame(columns=['id_bess', 'tensao', 'corrente', 'potencia', 'timestamp'])
    st.session_state.last_message = "Aguardando a primeira mensagem..."
    st.toast("Tabela de dados limpa!")

placeholder = st.empty()

# Loop principal da aplicação Streamlit
while True:
    # Processa todas as mensagens que estiverem na fila
    while not DATA_QUEUE.empty():
        dados = DATA_QUEUE.get() # Pega a mensagem mais antiga da fila

        # Agora, na thread principal, podemos atualizar o estado do Streamlit com segurança
        timestamp_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dados['timestamp'] = timestamp_atual
        st.session_state.last_message = f"ID: {dados['id_bess']} | Tensão: {dados['tensao']}V | Corrente: {dados['corrente']}A | Potência: {dados['potencia']}kW (às {timestamp_atual})"
        
        nova_linha = pd.DataFrame([dados])
        st.session_state.data = pd.concat([st.session_state.data, nova_linha], ignore_index=True)

    # Redesenha a interface com os dados atualizados
    with placeholder.container():
        st.info(f"Última Mensagem: {st.session_state.last_message}")
        st.subheader("Histórico de Leituras Recebidas")
        
        if not st.session_state.data.empty:
            st.dataframe(st.session_state.data.sort_index(ascending=False), use_container_width=True)
        else:
            st.warning("Nenhum dado recebido ainda.")

    time.sleep(1)
