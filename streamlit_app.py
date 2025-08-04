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
BROKER_ADDRESS = "test.mosquitto.org"
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
    """
    Cria e gerencia a conexão do cliente MQTT, garantindo que seja criada apenas uma vez.
    O cliente MQTT é armazenado no st.session_state para persistir entre as execuções do script.
    """
    if 'mqtt_client' not in st.session_state:
        print("Criando uma nova instância do cliente MQTT e conectando...")
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start()
            st.session_state.mqtt_client = client # Armazena a instância do cliente
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

# Adiciona a caixa de seleção para filtrar por BESS
selected_bess = st.selectbox(
    "Selecione o BESS para visualizar:",
    options=['Todos', 'BESS001', 'BESS002']
)

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

        timestamp_atual = datetime.now()
        dados['timestamp'] = timestamp_atual
        
        timestamp_str = timestamp_atual.strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.last_message = f"ID: {dados['id_bess']} | Tensão: {dados['tensao']}V | Corrente: {dados['corrente']}A | Potência: {dados['potencia']}kW (às {timestamp_str})"
        
        nova_linha = pd.DataFrame([dados])
        st.session_state.data = pd.concat([st.session_state.data, nova_linha], ignore_index=True)

    # Redesenha a interface com os dados atualizados
    with placeholder.container():
        st.info(f"Última Mensagem: {st.session_state.last_message}")

        # Filtra o DataFrame com base na seleção do usuário
        data_to_display = st.session_state.data
        if selected_bess != 'Todos':
            data_to_display = st.session_state.data[st.session_state.data['id_bess'] == selected_bess]

        # Verifica se há dados para exibir antes de tentar plotar
        if not data_to_display.empty:
            st.subheader(f"Gráficos em Tempo Real para: {selected_bess}")
            
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("##### Tensão (V)")
                st.line_chart(data_to_display, x='timestamp', y='tensao', use_container_width=True)

            with col2:
                st.markdown("##### Corrente (A)")
                st.line_chart(data_to_display, x='timestamp', y='corrente', use_container_width=True)
            
            with col3:
                st.markdown("##### Potência (kW)")
                st.line_chart(data_to_display, x='timestamp', y='potencia', use_container_width=True)
        
        st.subheader(f"Histórico de Leituras para: {selected_bess}")
        
        if not data_to_display.empty:
            df_display = data_to_display.copy()
            df_display['timestamp'] = df_display['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(df_display.sort_index(ascending=False), use_container_width=True)
        else:
            st.warning(f"Nenhum dado recebido ainda para {selected_bess}.")

    time.sleep(1)
