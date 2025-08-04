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

# --- Configurações ---
# Devem ser as mesmas do script publicador
BROKER_ADDRESS = "broker.hivemq.com"
TOPIC = "bess/leituras/simulador"

# Função para inicializar o DataFrame no estado da sessão do Streamlit.
# Isso garante que nossos dados não sejam perdidos a cada interação do usuário.
def inicializar_dados():
    if 'data' not in st.session_state:
        st.session_state.data = pd.DataFrame(columns=['id_bess', 'tensao', 'corrente', 'potencia', 'timestamp'])

# --- Funções do Cliente MQTT ---

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
    Callback executado quando uma mensagem é recebida.
    Esta função atualiza o DataFrame no estado da sessão.
    """
    try:
        dados = json.loads(msg.payload.decode())
        print(f"Mensagem recebida: {dados}")
        
        # Adiciona o timestamp exato do momento do recebimento
        dados['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Cria um DataFrame de uma única linha com os novos dados
        nova_linha = pd.DataFrame([dados])

        # Concatena a nova linha ao DataFrame existente no estado da sessão
        st.session_state.data = pd.concat([st.session_state.data, nova_linha], ignore_index=True)

    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

def conectar_mqtt():
    """
    Cria e gerencia a conexão do cliente MQTT usando o estado da sessão
    para evitar reconexões a cada atualização da página.
    """
    if 'mqtt_client_connected' not in st.session_state:
        st.session_state.mqtt_client_connected = False

    if not st.session_state.mqtt_client_connected:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect(BROKER_ADDRESS, 1883, 60)
            client.loop_start() # Inicia o loop em uma thread separada
            st.session_state.mqtt_client_connected = True
            print("Cliente MQTT conectado e loop iniciado.")
        except Exception as e:
            st.error(f"Não foi possível conectar ao broker MQTT: {e}")
            st.session_state.mqtt_client_connected = False

# --- Interface Gráfica do Streamlit ---

st.set_page_config(page_title="Monitor MQTT em Tempo Real", layout="wide")

st.title("📊 Monitor de Dados BESS em Tempo Real")
st.markdown(f"Recebendo dados do tópico `{TOPIC}`.")

# Inicializa os dados e a conexão MQTT na primeira execução
inicializar_dados()
conectar_mqtt()

if st.session_state.mqtt_client_connected:
    st.success("✅ Conectado ao broker MQTT e recebendo dados.")
else:
    st.error("❌ Desconectado do broker MQTT. Verifique a conexão e o broker.")

# Botão para limpar os dados da tabela
if st.button("Limpar Dados da Tabela"):
    st.session_state.data = pd.DataFrame(columns=['id_bess', 'tensao', 'corrente', 'potencia', 'timestamp'])
    st.toast("Tabela de dados limpa!")

# Placeholder para a tabela, que será atualizada continuamente
placeholder = st.empty()

# Loop para atualizar a interface a cada segundo
while True:
    with placeholder.container():
        st.subheader("Últimas Leituras Recebidas")
        
        # Exibe a tabela de dados, mostrando as mais recentes primeiro
        # A propriedade `use_container_width` faz a tabela ocupar todo o espaço disponível
        st.dataframe(st.session_state.data.sort_index(ascending=False), use_container_width=True)

    time.sleep(1) # Pausa para não sobrecarregar o navegador
