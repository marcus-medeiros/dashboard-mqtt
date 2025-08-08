# -*- coding: utf-8 -*-
# SCRIPT 2: PUBLICADOR (PUBLISHER) - VERSÃO COM ALARMES
# Este script gera dados simulados, com a tensão seguindo uma onda senoidal,
# e publica tanto as leituras quanto os alarmes em tópicos MQTT distintos.

import paho.mqtt.publish as publish
import json
import random
import time
import math # Importa a biblioteca de matemática para usar a função seno

# --- Configurações ---
# Devem ser as mesmas do script receptor
BROKER_ADDRESS = "test.mosquitto.org"
TOPIC_LEITURAS = "bess/leituras/simulador"
TOPIC_ALARMES = "bess/alarmes/simulador"

# --- Parâmetros da Onda Senoidal ---
TENSAO_BASE_BESS1 = 485  # Valor médio da tensão
AMPLITUDE_BESS1 = 15    # Aumentei a amplitude para forçar alarmes
TENSAO_BASE_BESS2 = 220
AMPLITUDE_BESS2 = 25
FREQUENCIA = 0.5        # Controla a "velocidade" da onda

# --- Limites para Geração de Alarmes ---
LIMITE_MAX_TENSAO_BESS1 = 497.0
LIMITE_MIN_TENSAO_BESS1 = 400.0
LIMITE_MAX_TENSAO_BESS2 = 300.0
LIMITE_MIN_TENSAO_BESS2 = 190.0


def checar_e_enviar_alarme(id_bess, tensao, limite_max, limite_min):
    """Verifica se a tensão está fora dos limites e envia um alarme se necessário."""
    tipo_alarme = None
    mensagem = None

    if tensao > limite_max:
        tipo_alarme = "Sobretensão"
        mensagem = f"Tensão de {tensao}V excedeu o limite máximo de {limite_max}V."
    elif tensao < limite_min:
        tipo_alarme = "Subtensão"
        mensagem = f"Tensão de {tensao}V está abaixo do limite mínimo de {limite_min}V."

    if tipo_alarme:
        dados_alarme = {
            "id_bess": id_bess,
            "tipo_alarme": tipo_alarme,
            "mensagem": mensagem
        }
        publish.single(TOPIC_ALARMES, json.dumps(dados_alarme), hostname=BROKER_ADDRESS)
        print(f"🚨 [ALARME ENVIADO] para {id_bess}: {tipo_alarme}")


# --- Função Principal ---

if __name__ == "__main__":
    print("🚀 Iniciando o publicador de dados e alarmes...")
    print(f"-> Publicando leituras em: {TOPIC_LEITURAS}")
    print(f"-> Publicando alarmes em: {TOPIC_ALARMES}")
    print("Pressione Ctrl+C para parar.")
    
    contador_tempo = 0 # Variável que avança com o tempo para a função seno
    
    try:
        while True:
            # --- Simula dados para o BESS001 ---
            tensao_senoidal_1 = TENSAO_BASE_BESS1 + AMPLITUDE_BESS1 * math.sin(FREQUENCIA * contador_tempo)
            tensao_final_1 = round(tensao_senoidal_1 + random.uniform(-0.5, 0.5), 2)
            
            dados_bess1 = {
                "id_bess": "BESS001",
                "tensao": tensao_final_1,
                "corrente": round(random.uniform(150.0, 160.0), 2),
                "potencia": round(tensao_final_1 * random.uniform(0.15, 0.16), 2)
            }
            publish.single(TOPIC_LEITURAS, json.dumps(dados_bess1), hostname=BROKER_ADDRESS)
            print(f"-> [Leitura] Dados do BESS001: Tensão={tensao_final_1}V")
            
            # Checa por alarmes no BESS001
            checar_e_enviar_alarme("BESS001", tensao_final_1, LIMITE_MAX_TENSAO_BESS1, LIMITE_MIN_TENSAO_BESS1)
            
            # --- Simula dados para o BESS002 ---
            tensao_senoidal_2 = TENSAO_BASE_BESS2 + AMPLITUDE_BESS2 * math.sin(FREQUENCIA * contador_tempo + math.pi) # Desfasado
            tensao_final_2 = round(tensao_senoidal_2 + random.uniform(-0.5, 0.5), 2)
            
            dados_bess2 = {
                "id_bess": "BESS002",
                "tensao": tensao_final_2,
                "corrente": round(random.uniform(90.0, 105.0), 2),
                "potencia": round(tensao_final_2 * random.uniform(0.18, 0.2), 2)
            }
            publish.single(TOPIC_LEITURAS, json.dumps(dados_bess2), hostname=BROKER_ADDRESS)
            print(f"-> [Leitura] Dados do BESS002: Tensão={tensao_final_2}V")
            
            # Checa por alarmes no BESS002
            checar_e_enviar_alarme("BESS002", tensao_final_2, LIMITE_MAX_TENSAO_BESS2, LIMITE_MIN_TENSAO_BESS2)

            contador_tempo += 0.5 # Incrementa o contador para a onda avançar
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Publicador interrompido pelo utilizador. Fim.")
    except Exception as e:
        print(f"\n❌ Ocorreu um erro no publicador: {e}")
