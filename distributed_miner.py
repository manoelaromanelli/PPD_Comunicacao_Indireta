import paho.mqtt.client as mqtt
import hashlib
import json
import random
import time
from threading import Thread, Lock

# -------------------------------
# Configurações MQTT e do Sistema
# -------------------------------
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

NUM_PARTICIPANTS = 3   # Total de nós no sistema (pode alterar)
CHALLENGE_MIN = 1
CHALLENGE_MAX = 20

# Tópicos MQTT
TOPIC_INIT = "sd/init"
TOPIC_ELECTION = "sd/election"
TOPIC_CHALLENGE = "sd/challenge"
TOPIC_SOLUTION = "sd/solution"
TOPIC_RESULT = "sd/result"

# -------------------------------
# Estado do Nó
# -------------------------------
ClientID = random.randint(0, 65535)
VoteID = random.randint(0, 65535)
leader_id = None
init_received = set()
votes_received = {}
transaction_table = {}
table_lock = Lock()

print(f"[INFO] ClientID: {ClientID} | VoteID: {VoteID}")

# -------------------------------
# Funções de Hash e Desafio
# -------------------------------
def sha1_hash(s):
    return hashlib.sha1(s.encode()).hexdigest()

def generate_challenge():
    return random.randint(CHALLENGE_MIN, CHALLENGE_MAX)

def solve_challenge(challenge):
    # Simple proof-of-work: find nonce such that sha1(str(nonce)) starts with challenge zeros
    nonce = 0
    prefix = "0" * challenge
    while True:
        candidate = f"{ClientID}-{nonce}"
        if sha1_hash(candidate).startswith(prefix):
            return candidate
        nonce += 1

# -------------------------------
# Callbacks MQTT
# -------------------------------
def on_connect(client, userdata, flags, rc):
    print("[MQTT] Conectado com broker.")
    client.subscribe([(TOPIC_INIT,0), (TOPIC_ELECTION,0), (TOPIC_CHALLENGE,0),
                      (TOPIC_SOLUTION,0), (TOPIC_RESULT,0)])

def on_message(client, userdata, msg):
    global leader_id
    topic = msg.topic
    payload = json.loads(msg.payload.decode())
    
    # -------------------------------
    # Fase de Inicialização
    # -------------------------------
    if topic == TOPIC_INIT:
        cid = payload["ClientID"]
        if cid != ClientID:
            init_received.add(cid)
        print(f"[INIT] Recebido ClientID: {cid}")

    # -------------------------------
    # Fase de Eleição
    # -------------------------------
    elif topic == TOPIC_ELECTION:
        cid = payload["ClientID"]
        vote = payload["VoteID"]
        votes_received[cid] = vote
        print(f"[ELECTION] Recebido voto {vote} de ClientID {cid}")

    # -------------------------------
    # Recepção de Desafios
    # -------------------------------
    elif topic == TOPIC_CHALLENGE:
        tx_id = payload["TransactionID"]
        challenge = payload["Challenge"]
        print(f"[CHALLENGE] TransactionID: {tx_id}, Challenge: {challenge}")

        # Inicia thread de mineração
        Thread(target=mine_solution, args=(tx_id, challenge)).start()

    # -------------------------------
    # Recepção de Resultados
    # -------------------------------
    elif topic == TOPIC_RESULT:
        print(f"[RESULT] {payload}")

# -------------------------------
# Função de Mineração
# -------------------------------
def mine_solution(tx_id, challenge):
    solution = solve_challenge(challenge)
    msg = {
        "ClientID": ClientID,
        "TransactionID": tx_id,
        "Solution": solution
    }
    client.publish(TOPIC_SOLUTION, json.dumps(msg))
    print(f"[MINE] Enviado solution para TransactionID {tx_id}")

# -------------------------------
# Função do Controlador
# -------------------------------
def controller_loop():
    global transaction_table
    tx_id = 0
    while True:
        challenge = generate_challenge()
        with table_lock:
            transaction_table[tx_id] = {
                "Challenge": challenge,
                "Solution": None,
                "Winner": -1
            }

        # Publica desafio
        msg = {"TransactionID": tx_id, "Challenge": challenge}
        client.publish(TOPIC_CHALLENGE, json.dumps(msg))
        print(f"[CONTROL] Novo desafio enviado: TransactionID {tx_id}, Challenge {challenge}")

        # Aguarda soluções
        time.sleep(10)
        tx_id += 1

# -------------------------------
# Callback para soluções recebidas
# -------------------------------
def handle_solutions():
    def inner(client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        tx_id = payload["TransactionID"]
        solution = payload["Solution"]
        cid = payload["ClientID"]

        with table_lock:
            if tx_id in transaction_table and transaction_table[tx_id]["Winner"] == -1:
                # Verifica solução
                challenge = transaction_table[tx_id]["Challenge"]
                if sha1_hash(solution).startswith("0" * challenge):
                    transaction_table[tx_id]["Winner"] = cid
                    transaction_table[tx_id]["Solution"] = solution
                    result_msg = {
                        "ClientID": cid,
                        "TransactionID": tx_id,
                        "Solution": solution,
                        "Result": 1
                    }
                    client.publish(TOPIC_RESULT, json.dumps(result_msg))
                    print(f"[CONTROL] Solução válida de ClientID {cid} para TransactionID {tx_id}")
                else:
                    result_msg = {
                        "ClientID": cid,
                        "TransactionID": tx_id,
                        "Solution": solution,
                        "Result": 0
                    }
                    client.publish(TOPIC_RESULT, json.dumps(result_msg))
                    print(f"[CONTROL] Solução inválida de ClientID {cid} para TransactionID {tx_id}")
    return inner

# -------------------------------
# Inicialização MQTT
# -------------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.message_callback_add(TOPIC_SOLUTION, handle_solutions())
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# -------------------------------
# Inicialização e Eleição
# -------------------------------
def init_and_election():
    # Envia InitMsg
    init_msg = {"ClientID": ClientID}
    client.publish(TOPIC_INIT, json.dumps(init_msg))
    print("[INIT] InitMsg enviado.")

    # Aguarda todos os participantes
    while len(init_received) < NUM_PARTICIPANTS - 1:
        time.sleep(1)

    # Envia ElectionMsg
    election_msg = {"ClientID": ClientID, "VoteID": VoteID}
    client.publish(TOPIC_ELECTION, json.dumps(election_msg))
    print("[ELECTION] ElectionMsg enviado.")

    # Aguarda votos
    while len(votes_received) < NUM_PARTICIPANTS - 1:
        time.sleep(1)

    # Determina líder
    all_votes = votes_received.copy()
    all_votes[ClientID] = VoteID
    leader = max(all_votes.items(), key=lambda x: (x[1], x[0]))[0]
    print(f"[ELECTION] Líder eleito: ClientID {leader}")
    return leader

# -------------------------------
# Threads principais
# -------------------------------
def main_loop():
    global leader_id
    leader_id = init_and_election()
    if leader_id == ClientID:
        print("[INFO] Eu sou o líder, iniciando controlador.")
        Thread(target=controller_loop, daemon=True).start()
    client.loop_forever()

# -------------------------------
# Execução
# -------------------------------
if __name__ == "__main__":
    main_loop()
