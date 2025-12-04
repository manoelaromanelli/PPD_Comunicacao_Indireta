import paho.mqtt.client as mqtt
import hashlib
import json
import random
import time
from threading import Thread, Lock

# -------------------------------
# Configurações MQTT
# -------------------------------
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

NUM_PARTICIPANTS = 3
CHALLENGE_MIN = 1
CHALLENGE_MAX = 20

# -------------------------------
# Tópicos MQTT
# -------------------------------
TOPIC_INIT = "sd/init"
TOPIC_ELECTION = "sd/election"
TOPIC_CHALLENGE = "sd/challenge"
TOPIC_SOLUTION = "sd/solution"
TOPIC_RESULT = "sd/result"

# -------------------------------
# Resultados
# -------------------------------
RESULT_PENDING = -1
RESULT_REJECTED = 0
RESULT_ACCEPTED = 1

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

print(f">>> Iniciando nó {ClientID} (VoteID {VoteID})")

# -------------------------------
# Funções de Hash e Desafio
# -------------------------------
def sha1_hash(s):
    return hashlib.sha1(s.encode()).hexdigest()

def generate_challenge():
    return random.randint(CHALLENGE_MIN, CHALLENGE_MAX)

def solve_challenge(challenge):
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
    if rc == 0:
        print(">>> Conectado ao broker MQTT.")
        client.subscribe([(TOPIC_INIT,0), (TOPIC_ELECTION,0),
                          (TOPIC_CHALLENGE,0), (TOPIC_SOLUTION,0),
                          (TOPIC_RESULT,0)])
    else:
        print(f"[ERRO] Conexão MQTT falhou ({rc})")

def on_message(client, userdata, msg):
    global leader_id
    topic = msg.topic
    payload = json.loads(msg.payload.decode())

    if topic == TOPIC_INIT:
        cid = payload["ClientID"]
        if cid != ClientID:
            init_received.add(cid)
        print(f">>> [INICIALIZAÇÃO] Recebido ClientID {cid}")

    elif topic == TOPIC_ELECTION:
        cid = payload["ClientID"]
        vote = payload["VoteID"]
        votes_received[cid] = vote
        print(f">>> [VOTAÇÃO] Recebido voto {vote} de ClientID {cid}")

    elif topic == TOPIC_CHALLENGE:
        tx_id = payload["TransactionID"]
        challenge = payload["Challenge"]
        print(f"--- T{tx_id} RECEBIDA. Dificuldade {challenge}. Iniciando mineração...")
        Thread(target=mine_solution, args=(tx_id, challenge), daemon=True).start()

    elif topic == TOPIC_SOLUTION:
        if ClientID != leader_id:
            return
        handle_solution_leader(payload)

    elif topic == TOPIC_RESULT:
        tx_id = payload.get("TransactionID")
        result = payload.get("Result")
        winner = payload.get("ClientID")
        status = "ACEITA" if result == RESULT_ACCEPTED else "REJEITADA"
        print(f">>> T{tx_id} RESULTADO {status}. Vencedor: {winner}")

# -------------------------------
# Função de Mineração
# -------------------------------
def mine_solution(tx_id, challenge):
    solution = solve_challenge(challenge)
    client.publish(TOPIC_SOLUTION, json.dumps({
        "ClientID": ClientID,
        "TransactionID": tx_id,
        "Solution": solution
    }))
    print(f"--- Solução encontrada para T{tx_id}: {solution}")

# -------------------------------
# Função para líder processar soluções
# -------------------------------
def handle_solution_leader(payload):
    tx_id = payload["TransactionID"]
    solution = payload["Solution"]
    cid = payload["ClientID"]

    with table_lock:
        if tx_id not in transaction_table:
            return
        if transaction_table[tx_id]["Winner"] != RESULT_PENDING:
            return

        challenge = transaction_table[tx_id]["Challenge"]
        if sha1_hash(solution).startswith("0" * challenge):
            transaction_table[tx_id]["Winner"] = cid
            transaction_table[tx_id]["Solution"] = solution
            client.publish(TOPIC_RESULT, json.dumps({
                "ClientID": cid,
                "TransactionID": tx_id,
                "Solution": solution,
                "Result": RESULT_ACCEPTED
            }))
            print(f"--- [LIDER] Solução ACEITA de {cid} para T{tx_id}")
            Thread(target=create_new_transaction, daemon=True).start()
        else:
            client.publish(TOPIC_RESULT, json.dumps({
                "ClientID": cid,
                "TransactionID": tx_id,
                "Solution": solution,
                "Result": RESULT_REJECTED
            }))
            print(f"--- [LIDER] Solução REJEITADA de {cid} para T{tx_id}")

# -------------------------------
# Função do Controlador / líder
# -------------------------------
tx_counter = 0
def create_new_transaction():
    global tx_counter
    tx_id = tx_counter
    tx_counter += 1
    challenge = generate_challenge()
    with table_lock:
        transaction_table[tx_id] = {"Challenge": challenge, "Solution": "", "Winner": RESULT_PENDING}

    client.publish(TOPIC_CHALLENGE, json.dumps({
        "TransactionID": tx_id,
        "Challenge": challenge
    }))
    print(f"--- [LIDER] Criando T{tx_id} (Dificuldade {challenge})")

# -------------------------------
# Inicialização e Eleição com sincronização
# -------------------------------
def init_and_election():
    # Envia InitMsg
    client.publish(TOPIC_INIT, json.dumps({"ClientID": ClientID}))
    print(">>> [INICIALIZAÇÃO] InitMsg enviado.")

    # Espera todos os nós conectarem
    while len(init_received) < NUM_PARTICIPANTS - 1:
        time.sleep(0.5)

    # Envia ElectionMsg
    client.publish(TOPIC_ELECTION, json.dumps({"ClientID": ClientID, "VoteID": VoteID}))
    print(">>> [VOTAÇÃO] ElectionMsg enviado.")

    # Aguarda votos
    while len(votes_received) < NUM_PARTICIPANTS - 1:
        time.sleep(0.5)

    # Determina líder
    all_votes = votes_received.copy()
    all_votes[ClientID] = VoteID
    leader = max(all_votes.items(), key=lambda x: (x[1], x[0]))[0]
    print(f">>> [VOTAÇÃO] Líder eleito: {leader} (Sou eu? {ClientID == leader})")
    return leader

# -------------------------------
# Loop principal
# -------------------------------
def main_loop():
    global leader_id
    client.loop_start()
    leader_id = init_and_election()

    if leader_id == ClientID:
        print(f">>> [INFO] Eu sou o líder. Iniciando controlador...")
        # cria a primeira transação somente após sincronização completa
        create_new_transaction()

    while True:
        time.sleep(1)

# -------------------------------
# Inicialização MQTT
# -------------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# -------------------------------
# Execução
# -------------------------------
if __name__ == "__main__":
    main_loop()
