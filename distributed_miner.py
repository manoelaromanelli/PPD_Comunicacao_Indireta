import paho.mqtt.client as mqtt
import hashlib
import random
import json
import time
from threading import Thread, Lock

# -------------------------------
# Configurações MQTT e do Sistema
# -------------------------------
HOST_BROKER = "broker.emqx.io"
PORT_BROKER = 1883
TOTAL_NODES = 3
CHALLENGE_MIN = 1
CHALLENGE_MAX = 20
NONCE_STEP = 50000

SUFIXO = "PPD_lab3"

TOPICS = {
    "INIT": f"sd/{SUFIXO}/init",
    "ELECTION": f"sd/{SUFIXO}/election",
    "CHALLENGE": f"sd/{SUFIXO}/challenge",
    "SOLUTION": f"sd/{SUFIXO}/solution",
    "RESULT": f"sd/{SUFIXO}/result",
}

RESULTS = {
    "PENDING": -1,
    "REJECTED": 0,
    "ACCEPTED": 1
}

# -------------------------------
# Funções auxiliares
# -------------------------------
def sha1_hash(s):
    return hashlib.sha1(s.encode()).hexdigest()

def solve_challenge(node_id, challenge):
    prefix = "0" * challenge
    nonce = 0
    while True:
        candidate = f"{node_id}-{nonce}"
        if sha1_hash(candidate).startswith(prefix):
            return candidate
        nonce += 1
        if nonce % NONCE_STEP == 0:
            time.sleep(0.001)

# -------------------------------
# Thread de mineração
# -------------------------------
class MiningThread(Thread):
    def __init__(self, node, tx_id, challenge):
        super().__init__()
        self.node = node
        self.tx_id = tx_id
        self.challenge = challenge
        self.active = True

    def run(self):
        solution = solve_challenge(self.node.id, self.challenge)
        if self.active:
            self.node.send_solution(self.tx_id, solution)

    def stop(self):
        self.active = False

# -------------------------------
# Classe do Nó Distribuído
# -------------------------------
class DistributedNode:
    def __init__(self, total_nodes):
        self.id = random.randint(0, 65535)
        self.vote_id = random.randint(0, 65535)
        self.total_nodes = total_nodes
        self.peers = set()
        self.votes = {}
        self.transactions = {}
        self.mining_thread = None
        self.tx_lock = Lock()
        self.current_tx = 0
        self.leader = None
        self.is_leader = False

        # Inicializa MQTT
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        print(f">>> Inicializando nó {self.id} (Esperando {self.total_nodes} nós)")

    # -------------------------------
    # MQTT
    # -------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(">>> Conectado ao broker MQTT.")
            self.client.subscribe([(TOPICS[key], 0) for key in TOPICS])
        else:
            print(f"Erro ao conectar ({rc})")

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode('utf-8'))
        except:
            return
        topic = msg.topic

        if topic == TOPICS["INIT"]:
            self.handle_init(data)
        elif topic == TOPICS["ELECTION"]:
            self.handle_vote(data)
        elif topic == TOPICS["CHALLENGE"]:
            self.handle_challenge(data)
        elif topic == TOPICS["SOLUTION"]:
            if self.is_leader:
                self.handle_solution(data)
        elif topic == TOPICS["RESULT"]:
            self.handle_result(data)

    # -------------------------------
    # Inicialização e votação
    # -------------------------------
    def start(self):
        self.client.connect(HOST_BROKER, PORT_BROKER, 60)
        self.client.loop_start()

        # Envia InitMsg até detectar todos os nós
        while len(self.peers) < self.total_nodes:
            self.client.publish(TOPICS["INIT"], json.dumps({"ClientID": self.id}))
            print(">>> [INICIALIZAÇÃO] InitMsg enviado.")
            time.sleep(1)

        # Envia o voto
        self.votes[self.id] = self.vote_id
        self.client.publish(TOPICS["ELECTION"], json.dumps({"ClientID": self.id, "VoteID": self.vote_id}))
        print(">>> [VOTAÇÃO] ElectionMsg enviado.")

        # Aguarda todos os votos
        while len(self.votes) < self.total_nodes:
            time.sleep(0.5)

        # Determina líder
        self.leader = max(self.votes.items(), key=lambda x: (x[1], x[0]))[0]
        self.is_leader = (self.id == self.leader)
        print(f">>> [VOTAÇÃO] Líder eleito: {self.leader} (Sou eu? {self.is_leader})")

        # Se líder, inicia primeira transação
        if self.is_leader:
            print(f">>> [INFO] Eu sou o líder. Iniciando controlador...")
            self.create_transaction()

        # Loop infinito
        while True:
            time.sleep(1)

    def handle_init(self, data):
        cid = data.get("ClientID")
        if cid is not None and cid != self.id:
            self.peers.add(cid)

    def handle_vote(self, data):
        cid = data.get("ClientID")
        vote = data.get("VoteID")
        if cid is not None and vote is not None:
            self.votes[cid] = vote
            print(f">>> [VOTAÇÃO] Voto recebido de {cid}: {vote}")

    # -------------------------------
    # Transações e mineração
    # -------------------------------
    def create_transaction(self):
        with self.tx_lock:
            self.current_tx += 1
            difficulty = random.randint(CHALLENGE_MIN, CHALLENGE_MAX)
            tx_id = self.current_tx
            self.transactions[tx_id] = {"Challenge": difficulty, "Solution": "", "Winner": RESULTS["PENDING"]}
            print(f"--- [LIDER] Criando T{tx_id} (Dificuldade {difficulty}) ---")
            self.client.publish(TOPICS["CHALLENGE"], json.dumps({"TransactionID": tx_id, "Challenge": difficulty}))

    def handle_challenge(self, data):
        if self.is_leader:
            return
        tx_id = data.get("TransactionID")
        difficulty = data.get("Challenge")
        print(f"--- T{tx_id} RECEBIDA. Dificuldade {difficulty}. Iniciando mineração...")
        if self.mining_thread and self.mining_thread.is_alive():
            self.mining_thread.stop()
        self.transactions[tx_id] = {"Challenge": difficulty, "Solution": "", "Winner": RESULTS["PENDING"]}
        self.mining_thread = MiningThread(self, tx_id, difficulty)
        self.mining_thread.start()

    def send_solution(self, tx_id, solution):
        print(f"Solução encontrada para T{tx_id}: {solution}")
        self.client.publish(TOPICS["SOLUTION"], json.dumps({
            "ClientID": self.id,
            "TransactionID": tx_id,
            "Solution": solution
        }))

    def handle_solution(self, data):
        tx_id = data.get("TransactionID")
        cid = data.get("ClientID")
        solution = data.get("Solution")
        if tx_id in self.transactions and self.transactions[tx_id]["Winner"] == RESULTS["PENDING"]:
            challenge = self.transactions[tx_id]["Challenge"]
            if sha1_hash(solution).startswith("0" * challenge):
                self.transactions[tx_id]["Winner"] = cid
                self.transactions[tx_id]["Solution"] = solution
                print(f"--- [LIDER] Solução Aceita de {cid} para T{tx_id} ---")
                self.client.publish(TOPICS["RESULT"], json.dumps({
                    "ClientID": cid,
                    "TransactionID": tx_id,
                    "Solution": solution,
                    "Result": RESULTS["ACCEPTED"]
                }))
                time.sleep(1)
                self.create_transaction()
            else:
                print(f"[LIDER] Solução Rejeitada de {cid}")
                self.client.publish(TOPICS["RESULT"], json.dumps({
                    "ClientID": cid,
                    "TransactionID": tx_id,
                    "Solution": solution,
                    "Result": RESULTS["REJECTED"]
                }))

    def handle_result(self, data):
        tx_id = data.get("TransactionID")
        result = data.get("Result")
        winner = data.get("ClientID")
        if result == RESULTS["ACCEPTED"]:
            print(f">>> T{tx_id} FINALIZADA. Vencedor: {winner}")
            if self.mining_thread and self.mining_thread.tx_id == tx_id:
                self.mining_thread.stop()

# -------------------------------
# Execução
# -------------------------------
if __name__ == "__main__":
    node = DistributedNode(TOTAL_NODES)
    try:
        node.start()
    except KeyboardInterrupt:
        print("Encerrando nó...")
