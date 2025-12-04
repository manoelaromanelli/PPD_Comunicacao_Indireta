import paho.mqtt.client as mqtt
import hashlib
import json
import random
import time
from threading import Thread, Lock

# -------------------------------
# Configurações MQTT
# -------------------------------
BROKER = "broker.emqx.io"
PORT = 1883

NUM_NODES = 3
CHALLENGE_RANGE = (1, 20)
MINING_STEP = 50000

# -------------------------------
# Tópicos MQTT
# -------------------------------
TOPICS = {
    "INIT": "sd/init",
    "ELECTION": "sd/election",
    "CHALLENGE": "sd/challenge",
    "SOLUTION": "sd/solution",
    "RESULT": "sd/result"
}

# -------------------------------
# Resultados
# -------------------------------
PENDING = -1
REJECTED = 0
ACCEPTED = 1

# -------------------------------
# Estado do nó
# -------------------------------
node_id = random.randint(0, 65535)
vote_id = random.randint(0, 65535)
leader_id = None
init_nodes = set()
votes = {}
transactions = {}
tx_lock = Lock()

print(f">>> Nó iniciado: {node_id} (VoteID {vote_id})")

# -------------------------------
# Funções auxiliares
# -------------------------------
def hash_sha1(text):
    return hashlib.sha1(text.encode()).hexdigest()

def generate_challenge():
    return random.randint(*CHALLENGE_RANGE)

def solve_challenge(challenge):
    prefix = "0" * challenge
    nonce = 0
    while True:
        candidate = f"{node_id}-{nonce}"
        if hash_sha1(candidate).startswith(prefix):
            return candidate
        nonce += 1

# -------------------------------
# Thread de mineração
# -------------------------------
class MiningTask(Thread):
    def __init__(self, node, tx_id, difficulty):
        super().__init__()
        self.node = node
        self.tx_id = tx_id
        self.difficulty = difficulty
        self.active = True

    def run(self):
        nonce = 0
        while self.active:
            candidate = f"{self.tx_id}:{nonce}"
            h = hash_sha1(candidate)
            if h.startswith("0" * self.difficulty):
                self.node.send_solution(self.tx_id, candidate)
                self.active = False
                break
            nonce += 1
            if nonce % MINING_STEP == 0:
                time.sleep(0.001)

    def stop(self):
        self.active = False

# -------------------------------
# Nó distribuído
# -------------------------------
class DistributedNode:
    def __init__(self, broker, port, total_nodes):
        self.broker = broker
        self.port = port
        self.total_nodes = total_nodes
        self.id = node_id
        self.vote_id = vote_id
        self.is_leader = False
        self.leader = None
        self.current_tx = 0
        self.peers = set()
        self.votes = {}
        self.transactions = {}
        self.mining_thread = None
        self.client = self.setup_mqtt()

    def setup_mqtt(self):
        c = mqtt.Client()
        c.on_connect = self.on_connect
        c.on_message = self.on_message
        return c

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def start(self):
        print(f">>> Inicializando nó {self.id} (Esperando {self.total_nodes} nós)")
        self.connect()
        self.send_init()
        self.wait_for_init()
        self.send_election()
        self.wait_for_votes()
        self.determine_leader()
        if self.is_leader:
            print(f">>> [INFO] Líder ativo: {self.id}. Controlador iniciado.")
            self.create_transaction()
        while True:
            time.sleep(1)

    # -------------------------------
    # MQTT Callbacks
    # -------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(">>> Conectado ao broker MQTT.")
            client.subscribe([(TOPICS[k], 0) for k in TOPICS])
        else:
            print(f"[ERRO] Conexão MQTT falhou ({rc})")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except:
            return
        topic = msg.topic

        if topic == TOPICS["INIT"]:
            self.handle_init(payload)
        elif topic == TOPICS["ELECTION"]:
            self.handle_vote(payload)
        elif topic == TOPICS["CHALLENGE"]:
            self.handle_challenge(payload)
        elif topic == TOPICS["SOLUTION"] and self.is_leader:
            self.handle_solution(payload)
        elif topic == TOPICS["RESULT"]:
            self.handle_result(payload)

    # -------------------------------
    # Processamento de mensagens
    # -------------------------------
    def handle_init(self, data):
        cid = data.get("ClientID")
        if cid and cid != self.id:
            init_nodes.add(cid)
            print(f">>> [INICIALIZAÇÃO] Nó detectado: {cid}")

    def handle_vote(self, data):
        cid = data.get("ClientID")
        vid = data.get("VoteID")
        if cid and vid is not None:
            votes[cid] = vid
            print(f">>> [VOTAÇÃO] Voto recebido de {cid}: {vid}")

    def handle_challenge(self, data):
        tx_id = data.get("TransactionID")
        diff = data.get("Challenge")
        print(f"--- Tarefa recebida T{tx_id} (Dif: {diff}). Mineração iniciada.")
        if self.mining_thread and self.mining_thread.is_alive():
            self.mining_thread.stop()
        self.transactions[tx_id] = {"Challenge": diff, "Solution": "", "Winner": PENDING}
        self.mining_thread = MiningTask(self, tx_id, diff)
        self.mining_thread.start()

    def handle_solution(self, data):
        tx_id = data.get("TransactionID")
        solution = data.get("Solution")
        cid = data.get("ClientID")

        with tx_lock:
            if tx_id not in self.transactions or self.transactions[tx_id]["Winner"] != PENDING:
                return
            challenge = self.transactions[tx_id]["Challenge"]
            if hash_sha1(solution).startswith("0" * challenge):
                self.transactions[tx_id]["Winner"] = cid
                self.transactions[tx_id]["Solution"] = solution
                self.client.publish(TOPICS["RESULT"], json.dumps({
                    "ClientID": cid,
                    "TransactionID": tx_id,
                    "Solution": solution,
                    "Result": ACCEPTED
                }))
                print(f"--- [LIDER] Solução ACEITA de {cid} para T{tx_id}")
                self.create_transaction()
            else:
                self.client.publish(TOPICS["RESULT"], json.dumps({
                    "ClientID": cid,
                    "TransactionID": tx_id,
                    "Solution": solution,
                    "Result": REJECTED
                }))
                print(f"--- [LIDER] Solução REJEITADA de {cid} para T{tx_id}")

    def handle_result(self, data):
        tx_id = data.get("TransactionID")
        cid = data.get("ClientID")
        result = data.get("Result")
        status = "ACEITA" if result == ACCEPTED else "REJEITADA"
        print(f">>> T{tx_id} RESULTADO {status} - Vencedor: {cid}")
        if self.mining_thread and self.mining_thread.tx_id == tx_id:
            self.mining_thread.stop()

    # -------------------------------
    # Funções auxiliares
    # -------------------------------
    def send_init(self):
        self.client.publish(TOPICS["INIT"], json.dumps({"ClientID": self.id}))
        print(">>> [INICIALIZAÇÃO] InitMsg enviado.")

    def wait_for_init(self):
        while len(init_nodes) < self.total_nodes - 1:
            time.sleep(0.5)

    def send_election(self):
        self.client.publish(TOPICS["ELECTION"], json.dumps({"ClientID": self.id, "VoteID": self.vote_id}))
        print(">>> [VOTAÇÃO] ElectionMsg enviado.")

    def wait_for_votes(self):
        while len(votes) < self.total_nodes - 1:
            time.sleep(0.5)

    def determine_leader(self):
        all_votes = votes.copy()
        all_votes[self.id] = self.vote_id
        leader = max(all_votes.items(), key=lambda x: (x[1], x[0]))[0]
        self.leader = leader
        self.is_leader = (self.id == leader)
        print(f">>> [VOTAÇÃO] Líder eleito: {leader} (Sou eu? {self.is_leader})")

    def create_transaction(self):
        self.current_tx += 1
        tx_id = self.current_tx
        diff = generate_challenge()
        self.transactions[tx_id] = {"Challenge": diff, "Solution": "", "Winner": PENDING}
        print(f"--- [LIDER] Criando T{tx_id} (Dificuldade {diff})")
        self.client.publish(TOPICS["CHALLENGE"], json.dumps({"TransactionID": tx_id, "Challenge": diff}))

    def send_solution(self, tx_id, solution):
        print(f"Solução encontrada para T{tx_id}: {solution}")
        self.client.publish(TOPICS["SOLUTION"], json.dumps({
            "ClientID": self.id,
            "TransactionID": tx_id,
            "Solution": solution
        }))

# -------------------------------
# Execução
# -------------------------------
if __name__ == "__main__":
    node = DistributedNode(BROKER, PORT, NUM_NODES)
    try:
        node.start()
    except KeyboardInterrupt:
        print(">>> Encerrando nó...")
