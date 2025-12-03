import paho.mqtt.client as mqtt
import hashlib
import random
import json
import time
from threading import Thread
import datetime

# --- Função para gerar sufixo dinâmico ---
def gerar_sufixo():
    agora = datetime.datetime.now()
    # Formato: lab_YYYYMMDD_HHMMSS
    return f"lab_{agora.year}{agora.month:02d}{agora.day:02d}_{agora.hour:02d}{agora.minute:02d}{agora.second:02d}"

IDENTIFIER_SUFFIX = gerar_sufixo()
print("Sufixo dos tópicos gerado:", IDENTIFIER_SUFFIX)

# --- Configurações gerais ---
BROKER_URL = "broker.emqx.io"
BROKER_PORT = 1883
DIFFICULTY_RANGE = (1, 20)
NONCE_STEP = 50000
SLEEP_INTERVAL = 0.001

# --- Tópicos MQTT ---
CHANNELS = {
    "INIT": f"sd/{IDENTIFIER_SUFFIX}/init",
    "VOTE": f"sd/{IDENTIFIER_SUFFIX}/voting",
    "TASK": f"sd/{IDENTIFIER_SUFFIX}/challenge",
    "SOLUTION": f"sd/{IDENTIFIER_SUFFIX}/solution",
    "OUTCOME": f"sd/{IDENTIFIER_SUFFIX}/result"
}

# --- Estados e códigos ---
STATES = {
    "INIT": "Inicialização",
    "ELECTION": "Votação",
    "CHALLENGE": "Execução"
}

RESULT_CODES = {
    "PENDING": -1,
    "REJECTED": 0,
    "ACCEPTED": 1
}

# --- Função de hashing ---
def hash_challenge(challenge_str, difficulty):
    prefix = "0" * difficulty
    h = hashlib.sha1(challenge_str.encode("utf-8")).hexdigest()
    return h, h.startswith(prefix)

# --- Thread de mineração ---
class MinerThread(Thread):
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
            hashed, valid = hash_challenge(candidate, self.difficulty)
            if valid:
                self.node.submit_solution(self.tx_id, candidate)
                self.active = False
                break
            nonce += 1
            if nonce % NONCE_STEP == 0:
                time.sleep(SLEEP_INTERVAL)

    def stop(self):
        self.active = False

# --- Classe do nó distribuído ---
class DistributedNode:
    def __init__(self, broker_url, broker_port, total_nodes):
        self.broker_url = broker_url
        self.broker_port = broker_port
        self.total_nodes = total_nodes
        self.node_id = random.randint(0, 65535)
        self.state = STATES["INIT"]
        self.is_leader = False
        self.leader_id = -1
        # Tabela inicial com TransactionID = 0
        self.transactions = {0: {"Difficulty": random.randint(*DIFFICULTY_RANGE), "Solution": "", "Winner": RESULT_CODES["PENDING"]}}
        self.current_tx = 0
        self.peers = set()
        self.votes = {}
        self.mining_thread = None
        self.client = self._setup_mqtt_client()

    def _setup_mqtt_client(self):
        client = mqtt.Client(client_id=f"Node_{self.node_id}", protocol=mqtt.MQTTv311)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        return client

    def connect(self):
        self.client.connect(self.broker_url, self.broker_port, keepalive=60)
        self.client.loop_start()

    def start(self):
        print(f"Nó {self.node_id} iniciando. Número total de nós esperado: {self.total_nodes}.")
        self.connect()
        while True:
            if self.state == STATES["INIT"]:
                self._send_init()
            elif self.state == STATES["ELECTION"]:
                self._cast_vote()
            time.sleep(2)

    def _send_init(self):
        self._publish(CHANNELS["INIT"], {"NodeID": self.node_id})
        # Reenvio contínuo até receber todos os nós
        if len(self.peers) >= self.total_nodes:
            self._start_election()

    def _publish(self, channel, payload):
        self.client.publish(channel, json.dumps(payload))

    def _start_election(self):
        if self.state != STATES["ELECTION"]:
            print(">>> Todos os nós registrados. Iniciando fase de eleição.")
            self.state = STATES["ELECTION"]
            for _ in range(3):
                self._publish(CHANNELS["INIT"], {"NodeID": self.node_id})
                time.sleep(0.1)

    def _cast_vote(self):
        if self.node_id not in self.votes:
            self.votes[self.node_id] = random.randint(0, 65535)
        vote_val = self.votes[self.node_id]
        self._publish(CHANNELS["VOTE"], {"NodeID": self.node_id, "VoteID": vote_val})
        if len(self.votes) >= self.total_nodes:
            self._determine_leader()

    def _determine_leader(self):
        self.leader_id = max(self.votes, key=lambda k: (self.votes[k], k))
        self.is_leader = (self.node_id == self.leader_id)
        print(f">>> Eleição concluída. Líder: {self.leader_id} (Sou eu o líder? {self.is_leader})")
        self._enter_challenge_phase()

    def _enter_challenge_phase(self):
        self.state = STATES["CHALLENGE"]
        if self.is_leader:
            time.sleep(2)
            self._create_transaction()

    def _create_transaction(self):
        self.current_tx += 1
        difficulty = random.randint(*DIFFICULTY_RANGE)
        self.transactions[self.current_tx] = {"Difficulty": difficulty, "Solution": "", "Winner": RESULT_CODES["PENDING"]}
        print(f"--- [LÍDER] Transação T{self.current_tx} criada (Dificuldade {difficulty}) ---")
        self._publish(CHANNELS["TASK"], {"TransactionID": self.current_tx, "Difficulty": difficulty})

    def _handle_task(self, payload):
        if self.is_leader:
            return
        tx_id = payload.get("TransactionID")
        difficulty = payload.get("Difficulty")
        print(f"Recebida transação T{tx_id} com dificuldade {difficulty}. Iniciando mineração...")
        if self.mining_thread and self.mining_thread.is_alive():
            self.mining_thread.stop()
        self.transactions[tx_id] = {"Difficulty": difficulty, "Solution": "", "Winner": RESULT_CODES["PENDING"]}
        self.mining_thread = MinerThread(self, tx_id, difficulty)
        self.mining_thread.start()

    def submit_solution(self, tx_id, solution):
        print(f"Solução encontrada para T{tx_id}: {solution}")
        self._publish(CHANNELS["SOLUTION"], {"NodeID": self.node_id, "TransactionID": tx_id, "Solution": solution})

    def _handle_solution(self, payload):
        node_id = payload.get("NodeID")
        tx_id = payload.get("TransactionID")
        solution = payload.get("Solution")
        if tx_id in self.transactions and self.transactions[tx_id]["Winner"] == RESULT_CODES["PENDING"]:
            if self._validate_solution(tx_id, solution):
                self._approve_solution(node_id, tx_id, solution)
            else:
                self._reject_solution(node_id, tx_id, solution)

    def _validate_solution(self, tx_id, solution):
        difficulty = self.transactions[tx_id]["Difficulty"]
        _, valid = hash_challenge(solution, difficulty)
        return valid

    def _approve_solution(self, node_id, tx_id, solution):
        self.transactions[tx_id]["Winner"] = node_id
        self.transactions[tx_id]["Solution"] = solution
        print(f"--- [LÍDER] Solução aceita de {node_id} para T{tx_id} ---")
        self._publish(CHANNELS["OUTCOME"], {"NodeID": node_id, "TransactionID": tx_id, "Solution": solution, "Result": RESULT_CODES["ACCEPTED"]})
        time.sleep(3)
        self._create_transaction()

    def _reject_solution(self, node_id, tx_id, solution):
        print(f"[LÍDER] Solução rejeitada de {node_id} para T{tx_id}")
        self._publish(CHANNELS["OUTCOME"], {"NodeID": node_id, "TransactionID": tx_id, "Solution": solution, "Result": RESULT_CODES["REJECTED"]})

    def _handle_outcome(self, payload):
        tx_id = payload.get("TransactionID")
        winner = payload.get("NodeID")
        result = payload.get("Result")
        if result == RESULT_CODES["ACCEPTED"]:
            print(f">>> Transação T{tx_id} concluída. Vencedor: {winner}")
            if self.mining_thread and self.mining_thread.tx_id == tx_id:
                self.mining_thread.stop()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT.")
            for topic in CHANNELS.values():
                client.subscribe(topic)
        else:
            print(f"Falha na conexão, código: {rc}")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except:
            return
        topic = msg.topic
        if topic == CHANNELS["INIT"]:
            self._handle_init(data)
        elif topic == CHANNELS["VOTE"]:
            self._handle_vote(data)
        elif topic == CHANNELS["TASK"]:
            self._handle_task(data)
        elif topic == CHANNELS["SOLUTION"] and self.is_leader:
            self._handle_solution(data)
        elif topic == CHANNELS["OUTCOME"]:
            self._handle_outcome(data)

    def _handle_init(self, payload):
        peer_id = payload.get("NodeID")
        if peer_id:
            self.peers.add(peer_id)
            if len(self.peers) >= self.total_nodes and self.state == STATES["INIT"]:
                self._start_election()

    def _handle_vote(self, payload):
        peer_id = payload.get("NodeID")
        vote_id = payload.get("VoteID")
        if peer_id and vote_id is not None:
            self.votes[peer_id] = vote_id

if __name__ == "__main__":
    TOTAL_NODES = 3
    node = DistributedNode(BROKER_URL, BROKER_PORT, TOTAL_NODES)
    try:
        node.start()
    except KeyboardInterrupt:
        print("Encerrando nó...")
