import paho.mqtt.client as mqtt
import hashlib
import random
import json
import time
from threading import Thread

BROKER_HOST = "broker.emqx.io"
BROKER_PORT = 1883

MIN_MAX_DIFF = (1, 4)
SLEEP_STEP = 0.001
NONCE_INTERVAL = 50000

TOPIC_SUFFIX = "1dezembro"

CHANNELS = {
    "GREETING": f"sd/{TOPIC_SUFFIX}/init",
    "VOTE": f"sd/{TOPIC_SUFFIX}/voting",
    "TASK": f"sd/{TOPIC_SUFFIX}/challenge",
    "ANSWER": f"sd/{TOPIC_SUFFIX}/solution",
    "FEEDBACK": f"sd/{TOPIC_SUFFIX}/result",
}

STATES = {
    "START": "Inicialização",
    "VOTING": "Votação",
    "WORKING": "Execução"
}

STATUS = {
    "PENDING": -1,
    "DENIED": 0,
    "APPROVED": 1
}


def check_hash(difficulty, data):
    prefix = "0" * difficulty
    digest = hashlib.sha1(data.encode("utf-8")).hexdigest()
    return digest, digest.startswith(prefix)


class MiningThread(Thread):
    def __init__(self, node, transaction_id, difficulty):
        super().__init__()
        self.node = node
        self.tx_id = transaction_id
        self.diff = difficulty
        self.active = True

    def run(self):
        nonce = 0
        while self.active:
            candidate = f"{self.tx_id}:{nonce}"
            hash_val, valid = check_hash(self.diff, candidate)
            if valid:
                self.node.submit_solution(self.tx_id, candidate)
                self.active = False
                break
            nonce += 1
            if nonce % NONCE_INTERVAL == 0:
                time.sleep(SLEEP_STEP)

    def stop(self):
        self.active = False


class DistributedNode:
    def __init__(self, broker_host, broker_port, expected_nodes):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.expected_nodes = expected_nodes
        self.node_id = random.randint(0, 65535)
        self.state = STATES["START"]
        self.is_leader = False
        self.leader_id = -1
        self.current_tx_id = 0
        self.peers = set()
        self.votes = {}
        self.transactions = {}
        self.mining_task = None
        self.client = self._setup_client()

    def _setup_client(self):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f"Node_{self.node_id}")
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        return client

    def _connect_to_broker(self):
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()

    def start(self):
        print(f"Iniciando nó {self.node_id} (Esperado: {self.expected_nodes})")
        self._connect_to_broker()
        while True:
            if self.state == STATES["START"]:
                self._announce()
            elif self.state == STATES["VOTING"]:
                self._cast_vote()
            time.sleep(2)

    def _announce(self):
        self._publish(CHANNELS["GREETING"], {"NodeID": self.node_id})
        if len(self.peers) >= self.expected_nodes:
            self._begin_voting()

    def _publish(self, channel, message):
        self.client.publish(channel, json.dumps(message))

    def _begin_voting(self):
        if self.state == STATES["VOTING"]:
            return
        print(">>> Rede sincronizada. Iniciando votação...")
        self.state = STATES["VOTING"]
        for _ in range(3):
            self._publish(CHANNELS["GREETING"], {"NodeID": self.node_id})
            time.sleep(0.1)

    def _cast_vote(self):
        if self.node_id not in self.votes:
            self.votes[self.node_id] = random.randint(0, 65535)
        self._publish(CHANNELS["VOTE"], {"NodeID": self.node_id, "Vote": self.votes[self.node_id]})

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT.")
            client.subscribe([(CHANNELS[key], 0) for key in CHANNELS])
        else:
            print(f"Falha na conexão ({rc})")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except:
            return

        topic = msg.topic
        if topic == CHANNELS["GREETING"]:
            self._handle_greeting(data)
        elif topic == CHANNELS["VOTE"]:
            self._handle_vote(data)
        elif topic == CHANNELS["TASK"]:
            self._handle_task(data)
        elif topic == CHANNELS["ANSWER"] and self.is_leader:
            self._handle_solution(data)
        elif topic == CHANNELS["FEEDBACK"]:
            self._handle_feedback(data)

    def _handle_greeting(self, data):
        peer_id = data.get("NodeID")
        if peer_id is not None:
            self.peers.add(peer_id)
            if len(self.peers) >= self.expected_nodes and self.state == STATES["START"]:
                self._begin_voting()

    def _handle_vote(self, data):
        peer_id = data.get("NodeID")
        vote_val = data.get("Vote")
        if peer_id is not None and vote_val is not None:
            self.votes[peer_id] = vote_val
        if len(self.votes) >= self.expected_nodes and self.state == STATES["VOTING"]:
            self._tally_votes()

    def _tally_votes(self):
        winner = max(self.votes, key=lambda k: (self.votes[k], k))
        self.leader_id = winner
        self.is_leader = (self.node_id == self.leader_id)
        print(f">>> Votação finalizada. Líder: {self.leader_id} (Sou eu? {self.is_leader})")
        self._start_execution()

    def _start_execution(self):
        self.state = STATES["WORKING"]
        if self.is_leader:
            time.sleep(2)
            self._create_transaction()

    def _create_transaction(self):
        self.current_tx_id += 1
        difficulty = random.randint(*MIN_MAX_DIFF)
        self.transactions[self.current_tx_id] = {"Challenge": difficulty, "Solution": "", "Winner": STATUS["PENDING"]}
        print(f"--- [LIDER] Nova transação T{self.current_tx_id} (Dificuldade {difficulty}) ---")
        self._publish(CHANNELS["TASK"], {"TransactionID": self.current_tx_id, "Challenge": difficulty})

    def _handle_task(self, data):
        if self.is_leader:
            return
        tx_id = data.get("TransactionID")
        difficulty = data.get("Challenge")
        print(f"Tarefa recebida T{tx_id} (Dif: {difficulty}). Iniciando mineração...")

        if self.mining_task and self.mining_task.is_alive():
            self.mining_task.stop()

        self.transactions[tx_id] = {"Challenge": difficulty, "Solution": "", "Winner": STATUS["PENDING"]}
        self.mining_task = MiningThread(self, tx_id, difficulty)
        self.mining_task.start()

    def submit_solution(self, tx_id, solution):
        print(f"Solução encontrada para T{tx_id}: {solution}")
        self._publish(CHANNELS["ANSWER"], {"NodeID": self.node_id, "TransactionID": tx_id, "Solution": solution})

    def _handle_solution(self, data):
        peer_id = data.get("NodeID")
        tx_id = data.get("TransactionID")
        solution = data.get("Solution")

        tx_info = self.transactions.get(tx_id)
        if tx_info and tx_info["Winner"] == STATUS["PENDING"]:
            if self._verify_solution(tx_id, solution):
                self._approve_solution(peer_id, tx_id, solution)
            else:
                self._deny_solution(peer_id, tx_id, solution)

    def _verify_solution(self, tx_id, solution):
        challenge = self.transactions[tx_id]["Challenge"]
        _, valid = check_hash(challenge, solution)
        return valid

    def _approve_solution(self, peer_id, tx_id, solution):
        self.transactions[tx_id]["Winner"] = peer_id
        self.transactions[tx_id]["Solution"] = solution
        print(f"--- [LIDER] Solução aceita de {peer_id} para T{tx_id} ---")
        self._publish(CHANNELS["FEEDBACK"], {"NodeID": peer_id, "TransactionID": tx_id, "Solution": solution, "Result": STATUS["APPROVED"]})
        time.sleep(3)
        self._create_transaction()

    def _deny_solution(self, peer_id, tx_id, solution):
        print(f"[LIDER] Solução rejeitada de {peer_id}")
        self._publish(CHANNELS["FEEDBACK"], {"NodeID": peer_id, "TransactionID": tx_id, "Solution": solution, "Result": STATUS["DENIED"]})

    def _handle_feedback(self, data):
        tx_id = data.get("TransactionID")
        result = data.get("Result")
        winner = data.get("NodeID")

        if result == STATUS["APPROVED"]:
            print(f">>> T{tx_id} finalizada. Vencedor: {winner}")
            if self.mining_task and self.mining_task.tx_id == tx_id:
                self.mining_task.stop()


if __name__ == "__main__":
    NUM_NODES = 3
    node = DistributedNode(BROKER_HOST, BROKER_PORT, NUM_NODES)
    try:
        node.start()
    except KeyboardInterrupt:
        print("Encerrando nó...")
