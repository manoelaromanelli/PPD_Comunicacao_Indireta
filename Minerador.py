import paho.mqtt.client as mqtt
import hashlib
import random
import json
import time
from threading import Event, Thread

BROKER = "broker.emqx.io"
PORT = 1883

DIFFICULTY_RANGE = (1, 4)
THROTTLE_STEP = 50000
SUFFIX = "1dezembro"

TOPICS = {
    "HELLO": f"sd/{SUFFIX}/init",
    "VOTE": f"sd/{SUFFIX}/voting",
    "TASK": f"sd/{SUFFIX}/challenge",
    "SOLUTION": f"sd/{SUFFIX}/solution",
    "RESULT": f"sd/{SUFFIX}/result",
}

STATES = {
    "INIT": "Inicialização",
    "VOTING": "Votação",
    "EXECUTION": "Execução"
}

RESULTS = {
    "PENDING": -1,
    "REJECTED": 0,
    "ACCEPTED": 1
}


def hash_valid(difficulty, text):
    hashed = hashlib.sha1(text.encode()).hexdigest()
    return hashed, hashed.startswith('0' * difficulty)


class Miner(Thread):
    def __init__(self, node, tx_id, difficulty):
        super().__init__()
        self.node = node
        self.tx_id = tx_id
        self.difficulty = difficulty
        self.stop_event = Event()

    def run(self):
        nonce = 0
        while not self.stop_event.is_set():
            candidate = f"{self.tx_id}:{nonce}"
            hash_val, valid = hash_valid(self.difficulty, candidate)
            if valid:
                self.node.submit_solution(self.tx_id, candidate)
                break
            nonce += 1
            if nonce % THROTTLE_STEP == 0:
                time.sleep(0.001)

    def stop(self):
        self.stop_event.set()


class DistributedNode:
    def __init__(self, broker, port, expected_nodes):
        self.broker = broker
        self.port = port
        self.expected_nodes = expected_nodes
        self.node_id = random.randint(0, 65535)
        self.state = STATES["INIT"]
        self.is_leader = False
        self.leader_id = None
        self.tx_id_counter = 0
        self.peers = set()
        self.votes = {}
        self.transactions = {}
        self.miner_thread = None
        self.client = self.setup_mqtt()

    def setup_mqtt(self):
        client = mqtt.Client(client_id=f"Node_{self.node_id}")
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        return client

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def start(self):
        print(f"Nó {self.node_id} inicializando (Esperados: {self.expected_nodes})")
        self.connect()
        while True:
            if self.state == STATES["INIT"]:
                self.broadcast_presence()
            elif self.state == STATES["VOTING"]:
                self.cast_vote()
            time.sleep(2)

    def broadcast_presence(self):
        self.publish(TOPICS["HELLO"], {"NodeID": self.node_id})
        if len(self.peers) >= self.expected_nodes:
            self.transition_to_voting()

    def publish(self, topic, data):
        self.client.publish(topic, json.dumps(data))

    def transition_to_voting(self):
        if self.state == STATES["VOTING"]:
            return
        print(">>> Todos os nós conectados. Iniciando votação...")
        self.state = STATES["VOTING"]
        for _ in range(3):
            self.publish(TOPICS["HELLO"], {"NodeID": self.node_id})
            time.sleep(0.1)

    def cast_vote(self):
        if self.node_id not in self.votes:
            self.votes[self.node_id] = random.randint(0, 65535)
        vote = self.votes[self.node_id]
        self.publish(TOPICS["VOTE"], {"NodeID": self.node_id, "VoteID": vote})

    # MQTT Callbacks
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT.")
            self.client.subscribe([(TOPICS[k], 0) for k in TOPICS])
        else:
            print(f"Falha na conexão ({rc})")

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            return

        topic_map = {
            TOPICS["HELLO"]: self.handle_hello,
            TOPICS["VOTE"]: self.handle_vote,
            TOPICS["TASK"]: self.handle_task,
            TOPICS["SOLUTION"]: self.handle_solution if self.is_leader else lambda x: None,
            TOPICS["RESULT"]: self.handle_result
        }

        handler = topic_map.get(msg.topic)
        if handler:
            handler(data)

    # Handlers
    def handle_hello(self, data):
        peer_id = data.get("NodeID")
        if peer_id is not None:
            self.peers.add(peer_id)
            if len(self.peers) >= self.expected_nodes and self.state == STATES["INIT"]:
                self.transition_to_voting()

    def handle_vote(self, data):
        peer_id = data.get("NodeID")
        vote = data.get("VoteID")
        if peer_id is not None and vote is not None:
            self.votes[peer_id] = vote

        if len(self.votes) >= self.expected_nodes and self.state == STATES["VOTING"]:
            self.count_votes()

    def count_votes(self):
        self.leader_id = max(self.votes, key=lambda k: (self.votes[k], k))
        self.is_leader = (self.node_id == self.leader_id)
        print(f">>> Votação finalizada. Líder: {self.leader_id} (Sou eu? {self.is_leader})")
        self.transition_to_execution()

    def transition_to_execution(self):
        self.state = STATES["EXECUTION"]
        if self.is_leader:
            time.sleep(2)
            self.create_transaction()

    def create_transaction(self):
        self.tx_id_counter += 1
        difficulty = random.randint(*DIFFICULTY_RANGE)
        self.transactions[self.tx_id_counter] = {"Challenge": difficulty, "Solution": "", "Winner": RESULTS["PENDING"]}
        print(f"--- [LÍDER] Nova Transação T{self.tx_id_counter} (Dif: {difficulty}) ---")
        self.publish(TOPICS["TASK"], {"TransactionID": self.tx_id_counter, "Challenge": difficulty})

    def handle_task(self, data):
        if self.is_leader:
            return

        tx_id = data.get("TransactionID")
        difficulty = data.get("Challenge")
        print(f"Tarefa recebida T{tx_id} (Dif: {difficulty}). Iniciando mineração...")

        if self.miner_thread and self.miner_thread.is_alive():
            self.miner_thread.stop()

        self.transactions[tx_id] = {"Challenge": difficulty, "Solution": "", "Winner": RESULTS["PENDING"]}
        self.miner_thread = Miner(self, tx_id, difficulty)
        self.miner_thread.start()

    def submit_solution(self, tx_id, solution):
        print(f"Solução T{tx_id} encontrada: {solution}")
        self.publish(TOPICS["SOLUTION"], {"NodeID": self.node_id, "TransactionID": tx_id, "Solution": solution})

    def handle_solution(self, data):
        peer_id = data.get("NodeID")
        tx_id = data.get("TransactionID")
        solution = data.get("Solution")

        if tx_id in self.transactions and self.transactions[tx_id]["Winner"] == RESULTS["PENDING"]:
            if self.validate_solution(tx_id, solution):
                self.approve_solution(peer_id, tx_id, solution)
            else:
                self.reject_solution(peer_id, tx_id, solution)

    def validate_solution(self, tx_id, solution):
        challenge = self.transactions[tx_id]["Challenge"]
        _, valid = hash_valid(challenge, solution)
        return valid

    def approve_solution(self, peer_id, tx_id, solution):
        self.transactions[tx_id]["Winner"] = peer_id
        self.transactions[tx_id]["Solution"] = solution
        print(f"--- [LÍDER] Solução aceita de {peer_id} para T{tx_id} ---")
        self.publish(TOPICS["RESULT"], {"NodeID": peer_id, "TransactionID": tx_id, "Solution": solution, "Result": RESULTS["ACCEPTED"]})
        time.sleep(3)
        self.create_transaction()

    def reject_solution(self, peer_id, tx_id, solution):
        print(f"[LÍDER] Solução rejeitada de {peer_id}")
        self.publish(TOPICS["RESULT"], {"NodeID": peer_id, "TransactionID": tx_id, "Solution": solution, "Result": RESULTS["REJECTED"]})

    def handle_result(self, data):
        tx_id = data.get("TransactionID")
        result = data.get("Result")
        winner = data.get("NodeID")

        if result == RESULTS["ACCEPTED"]:
            print(f">>> T{tx_id} finalizada. Vencedor: {winner}")
            if self.miner_thread and self.miner_thread.tx_id == tx_id:
                self.miner_thread.stop()


if __name__ == "__main__":
    node_count = 3
    node = DistributedNode(BROKER, PORT, node_count)
    try:
        node.start()
    except KeyboardInterrupt:
        print("Encerrando nó...")
