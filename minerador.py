import paho.mqtt.client as mqtt
import hashlib, json, random, time
from threading import Thread, Event

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

DIFFICULTY_RANGE = (1, 20)
NONCE_STEP = 50000
MINING_PAUSE = 0.001
SUFFIX = "PPD_lab3"

TOPICS = {
    "INIT": f"sd/{SUFFIX}/init",
    "VOTE": f"sd/{SUFFIX}/voting",
    "CHALLENGE": f"sd/{SUFFIX}/challenge",
    "SOLUTION": f"sd/{SUFFIX}/solution",
    "RESULT": f"sd/{SUFFIX}/result"
}

STATES = {
    "INIT": "Inicialização",
    "VOTE": "Votação",
    "WORK": "Execução"
}

STATUS = {
    "PENDING": -1,
    "REJECTED": 0,
    "ACCEPTED": 1
}

def compute_hash(challenge, content):
    digest = hashlib.sha1(content.encode()).hexdigest()
    return digest, digest.startswith("0" * challenge)

class Miner(Thread):
    def __init__(self, node, tx_id, challenge):
        super().__init__()
        self.node = node
        self.tx_id = tx_id
        self.challenge = challenge
        self.running = True

    def run(self):
        nonce = 0
        while self.running:
            candidate = f"{self.tx_id}:{nonce}"
            _, valid = compute_hash(self.challenge, candidate)
            if valid:
                self.node.submit_solution(self.tx_id, candidate)
                break
            nonce += 1
            if nonce % NONCE_STEP == 0:
                time.sleep(MINING_PAUSE)

    def stop(self):
        self.running = False

class Node:
    def __init__(self, broker, port, total_nodes):
        self.broker = broker
        self.port = port
        self.total_nodes = total_nodes
        self.client_id = random.randint(0, 65535)
        self.state = STATES["INIT"]
        self.leader_id = None
        self.is_leader = False
        self.votes = {}
        self.peers = set()
        self.transactions = {0: {"Challenge": random.randint(*DIFFICULTY_RANGE),
                                 "Solution": "",
                                 "Winner": STATUS["PENDING"]}}
        self.current_tx = 0
        self.miner_thread = None
        self.first_challenge_event = Event()
        self.client = self._setup_mqtt()

    def _setup_mqtt(self):
        c = mqtt.Client(client_id=f"Node_{self.client_id}")
        c.on_connect = self._on_connect
        c.on_message = self._on_message
        return c

    def _connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def run(self):
        print(f"Nó {self.client_id} iniciando (esperados: {self.total_nodes})")
        self._connect()
        while True:
            if self.state == STATES["INIT"]:
                self._announce_presence()
            elif self.state == STATES["VOTE"]:
                self._broadcast_vote()
            time.sleep(2)

    def _announce_presence(self):
        self._send(TOPICS["INIT"], {"ID": self.client_id})
        if len(self.peers) >= self.total_nodes:
            self._start_voting()

    def _start_voting(self):
        if self.state != STATES["VOTE"]:
            print(">>> Todos os nós conectados. Iniciando votação...")
            self.state = STATES["VOTE"]
            for _ in range(3):
                self._send(TOPICS["INIT"], {"ID": self.client_id})
                time.sleep(0.1)

    def _broadcast_vote(self):
        if self.client_id not in self.votes:
            self.votes[self.client_id] = random.randint(0, 65535)
        self._send(TOPICS["VOTE"], {"ID": self.client_id, "Vote": self.votes[self.client_id]})

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT")
            for topic in TOPICS.values():
                client.subscribe(topic)
        else:
            print(f"Erro de conexão ({rc})")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except:
            return
        topic = msg.topic
        handlers = {
            TOPICS["INIT"]: self._handle_init,
            TOPICS["VOTE"]: self._handle_vote,
            TOPICS["CHALLENGE"]: self._handle_challenge,
            TOPICS["SOLUTION"]: self._handle_solution if self.is_leader else lambda x: None,
            TOPICS["RESULT"]: self._handle_result
        }
        handler = handlers.get(topic)
        if handler:
            handler(data)

    # Inicialização
    def _handle_init(self, data):
        peer_id = data.get("ID")
        if peer_id is not None:
            self.peers.add(peer_id)
            if len(self.peers) >= self.total_nodes and self.state == STATES["INIT"]:
                self._start_voting()

    # Votação
    def _handle_vote(self, data):
        peer_id = data.get("ID")
        vote_val = data.get("Vote")
        if peer_id is not None and vote_val is not None:
            self.votes[peer_id] = vote_val
        if len(self.votes) >= self.total_nodes and self.state == STATES["VOTE"]:
            self._count_votes()

    def _count_votes(self):
        self.leader_id = max(self.votes, key=lambda k: (self.votes[k], k))
        self.is_leader = (self.client_id == self.leader_id)
        print(f">>> Votação concluída. Líder: {self.leader_id} (Sou eu? {self.is_leader})")
        self._enter_work_phase()

    # Fase de desafio
    def _enter_work_phase(self):
        self.state = STATES["WORK"]
        if self.is_leader:
            time.sleep(2)
            self._issue_transaction(self.current_tx)

    def _issue_transaction(self, tx_id):
        diff = self.transactions[tx_id]["Challenge"]
        print(f"--- [CONTROLADOR] Criando transação T{tx_id} (Dificuldade {diff}) ---")
        self._send(TOPICS["CHALLENGE"], {"TransactionID": tx_id, "Challenge": diff})

    def _handle_challenge(self, data):
        tx_id = data.get("TransactionID")
        diff = data.get("Challenge")
        print(f"Recebido desafio T{tx_id} (Dif: {diff})")
        if not self.is_leader:
            if self.miner_thread and self.miner_thread.is_alive():
                self.miner_thread.stop()
            self.transactions[tx_id] = {"Challenge": diff, "Solution": "", "Winner": STATUS["PENDING"]}
            self.miner_thread = Miner(self, tx_id, diff)
            self.miner_thread.start()
            self.first_challenge_event.set()  # Desbloqueia o minerador

    def submit_solution(self, tx_id, solution):
        print(f"Solução para T{tx_id} encontrada: {solution}")
        self._send(TOPICS["SOLUTION"], {"ID": self.client_id, "TransactionID": tx_id, "Solution": solution})

    # Controlador valida soluções
    def _handle_solution(self, data):
        tx_id = data.get("TransactionID")
        sol = data.get("Solution")
        peer = data.get("ID")
        tx_info = self.transactions.get(tx_id)
        if tx_info and tx_info["Winner"] == STATUS["PENDING"]:
            if self._verify_solution(tx_id, sol):
                self._approve_solution(peer, tx_id, sol)
            else:
                self._reject_solution(peer, tx_id, sol)

    def _verify_solution(self, tx_id, sol):
        challenge = self.transactions[tx_id]["Challenge"]
        _, valid = compute_hash(challenge, sol)
        return valid

    def _approve_solution(self, peer, tx_id, sol):
        self.transactions[tx_id]["Winner"] = peer
        self.transactions[tx_id]["Solution"] = sol
        print(f"--- [CONTROLADOR] Solução aprovada de {peer} para T{tx_id} ---")
        self._send(TOPICS["RESULT"], {"ID": peer, "TransactionID": tx_id, "Solution": sol, "Result": STATUS["ACCEPTED"]})
        time.sleep(3)
        # Cria próxima transação
        self.current_tx += 1
        self.transactions[self.current_tx] = {"Challenge": random.randint(*DIFFICULTY_RANGE),
                                              "Solution": "", "Winner": STATUS["PENDING"]}
        self._issue_transaction(self.current_tx)

    def _reject_solution(self, peer, tx_id, sol):
        print(f"[CONTROLADOR] Solução rejeitada de {peer} para T{tx_id}")
        self._send(TOPICS["RESULT"], {"ID": peer, "TransactionID": tx_id, "Solution": sol, "Result": STATUS["REJECTED"]})

    def _handle_result(self, data):
        tx_id = data.get("TransactionID")
        result = data.get("Result")
        winner = data.get("ID")
        if result == STATUS["ACCEPTED"]:
            print(f">>> T{tx_id} finalizada. Vencedor: {winner}")
            if self.miner_thread and self.miner_thread.tx_id == tx_id:
                self.miner_thread.stop()

if __name__ == "__main__":
    TOTAL_NODES = 3
    node = Node(MQTT_BROKER, MQTT_PORT, TOTAL_NODES)
    try:
        node.run()
    except KeyboardInterrupt:
        print("Encerrando nó...")
