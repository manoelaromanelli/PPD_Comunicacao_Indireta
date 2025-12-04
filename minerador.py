import paho.mqtt.client as mqtt
import hashlib, json, random, time
from threading import Thread

MQTT_HOST = "broker.emqx.io"
MQTT_PORT = 1883

DIFFICULTY_RANGE = (1, 20)
MINING_PAUSE = 0.001
NONCE_STEP = 50000

SUFFIX = "PPD_lab3"

CHANNELS = {
    "INIT": f"sd/{SUFFIX}/init",
    "VOTE": f"sd/{SUFFIX}/voting",
    "CHALLENGE": f"sd/{SUFFIX}/challenge",
    "SOLUTION": f"sd/{SUFFIX}/solution",
    "RESULT": f"sd/{SUFFIX}/result"
}

STATES = {
    "START": "Inicialização",
    "VOTE_PHASE": "Votação",
    "WORK_PHASE": "Execução"
}

STATUS = {
    "PENDING": -1,
    "REJECTED": 0,
    "ACCEPTED": 1
}


def compute_hash(challenge, content):
    digest = hashlib.sha1(content.encode()).hexdigest()
    return digest, digest.startswith("0" * challenge)


class MiningWorker(Thread):
    def __init__(self, node, tx, difficulty):
        super().__init__()
        self.node = node
        self.tx = tx
        self.diff = difficulty
        self.running = True

    def run(self):
        nonce = 0
        while self.running:
            candidate = f"{self.tx}:{nonce}"
            hashed, valid = compute_hash(self.diff, candidate)
            if valid:
                self.node.submit_solution(self.tx, candidate)
                self.running = False
                break
            nonce += 1
            if nonce % NONCE_STEP == 0:
                time.sleep(MINING_PAUSE)

    def stop(self):
        self.running = False


class Node:
    def __init__(self, broker, port, expected_count):
        self.broker = broker
        self.port = port
        self.expected_count = expected_count
        self.id = random.randint(0, 65535)
        self.state = STATES["START"]
        self.leader_id = None
        self.is_leader = False
        self.current_tx = 0  # Transação inicial = 0
        self.peers = set()
        self.votes = {}
        self.transactions = {}
        self.mining_thread = None
        self.client = self._setup_mqtt()

    def _setup_mqtt(self):
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f"Node_{self.id}")
        c.on_connect = self._on_connect
        c.on_message = self._on_message
        return c

    def _connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def run(self):
        print(f"Nó {self.id} iniciando (esperados: {self.expected_count})")
        self._connect()
        while True:
            state_actions = {
                STATES["START"]: self._announce_presence,
                STATES["VOTE_PHASE"]: self._broadcast_vote
            }
            action = state_actions.get(self.state)
            if action:
                action()
            time.sleep(2)

    def _announce_presence(self):
        self._send(CHANNELS["INIT"], {"ID": self.id})
        if len(self.peers) >= self.expected_count:
            self._start_voting_phase()

    def _send(self, channel, data):
        self.client.publish(channel, json.dumps(data))

    def _start_voting_phase(self):
        if self.state == STATES["VOTE_PHASE"]:
            return
        print(">>> Todos os nós conectados. Iniciando votação...")
        self.state = STATES["VOTE_PHASE"]
        for _ in range(3):
            self._send(CHANNELS["INIT"], {"ID": self.id})
            time.sleep(0.1)

    def _broadcast_vote(self):
        if self.id not in self.votes:
            self.votes[self.id] = random.randint(0, 65535)
        self._send(CHANNELS["VOTE"], {"ID": self.id, "Vote": self.votes[self.id]})

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT")
            for ch in CHANNELS.values():
                client.subscribe(ch)
        else:
            print(f"Erro de conexão ({rc})")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except:
            return
        topic = msg.topic
        handler_map = {
            CHANNELS["INIT"]: self._handle_hello,
            CHANNELS["VOTE"]: self._handle_vote,
            CHANNELS["CHALLENGE"]: self._handle_task,
            CHANNELS["SOLUTION"]: self._handle_solution if self.is_leader else lambda x: None,
            CHANNELS["RESULT"]: self._handle_result
        }
        handler = handler_map.get(topic)
        if handler:
            handler(data)

    def _handle_hello(self, data):
        peer_id = data.get("ID")
        if peer_id is not None:
            self.peers.add(peer_id)
            if len(self.peers) >= self.expected_count and self.state == STATES["START"]:
                self._start_voting_phase()

    def _handle_vote(self, data):
        peer_id = data.get("ID")
        vote_val = data.get("Vote")
        if peer_id is not None and vote_val is not None:
            self.votes[peer_id] = vote_val
        if len(self.votes) >= self.expected_count and self.state == STATES["VOTE_PHASE"]:
            self._count_votes()

    def _count_votes(self):
        self.leader_id = max(self.votes, key=lambda k: (self.votes[k], k))
        self.is_leader = (self.id == self.leader_id)
        print(f">>> Votação concluída. Líder: {self.leader_id} (Sou eu? {self.is_leader})")
        self._enter_work_phase()

    def _enter_work_phase(self):
        self.state = STATES["WORK_PHASE"]
        if self.is_leader:
            time.sleep(2)
            self._issue_transaction()

    def _issue_transaction(self):
        self.current_tx += 1

        # Dificuldade sequencial de 1 a 20
        diff = ((self.current_tx - 1) % DIFFICULTY_RANGE[1]) + 1

        self.transactions[self.current_tx] = {"Challenge": diff, "Solution": "", "Winner": STATUS["PENDING"]}
        print(f"--- [LÍDER] Criando transação T{self.current_tx} (Dificuldade {diff}) ---")
        self._send(CHANNELS["CHALLENGE"], {"TransactionID": self.current_tx, "Challenge": diff})

    def _handle_task(self, data):
        if self.is_leader:
            return
        tx = data.get("TransactionID")
        diff = data.get("Challenge")
        print(f"Recebido desafio T{tx} (Dif: {diff}), iniciando mineração")
        if self.mining_thread and self.mining_thread.is_alive():
            self.mining_thread.stop()
        self.transactions[tx] = {"Challenge": diff, "Solution": "", "Winner": STATUS["PENDING"]}
        self.mining_thread = MiningWorker(self, tx, diff)
        self.mining_thread.start()

    def submit_solution(self, tx, solution):
        print(f"Solução para T{tx} encontrada: {solution}")
        self._send(CHANNELS["SOLUTION"], {"ID": self.id, "TransactionID": tx, "Solution": solution})

    def _handle_solution(self, data):
        tx = data.get("TransactionID")
        sol = data.get("Solution")
        peer = data.get("ID")
        tx_info = self.transactions.get(tx)
        if tx_info and tx_info["Winner"] == STATUS["PENDING"]:
            if self._verify_solution(tx, sol):
                self._approve_solution(peer, tx, sol)
            else:
                self._reject_solution(peer, tx, sol)

    def _verify_solution(self, tx, sol):
        challenge = self.transactions[tx]["Challenge"]
        _, valid = compute_hash(challenge, sol)
        return valid

    def _approve_solution(self, peer, tx, sol):
        self.transactions[tx]["Winner"] = peer
        self.transactions[tx]["Solution"] = sol
        print(f"--- [LÍDER] Solução aprovada de {peer} para T{tx} ---")
        self._send(CHANNELS["RESULT"], {"ID": peer, "TransactionID": tx, "Solution": sol, "Result": STATUS["ACCEPTED"]})
        time.sleep(3)
        self._issue_transaction()

    def _reject_solution(self, peer, tx, sol):
        print(f"[LÍDER] Solução rejeitada de {peer} para T{tx}")
        self._send(CHANNELS["RESULT"], {"ID": peer, "TransactionID": tx, "Solution": sol, "Result": STATUS["REJECTED"]})

    def _handle_result(self, data):
        tx = data.get("TransactionID")
        result = data.get("Result")
        winner = data.get("ID")
        if result == STATUS["ACCEPTED"]:
            print(f">>> T{tx} finalizada. Vencedor: {winner}")
            if self.mining_thread and self.mining_thread.tx == tx:
                self.mining_thread.stop()


if __name__ == "__main__":
    NODES = 3
    node = Node(MQTT_HOST, MQTT_PORT, NODES)
    try:
        node.run()
    except KeyboardInterrupt:
        print("Encerrando nó...")
