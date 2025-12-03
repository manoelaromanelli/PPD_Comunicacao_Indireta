import paho.mqtt.client as mqtt
import json
import random
import hashlib
import time
import threading

BROKER = "broker.emqx.io"
PORT = 1883
NUM_PARTICIPANTS = 10  # número fixo de nós

class Participant:
    def __init__(self):
        self.client_id = random.randint(0, 65535)
        self.vote_id = None
        self.leader_id = None
        self.init_received = set()
        self.votes_received = {}
        self.transaction_table = {}  # TransactionID -> {Challenge, Solution, Winner}
        self.client = mqtt.Client(str(self.client_id))
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    # --- MQTT Handlers ---
    def on_connect(self, client, userdata, flags, rc):
        print(f"[{self.client_id}] Conectado ao broker com código {rc}")
        # Assinar tópicos
        client.subscribe("sd/init")
        client.subscribe("sd/election")
        client.subscribe("sd/challenge")
        client.subscribe("sd/solution")
        client.subscribe("sd/result")

    def on_message(self, client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        if topic == "sd/init":
            self.handle_init(payload)
        elif topic == "sd/election":
            self.handle_election(payload)
        elif topic == "sd/challenge":
            self.handle_challenge(payload)
        elif topic == "sd/result":
            self.handle_result(payload)
        elif topic == "sd/solution":
            self.handle_solution(payload)

    # --- Eleição Distribuída ---
    def start_election(self):
        # Enviar InitMsg
        self.client.publish("sd/init", json.dumps({"ClientID": self.client_id}))
        print(f"[{self.client_id}] Enviou InitMsg")
        
        # Esperar n-1 InitMsgs
        while len(self.init_received) < NUM_PARTICIPANTS - 1:
            time.sleep(0.5)

        # Fase de eleição
        self.vote_id = random.randint(0, 65535)
        self.client.publish("sd/election", json.dumps({"ClientID": self.client_id, "VoteID": self.vote_id}))
        print(f"[{self.client_id}] Enviou ElectionMsg com VoteID={self.vote_id}")
        
        while len(self.votes_received) < NUM_PARTICIPANTS - 1:
            time.sleep(0.5)
        
        # Escolher líder
        all_votes = self.votes_received.copy()
        all_votes[self.client_id] = self.vote_id
        sorted_votes = sorted(all_votes.items(), key=lambda x: (x[1], x[0]), reverse=True)
        self.leader_id = sorted_votes[0][0]
        print(f"[{self.client_id}] Líder eleito: {self.leader_id}")

        # Se for líder, inicia como controlador
        if self.leader_id == self.client_id:
            threading.Thread(target=self.controlador_loop, daemon=True).start()

    def handle_init(self, payload):
        cid = payload["ClientID"]
        if cid != self.client_id:
            self.init_received.add(cid)
            print(f"[{self.client_id}] Recebeu InitMsg de {cid}")

    def handle_election(self, payload):
        cid = payload["ClientID"]
        vote = payload["VoteID"]
        if cid != self.client_id:
            self.votes_received[cid] = vote
            print(f"[{self.client_id}] Recebeu ElectionMsg de {cid} com VoteID={vote}")

    # --- Controlador ---
    def controlador_loop(self):
        tx_id = 0
        while True:
            challenge = random.randint(1, 20)
            self.transaction_table[tx_id] = {"Challenge": challenge, "Solution": "", "Winner": -1}
            self.client.publish("sd/challenge", json.dumps({"TransactionID": tx_id, "Challenge": challenge}))
            print(f"[Controlador {self.client_id}] Enviou Challenge TX={tx_id}, Dificuldade={challenge}")
            tx_id += 1
            time.sleep(10)  # intervalo entre desafios

    def handle_solution(self, payload):
        tx_id = payload["TransactionID"]
        client_id = payload["ClientID"]
        solution = payload["Solution"]

        if tx_id in self.transaction_table and self.transaction_table[tx_id]["Winner"] == -1:
            # Validar solução
            challenge = self.transaction_table[tx_id]["Challenge"]
            hash_val = hashlib.sha1(solution.encode()).hexdigest()
            if hash_val.startswith("0" * challenge):
                self.transaction_table[tx_id]["Winner"] = client_id
                self.transaction_table[tx_id]["Solution"] = solution
                self.client.publish("sd/result", json.dumps({"ClientID": client_id, "TransactionID": tx_id, "Solution": solution, "Result": 1}))
                print(f"[Controlador {self.client_id}] Solução válida de {client_id} para TX={tx_id}")
            else:
                self.client.publish("sd/result", json.dumps({"ClientID": client_id, "TransactionID": tx_id, "Solution": solution, "Result": 0}))
                print(f"[Controlador {self.client_id}] Solução inválida de {client_id} para TX={tx_id}")

    # --- Minerador ---
    def handle_challenge(self, payload):
        tx_id = payload["TransactionID"]
        challenge = payload["Challenge"]
        print(f"[Minerador {self.client_id}] Recebeu desafio TX={tx_id} com dificuldade {challenge}")
        threading.Thread(target=self.solve_challenge, args=(tx_id, challenge), daemon=True).start()

    def solve_challenge(self, tx_id, challenge):
        nonce = 0
        while True:
            solution = f"{self.client_id}-{tx_id}-{nonce}"
            hash_val = hashlib.sha1(solution.encode()).hexdigest()
            if hash_val.startswith("0" * challenge):
                self.client.publish("sd/solution", json.dumps({"ClientID": self.client_id, "TransactionID": tx_id, "Solution": solution}))
                print(f"[Minerador {self.client_id}] Enviou solução TX={tx_id}: {solution}")
                break
            nonce += 1

    def handle_result(self, payload):
        print(f"[Minerador {self.client_id}] Resultado recebido: {payload}")

    # --- Start MQTT ---
    def start(self):
        self.client.connect(BROKER, PORT, 60)
        threading.Thread(target=self.client.loop_forever, daemon=True).start()
        time.sleep(1)
        self.start_election()

# --- Execução ---
if __name__ == "__main__":
    participant = Participant()
    participant.start()

    while True:
        time.sleep(1)
