#!/usr/bin/env python3
"""
distributed_miner.py
Protótipo de minerador/controlador com eleição via MQTT (EMQX broker).
Uso:
  python distributed_miner.py --expected 3 --broker broker.emqx.io --port 1883 --name "nodeA"
Parâmetros:
  --expected : número total de participantes esperados (n). (obrigatório)
  --broker   : endereço do broker MQTT (default: broker.emqx.io)
  --port     : porta do broker (default: 1883)
  --name     : rótulo opcional para logs
"""

import argparse
import json
import random
import string
import threading
import time
import hashlib
import sys
from uuid import uuid4
from collections import defaultdict
import paho.mqtt.client as mqtt

# -----------------------
# Config / tópicos
# -----------------------
TOPIC_INIT = "sd/init"
TOPIC_ELECTION = "sd/election"
TOPIC_CHALLENGE = "sd/challenge"
TOPIC_SOLUTION = "sd/solution"
TOPIC_RESULT = "sd/result"

# -----------------------
# Helper functions
# -----------------------
def sha1_hexdigest(s: str) -> str:
    return hashlib.sha1(s.encode('utf-8')).hexdigest()

def make_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# -----------------------
# Participant class
# -----------------------
class Participant:
    def __init__(self, expected, broker, port, label=None):
        self.expected = expected
        self.broker = broker
        self.port = port
        self.label = label or ""
        # Generate 16-bit ClientID
        self.clientid = random.randint(0, 0xFFFF)
        # Use a unique mqtt client id to connect
        self.mqtt_clientid = f"participant-{self.clientid}-{uuid4().hex[:6]}"
        self.mqtt = mqtt.Client(client_id=self.mqtt_clientid, clean_session=True)
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message
        # local tables/state
        self.init_received = set()
        self.votes_received = {}  # clientid -> voteid
        self.lock = threading.Lock()
        self.is_leader = False
        # transaction table: txid -> dict with Challenge, Solution, Winner
        self.tx_table = {}
        # events for phase advance
        self.init_done = threading.Event()
        self.election_done = threading.Event()
        self.challenge_event = threading.Event()  # signaled when a challenge is published
        # For leader: txid counter
        self.next_txid = 0
        # To stop threads gracefully
        self.stop_event = threading.Event()

    # -----------------------
    # Connect / MQTT callbacks
    # -----------------------
    def start(self):
        print(f"[{self._tag()}] Starting participant. ClientID={self.clientid}, expected={self.expected}")
        self.mqtt.connect(self.broker, self.port, keepalive=60)
        # Subscribe to topics needed
        self.mqtt.subscribe([(TOPIC_INIT, 0), (TOPIC_ELECTION, 0), (TOPIC_CHALLENGE, 0), (TOPIC_SOLUTION, 0), (TOPIC_RESULT, 0)])
        self.mqtt.loop_start()

        # start phases in separate threads
        threading.Thread(target=self.phase_init, daemon=True).start()
        threading.Thread(target=self.phase_election, daemon=True).start()
        threading.Thread(target=self.run_miner, daemon=True).start()
        # controller handler listens on sd/solution to validate (only effective if leader)
        print(f"[{self._tag()}] Running. Waiting phases...")

    def stop(self):
        print(f"[{self._tag()}] Stopping participant.")
        self.stop_event.set()
        try:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
        except Exception:
            pass

    def on_connect(self, client, userdata, flags, rc):
        print(f"[{self._tag()}] Connected to MQTT broker {self.broker}:{self.port} rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
        except Exception as e:
            print(f"[{self._tag()}] Received malformed message on {msg.topic}: {e}")
            return

        if msg.topic == TOPIC_INIT:
            self.handle_init(data)
        elif msg.topic == TOPIC_ELECTION:
            self.handle_election_msg(data)
        elif msg.topic == TOPIC_CHALLENGE:
            self.handle_challenge(data)
        elif msg.topic == TOPIC_SOLUTION:
            self.handle_solution(data)
        elif msg.topic == TOPIC_RESULT:
            self.handle_result(data)

    # -----------------------
    # Handlers per-topic
    # -----------------------
    def handle_init(self, data):
        cid = int(data.get("ClientID", -1))
        if cid < 0:
            return
        with self.lock:
            if cid not in self.init_received:
                self.init_received.add(cid)
                print(f"[{self._tag()}] sd/init received from ClientID={cid} (total={len(self.init_received)}/{self.expected})")
            # If we have expected unique init messages (including our own), set event
            if len(self.init_received) >= self.expected:
                self.init_done.set()

    def handle_election_msg(self, data):
        try:
            cid = int(data.get("ClientID"))
            vote = int(data.get("VoteID"))
        except Exception:
            return
        with self.lock:
            if cid not in self.votes_received:
                self.votes_received[cid] = vote
                print(f"[{self._tag()}] sd/election vote received: ClientID={cid} VoteID={vote} (count={len(self.votes_received)}/{self.expected})")
            if len(self.votes_received) >= self.expected:
                self.election_done.set()

    def handle_challenge(self, data):
        try:
            txid = int(data.get("TransactionID"))
            challenge = int(data.get("Challenge"))
        except Exception:
            return
        with self.lock:
            if txid not in self.tx_table:
                self.tx_table[txid] = {"Challenge": challenge, "Solution": None, "Winner": -1}
                print(f"[{self._tag()}] sd/challenge received: TransactionID={txid} Challenge={challenge}")
                # signal miners that there's work
                self.challenge_event.set()
            else:
                # update if needed
                print(f"[{self._tag()}] sd/challenge for existing TX {txid} ignored (already known)")

    def handle_solution(self, data):
        # Any node receives solutions; leader will validate and respond
        try:
            client = int(data.get("ClientID"))
            txid = int(data.get("TransactionID"))
            sol = str(data.get("Solution"))
        except Exception:
            return
        print(f"[{self._tag()}] sd/solution received: ClientID={client} tx={txid} sol={sol[:20]}...")
        # If I'm leader, I must validate and publish result
        if self.is_leader:
            self.validate_and_publish_result(client, txid, sol)

    def handle_result(self, data):
        try:
            client = int(data.get("ClientID"))
            txid = int(data.get("TransactionID"))
            sol = str(data.get("Solution"))
            result = int(data.get("Result"))
        except Exception:
            return
        # Update local table if present
        with self.lock:
            rec = self.tx_table.get(txid)
            if rec:
                if result != 0 and rec["Winner"] == -1:
                    rec["Solution"] = sol
                    rec["Winner"] = client
                print(f"[{self._tag()}] sd/result: TransactionID={txid} Result={'ACCEPTED' if result!=0 else 'REJECTED'} Winner={client}")
            else:
                print(f"[{self._tag()}] sd/result for unknown tx {txid} (Result={result})")

    # -----------------------
    # Phase: Init
    # -----------------------
    def phase_init(self):
        # publish our init message repeatedly until we see expected count (simple strategy)
        payload = json.dumps({"ClientID": self.clientid})
        # Immediately add self.received own init as sent
        with self.lock:
            self.init_received.add(self.clientid)
        # Publish and keep republishing every 1s until event set
        while not self.init_done.is_set() and not self.stop_event.is_set():
            self.mqtt.publish(TOPIC_INIT, payload)
            time.sleep(1.0)
        print(f"[{self._tag()}] Init phase done (collected {len(self.init_received)} init msgs).")

    # -----------------------
    # Phase: Election
    # -----------------------
    def phase_election(self):
        # wait for init_done
        self.init_done.wait()
        # small random jitter to reduce simultaneous collisions
        time.sleep(random.random() * 0.5)
        # generate VoteID
        voteid = random.randint(0, 0xFFFF)
        msg = json.dumps({"ClientID": self.clientid, "VoteID": voteid})
        # Add own vote
        with self.lock:
            self.votes_received[self.clientid] = voteid
        # Publish vote repeatedly until election_done
        while not self.election_done.is_set() and not self.stop_event.is_set():
            self.mqtt.publish(TOPIC_ELECTION, msg)
            time.sleep(0.5)
        # Once collected, compute winner
        with self.lock:
            # pick max voteid, tie-break by ClientID
            best = None  # tuple (voteid, clientid)
            for c, v in self.votes_received.items():
                tup = (v, c)
                if best is None or tup > best:
                    best = tup
            if best:
                chosen_voteid, chosen_cid = best
                self.is_leader = (chosen_cid == self.clientid)
                print(f"[{self._tag()}] Election finished. Leader ClientID={chosen_cid} (VoteID={chosen_voteid}). I am leader? {self.is_leader}")
        # If leader, start controller thread
        if self.is_leader:
            threading.Thread(target=self.controller_loop, daemon=True).start()

    # -----------------------
    # Controller (leader) loop: create challenges and validate solutions
    # -----------------------
    def controller_loop(self):
        print(f"[{self._tag()}] Starting controller loop (will create challenges).")
        # On startup, leader creates txid=0 per spec
        time.sleep(1.0)
        while not self.stop_event.is_set():
            txid = self.next_txid
            # choose challenge method: random 1..20 (can be switched to sequential)
            challenge = random.randint(1, 20)
            with self.lock:
                self.tx_table[txid] = {"Challenge": challenge, "Solution": None, "Winner": -1}
                self.next_txid += 1
            payload = json.dumps({"TransactionID": txid, "Challenge": challenge})
            self.mqtt.publish(TOPIC_CHALLENGE, payload)
            print(f"[{self._tag()}] Controller published sd/challenge tx={txid} challenge={challenge}")
            # Wait until someone solves it (i.e., tx_table updated with Winner != -1), or timeout then republish/new tx
            waited = 0
            while waited < 60 and not self.stop_event.is_set():
                with self.lock:
                    if self.tx_table[txid]["Winner"] != -1:
                        print(f"[{self._tag()}] Transaction {txid} already solved by ClientID={self.tx_table[txid]['Winner']}")
                        break
                time.sleep(1.0)
                waited += 1
            # After timeout, either create next txid or continue
            # small pause before next challenge
            time.sleep(2.0)

    def validate_and_publish_result(self, client, txid, sol):
        with self.lock:
            rec = self.tx_table.get(txid)
            if not rec:
                # unknown transaction => reject
                result = 0
                print(f"[{self._tag()}] Validator: unknown tx {txid}, rejecting solution from {client}")
            elif rec["Winner"] != -1:
                # already solved
                result = 0
                print(f"[{self._tag()}] Validator: tx {txid} already has winner {rec['Winner']}, rejecting {client}")
            else:
                # validate hash requirement: sha1(sol) hex must start with '0'*challenge
                h = sha1_hexdigest(sol)
                prefix = '0' * rec["Challenge"]
                if h.startswith(prefix):
                    # accept
                    rec["Solution"] = sol
                    rec["Winner"] = client
                    result = 1
                    print(f"[{self._tag()}] Validator: ACCEPT tx={txid} by {client} (hash={h[:12]}...)")
                else:
                    result = 0
                    print(f"[{self._tag()}] Validator: REJECT tx={txid} by {client} (hash={h[:12]}...)")
            # publish result
            payload = json.dumps({"ClientID": client, "TransactionID": txid, "Solution": sol, "Result": result})
            self.mqtt.publish(TOPIC_RESULT, payload)

    # -----------------------
    # Miner loop: wait for challenges and try to solve
    # -----------------------
    def run_miner(self):
        while not self.stop_event.is_set():
            # wait for a challenge or timeout
            if not self.challenge_event.wait(timeout=5.0):
                continue
            # copy current pending txs
            with self.lock:
                pending = [(txid, rec["Challenge"]) for txid, rec in self.tx_table.items() if rec["Winner"] == -1]
            # try to solve each pending tx
            for txid, challenge in pending:
                if self.stop_event.is_set():
                    break
                # try random solutions until found or another winner appears
                print(f"[{self._tag()}] Miner: attempting tx={txid} challenge={challenge}")
                attempts = 0
                start = time.time()
                while not self.stop_event.is_set():
                    # before trying, check if someone else already won
                    with self.lock:
                        if self.tx_table.get(txid, {}).get("Winner", -1) != -1:
                            print(f"[{self._tag()}] Miner: tx={txid} already solved by other.")
                            break
                    candidate = make_random_string(12)
                    h = sha1_hexdigest(candidate)
                    attempts += 1
                    if h.startswith('0' * challenge):
                        # publish solution
                        payload = json.dumps({"ClientID": self.clientid, "TransactionID": txid, "Solution": candidate})
                        self.mqtt.publish(TOPIC_SOLUTION, payload)
                        print(f"[{self._tag()}] Miner: FOUND solution for tx={txid} after {attempts} attempts in {time.time()-start:.2f}s")
                        # wait small while for result/validation to arrive
                        time.sleep(1.0)
                        break
                # small rest between pending txs
                time.sleep(0.2)
            # reset challenge_event if no pending left
            with self.lock:
                if all(rec["Winner"] != -1 for rec in self.tx_table.values()):
                    self.challenge_event.clear()

    # -----------------------
    # Utilities
    # -----------------------
    def _tag(self):
        return f"{self.label or 'Node'}[{self.clientid}]"

# -----------------------
# CLI / run
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Distributed Miner/Controller using MQTT (EMQX)")
    parser.add_argument("--expected", type=int, required=True, help="Número total de participantes esperados (n)")
    parser.add_argument("--broker", type=str, default="broker.emqx.io", help="Endereço do broker MQTT")
    parser.add_argument("--port", type=int, default=1883, help="Porta MQTT")
    parser.add_argument("--name", type=str, default=None, help="Label para logs")
    args = parser.parse_args()

    p = Participant(expected=args.expected, broker=args.broker, port=args.port, label=args.name)
    try:
        p.start()
        # keep running until Ctrl+C
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nCtrl+C recebido. Finalizando...")
    finally:
        p.stop()
        time.sleep(0.5)

if __name__ == "__main__":
    main()
