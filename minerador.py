#!/usr/bin/env python3
#Olivia Gaona - 2213372

import argparse
import json
import random
import time
import threading
import sys

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

def make_block(prev_hash='0'*64):
    nonce = random.randint(0, 1_000_000)
    content = {
        "prev_hash": prev_hash,
        "txs": [],
        "nonce": nonce
    }
    block_hash = f"{abs(hash(str(content))) % (10**16):016d}"
    return {"hash": block_hash, "nonce": nonce, "content": content}

class MinerNode:
    def __init__(self, node_id, broker, topic):
        self.node_id = node_id
        self.broker = broker
        self.topic = topic
        self.client = None
        self.running = False
        self.last_hash = "0"*16

    def on_connect(self, client, userdata, flags, rc):
        print(f"[{self.node_id}] Connected to broker {self.broker} (rc={rc})")
        client.subscribe(self.topic)

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            print(f"[{self.node_id}] Received block from topic {msg.topic}: hash={data.get('hash')} nonce={data.get('nonce')}")
            self.last_hash = data.get('hash') or self.last_hash
        except Exception as e:
            print("Received non-json or malformed message:", e)

    def start(self):
        if mqtt is None:
            print("paho-mqtt not installed. Install with: pip install paho-mqtt")
            return
        self.client = mqtt.Client(self.node_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.broker, 1883, 60)
        self.running = True
        t = threading.Thread(target=self._loop)
        t.daemon = True
        t.start()
        try:
            while self.running:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Stopping miner...")
            self.running = False
            self.client.disconnect()

    def _loop(self):
        self.client.loop_start()
        while self.running:
            time.sleep(random.uniform(2.0, 6.0))
            block = make_block(self.last_hash)
            payload = json.dumps(block)
            self.client.publish(self.topic, payload)
            print(f"[{self.node_id}] Published block hash={block['hash']} nonce={block['nonce']}")
            self.last_hash = block['hash']
        self.client.loop_stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Minerador MQTT simulado")
    parser.add_argument("--id", required=True, help="ID do nó")
    parser.add_argument("--broker", default="localhost", help="Endereço do broker MQTT (padrão: localhost)")
    parser.add_argument("--topic", default="mining/block", help="Tópico de publicação/inscrição")
    args = parser.parse_args()

    node = MinerNode(args.id, args.broker, args.topic)
    node.start()

#Fontes
# Como Minerar Bitcoin com Python (Código em Python para Minerar Bitcoin) HASGTAG PROGRAMAÇÃO - (Base e pesquisa para o código)