import paho.mqtt.client as mqtt
import hashlib, json, random, time
from threading import Thread

BROKER = "broker.emqx.io"
PORT = 1883
DIFF_RANGE = (1, 4)
PAUSE = 0.001
NONCE_STEP = 50000
TAG = "1dezembro"

TOPICS = {
    "JOIN": f"sd/{TAG}/init",
    "VOTE": f"sd/{TAG}/voting",
    "TASK": f"sd/{TAG}/challenge",
    "SUBMIT": f"sd/{TAG}/solution",
    "RESULT": f"sd/{TAG}/result"
}

STATE = {"INIT":"S", "VOTE":"V", "WORK":"W"}
STATUS = {"PENDING":-1, "FAIL":0, "SUCCESS":1}

def hash_check(diff, msg):
    h = hashlib.sha1(msg.encode()).hexdigest()
    return h, h.startswith("0"*diff)

class Miner(Thread):
    def __init__(self, node, tx, diff):
        super().__init__()
        self.node,self.tx,self.diff = node,tx,diff
        self.active=True
    def run(self):
        n=0
        while self.active:
            candidate=f"{self.tx}:{n}"
            h,v=hash_check(self.diff,candidate)
            if v:
                self.node.submit(self.tx,candidate)
                break
            n+=1
            if n%NONCE_STEP==0: time.sleep(PAUSE)
    def stop(self): self.active=False

class Node:
    def __init__(self, broker,port,total):
        self.broker,self.port,self.total = broker,port,total
        self.id=random.randint(0,65535)
        self.state=STATE["INIT"]
        self.peers=set()
        self.votes={}
        self.txs={}
        self.leader=None
        self.is_leader=False
        self.tx_id=0
        self.worker=None
        self.client=self._mqtt_init()
    def _mqtt_init(self):
        c=mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,f"N{self.id}")
        c.on_connect=self._on_connect
        c.on_message=self._on_message
        return c
    def _connect(self):
        self.client.connect(self.broker,self.port,60)
        self.client.loop_start()
    def run(self):
        self._connect()
        while True:
            time.sleep(2)
            self._state_flow()
    def _state_flow(self):
        if self.state==STATE["INIT"]: self._join()
        elif self.state==STATE["VOTE"]: self._vote()
        elif self.state==STATE["WORK"] and self.is_leader:
            if all(tx["Winner"]!=STATUS["PENDING"] for tx in self.txs.values()):
                self._new_tx()
    def _join(self):
        self._send(TOPICS["JOIN"],{"ID":self.id})
        if len(self.peers)>=self.total: self.state=STATE["VOTE"]
    def _vote(self):
        if self.id not in self.votes: self.votes[self.id]=random.randint(0,65535)
        self._send(TOPICS["VOTE"],{"ID":self.id,"V":self.votes[self.id]})
        if len(self.votes)>=self.total: self._count_votes()
    def _count_votes(self):
        self.leader=max(self.votes,key=lambda k:(self.votes[k],k))
        self.is_leader=self.id==self.leader
        self.state=STATE["WORK"]
    def _new_tx(self):
        self.tx_id+=1
        tx=self.tx_id
        diff=random.randint(*DIFF_RANGE)
        self.txs[tx]={"Challenge":diff,"Solution":"","Winner":STATUS["PENDING"]}
        self._send(TOPICS["TASK"],{"TX":tx,"C":diff})
    def _handle_task(self,data):
        if self.is_leader: return
        tx=data.get("TX"); diff=data.get("C")
        if self.worker and self.worker.is_alive(): self.worker.stop()
        self.txs[tx]={"Challenge":diff,"Solution":"","Winner":STATUS["PENDING"]}
        self.worker=Miner(self,tx,diff)
        self.worker.start()
    def submit(self,tx,sol):
        self._send(TOPICS["SUBMIT"],{"ID":self.id,"TX":tx,"S":sol})
    def _handle_submit(self,data):
        tx=data.get("TX"); sol=data.get("S"); nid=data.get("ID")
        info=self.txs.get(tx)
        if info and info["Winner"]==STATUS["PENDING"]:
            if self._valid(tx,sol): self._approve(nid,tx,sol)
            else: self._reject(nid,tx,sol)
    def _valid(self,tx,sol):
        diff=self.txs[tx]["Challenge"]
        _,v=hash_check(diff,sol)
        return v
    def _approve(self,nid,tx,sol):
        self.txs[tx]["Winner"]=nid
        self.txs[tx]["Solution"]=sol
        self._send(TOPICS["RESULT"],{"ID":nid,"TX":tx,"S":sol,"R":STATUS["SUCCESS"]})
    def _reject(self,nid,tx,sol):
        self._send(TOPICS["RESULT"],{"ID":nid,"TX":tx,"S":sol,"R":STATUS["FAIL"]})
    def _handle_result(self,data):
        tx=data.get("TX"); res=data.get("R"); nid=data.get("ID")
        if res==STATUS["SUCCESS"]:
            if self.worker and self.worker.tx==tx: self.worker.stop()
    def _on_connect(self,client,user,flags,rc):
        if rc==0:
            for ch in TOPICS.values(): client.subscribe(ch)
    def _on_message(self,client,user,msg):
        try:data=json.loads(msg.payload.decode())
        except:return
        hmap={
            TOPICS["JOIN"]:self._join,
            TOPICS["VOTE"]:self._vote,
            TOPICS["TASK"]:self._handle_task,
            TOPICS["SUBMIT"]:self._handle_submit if self.is_leader else lambda x:None,
            TOPICS["RESULT"]:self._handle_result
        }
        handler=hmap.get(msg.topic)
        if handler: handler(data)

if __name__=="__main__":
    node=Node(BROKER,PORT,3)
    node.run()
