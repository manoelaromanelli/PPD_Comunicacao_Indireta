
import paho.mqtt.client as mqtt
import hashlib
import random
import json
import time
from threading import Thread

HOST_BROKER = "broker.emqx.io"
PORTA_BROKER = 1883

FAIXA_DIFICULDADE = (1, 4)
PASSO_THROTTLE = 50000

SUFIXO = "1dezembro"

TOPICOS = {
    "HELLO": f"sd/{SUFIXO}/init",
    "VOTO": f"sd/{SUFIXO}/voting",
    "TAREFA": f"sd/{SUFIXO}/challenge",
    "SOLUCAO": f"sd/{SUFIXO}/solution",
    "RESULTADO": f"sd/{SUFIXO}/result",
}

ESTADOS = {
    "INICIAL": "Inicialização",
    "VOTACAO": "Votação",
    "EXECUCAO": "Execução"
}

RESULTADOS = {
    "PENDENTE": -1,
    "REJEITADO": 0,
    "ACEITO": 1
}

def validar_hash(dificuldade, texto):
    prefixo = '0' * dificuldade
    hash_valor = hashlib.sha1(texto.encode('utf-8')).hexdigest()
    return hash_valor, hash_valor.startswith(prefixo)

class ProcessoMineracao(Thread):
    def __init__(self, no, tx_id, dificuldade):
        super().__init__()
        self.no = no
        self.tx_id = tx_id
        self.dificuldade = dificuldade
        self.ativo = True

    def run(self):
        nonce = 0
        while self.ativo:
            candidato = f"{self.tx_id}:{nonce}"
            hash_valor, valido = validar_hash(self.dificuldade, candidato)
            if valido:
                self.no.enviar_solucao(self.tx_id, candidato)
                self.ativo = False
                break
            nonce += 1
            if nonce % PASSO_THROTTLE == 0:
                time.sleep(0.001)

    def parar(self):
        self.ativo = False

class NoDistribuido:
    def __init__(self, host_broker, porta_broker, total_nos):
        self.host_broker = host_broker
        self.porta_broker = porta_broker
        self.total_nos = total_nos
        self.id_no = random.randint(0, 65535)
        self.estado_atual = ESTADOS["INICIAL"]
        self.eh_lider = False
        self.id_lider = -1
        self.tx_atual_id = 0
        self.peers = set()
        self.votos = {}
        self.transacoes = {}
        self.processo_mineracao = None
        self.cliente_mqtt = self._configurar_cliente_mqtt()

    def _configurar_cliente_mqtt(self):
            # Adicionamos VERSION1 para manter compatibilidade com a nova biblioteca
            cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f"Node_{self.id_no}")
            cliente.on_connect = self._ao_conectar
            cliente.on_message = self._ao_receber
            return cliente

    def _conectar_broker(self):
        self.cliente_mqtt.connect(self.host_broker, self.porta_broker, 60)
        self.cliente_mqtt.loop_start()

    def iniciar(self):
        print(f"Iniciando nó {self.id_no} (Total esperado: {self.total_nos})")
        self._conectar_broker()
        while True:
            if self.estado_atual == ESTADOS["INICIAL"]:
                self._anunciar_presenca()
            elif self.estado_atual == ESTADOS["VOTACAO"]:
                self._votar()
            time.sleep(2)

    def _anunciar_presenca(self):
        self._publicar(TOPICOS["HELLO"], {"ClientID": self.id_no})
        if len(self.peers) >= self.total_nos:
            self._transitar_para_votacao()

    def _publicar(self, topico, dados):
        payload = json.dumps(dados)
        self.cliente_mqtt.publish(topico, payload)

    def _transitar_para_votacao(self):
        if self.estado_atual == ESTADOS["VOTACAO"]: return
        print(">>> Rede sincronizada. Iniciando fase de votação...")
        self.estado_atual = ESTADOS["VOTACAO"]
        for _ in range(3):
            self._publicar(TOPICOS["HELLO"], {"ClientID": self.id_no})
            time.sleep(0.1)

    def _votar(self):
        if self.id_no not in self.votos:
            self.votos[self.id_no] = random.randint(0, 65535)
        voto = self.votos.get(self.id_no)
        self._publicar(TOPICOS["VOTO"], {"ClientID": self.id_no, "VoteID": voto})

    def _ao_conectar(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker MQTT.")
            self.cliente_mqtt.subscribe([(TOPICOS[key], 0) for key in TOPICOS])
        else:
            print(f"Erro ao conectar ({rc})")

    def _ao_receber(self, client, userdata, msg):
        try:
            dados = json.loads(msg.payload.decode('utf-8'))
        except:
            return
        topico = msg.topic
        
        if topico == TOPICOS["HELLO"]:
            self._tratar_hello(dados)
        elif topico == TOPICOS["VOTO"]:
            self._tratar_voto(dados)
        elif topico == TOPICOS["TAREFA"]:
            self._tratar_tarefa(dados)
        elif topico == TOPICOS["SOLUCAO"]: 
            if self.eh_lider: self._tratar_solucao(dados)
        elif topico == TOPICOS["RESULTADO"]:
            self._tratar_resultado(dados)

    def _tratar_hello(self, dados):
        id_peer = dados.get("ClientID")
        if id_peer is not None:
            self.peers.add(id_peer)
            if len(self.peers) >= self.total_nos and self.estado_atual == ESTADOS["INICIAL"]:
                self._transitar_para_votacao()

    def _tratar_voto(self, dados):
        id_peer = dados.get("ClientID")
        voto = dados.get("VoteID")
        if id_peer is not None and voto is not None:
            self.votos[id_peer] = voto
        
        if len(self.votos) >= self.total_nos and self.estado_atual == ESTADOS["VOTACAO"]:
            self._contabilizar_votos()

    def _contabilizar_votos(self):
        vencedor = max(self.votos, key=lambda k: (self.votos[k], k))
        self.id_lider = vencedor
        self.eh_lider = (self.id_no == self.id_lider)
        print(f">>> Votação encerrada. Líder: {self.id_lider} (Sou eu? {self.eh_lider})")
        self._transitar_para_execucao()

    def _transitar_para_execucao(self):
        self.estado_atual = ESTADOS["EXECUCAO"]
        if self.eh_lider:
            time.sleep(2)
            self._criar_nova_transacao()

    def _criar_nova_transacao(self):
        self.tx_atual_id += 1
        dificuldade = random.randint(*FAIXA_DIFICULDADE)
        self.transacoes[self.tx_atual_id] = {'Challenge': dificuldade, 'Solution': "", 'Winner': RESULTADOS["PENDENTE"]}
        print(f"--- [LIDER] Criando Transação T{self.tx_atual_id} (Dificuldade {dificuldade}) ---")
        self._publicar(TOPICOS["TAREFA"], {"TransactionID": self.tx_atual_id, "Challenge": dificuldade})

    def _tratar_tarefa(self, dados):
        if self.eh_lider: return
        
        tx_id = dados.get("TransactionID")
        dificuldade = dados.get("Challenge")
        print(f"Recebida Tarefa T{tx_id} (Dif: {dificuldade}). Iniciando mineração...")
        
        if self.processo_mineracao and self.processo_mineracao.is_alive():
            self.processo_mineracao.parar()
        
        self.transacoes[tx_id] = {'Challenge': dificuldade, 'Solution': "", 'Winner': RESULTADOS["PENDENTE"]}
        self.processo_mineracao = ProcessoMineracao(self, tx_id, dificuldade)
        self.processo_mineracao.start()

    def enviar_solucao(self, tx_id, solucao):
        print(f"Solução encontrada para T{tx_id}: {solucao}")
        self._publicar(TOPICOS["SOLUCAO"], {"ClientID": self.id_no, "TransactionID": tx_id, "Solution": solucao})

    def _tratar_solucao(self, dados):
        id_peer = dados.get("ClientID")
        tx_id = dados.get("TransactionID")
        solucao = dados.get("Solution")
        
        if tx_id in self.transacoes and self.transacoes[tx_id]['Winner'] == RESULTADOS["PENDENTE"]:
            if self._validar_solucao(tx_id, solucao):
                self._aprovar_solucao(id_peer, tx_id, solucao)
            else:
                self._rejeitar_solucao(id_peer, tx_id, solucao)

    def _validar_solucao(self, tx_id, solucao):
        desafio = self.transacoes[tx_id]['Challenge']
        _, valido = validar_hash(desafio, solucao)
        return valido

    def _aprovar_solucao(self, id_peer, tx_id, solucao):
        self.transacoes[tx_id]['Winner'] = id_peer
        self.transacoes[tx_id]['Solution'] = solucao
        print(f"--- [LIDER] Solução Aceita de {id_peer} para T{tx_id} ---")
        self._publicar(TOPICOS["RESULTADO"], {"ClientID": id_peer, "TransactionID": tx_id, "Solution": solucao, "Result": RESULTADOS["ACEITO"]})
        time.sleep(3)
        self._criar_nova_transacao()

    def _rejeitar_solucao(self, id_peer, tx_id, solucao):
        print(f"[LIDER] Solução Rejeitada de {id_peer}")
        self._publicar(TOPICOS["RESULTADO"], {"ClientID": id_peer, "TransactionID": tx_id, "Solution": solucao, "Result": RESULTADOS["REJEITADO"]})

    def _tratar_resultado(self, dados):
        tx_id = dados.get("TransactionID")
        resultado = dados.get("Result")
        vencedor = dados.get("ClientID")
        
        if resultado == RESULTADOS["ACEITO"]:
            print(f">>> T{tx_id} FINALIZADA. Vencedor: {vencedor}")
            if self.processo_mineracao and self.processo_mineracao.tx_id == tx_id:
                self.processo_mineracao.parar()

if __name__ == "__main__":
    total_nos = 3
    no = NoDistribuido(HOST_BROKER, PORTA_BROKER, total_nos)
    try:
        no.iniciar()
    except KeyboardInterrupt:
        print("Encerrando nó...")
