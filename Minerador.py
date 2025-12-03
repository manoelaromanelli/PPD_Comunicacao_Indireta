import json
import time
import random
import hashlib
import paho.mqtt.client as mqtt
from threading import Thread

BROKER = "broker.emqx.io"
PORTA = 1883

INTERVALO_DIFICULDADE = (1, 20)  # Ajustado para [1..20]
LIMITE_LOOP = 50000

TOPICOS = {
    "Ola":      "sd/init",
    "Votacao":  "sd/voting",
    "Tarefa":   "sd/challenge",
    "Resposta": "sd/solution",
    "Status":   "sd/result",
}

ESTAGIOS = {
    "SETUP": "Inicialização",
    "ELEICAO": "Votação",
    "TRABALHO": "Execução"
}

RETORNO = {
    "NAO_AVALIADO": -1,
    "INVALIDO": 0,
    "VALIDO": 1
}

def calcula_hash(dificuldade, conteudo):
    alvo = "0" * dificuldade
    resultado = hashlib.sha1(conteudo.encode()).hexdigest()
    return resultado, resultado.startswith(alvo)

class Mineracao(Thread):
    def __init__(self, instancia, tx, dificuldade):
        super().__init__()
        self.ctx = instancia
        self.tx = tx
        self.dificuldade = dificuldade
        self.rodando = True

    def run(self):
        numero = 0
        while self.rodando:
            tentativa = f"{self.tx}:{numero}"
            hash_val, ok = calcula_hash(self.dificuldade, tentativa)

            if ok:
                self.ctx.reportar_solucao(self.tx, tentativa)
                self.rodando = False
                break

            numero += 1

            if numero % LIMITE_LOOP == 0:
                time.sleep(0.001)

    def parar(self):
        self.rodando = False

class Nodo:
    def __init__(self, broker, porta, quantidade):
        self.broker = broker
        self.porta = porta
        self.qtd_nos = quantidade

        self.id = random.randint(0, 65000)
        self.estado = ESTAGIOS["SETUP"]

        self.sou_lider = False
        self.lider_id = None

        self.contagem_tx = -1  # Para que o primeiro TransactionID seja 0
        self.participantes = set()
        self.votacoes = {}
        self.ativos = {}
        self.minerador = None

        self.mqtt = self._configurar_mqtt()

    def _configurar_mqtt(self):
        cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f"NODO_{self.id}")
        cliente.on_connect = self._quando_conectar
        cliente.on_message = self._quando_receber
        return cliente

    def iniciar(self):
        print(f"Iniciando nodo {self.id} aguardando {self.qtd_nos} participantes...")
        self.mqtt.connect(self.broker, self.porta, 60)
        self.mqtt.loop_start()

        while True:
            if self.estado == ESTAGIOS["SETUP"]:
                self._enviar_presenca()
            elif self.estado == ESTAGIOS["ELEICAO"]:
                self._enviar_voto()
            time.sleep(2)

    def _enviar_presenca(self):
        self._publicar(TOPICOS["Ola"], {"ClientID": self.id})

        if len(self.participantes) >= self.qtd_nos:
            self._mudar_para_votacao()

    def _mudar_para_votacao(self):
        if self.estado == ESTAGIOS["ELEICAO"]:
            return

        print(">>> Todos presentes. Começando votação...")
        self.estado = ESTAGIOS["ELEICAO"]

        for _ in range(3):
            self._publicar(TOPICOS["Ola"], {"ClientID": self.id})
            time.sleep(0.1)

    def _enviar_voto(self):
        if self.id not in self.votacoes:
            self.votacoes[self.id] = random.randint(0, 65000)

        voto = self.votacoes[self.id]
        self._publicar(TOPICOS["Votacao"], {"ClientID": self.id, "VoteID": voto})

    def _contar_votos(self):
        vencedor = max(self.votacoes, key=lambda k: (self.votacoes[k], k))
        self.lider_id = vencedor
        self.sou_lider = (vencedor == self.id)

        print(f">>> Líder eleito: {self.lider_id}. Eu sou o líder? {self.sou_lider}")
        self._mudar_para_execucao()

    def _mudar_para_execucao(self):
        self.estado = ESTAGIOS["TRABALHO"]

        if self.sou_lider:
            time.sleep(2)
            self._nova_transacao()  # Primeiro TransactionID será 0

    def _nova_transacao(self):
        self.contagem_tx += 1
        tx_id = self.contagem_tx
        dificuldade = random.randint(*INTERVALO_DIFICULDADE)

        self.ativos[tx_id] = {
            "Challenge": dificuldade,
            "Solution": "",
            "Winner": RETORNO["NAO_AVALIADO"]
        }

        print(f"--- [LIDER] Criada T{tx_id} (Dif = {dificuldade}) ---")
        self._publicar(TOPICOS["Tarefa"], {
            "TransactionID": tx_id,
            "Challenge": dificuldade
        })

    def reportar_solucao(self, tx, sol):
        print(f"Encontrei solução para T{tx}: {sol}")
        self._publicar(TOPICOS["Resposta"], {
            "ClientID": self.id,
            "TransactionID": tx,
            "Solution": sol
        })

    def _quando_conectar(self, client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao broker.")
            self.mqtt.subscribe([(TOPICOS[t], 0) for t in TOPICOS])
        else:
            print(f"Falha ao conectar: {rc}")

    def _quando_receber(self, client, userdata, msg):
        try:
            dados = json.loads(msg.payload.decode())
        except:
            return

        if msg.topic == TOPICOS["Ola"]:
            self._rx_presenca(dados)
        elif msg.topic == TOPICOS["Votacao"]:
            self._rx_votacao(dados)
        elif msg.topic == TOPICOS["Tarefa"]:
            self._rx_tarefa(dados)
        elif msg.topic == TOPICOS["Resposta"] and self.sou_lider:
            self._rx_sol(dados)
        elif msg.topic == TOPICOS["Status"]:
            self._rx_status(dados)

    def _rx_presenca(self, dados):
        cid = dados.get("ClientID")
        if cid is None:
            return

        self.participantes.add(cid)
        if len(self.participantes) >= self.qtd_nos and self.estado == ESTAGIOS["SETUP"]:
            self._mudar_para_votacao()

    def _rx_votacao(self, dados):
        cid = dados.get("ClientID")
        voto = dados.get("VoteID")

        if cid is None or voto is None:
            return

        self.votacoes[cid] = voto

        if len(self.votacoes) >= self.qtd_nos and self.estado == ESTAGIOS["ELEICAO"]:
            self._contar_votos()

    def _rx_tarefa(self, dados):
        if self.sou_lider:
            return

        tx = dados.get("TransactionID")
        dif = dados.get("Challenge")

        print(f"T{tx} recebida. Iniciando busca...")

        if self.minerador and self.minerador.is_alive():
            self.minerador.parar()

        self.ativos[tx] = {
            "Challenge": dif,
            "Solution": "",
            "Winner": RETORNO["NAO_AVALIADO"]
        }

        self.minerador = Mineracao(self, tx, dif)
        self.minerador.start()

    def _rx_sol(self, dados):
        cid = dados.get("ClientID")
        tx = dados.get("TransactionID")
        sol = dados.get("Solution")

        if tx not in self.ativos or self.ativos[tx]["Winner"] != RETORNO["NAO_AVALIADO"]:
            return

        if self._confere(tx, sol):
            self._aprova(cid, tx, sol)
        else:
            self._nega(cid, tx, sol)

    def _confere(self, tx, sol):
        dificuldade = self.ativos[tx]["Challenge"]
        _, ok = calcula_hash(dificuldade, sol)
        return ok

    def _aprova(self, cid, tx, sol):
        self.ativos[tx]["Winner"] = cid
        self.ativos[tx]["Solution"] = sol

        print(f"--- [LIDER] Solução aceita de {cid} para T{tx} ---")

        self._publicar(TOPICOS["Status"], {
            "ClientID": cid,
            "TransactionID": tx,
            "Solution": sol,
            "Result": RETORNO["VALIDO"]
        })

        time.sleep(3)
        self._nova_transacao()

    def _nega(self, cid, tx, sol):
        print(f"[LIDER] Solução inválida de {cid} para T{tx}")
        self._publicar(TOPICOS["Status"], {
            "ClientID": cid,
            "TransactionID": tx,
            "Solution": sol,
            "Result": RETORNO["INVALIDO"]
        })

    def _rx_status(self, dados):
        tx = dados.get("TransactionID")
        res = dados.get("Result")
        cid = dados.get("ClientID")

        if res == RETORNO["VALIDO"]:
            print(f">>> T{tx} resolvida por {cid}")

            if self.minerador and self.minerador.tx == tx:
                self.minerador.parar()

    def _publicar(self, topico, conteudo):
        self.mqtt.publish(topico, json.dumps(conteudo))

if __name__ == "__main__":
    N = 3
    nodo = Nodo(BROKER, PORTA, N)

    try:
        nodo.iniciar()
    except KeyboardInterrupt:
        print("Encerrando...")
