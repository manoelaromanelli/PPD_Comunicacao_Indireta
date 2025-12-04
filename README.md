# Laboratório III – Programação Paralela e Distribuída  
Sistema distribuído com eleição de coordenador e execução cooperativa de tarefas via MQTT

Este repositório reúne a solução desenvolvida para o **Laboratório III** da disciplina de Programação Paralela e Distribuída.  
O arquivo principal (**miner**) implementa um conjunto de nós que se comunicam por MQTT, elegem um coordenador e processam desafios de mineração utilizando um esquema simples de Proof of Work.

---

## 1) Requisitos para execução

Para rodar o projeto, é necessário:
- **Internet ativa**, pois os nós se conectam ao broker MQTT público `broker.emqx.io`  
- Terminal (PowerShell, CMD, Bash ou equivalente)
- **Python 3.8 ou mais recente**
- `pip` acessível no sistema  


---

## 2) Tecnologias Utilizadas
* **Linguagem:** Python 3.x
* **Comunicação:** MQTT via biblioteca **Paho MQTT**.
* **Infraestrutura:** Docker (Container **EMQX** como Broker de Mensagens).
* **Concorrência:** Biblioteca `threading` (para mineração paralela e controle de fluxo).

---

## 3) Ambiente e instalação de dependências

Para preparar o ambiente, será necessário instalar a biblioteca Paho MQTT e criar e ativar venv. Abra o terminal e execute:

---

### 3.1 Windows (PowerShell)

```powershell
cd [caminho para a pasta]
py -m venv .venv
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
pip install paho-mqtt
```

---

### 3.2 Linux / macOS

```bash
cd [caminho para a pasta]
python3 -m venv .venv
source .venv/bin/activate
pip install paho-mqtt
```

---

## 4) Execução

O sistema exige no mínimo 3 participantes, o que significa que iremos utilizar 3 terminais diferentres.

---

### Terminal 1 (mesmo terminal usado na configuração do ambiente)

```bash
python miner.py
```

### Terminal 2

```bash
cd [caminho para a pasta]
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
python miner.py
```

### Terminal 3

```bash
cd [caminho para a pasta]
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
python miner.py
```

---

## 5) Funcionamento geral do sistema

Depois que as três instâncias estão ativas, o programa passa por três grandes etapas: sincronização, eleição e mineração.

---

### Sincronização

Cada nó anuncia sua presença no tópico MQTT apropriado até que o grupo alcance o total necessário.

---

### Eleição

Quando o conjunto mínimo de nós está ativo, todos participam de uma votação simples:

1. Cada nó gera um valor numérico aleatório.  
2. Todos publicam sua proposta de voto.  
3. O maior valor enviado define o **líder** 

---

### Mineração

Depois da eleição, inicia-se um ciclo contínuo de criação, distribuição e validação de tarefas:

#### Líder
O nó líder cria uma nova transação, define uma dificuldade e publica a tarefa.

#### mineradores
Os nós restantes tentam resolver a tarefa testando diferentes valores até encontrar um hash válido.

A primeira solução válida é enviada ao líder.

#### C) Validação e anúncio do resultado
O líder verifica a solução recebida e, se estiver correta, publica o resultado para todos.

Em seguida, uma nova tarefa é criada e o ciclo recomeça.

---

## 6) Observações finais

- Todos os nós utilizam o mesmo arquivo Python; os papéis mudam dinamicamente.  
- O sistema é apenas demonstrativo e não otimizado para mineração real.  
- A comunicação depende de um broker MQTT público, portanto eventuais atrasos podem acontecer.

---

## 7) Documentação complementar

Mais detalhes teóricos e metodológicos podem ser encontrados em: **Relatorio_Tecnico.pdf**
