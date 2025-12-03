# Laboratório III – Programação Paralela e Distribuída  
Sistema distribuído com eleição de coordenador e execução cooperativa de tarefas via MQTT

Este repositório reúne a solução desenvolvida para o **Laboratório III** da disciplina de Programação Paralela e Distribuída.  
O arquivo principal (**minerador**) implementa um conjunto de nós que se comunicam por MQTT, elegem um coordenador e processam desafios de mineração utilizando um esquema simples de Proof of Work.

---

## 1) Requisitos para execução

Para rodar o projeto, é necessário:

- **Python 3.8 ou mais recente** (idealmente versão 3.10+)  
- `pip` acessível no sistema  
- **Internet ativa**, pois os nós se conectam ao broker MQTT público `broker.emqx.io`  
- Terminal (PowerShell, CMD, Bash ou equivalente)

---

## 2) Organização dos arquivos

A estrutura do diretório é simples e contém:

```
Trabalho_PPD-CI/
├─ Relatorio_Tecnico.pdf     # Documento explicando conceitos, testes e metodologia
├─ minerador.py              # Aplicação completa (nó líder e nós trabalhadores)
└─ README.md                 # Instruções de uso
```

---

## 3) Ambiente e instalação de dependências

O programa utiliza a biblioteca `paho-mqtt` para comunicação entre os nós.  
Recomenda-se criar um ambiente virtual para evitar conflitos com dependências existentes.

---

### 3.1 Windows (PowerShell)

```powershell
cd C:\caminho\para\Trabalho_PPD
py -m venv .venv
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
pip install paho-mqtt
```

---

### 3.2 Linux / macOS

```bash
cd /caminho/para/Trabalho_PPD
python3 -m venv .venv
source .venv/bin/activate
pip install paho-mqtt
```

---

## 4) Como executar o sistema (mínimo: 3 nós)

O comportamento distribuído só acontece quando **três instâncias** do programa estão ativas ao mesmo tempo.  
Cada instância deve rodar em um terminal diferente.

---

### Terminal 1 (Nó A)

```bash
python minerador.py
```

### Terminal 2 (Nó B)

```bash
cd C:\caminho\para\Trabalho_PPD
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
python minerador.py
```

### Terminal 3 (Nó C)

```bash
cd C:\caminho\para\Trabalho_PPD
Set-ExecutionPolicy Bypass -Scope Process
.\.venv\Scripts\Activate.ps1
python minerador.py
```

---

### Exemplo de início da execução

```
>>> Todos os nós conectados. Iniciando processo de eleição...
Coordenador definido: <ID>
```

---

## 5) Funcionamento geral do sistema

Depois que as três instâncias estão ativas, o programa passa por três grandes etapas: sincronização, eleição e mineração.

---

### 5.1 Sincronização inicial

Cada nó anuncia sua presença no tópico MQTT apropriado até que o grupo alcance o total necessário.

- Mensagem típica: `Presença detectada (X/3)...`
- Comportamento: aguarda os demais nós para formar o grupo

---

### 5.2 Processo de eleição

Quando o conjunto mínimo de nós está ativo, todos participam de uma votação simples:

1. Cada nó gera um valor numérico aleatório.  
2. Todos publicam sua proposta de voto.  
3. O maior valor enviado define o **coordenador** (com desempate por ID).  

Mensagem esperada:

```
Coordenador definido: <ID_do_vencedor>
```

---

### 5.3 Execução das tarefas de mineração

Depois da eleição, inicia-se um ciclo contínuo de criação, distribuição e validação de desafios:

#### A) Função do coordenador
O nó líder cria uma nova tarefa, define uma dificuldade (ex.: quantidade de zeros no início do hash) e publica o desafio.

Log típico:

```
[LIDER] Nova tarefa T1 criada (dificuldade 3)
```

#### B) Função dos trabalhadores
Os nós restantes tentam resolver o desafio testando diferentes valores (nonce) até encontrar um hash válido.

Exemplos:

```
T1 recebida. Iniciando busca...
Solução encontrada: <nonce>
```

A primeira solução válida é enviada ao coordenador.

#### C) Validação e anúncio do resultado
O coordenador verifica a solução recebida e, se estiver correta, publica o resultado para todos.

Mensagem:

```
>>> T1 concluída. Nó vencedor: <ID>
```

Em seguida, uma nova tarefa é criada e o ciclo recomeça.

---

## 6) Observações finais

- Todos os nós utilizam o mesmo arquivo Python; os papéis mudam dinamicamente.  
- O sistema é apenas demonstrativo e não otimizado para mineração real.  
- A comunicação depende de um broker MQTT público, portanto eventuais atrasos podem acontecer.

---

## 7) Documentação complementar

Mais detalhes teóricos e metodológicos podem ser encontrados em:

📄 **Relatorio_Tecnico.pdf**
