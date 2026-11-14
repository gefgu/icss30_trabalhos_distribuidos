# Trabalho 3: guia de defesa

Replicação de dados com gRPC, modelo Pull (estilo KRaft) e eleição de líder (estilo Raft).
Linhas citadas: `src/backend/cluster/cluster.py`, salvo indicação.

---

## 1. Os conceitos, com analogia

Pense num **grupo de 4 colegas** que precisa manter **o mesmo caderno** de anotações.

| Conceito | Na analogia | No código |
| --- | --- | --- |
| **Nó** | cada colega | um processo `cluster.py --id N` |
| **Líder** | o colega que escreve no caderno oficial | `papel = "Lider"` |
| **Seguidor (réplica)** | os outros, que copiam do líder | `papel = "Seguidor"` |
| **Candidato** | quem acha que o líder sumiu e pede votos | `papel = "Candidato"` |
| **Log** | o rascunho: tudo que foi escrito, em ordem | `self.log` (lista de `{termo, indice, dado}`) |
| **Índice** | o número da linha no rascunho | `indice` (começa em 1) |
| **Termo** | o "mandato" do líder (1º, 2º, 3º...) | `self.termo` |
| **Quórum** | maioria: 3 de 4 precisam ter a linha | `QUORUM = 3` (linha 44) |
| **Commit** | a linha passa a valer de verdade (vai pro caderno oficial) | `self.commit_index` |
| **Banco final** | o caderno oficial, o único que o cliente lê | `self.banco` |
| **Fetch (Pull)** | cada colega pergunta ao líder "tem novidade?" a cada 100 ms | `loop_fetch` (456) |
| **Heartbeat** | esse mesmo "tem novidade?" prova que o colega está vivo | `ultimo_fetch` |
| **gRPC** | o "telefone" entre os processos, com um contrato fixo | `service.proto` |

**gRPC em uma frase:** é como o REST do trabalho 2 (chamar e esperar resposta), mas em vez de URLs e JSON livre, há um **contrato** (`.proto`) com as funções e os campos exatos. O `protoc` gera o código (`service_pb2*.py`) a partir dele.

**Pull x Push:** no Raft clássico o líder **empurra** dados para as réplicas. Aqui, como no Kafka/KRaft, **as réplicas puxam** (Fetch). Por isso o líder não precisa saber o endereço de ninguém para replicar: ele só responde.

---

## 2. Estrutura dos arquivos

| Arquivo | O que é |
| --- | --- |
| `src/backend/proto/service.proto` | o contrato: `ClienteService` (Gravar, Consultar) e `ClusterService` (RequestVote, Fetch) |
| `src/backend/proto/service_pb2*.py` | gerados pelo protoc; não se edita à mão |
| `src/backend/cluster/cluster.py` | o nó inteiro |
| `src/backend/cliente/cliente.py` | o cliente com menu |
| `dados/no_<id>/` | o "HD" de cada nó: `estado.json`, `log.json`, `banco.json` |

Cada nó tem **um servidor gRPC** com os dois serviços (cliente e cluster) e **3 threads** de fundo: eleição, fetch e status.

---

## 3. `cluster.py`, função por função

### Configuração (linhas 38 a 64)
`NOS` mapeia id para endereço (50051 a 50054). `QUORUM = 4 // 2 + 1 = 3`. Fetch a cada 100 ms, election timeout sorteado entre 1,5 e 3 s, réplica inativa após 1 s sem Fetch, líder espera o quórum até 15 s. `OPCOES_CANAL` faz o gRPC reconectar rápido quando um nó volta.

### `Persistencia` (83 a 119): o caderno no disco
- `carregar`: ao ligar, lê termo, voto, log e banco.
- `salvar_estado`: termo + `voted_for`. Chamado **antes** de responder um voto, senão um nó que cai e volta poderia votar duas vezes no mesmo termo.
- `salvar_log`, `salvar_banco`: log e dados comitados.
- `_gravar` (95): escrita atômica (grava `.tmp` e troca), o arquivo nunca fica pela metade.

### `No.__init__` (126): estado do nó
Carrega o que estava no disco. Todo nó nasce **Seguidor**. Cria um stub gRPC para cada outro nó. Recebe também o `intervalo_fetch` (normal 100 ms; maior só para a demonstração do Cenário 5).

### Funções de apoio (162 a 184)
- `_sortear_timeout`: timeout **aleatório**, para cada réplica virar candidata num momento diferente (evita **split vote**, empate). Num nó lento, o timeout cresce junto (3 a 6 vezes o intervalo), senão ele acharia que o líder caiu entre um Fetch e outro.
- `ultimo_indice`, `ultimo_termo`, `termo_no_indice`: leem o log.
- `tag`: o prefixo colorido dos prints, como `Seguidor t3 c5 |` (t = termo, c = commit). As cores ficam no topo do arquivo (`COR_PAPEL`, `DESTAQUES`).

### `_atualizar_termo_se_maior` (186): a regra do termo maior
Se receber qualquer mensagem com termo **maior** que o seu: adota o termo, zera o voto, vira Seguidor e salva. Vale até para o líder (é assim que um líder antigo "descobre" que foi substituído).

### `_virar_lider` (201)
Vira líder e dá a todas as réplicas um "crédito" de 1 s como ativas, até elas mandarem o primeiro Fetch.

### `_aplicar_ate` (213): o commit
Copia do log para o banco tudo até o novo `commit_index` e salva. **Só aqui um dado fica visível para leitura.** Acorda o `Gravar` que estava esperando.

### `_avancar_commit_lider` (225): o cálculo do quórum
Junta o último índice do líder com o índice que cada réplica informou no Fetch, ordena do maior para o menor e pega a **3ª posição**: é o maior índice que pelo menos 3 nós têm.
Exemplo: líder 7, réplicas 7, 5 e 2 → `[7, 7, 5, 2]` → commit = 5.
Detalhe do Raft: só comita assim entradas do **termo atual** do líder (as antigas vão junto).

### `_verificar_replicas` (241) e `nos_ativos` (254): detecção de falhas
O líder guarda o instante do último Fetch de cada réplica (`ultimo_fetch`). Passou do Heartbeat Timeout individual dela → **INATIVA**. O timeout é 1 s, ou 3 vezes o intervalo que a réplica informa no Fetch (`intervalo_ms`), para uma réplica lenta não ser dada como morta. `nos_ativos` = 1 (o líder) + réplicas ativas.

### `tratar_request_vote` (260): a urna
1. Aplica a regra do termo maior.
2. **Validação de log** (265): o candidato precisa ter último termo maior, ou mesmo termo e log igual ou mais comprido.
3. **Voto único** (269): só vota se ainda não votou neste termo (ou se é no mesmo candidato).
4. Se concede: salva o voto no disco e reinicia o próprio timer.

### `tratar_fetch` (287): o líder atendendo uma réplica
1. Se não for líder, responde `sucesso=false` com a dica de quem é o líder.
2. Marca a réplica como viva (heartbeat) e guarda o Heartbeat Timeout dela.
3. **Confere se os logs batem** (306): o líder tem a mesma entrada, com o mesmo termo, na posição que a réplica disse ser a última?
4. Se **não** bate: responde `divergente=true, truncar_para=N` (313).
5. Se bate: anota o índice como **confirmação** (324), recalcula o commit e devolve as entradas que faltam + o `commit_index`.

### `tratar_gravar` (334): escrita do cliente
1. Não é líder → `redirecionar=true` com o id do líder.
2. **Sem quórum, sem escrita** (342): menos de 3 ativos → recusa na hora.
3. Grava no log local com **termo atual** e **próximo índice**, salva no disco.
4. Se `simular_crash` (Cenário B): derruba o processo aqui (358).
5. **Espera o commit** (371) até 15 s. Só responde sucesso quando o commit chegou ao índice.

### `tratar_consultar` (387): leitura eventual
Devolve **só o banco** (comitado), mais papel, termo, commit e tamanho do log, para mostrar a diferença.

### `loop_eleicao` (401): o timer de eleição
A cada 50 ms: se for líder, verifica as réplicas. Se não for e passou o timeout sem Fetch bem-sucedido: vira **Candidato**, faz **termo + 1** (413), vota em si, salva e chama `_pedir_votos`.

### `_pedir_votos` (428)
Manda `RequestVote` aos 3 outros nós **em paralelo**. Com 3 votos (o próprio + 2), vira líder.

### `loop_fetch` (456): a réplica puxando
A cada 100 ms (ou o `--intervalo-fetch` escolhido), se não for líder, manda `Fetch` com seu termo, último índice, último termo e o próprio intervalo. Se não sabe quem é o líder, pergunta a um nó por vez (rodízio) e segue a dica. Se o líder não responde, esquece ele.

### `_processar_resposta_fetch` (488): a réplica recebendo
1. Resposta válida do líder → vira/continua Seguidor e **reinicia o timer de eleição**.
2. `divergente` → **trunca** o log (508 a 511), nunca abaixo do commit.
3. Entradas novas → coloca no log, salva, mas **ainda não aparecem na leitura**.
4. Aplica até o `commit_index` do líder (527).

### `loop_status` (529)
Mostra `papel`, `termo`, `commit`, tamanho do log e líder sempre que algo muda (requisito de interface).

### `ClusterServico`, `ClienteServico` (552 a 571) e `iniciar` (574)
Os dois primeiros só repassam as chamadas gRPC para o `No`. O `iniciar` sobe o servidor na porta do nó e liga as 3 threads. No fim do arquivo, o `argparse` lê `--id` e o opcional `--intervalo-fetch` (em ms).

---

## 4. `cliente.py`
- `gravar` (23): manda ao nó escolhido; se vier `redirecionar`, tenta de novo no líder indicado.
- `consultar` (56): mostra papel, termo, commit, tamanho do log e os dados visíveis; avisa quantas entradas estão no log mas não comitadas.
- `menu` (78): 1 gravar, 2 consultar um nó, 3 consultar todos, 4 gravar e derrubar o líder.

---

## 5. Roteiro dos cenários na apresentação

Comece do zero: apague `trabalho3/dados`. Suba os 4 nós e o cliente (ver README). Espere aparecer `*** ELEITO LÍDER ***`.

| # | Cenário | O que fazer | O que mostrar |
| --- | --- | --- | --- |
| 1 | Escrita e leitura | menu 1, mandar para um **seguidor** | o redirecionamento; depois menu 3: o dado em todos os nós |
| 2 | Queda do líder | Ctrl+C no líder | um seguidor vira Candidato, depois Líder, com **termo +1** |
| 3 | Perda de quórum | Ctrl+C em réplicas até sobrar 2 nós; menu 1 | `RECUSADO: Sem quórum: 2 nós ativos, precisa de 3` |
| 4A | Réplica atrasada | Ctrl+C num seguidor, gravar 2 dados, religar | no log dela: `recebeu 2 entrada(s)` e `aplicou no banco` |
| 4B | Truncamento | menu **4** no líder (grava e cai); esperar novo líder; gravar outro dado; religar o antigo | o antigo vira Seguidor e mostra `TRUNCOU entrada não comitada` |
| 5 | Dado não comitado invisível | ver o roteiro abaixo | `log=2`, `visíveis=1` por uns 5 s, depois `visíveis=2` |

### Roteiro do Cenário 5 (nó lento)
Normalmente o dado fica "não comitado" na réplica só por uns 100 ms, rápido demais para ver. Por isso, nesta demonstração o nó 4 faz Fetch a cada 5 s:

1. Apague `trabalho3/dados`. Suba os nós 1, 2 e 3 normalmente e o nó 4 assim:
   `python -m backend.cluster.cluster --id 4 --intervalo-fetch 5000`
2. Espere o líder (será o 1, 2 ou 3). Grave um dado `a` e espere uns 6 s: todos têm `a`.
3. **Desligue um dos nós rápidos que não é o líder** (Ctrl+C). Agora só há 3 nós no ar, então o quórum **precisa** do nó 4.
   (Com os 4 no ar, o líder comitaria só com os 3 rápidos e o nó 4 receberia o dado já comitado.)
4. Grave `b`. O cliente fica esperando.
5. Enquanto espera, num **segundo** terminal de cliente, menu 2 no nó 4, várias vezes:
   - logo após o Fetch dele: `log=2 | visíveis=1` e o aviso `1 entrada(s) no log ainda NÃO comitadas, invisíveis`
   - uns 5 s depois: `commit=2 | visíveis=2`, e o primeiro cliente recebe `SUCESSO`
6. No terminal do nó 4 aparecem, com 5 s de diferença, `recebeu 1 entrada(s) ... (ainda NÃO comitadas, invisíveis para leitura)` e `aplicou no banco: índice 2 = 'b'`.

Por que funciona: o nó 4 recebe `b` no Fetch N, mas o líder só sabe que ele recebeu quando chega o Fetch N+1 (é aí que o índice dele é confirmado). Só então o quórum (líder + nó rápido + nó 4) fecha, o commit avança, e o nó 4 recebe o novo `commit_index` na resposta desse mesmo Fetch.

Persistência: Ctrl+C nos 4 nós e religar. A linha `Carregado do disco: termo=... log=... commit=...` prova que nada se perdeu.

---

## 6. Perguntas prováveis

| Pergunta | Resposta | Onde |
| --- | --- | --- |
| Por que o timeout é aleatório? | Para não virarem candidatos juntos e empatarem a votação (split vote). | 162 |
| Por que quórum 3 e não 2? | Maioria absoluta de 4. Dois grupos de 2 poderiam decidir coisas diferentes; dois grupos de 3 sempre têm alguém em comum. | 44 |
| Como o líder sabe que uma réplica caiu? | Ela para de mandar Fetch; após 1 s sem Fetch, fica inativa. | 241 |
| Como a réplica sabe que o líder caiu? | Os Fetches dela falham; sem Fetch bem-sucedido até o timeout, ela vira candidata. | 401 |
| O que impede votar duas vezes? | `voted_for` por termo, salvo no disco antes de responder. | 269 |
| Por que validar o log no voto? | Para um nó com log atrasado não virar líder e apagar dados já comitados. | 265 |
| Onde o FetchRequest confirma o recebimento? | O `ultimo_indice` que a réplica manda vira `indice_replica`, usado no cálculo do quórum. | 324 |
| Quando a réplica efetiva o dado? | No Fetch seguinte, quando recebe o `commit_index` atualizado. | 527 |
| Por que o líder não comita entradas de termos antigos por contagem? | Uma entrada antiga pode estar na maioria e ainda ser apagada por um líder futuro; só a do termo atual é segura. | 225 |
| Como funciona o truncamento? | O líder vê que o termo na posição informada não bate e manda apagar a última; a réplica repete até os logs baterem. | 306, 508 |
| Pode truncar algo comitado? | Não: o truncamento nunca vai abaixo do `commit_index`. | 508 |
| Por que a escrita do cliente às vezes falha mesmo com o dado no log? | O líder só diz "sucesso" após o quórum; se não conseguir em 15 s, responde falha, mesmo que o dado fique no log. | 371 |
| O que é o `.proto`? | O contrato gRPC; dele o protoc gera as classes e os stubs. | `service.proto` |
| Por que gRPC e não REST? | Contrato tipado, mensagens binárias menores e geração automática de código cliente/servidor. | |

## 7. Limitações (para responder com honestidade)
- Um nó isolado que volta com termo alto força uma nova eleição (o Raft real tem "pre-vote" para evitar isso).
- Após uma eleição, entradas de termos antigos ainda não comitadas só ficam visíveis quando o novo líder recebe uma escrita (o Raft real grava uma entrada vazia ao assumir).
- Todos os nós rodam em `localhost`; os endereços estão fixos em `NOS`.
