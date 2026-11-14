# =============================================================
# Nó do cluster (Trabalho 3): replicação Pull + eleição estilo Raft
#
# Os 4 nós rodam ESTE MESMO código; só muda o --id.
#   cd src
#   python -m backend.cluster.cluster --id 1      (repita com 2, 3 e 4)
#
# Para o Cenário 5 (nó "lento", Fetch a cada 5 s):
#   python -m backend.cluster.cluster --id 4 --intervalo-fetch 5000
#
# Cada nó roda 4 coisas ao mesmo tempo:
#   1. Servidor gRPC  -> atende Cliente (Gravar/Consultar) e outros nós
#                        (RequestVote/Fetch).
#   2. Thread de eleição -> se ficar tempo demais sem falar com o líder,
#                        vira Candidato e pede votos.
#   3. Thread de fetch   -> se NÃO for líder, puxa dados do líder a cada 100 ms
#                        (é o heartbeat do modelo Pull).
#   4. Thread de status  -> mostra papel, termo e commit na tela.
# =============================================================

import argparse
import json
import logging
import os
import random
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc

import backend.proto.service_pb2 as pb2
import backend.proto.service_pb2_grpc as pb2_grpc

# ---------------------------------------------------------------- Configuração
# id do nó -> endereço gRPC
NOS = {
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
    4: "localhost:50054",
}
QUORUM = len(NOS) // 2 + 1          # 4 nós -> 3 (maioria absoluta)

INTERVALO_FETCH = 0.1               # réplica pede dados ao líder a cada 100 ms
ELEICAO_MIN, ELEICAO_MAX = 1.5, 3.0 # election timeout aleatório (segundos)
HEARTBEAT_TIMEOUT = 1.0             # líder: sem Fetch há 1 s = réplica inativa
ESPERA_COMMIT = 15.0                # quanto o líder espera o quórum numa gravação
TIMEOUT_RPC = 0.5                   # tempo máximo de cada chamada gRPC
MAX_ENTRADAS_POR_FETCH = 50

PASTA_DADOS = Path(__file__).resolve().parents[3] / "dados"

LIDER, CANDIDATO, SEGUIDOR = "Lider", "Candidato", "Seguidor"

# Faz o gRPC tentar reconectar rápido quando um nó volta a subir.
OPCOES_CANAL = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 1000),
]

log = logging.getLogger("no")

# Cores no terminal (códigos ANSI), só para facilitar a leitura.
COR_PAPEL = {"Lider": "\033[1;32m", "Candidato": "\033[1;33m", "Seguidor": "\033[36m"}
CINZA, NEGRITO, VERMELHO, FIM = "\033[90m", "\033[1m", "\033[1;31m", "\033[0m"
DESTAQUES = {  # palavra no texto -> cor da linha inteira
    "ELEITO LÍDER": "\033[1;32m", "CANDIDATO": "\033[1;33m", "TRUNCOU": VERMELHO,
    "RECUSOU": VERMELHO, "INATIVA": VERMELHO, "SIMULANDO CRASH": VERMELHO,
    "aplicou no banco": "\033[32m", "NÃO comitadas": "\033[33m",
}


# =============================================================
# Persistência em disco
# Três arquivos por nó, na pasta dados/no_<id>/:
#   estado.json -> termo atual e voto concedido (voted_for)
#   log.json    -> log de operações (termo, índice, dado)
#   banco.json  -> banco final: só dados comitados + commit_index
# =============================================================
class Persistencia:
    def __init__(self, id_no):
        self.pasta = PASTA_DADOS / f"no_{id_no}"
        self.pasta.mkdir(parents=True, exist_ok=True)

    def _ler(self, nome, padrao):
        arquivo = self.pasta / nome
        if not arquivo.exists():
            return padrao
        with open(arquivo, encoding="utf-8") as f:
            return json.load(f)

    def _gravar(self, nome, conteudo):
        # Escrita atômica: grava num .tmp e troca. Se o processo cair no meio,
        # o arquivo antigo continua inteiro.
        arquivo = self.pasta / nome
        temporario = arquivo.with_suffix(".tmp")
        with open(temporario, "w", encoding="utf-8") as f:
            json.dump(conteudo, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        temporario.replace(arquivo)

    def carregar(self):
        estado = self._ler("estado.json", {"termo": 0, "voted_for": None})
        entradas = self._ler("log.json", [])
        banco = self._ler("banco.json", {"commit_index": 0, "dados": []})
        return estado, entradas, banco

    def salvar_estado(self, termo, voted_for):
        self._gravar("estado.json", {"termo": termo, "voted_for": voted_for})

    def salvar_log(self, entradas):
        self._gravar("log.json", entradas)

    def salvar_banco(self, commit_index, dados):
        self._gravar("banco.json", {"commit_index": commit_index, "dados": dados})


# =============================================================
# O nó: todo o estado e as regras de Raft/Pull
# =============================================================
class No:
    def __init__(self, id_no, intervalo_fetch=INTERVALO_FETCH):
        self.id = id_no
        # Normal: 0,1 s. Maior só para demonstrar o Cenário 5 com calma.
        self.intervalo_fetch = intervalo_fetch
        self.disco = Persistencia(id_no)

        estado, entradas, banco = self.disco.carregar()
        # Persistidos
        self.termo = estado["termo"]
        self.voted_for = estado["voted_for"]
        self.log = entradas                      # lista de {"termo","indice","dado"}
        self.commit_index = banco["commit_index"]
        self.banco = banco["dados"]              # o que o cliente consegue ler

        # Só em memória (recomeça a cada execução)
        self.papel = SEGUIDOR                    # todo nó nasce Seguidor
        self.lider_atual = None
        self.ultimo_contato = time.monotonic()   # último Fetch bem-sucedido
        self.timeout_eleicao = self._sortear_timeout()
        # Usados só quando é líder:
        self.indice_replica = {}                 # id -> último índice confirmado
        self.ultimo_fetch = {}                   # id -> instante do último Fetch
        self.timeout_replica = {}                # id -> Heartbeat Timeout dessa réplica
        self.ativas = set()                      # réplicas vistas há < 1 s

        self.trava = threading.RLock()
        self.commit_mudou = threading.Condition(self.trava)

        # Um "stub" (controle remoto gRPC) para cada outro nó
        self.pares = {
            id_par: pb2_grpc.ClusterServiceStub(grpc.insecure_channel(end, options=OPCOES_CANAL))
            for id_par, end in NOS.items()
            if id_par != id_no
        }

    # ---------------------------------------------------- utilidades do log
    def _sortear_timeout(self):
        # Aleatório para que cada réplica vire candidata num momento diferente
        # (evita split vote). Um nó lento espera proporcionalmente mais, senão
        # acharia que o líder caiu entre um Fetch e outro.
        minimo = max(ELEICAO_MIN, 3 * self.intervalo_fetch)
        maximo = max(ELEICAO_MAX, 6 * self.intervalo_fetch)
        return random.uniform(minimo, maximo)

    def ultimo_indice(self):
        return len(self.log)

    def ultimo_termo(self):
        return self.log[-1]["termo"] if self.log else 0

    def termo_no_indice(self, indice):
        if indice == 0:
            return 0
        return self.log[indice - 1]["termo"]

    def tag(self):
        cor = COR_PAPEL.get(self.papel, "")
        return f"{cor}{self.papel:<9}{FIM} {CINZA}t{self.termo} c{self.commit_index}{FIM} |"

    # ---------------------------------------------------- mudanças de papel
    def _atualizar_termo_se_maior(self, termo_recebido):
        """Regra obrigatória: viu termo maior -> adota o termo e vira Seguidor."""
        if termo_recebido > self.termo:
            antigo = self.papel
            self.termo = termo_recebido
            self.voted_for = None
            self.papel = SEGUIDOR
            self.lider_atual = None
            self.disco.salvar_estado(self.termo, self.voted_for)
            if antigo != SEGUIDOR:
                log.info(f"{self.tag()} viu termo maior ({termo_recebido}): {antigo} -> Seguidor")
            self.commit_mudou.notify_all()  # acorda gravações que estavam esperando
            return True
        return False

    def _virar_lider(self):
        self.papel = LIDER
        self.lider_atual = self.id
        agora = time.monotonic()
        # Dá às réplicas um "crédito" inicial: até provarem o contrário, contam
        # como ativas por HEARTBEAT_TIMEOUT.
        self.indice_replica = {p: 0 for p in self.pares}
        self.ultimo_fetch = {p: agora for p in self.pares}
        self.ativas = set(self.pares)
        log.info(f"{self.tag()} *** ELEITO LÍDER ***")

    # ---------------------------------------------------- commit e aplicação
    def _aplicar_ate(self, novo_commit):
        """Efetiva no banco final tudo até novo_commit (o que o cliente pode ler)."""
        novo_commit = min(novo_commit, self.ultimo_indice())
        if novo_commit <= self.commit_index:
            return
        for entrada in self.log[self.commit_index:novo_commit]:
            self.banco.append(entrada)
            log.info(f"{self.tag()} aplicou no banco: índice {entrada['indice']} = {entrada['dado']!r}")
        self.commit_index = novo_commit
        self.disco.salvar_banco(self.commit_index, self.banco)
        self.commit_mudou.notify_all()

    def _avancar_commit_lider(self):
        """
        Quórum: junta o último índice do líder com os índices que cada réplica
        informou no Fetch. Em ordem decrescente, a posição QUORUM-1 é o maior
        índice que pelo menos 3 nós já têm.
        Ex.: líder 7, réplicas 7, 5, 2 -> [7, 7, 5, 2] -> 3º maior = 5.
        """
        indices = sorted([self.ultimo_indice(), *self.indice_replica.values()], reverse=True)
        candidato = indices[QUORUM - 1]
        # Regra do Raft: o líder só comita por contagem entradas do PRÓPRIO termo
        # (as antigas vão junto). Evita comitar algo que ainda pode ser apagado.
        if candidato > self.commit_index and self.termo_no_indice(candidato) == self.termo:
            log.info(f"{self.tag()} quórum confirmou até o índice {candidato} (índices {indices})")
            self._aplicar_ate(candidato)

    # ---------------------------------------------------- monitor de réplicas
    def _verificar_replicas(self):
        """Líder: marca como inativa a réplica sem Fetch há mais de 1 s."""
        agora = time.monotonic()
        for id_par, instante in self.ultimo_fetch.items():
            limite = self.timeout_replica.get(id_par, HEARTBEAT_TIMEOUT)
            ativa = agora - instante <= limite
            if ativa and id_par not in self.ativas:
                self.ativas.add(id_par)
                log.info(f"{self.tag()} réplica {id_par} voltou (ATIVA)")
            elif not ativa and id_par in self.ativas:
                self.ativas.discard(id_par)
                log.info(f"{self.tag()} réplica {id_par} INATIVA (sem Fetch há {agora - instante:.1f}s)")

    def nos_ativos(self):
        return 1 + len(self.ativas)  # o próprio líder + réplicas ativas

    # =========================================================
    # Handlers gRPC (chamados por outros nós e pelo cliente)
    # =========================================================
    def tratar_request_vote(self, req):
        with self.trava:
            self._atualizar_termo_se_maior(req.termo_candidato)

            # Validação de log: o candidato precisa estar tão ou mais atualizado.
            log_ok = (req.ultimo_termo_log > self.ultimo_termo()) or (
                req.ultimo_termo_log == self.ultimo_termo()
                and req.ultimo_indice_log >= self.ultimo_indice()
            )
            # Voto único: no máximo um candidato por termo.
            pode_votar = self.voted_for in (None, req.id_candidato)

            concedido = req.termo_candidato == self.termo and pode_votar and log_ok
            if concedido:
                self.voted_for = req.id_candidato
                self.disco.salvar_estado(self.termo, self.voted_for)  # persiste ANTES de responder
                self.ultimo_contato = time.monotonic()  # quem votou espera o resultado
                log.info(f"{self.tag()} votou no nó {req.id_candidato}")
            else:
                motivo = (
                    "termo antigo" if req.termo_candidato < self.termo
                    else f"já votou no nó {self.voted_for}" if not pode_votar
                    else "log do candidato está desatualizado"
                )
                log.info(f"{self.tag()} NEGOU voto ao nó {req.id_candidato} ({motivo})")
            return pb2.VoteResponse(termo_atual=self.termo, voto_concedido=concedido)

    def tratar_fetch(self, req):
        with self.trava:
            self._atualizar_termo_se_maior(req.termo_atual)

            if self.papel != LIDER:
                # Não sou o líder: devolvo só a dica de quem é.
                return pb2.FetchResponse(
                    sucesso=False, termo=self.termo, lider_atual=self.lider_atual or 0
                )

            # Heartbeat: a réplica está viva.
            self.ultimo_fetch[req.id_replica] = time.monotonic()
            # Heartbeat Timeout individual: 1 s, ou 3x o intervalo de uma réplica lenta.
            self.timeout_replica[req.id_replica] = max(HEARTBEAT_TIMEOUT, 3 * req.intervalo_ms / 1000)
            if req.id_replica not in self.ativas:
                self.ativas.add(req.id_replica)
                log.info(f"{self.tag()} réplica {req.id_replica} voltou (ATIVA)")

            # O log da réplica bate com o meu até ultimo_indice?
            bate = req.ultimo_indice <= self.ultimo_indice() and (
                self.termo_no_indice(req.ultimo_indice) == req.ultimo_termo
            )
            if not bate:
                # Divergência: mando apagar a última entrada (ou o que eu não tenho)
                # e a réplica confere de novo no próximo Fetch, até achar o ponto
                # em que os logs batem.
                truncar = min(req.ultimo_indice - 1, self.ultimo_indice())
                log.info(
                    f"{self.tag()} log da réplica {req.id_replica} diverge no índice "
                    f"{req.ultimo_indice} -> mandando truncar para {truncar}"
                )
                return pb2.FetchResponse(
                    sucesso=True, termo=self.termo, lider_atual=self.id,
                    commit_index=self.commit_index, divergente=True, truncar_para=truncar,
                )

            # O índice informado é a CONFIRMAÇÃO do que a réplica já tem.
            self.indice_replica[req.id_replica] = req.ultimo_indice
            self._avancar_commit_lider()

            novas = self.log[req.ultimo_indice : req.ultimo_indice + MAX_ENTRADAS_POR_FETCH]
            return pb2.FetchResponse(
                sucesso=True, termo=self.termo, lider_atual=self.id,
                commit_index=self.commit_index,
                entradas=[pb2.Registro(**e) for e in novas],
            )

    def tratar_gravar(self, req):
        with self.trava:
            if self.papel != LIDER:
                return pb2.GravarResponse(
                    sucesso=False, redirecionar=True, lider_atual=self.lider_atual or 0,
                    mensagem=f"Nó {self.id} é {self.papel}; procure o líder.",
                )

            # Sem quórum, sem escrita: recusa na hora se sei que não vou conseguir.
            self._verificar_replicas()
            if self.nos_ativos() < QUORUM:
                log.info(f"{self.tag()} RECUSOU escrita: só {self.nos_ativos()} nós ativos (quórum {QUORUM})")
                return pb2.GravarResponse(
                    sucesso=False, lider_atual=self.id,
                    mensagem=f"Sem quórum: {self.nos_ativos()} nós ativos, precisa de {QUORUM}.",
                )

            # Grava no log local com o termo atual e o próximo índice.
            entrada = {"termo": self.termo, "indice": self.ultimo_indice() + 1, "dado": req.dado}
            self.log.append(entrada)
            self.disco.salvar_log(self.log)
            log.info(f"{self.tag()} gravou no log local: índice {entrada['indice']} = {req.dado!r} (aguardando quórum)")

            if req.simular_crash:
                log.info(f"{self.tag()} SIMULANDO CRASH antes de replicar!")
                os._exit(1)

            # Espera o commit avançar (quem acorda é o _aplicar_ate, quando os
            # Fetches das réplicas confirmarem a entrada).
            termo_gravacao = self.termo
            limite = time.monotonic() + ESPERA_COMMIT
            while self.commit_index < entrada["indice"]:
                if self.papel != LIDER or self.termo != termo_gravacao:
                    break
                restante = limite - time.monotonic()
                if restante <= 0:
                    break
                self.commit_mudou.wait(timeout=restante)

            comitado = (
                self.commit_index >= entrada["indice"]
                and self.termo_no_indice(entrada["indice"]) == termo_gravacao
            )
            if comitado:
                return pb2.GravarResponse(
                    sucesso=True, lider_atual=self.id, indice=entrada["indice"], termo=termo_gravacao,
                    mensagem=f"Comitado no índice {entrada['indice']} (termo {termo_gravacao}).",
                )
            return pb2.GravarResponse(
                sucesso=False, lider_atual=self.lider_atual or 0,
                mensagem="Não foi possível confirmar no quórum a tempo; escrita não comitada.",
            )

    def tratar_consultar(self):
        with self.trava:
            # Leitura eventual: SÓ o banco final (dados comitados). O que está no
            # log mas ainda não foi comitado fica invisível.
            return pb2.ConsultarResponse(
                dados=[pb2.Registro(**e) for e in self.banco],
                id_no=self.id, papel=self.papel, termo=self.termo,
                commit_index=self.commit_index, tamanho_log=self.ultimo_indice(),
                lider_atual=self.lider_atual or 0,
            )

    # =========================================================
    # Threads de fundo
    # =========================================================
    def loop_eleicao(self):
        """Seguidor/Candidato: sem contato com o líder por timeout -> eleição."""
        while True:
            time.sleep(0.05)
            with self.trava:
                if self.papel == LIDER:
                    self._verificar_replicas()
                    continue
                if time.monotonic() - self.ultimo_contato < self.timeout_eleicao:
                    continue
                # Timeout! Vira candidato.
                self.papel = CANDIDATO
                self.termo += 1                      # Termo = Termo + 1
                self.voted_for = self.id             # vota em si mesmo
                self.lider_atual = None
                self.disco.salvar_estado(self.termo, self.voted_for)
                self.ultimo_contato = time.monotonic()
                self.timeout_eleicao = self._sortear_timeout()
                termo_eleicao = self.termo
                pedido = pb2.VoteRequest(
                    termo_candidato=self.termo, id_candidato=self.id,
                    ultimo_indice_log=self.ultimo_indice(), ultimo_termo_log=self.ultimo_termo(),
                )
                log.info(f"{self.tag()} timeout sem líder -> CANDIDATO, pedindo votos")

            self._pedir_votos(pedido, termo_eleicao)

    def _pedir_votos(self, pedido, termo_eleicao):
        votos = 1  # o próprio voto
        trava_votos = threading.Lock()

        def pedir(id_par, stub):
            nonlocal votos
            try:
                resp = stub.RequestVote(pedido, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                return  # nó fora do ar
            with self.trava:
                if self._atualizar_termo_se_maior(resp.termo_atual):
                    return
                if resp.voto_concedido and self.papel == CANDIDATO and self.termo == termo_eleicao:
                    with trava_votos:
                        votos += 1
                        total = votos
                    log.info(f"{self.tag()} recebeu voto do nó {id_par} ({total}/{QUORUM})")
                    if total >= QUORUM:
                        self._virar_lider()

        # Pede os votos em paralelo
        threads = [threading.Thread(target=pedir, args=(i, s), daemon=True) for i, s in self.pares.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def loop_fetch(self):
        """Réplica: puxa dados do líder a cada 100 ms (modelo Pull + heartbeat)."""
        proximo_palpite = 0
        while True:
            time.sleep(self.intervalo_fetch)
            with self.trava:
                if self.papel == LIDER:
                    continue
                alvo = self.lider_atual
                if alvo is None:
                    # Não sei quem é o líder: pergunto a um nó por vez, em rodízio.
                    ids = list(self.pares)
                    alvo = ids[proximo_palpite % len(ids)]
                    proximo_palpite += 1
                pedido = pb2.FetchRequest(
                    id_replica=self.id, termo_atual=self.termo,
                    ultimo_indice=self.ultimo_indice(), ultimo_termo=self.ultimo_termo(),
                    intervalo_ms=int(self.intervalo_fetch * 1000),
                )

            try:
                resp = self.pares[alvo].Fetch(pedido, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                with self.trava:
                    if self.lider_atual == alvo:
                        log.info(f"{self.tag()} líder {alvo} não respondeu")
                        self.lider_atual = None
                continue

            with self.trava:
                self._processar_resposta_fetch(alvo, resp)

    def _processar_resposta_fetch(self, alvo, resp):
        self._atualizar_termo_se_maior(resp.termo)
        if not resp.sucesso:
            # Não era o líder; seguir a dica, se houver.
            self.lider_atual = resp.lider_atual if resp.lider_atual not in (0, self.id) else None
            return
        if resp.termo < self.termo:
            return  # resposta de um líder antigo; ignora

        # Fetch bem-sucedido com o líder atual: reinicia o timer de eleição.
        if self.papel == CANDIDATO:
            log.info(f"{self.tag()} encontrou o líder {alvo}: Candidato -> Seguidor")
        self.papel = SEGUIDOR
        if self.lider_atual != alvo:
            log.info(f"{self.tag()} seguindo o líder {alvo}")
        self.lider_atual = alvo
        self.ultimo_contato = time.monotonic()

        if resp.divergente:
            # Truncamento: apaga do ponto conflitante em diante (nunca o comitado).
            novo_tamanho = max(resp.truncar_para, self.commit_index)
            if novo_tamanho < self.ultimo_indice():
                apagadas = self.log[novo_tamanho:]
                self.log = self.log[:novo_tamanho]
                self.disco.salvar_log(self.log)
                for e in apagadas:
                    log.info(f"{self.tag()} TRUNCOU entrada não comitada: índice {e['indice']} termo {e['termo']} {e['dado']!r}")
            return

        if resp.entradas:
            for r in resp.entradas:
                self.log.append({"termo": r.termo, "indice": r.indice, "dado": r.dado})
            self.disco.salvar_log(self.log)
            log.info(
                f"{self.tag()} recebeu {len(resp.entradas)} entrada(s) até o índice "
                f"{self.ultimo_indice()} (ainda NÃO comitadas, invisíveis para leitura)"
            )

        # Efetiva o que o líder diz que já está comitado.
        self._aplicar_ate(resp.commit_index)

    def loop_status(self):
        """Mostra papel, termo e commit (requisito da interface)."""
        anterior = None
        ultimo_print = 0
        while True:
            time.sleep(0.5)
            with self.trava:
                atual = (self.papel, self.termo, self.commit_index, self.ultimo_indice(), self.lider_atual)
                extra = f" | ativos {self.nos_ativos()}/{len(NOS)}" if self.papel == LIDER else ""
                cor = COR_PAPEL.get(self.papel, "")
                linha = (
                    f"{NEGRITO}== Nó {self.id}: {cor}{self.papel}{FIM}{NEGRITO} | termo {self.termo} "
                    f"| commit {self.commit_index} | log {self.ultimo_indice()} "
                    f"| líder {self.lider_atual or '?'}{extra} =={FIM}"
                )
            if atual != anterior or time.monotonic() - ultimo_print > 30:
                log.info(linha)
                anterior, ultimo_print = atual, time.monotonic()


# =============================================================
# Adaptadores gRPC: só repassam a chamada para o No
# =============================================================
class ClusterServico(pb2_grpc.ClusterServiceServicer):
    def __init__(self, no):
        self.no = no

    def RequestVote(self, request, context):
        return self.no.tratar_request_vote(request)

    def Fetch(self, request, context):
        return self.no.tratar_fetch(request)


class ClienteServico(pb2_grpc.ClienteServiceServicer):
    def __init__(self, no):
        self.no = no

    def Gravar(self, request, context):
        return self.no.tratar_gravar(request)

    def Consultar(self, request, context):
        return self.no.tratar_consultar()


def iniciar(id_no, intervalo_fetch):
    no = No(id_no, intervalo_fetch)
    porta = NOS[id_no].split(":")[1]

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    pb2_grpc.add_ClusterServiceServicer_to_server(ClusterServico(no), servidor)
    pb2_grpc.add_ClienteServiceServicer_to_server(ClienteServico(no), servidor)
    servidor.add_insecure_port(f"[::]:{porta}")
    servidor.start()

    log.info(
        f"Nó {id_no} na porta {porta}. Carregado do disco: termo={no.termo} "
        f"voted_for={no.voted_for} log={no.ultimo_indice()} commit={no.commit_index} "
        f"| Fetch a cada {int(intervalo_fetch * 1000)} ms"
    )

    for alvo in (no.loop_eleicao, no.loop_fetch, no.loop_status):
        threading.Thread(target=alvo, daemon=True).start()

    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Desligando...")
        servidor.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nó do cluster")
    parser.add_argument("--id", type=int, required=True, choices=sorted(NOS))
    parser.add_argument(
        "--intervalo-fetch", type=int, default=int(INTERVALO_FETCH * 1000),
        help="ms entre Fetches (padrão 100). Use 5000 para demonstrar o Cenário 5.",
    )
    args = parser.parse_args()
    class Colorido(logging.Formatter):
        def format(self, registro):
            texto = super().format(registro)
            for palavra, cor in DESTAQUES.items():
                if palavra in registro.getMessage():
                    # reaplica a cor depois de cada "fim de cor" que já estiver no meio
                    return cor + texto.replace(FIM, FIM + cor) + FIM
            return texto

    tratador = logging.StreamHandler()
    tratador.setFormatter(Colorido("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[tratador])
    iniciar(args.id, args.intervalo_fetch / 1000)
