# =============================================================
# Cliente do Trabalho 3 (fala com o cluster só por gRPC)
#   cd src
#   python -m backend.cliente.cliente
# =============================================================

import grpc

import backend.proto.service_pb2 as pb2
import backend.proto.service_pb2_grpc as pb2_grpc
from backend.cluster.cluster import NOS, OPCOES_CANAL

TIMEOUT_GRAVAR = 20  # o líder espera até 15 s pelo quórum; damos uma folga
TIMEOUT_CONSULTA = 2

# Um stub de ClienteService para cada nó
stubs = {
    id_no: pb2_grpc.ClienteServiceStub(grpc.insecure_channel(end, options=OPCOES_CANAL))
    for id_no, end in NOS.items()
}


def gravar(dado, no_inicial, simular_crash=False):
    """
    Gravação com redirecionamento: tenta o nó escolhido; se ele não for o
    líder, a resposta diz quem é, e o cliente tenta de novo lá.
    """
    alvo = no_inicial
    for _ in range(len(NOS) + 1):
        print(f"  -> enviando Gravar({dado!r}) para o nó {alvo}")
        try:
            resp = stubs[alvo].Gravar(
                pb2.GravarRequest(dado=dado, simular_crash=simular_crash), timeout=TIMEOUT_GRAVAR
            )
        except grpc.RpcError as erro:
            if simular_crash:
                print(f"  <- nó {alvo} caiu, como esperado (crash simulado)")
            else:
                print(f"  <- nó {alvo} não respondeu ({erro.code().name})")
            return

        if resp.redirecionar:
            if resp.lider_atual == 0:
                print(f"  <- {resp.mensagem} Ainda não há líder conhecido; tente de novo em instantes.")
                return
            print(f"  <- {resp.mensagem} Líder atual: nó {resp.lider_atual}. Redirecionando...")
            alvo = resp.lider_atual
            continue

        status = "SUCESSO" if resp.sucesso else "RECUSADO"
        print(f"  <- {status}: {resp.mensagem}")
        return
    print("  <- desisti: redirecionamentos demais")


def consultar(id_no):
    try:
        r = stubs[id_no].Consultar(pb2.ConsultarRequest(), timeout=TIMEOUT_CONSULTA)
    except grpc.RpcError:
        print(f"  Nó {id_no}: FORA DO AR")
        return
    lider = r.lider_atual or "?"
    print(
        f"  Nó {r.id_no} ({r.papel}, termo {r.termo}, líder {lider}) | "
        f"commit={r.commit_index} | log={r.tamanho_log} entradas | visíveis={len(r.dados)}"
    )
    for d in r.dados:
        print(f"      [{d.indice}] termo {d.termo}: {d.dado}")
    if r.tamanho_log > r.commit_index:
        print(f"      ({r.tamanho_log - r.commit_index} entrada(s) no log ainda NÃO comitadas, invisíveis)")


def escolher_no(texto="Nó"):
    valor = input(f"{texto} {sorted(NOS)}: ").strip()
    return int(valor) if valor.isdigit() and int(valor) in NOS else None


def menu():
    while True:
        print("\n===== CLIENTE =====")
        print("1 - Gravar dado (com redirecionamento ao líder)")
        print("2 - Consultar um nó")
        print("3 - Consultar todos os nós")
        print("4 - Gravar e derrubar o líder (Cenário B)")
        print("0 - Sair")
        opcao = input("> ").strip()

        if opcao in ("1", "4"):
            no = escolher_no("Enviar para o nó")
            if no is None:
                print("Nó inválido.")
                continue
            dado = input("Dado: ").strip() or "dado-teste"
            gravar(dado, no, simular_crash=(opcao == "4"))
        elif opcao == "2":
            no = escolher_no()
            if no is None:
                print("Nó inválido.")
                continue
            consultar(no)
        elif opcao == "3":
            for no in sorted(NOS):
                consultar(no)
        elif opcao == "0":
            break


if __name__ == "__main__":
    menu()
