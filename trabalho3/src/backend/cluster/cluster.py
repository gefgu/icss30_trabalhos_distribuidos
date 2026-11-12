# Falta implementar:
# Implementar lógica de validação de log e concessão de voto
# Atualizar temporizador de inatividade da réplica no líder
# Calcular quórum e avançar índice de commit
# Lógica do líder para adicionar ao log e aguardar quórum antes de confirmar
# Persistência do estado do nó
# Iniciar thread do Temporizador Aleatório para eleição
# Iniciar thread do Loop de Fetch Ativo se o nó for uma réplica

import grpc
import logging
import threading
import time
from concurrent import futures
import backend.proto.service_pb2 as pb2
import backend.proto.service_pb2_grpc as pb2_grpc

QUORUM = 3

class ClusterService(pb2_grpc.ClusterServiceServicer):
    def __init__(self, no_estado):
        self.estado = no_estado

    def RequestVote(self, request, context):
        logging.info(f"Recebeu RequestVote do candidato {request.id_candidato}")
        
        return pb2.VoteResponse(
            termo_atual=self.estado['termo_atual'],
            voto_concedido=False
        )

    def Fetch(self, request, context):
        logging.info(f"Recebeu Fetch/Heartbeat da réplica {request.id_replica}")
        
        return pb2.FetchResponse(
            commit_index=self.estado['indice_commit'],
            log=[]
        )

class ClienteService(pb2_grpc.ClienteServiceServicer):
    def __init__(self, no_estado):
        self.estado = no_estado

    def Gravar(self, request, context):
        logging.info("Recebeu requisição de Gravar do cliente")
        
        if self.estado['papel'] != 'Lider':
            # Redirecionamento: informa quem é o líder atual
            return pb2.GravarResponse(
                sucesso=False, 
                redirecionar=True, 
                lider_atual=str(self.estado['lider_atual_id'])
            )
            
        return pb2.GravarResponse(sucesso=True, redirecionar=False, lider_atual="")

    def Consultar(self, request, context):
        logging.info("Recebeu requisição de Consultar do cliente")
        return pb2.ConsultarResponse(dados_comitados="Dados de exemplo")

def iniciar_servidor(porta, id_no):
    estado_local = {
        'id': id_no,
        'papel': 'Seguidor',     # Inicia sempre como Seguidor
        'termo_atual': 0,
        'voted_for': None,
        'log': [],
        'indice_commit': 0,
        'lider_atual_id': None
    }

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registra os dois serviços (Comunicação entre nós e comunicação com clientes) no mesmo servidor gRPC
    pb2_grpc.add_ClusterServiceServicer_to_server(ClusterService(estado_local), servidor)
    pb2_grpc.add_ClienteServiceServicer_to_server(ClienteService(estado_local), servidor)
    
    servidor.add_insecure_port(f'[::]:{porta}')
    servidor.start()
    logging.info(f"Nó {id_no} iniciado na porta {porta} como {estado_local['papel']}")
    
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        servidor.stop(0)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    iniciar_servidor(porta='50051', id_no=1)
    # Serão 4 servidores, um para cada nó 
    # Rodar no terminal: iniciar_servidor(porta='50051', id_no=1), mudar porta e id para cada nó