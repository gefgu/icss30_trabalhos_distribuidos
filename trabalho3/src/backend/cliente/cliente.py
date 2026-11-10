import grpc
import logging
import backend.proto.service_pb2 as pb2
import backend.proto.service_pb2_grpc as pb2_grpc

def tentar_gravar(stub, dado, porta_atual):
    try:
        requisicao = pb2.GravarRequest(dado=dado)
        resposta = stub.Gravar(requisicao)
        
        if resposta.redirecionar:
            logging.info(f"Redirecionado para o líder: Nó {resposta.lider_atual}")
            return False, resposta.lider_atual
            
        if resposta.sucesso:
            logging.info("Dado gravado com sucesso no cluster!")
            return True, None
            
    except grpc.RpcError as e:
        logging.error(f"Falha na comunicação: {e.code()}")
        return False, None

def consultar_dados(stub):
    try:
        requisicao = pb2.ConsultarRequest()
        resposta = stub.Consultar(requisicao)
        logging.info(f"Dados retornados (Leitura Eventual): {resposta.dados_comitados}")
        
    except grpc.RpcError as e:
        logging.error(f"Falha na consulta: {e.code()}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    porta_alvo = '50051'
    canal = grpc.insecure_channel(f'localhost:{porta_alvo}')
    stub = pb2_grpc.ClienteServiceStub(canal)
    
    consultar_dados(stub)
    sucesso, novo_lider = tentar_gravar(stub, "Novo dado do cliente", porta_alvo)
    
    if not sucesso and novo_lider:
        pass
