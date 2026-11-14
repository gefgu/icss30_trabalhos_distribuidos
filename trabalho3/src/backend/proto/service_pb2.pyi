from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Registro(_message.Message):
    __slots__ = ("termo", "indice", "dado")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    INDICE_FIELD_NUMBER: _ClassVar[int]
    DADO_FIELD_NUMBER: _ClassVar[int]
    termo: int
    indice: int
    dado: str
    def __init__(self, termo: _Optional[int] = ..., indice: _Optional[int] = ..., dado: _Optional[str] = ...) -> None: ...

class GravarRequest(_message.Message):
    __slots__ = ("dado", "simular_crash")
    DADO_FIELD_NUMBER: _ClassVar[int]
    SIMULAR_CRASH_FIELD_NUMBER: _ClassVar[int]
    dado: str
    simular_crash: bool
    def __init__(self, dado: _Optional[str] = ..., simular_crash: _Optional[bool] = ...) -> None: ...

class GravarResponse(_message.Message):
    __slots__ = ("sucesso", "redirecionar", "lider_atual", "mensagem", "indice", "termo")
    SUCESSO_FIELD_NUMBER: _ClassVar[int]
    REDIRECIONAR_FIELD_NUMBER: _ClassVar[int]
    LIDER_ATUAL_FIELD_NUMBER: _ClassVar[int]
    MENSAGEM_FIELD_NUMBER: _ClassVar[int]
    INDICE_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    sucesso: bool
    redirecionar: bool
    lider_atual: int
    mensagem: str
    indice: int
    termo: int
    def __init__(self, sucesso: _Optional[bool] = ..., redirecionar: _Optional[bool] = ..., lider_atual: _Optional[int] = ..., mensagem: _Optional[str] = ..., indice: _Optional[int] = ..., termo: _Optional[int] = ...) -> None: ...

class ConsultarRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ConsultarResponse(_message.Message):
    __slots__ = ("dados", "id_no", "papel", "termo", "commit_index", "tamanho_log", "lider_atual")
    DADOS_FIELD_NUMBER: _ClassVar[int]
    ID_NO_FIELD_NUMBER: _ClassVar[int]
    PAPEL_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    TAMANHO_LOG_FIELD_NUMBER: _ClassVar[int]
    LIDER_ATUAL_FIELD_NUMBER: _ClassVar[int]
    dados: _containers.RepeatedCompositeFieldContainer[Registro]
    id_no: int
    papel: str
    termo: int
    commit_index: int
    tamanho_log: int
    lider_atual: int
    def __init__(self, dados: _Optional[_Iterable[_Union[Registro, _Mapping]]] = ..., id_no: _Optional[int] = ..., papel: _Optional[str] = ..., termo: _Optional[int] = ..., commit_index: _Optional[int] = ..., tamanho_log: _Optional[int] = ..., lider_atual: _Optional[int] = ...) -> None: ...

class VoteRequest(_message.Message):
    __slots__ = ("termo_candidato", "id_candidato", "ultimo_indice_log", "ultimo_termo_log")
    TERMO_CANDIDATO_FIELD_NUMBER: _ClassVar[int]
    ID_CANDIDATO_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_LOG_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_TERMO_LOG_FIELD_NUMBER: _ClassVar[int]
    termo_candidato: int
    id_candidato: int
    ultimo_indice_log: int
    ultimo_termo_log: int
    def __init__(self, termo_candidato: _Optional[int] = ..., id_candidato: _Optional[int] = ..., ultimo_indice_log: _Optional[int] = ..., ultimo_termo_log: _Optional[int] = ...) -> None: ...

class VoteResponse(_message.Message):
    __slots__ = ("termo_atual", "voto_concedido")
    TERMO_ATUAL_FIELD_NUMBER: _ClassVar[int]
    VOTO_CONCEDIDO_FIELD_NUMBER: _ClassVar[int]
    termo_atual: int
    voto_concedido: bool
    def __init__(self, termo_atual: _Optional[int] = ..., voto_concedido: _Optional[bool] = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("id_replica", "termo_atual", "ultimo_indice", "ultimo_termo", "intervalo_ms")
    ID_REPLICA_FIELD_NUMBER: _ClassVar[int]
    TERMO_ATUAL_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_TERMO_FIELD_NUMBER: _ClassVar[int]
    INTERVALO_MS_FIELD_NUMBER: _ClassVar[int]
    id_replica: int
    termo_atual: int
    ultimo_indice: int
    ultimo_termo: int
    intervalo_ms: int
    def __init__(self, id_replica: _Optional[int] = ..., termo_atual: _Optional[int] = ..., ultimo_indice: _Optional[int] = ..., ultimo_termo: _Optional[int] = ..., intervalo_ms: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("sucesso", "termo", "lider_atual", "commit_index", "entradas", "divergente", "truncar_para")
    SUCESSO_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    LIDER_ATUAL_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENTRADAS_FIELD_NUMBER: _ClassVar[int]
    DIVERGENTE_FIELD_NUMBER: _ClassVar[int]
    TRUNCAR_PARA_FIELD_NUMBER: _ClassVar[int]
    sucesso: bool
    termo: int
    lider_atual: int
    commit_index: int
    entradas: _containers.RepeatedCompositeFieldContainer[Registro]
    divergente: bool
    truncar_para: int
    def __init__(self, sucesso: _Optional[bool] = ..., termo: _Optional[int] = ..., lider_atual: _Optional[int] = ..., commit_index: _Optional[int] = ..., entradas: _Optional[_Iterable[_Union[Registro, _Mapping]]] = ..., divergente: _Optional[bool] = ..., truncar_para: _Optional[int] = ...) -> None: ...
