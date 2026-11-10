from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GravarRequest(_message.Message):
    __slots__ = ("dado",)
    DADO_FIELD_NUMBER: _ClassVar[int]
    dado: str
    def __init__(self, dado: _Optional[str] = ...) -> None: ...

class GravarResponse(_message.Message):
    __slots__ = ("sucesso", "redirecionar", "lider_atual")
    SUCESSO_FIELD_NUMBER: _ClassVar[int]
    REDIRECIONAR_FIELD_NUMBER: _ClassVar[int]
    LIDER_ATUAL_FIELD_NUMBER: _ClassVar[int]
    sucesso: bool
    redirecionar: bool
    lider_atual: str
    def __init__(self, sucesso: _Optional[bool] = ..., redirecionar: _Optional[bool] = ..., lider_atual: _Optional[str] = ...) -> None: ...

class ConsultarRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ConsultarResponse(_message.Message):
    __slots__ = ("dados_comitados",)
    DADOS_COMITADOS_FIELD_NUMBER: _ClassVar[int]
    dados_comitados: str
    def __init__(self, dados_comitados: _Optional[str] = ...) -> None: ...

class Registro(_message.Message):
    __slots__ = ("termo", "indice", "dado")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    INDICE_FIELD_NUMBER: _ClassVar[int]
    DADO_FIELD_NUMBER: _ClassVar[int]
    termo: int
    indice: int
    dado: str
    def __init__(self, termo: _Optional[int] = ..., indice: _Optional[int] = ..., dado: _Optional[str] = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("ultimo_termo", "ultimo_indice", "id_replica")
    ULTIMO_TERMO_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_FIELD_NUMBER: _ClassVar[int]
    ID_REPLICA_FIELD_NUMBER: _ClassVar[int]
    ultimo_termo: int
    ultimo_indice: int
    id_replica: int
    def __init__(self, ultimo_termo: _Optional[int] = ..., ultimo_indice: _Optional[int] = ..., id_replica: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("commit_index", "log")
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    commit_index: int
    log: _containers.RepeatedCompositeFieldContainer[Registro]
    def __init__(self, commit_index: _Optional[int] = ..., log: _Optional[_Iterable[_Union[Registro, _Mapping]]] = ...) -> None: ...

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
