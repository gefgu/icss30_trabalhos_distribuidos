EXCHANGE_ECOMMERCE_NAME = 'ecommerce'
EXCHANGE_PROMOCOES_NAME = 'promocoes'

from pathlib import Path

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15


def assinar_mensagem(mensagem, private_key_path):
    """Assina uma mensagem e retorna a assinatura em hexadecimal."""
    private_key = RSA.import_key(Path(private_key_path).read_bytes())
    digest = SHA256.new(str(mensagem).encode("utf-8"))
    return pkcs1_15.new(private_key).sign(digest).hex()


def verificar_assinatura(mensagem, assinatura_hex, public_key_path):
    """Verifica uma assinatura hexadecimal e retorna True se ela for válida."""
    try:
        public_key = RSA.import_key(Path(public_key_path).read_bytes())
        assinatura = bytes.fromhex(assinatura_hex)
        digest = SHA256.new(str(mensagem).encode("utf-8"))
        pkcs1_15.new(public_key).verify(digest, assinatura)
        return True
    except (ValueError, TypeError, IndexError):
        return False


def init_promocoes_exchange(channel):
    """
    Inicializa o exchange de promoções no RabbitMQ.
    """
    channel.exchange_declare(exchange='promocoes', exchange_type='topic')

def init_ecommerce_exchange(channel):
    """
    Inicializa o exchange de e-commerce no RabbitMQ.
    """
    channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

def create_cryptography_keys(private_key_path, public_key_path):
    """
    Garante que cada serviço possua um par de chaves RSA consistente.
    Se o arquivo de chave estiver ausente, corrompido ou incompatível,
    um novo par é gerado e gravado nos arquivos.
    """
    private_path = Path(private_key_path)
    public_path = Path(public_key_path)

    if private_path.exists() and public_path.exists():
        try:
            private_key = RSA.import_key(private_path.read_bytes())
            public_key = RSA.import_key(public_path.read_bytes())
            if private_key.publickey().n == public_key.n:
                return private_path.read_bytes()
        except (ValueError, TypeError):
            pass

    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()

    with open(private_path, "wb") as f:
        f.write(private_key)
    with open(public_path, "wb") as f:
        f.write(public_key)

    return private_key
