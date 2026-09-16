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
    except (ValueError, TypeError):
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
    Cria um par de chaves RSA e salva em arquivos.
    """
    key = RSA.generate(2048)
    private_key = key.export_key()
    if not Path(private_key_path).exists():
        with open(private_key_path, "wb") as f:
            f.write(private_key)

    public_key = key.publickey().export_key()
    if not Path(public_key_path).exists():
        with open(public_key_path, "wb") as f:
            f.write(public_key)

    return private_key
