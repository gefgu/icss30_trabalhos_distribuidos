from pathlib import Path

from helpers.helper import verificar_assinatura


FILE_FOLDER_PATH = Path(__file__).resolve().parent
PROMOCOES_PUBLIC_KEY_FILE = (
    FILE_FOLDER_PATH.parent / "promocoes" / "promocoes_public.pem"
)


def processar_promocao(nome_consumidor, properties, body):
    """Valida e exibe uma promoção recebida por um consumidor."""
    body_str = (
        body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
    )
    headers = getattr(properties, "headers", None) or {}
    signature = headers.get("signature")

    if signature is None or not verificar_assinatura(
        body_str, signature, PROMOCOES_PUBLIC_KEY_FILE
    ):
        print(f"[{nome_consumidor}] Assinatura inválida. Promoção descartada.")
        return

    print(f"[{nome_consumidor}] Promoção válida: {body_str}")
