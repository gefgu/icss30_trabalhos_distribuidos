# (0,1) Consome o evento interesse.promocao.
# • (0,3) Gera as promoções e as notificações destas aos clientes
# interessados. Para isso, ele identifica os interesses e e-mails dos
# consumidores cadastrados e integra-se com uma API Externa de e-
# mail (ex: Resend) para disparar as notificações de promoções.
import threading
import pika

from fastapi import FastAPI

from helpers.helper import EXCHANGE_ECOMMERCE_NAME, EXCHANGE_ECOMMERCE_NAME, init_ecommerce_exchange