# Cores no terminal (códigos ANSI), só para facilitar a leitura.
COR_PAPEL = {"Lider": "\033[1;32m", "Candidato": "\033[1;33m", "Seguidor": "\033[36m"}
CINZA, NEGRITO, VERMELHO, FIM = "\033[90m", "\033[1m", "\033[1;31m", "\033[0m"
DESTAQUES = {  # palavra no texto -> cor da linha inteira
    "ELEITO LÍDER": "\033[1;32m", "CANDIDATO": "\033[1;33m", "TRUNCOU": VERMELHO,
    "RECUSOU": VERMELHO, "INATIVA": VERMELHO, "SIMULANDO CRASH": VERMELHO,
    "aplicou no banco": "\033[32m", "NÃO comitadas": "\033[33m",
}
