"""
================================================================
Domus — Microserviço de Processamento de Extratos
================================================================
Responsável por receber arquivos CSV/OFX,
processar e retornar JSON estruturado para o backend Java.

Porta padrão: 5000
Endpoint:     POST /processar-extrato
================================================================
"""

from flask import Flask, request, jsonify
import pandas as pd
import re
from datetime import datetime
from io import StringIO

app = Flask(__name__)

# ── Mapeamento de palavras-chave para categorias ─────────────
CATEGORY_MAP = {
    "supermercado": "Food & Dining",
    "mercado":      "Food & Dining",
    "padaria":      "Food & Dining",
    "restaurante":  "Food & Dining",
    "lanche":       "Food & Dining",
    "food":         "Food & Dining",
    "ifood":        "Food & Dining",
    "ifd":          "Food & Dining",
    "parmegian":    "Food & Dining",
    "zarelli":      "Food & Dining",
    "tauste":       "Food & Dining",
    "posto":        "Transportation",
    "combustivel":  "Transportation",
    "uber":         "Transportation",
    "uberrides":    "Transportation",
    "99":           "Transportation",
    "taxi":         "Transportation",
    "transporte":   "Transportation",
    "drogasil":     "Healthcare",
    "farmacia":     "Healthcare",
    "droga":        "Healthcare",
    "saude":        "Healthcare",
    "hospital":     "Healthcare",
    "clinica":      "Healthcare",
    "tim":          "Bills & Utilities",
    "vivo":         "Bills & Utilities",
    "claro":        "Bills & Utilities",
    "energia":      "Bills & Utilities",
    "agua":         "Bills & Utilities",
    "light":        "Bills & Utilities",
    "iof":          "Bills & Utilities",
    "netflix":      "Entertainment",
    "spotify":      "Entertainment",
    "amazon":       "Entertainment",
    "cinema":       "Entertainment",
    "claude":       "Entertainment",
    "subscription": "Entertainment",
    "shopping":     "Shopping",
    "loja":         "Shopping",
    "magazine":     "Shopping",
    "renner":       "Shopping",
    "riachuelo":    "Shopping",
    "escola":       "Education",
    "curso":        "Education",
    "faculdade":    "Education",
    "universidade": "Education",
}

# ── Regex para detectar parcelas no formato "Parcela X/Y" ────
PARCELA_PATTERN = re.compile(
    r"[- ]*parcela\s+(\d+)/(\d+)",
    re.IGNORECASE
)


def detectar_categoria(descricao: str) -> str:
    """Detecta a categoria com base em palavras-chave na descrição."""
    descricao_lower = descricao.lower()
    for keyword, category in CATEGORY_MAP.items():
        if keyword in descricao_lower:
            return category
    return "Other"


def processar_parcela(descricao: str) -> tuple[str, str, int]:
    """
    Verifica se a descrição contém informação de parcela.
    Retorna uma tupla: (descricao_limpa, frequency, durationInMonths)

    Exemplos:
      "Baby Care - Parcela 4/10"  →  ("Baby Care", "Monthly", 7)
      "Notebook - Parcela 1/12"   →  ("Notebook", "Monthly", 12)
      "Uber Uber * Pending"       →  ("Uber Uber * Pending", "One-time", 1)
    """
    match = PARCELA_PATTERN.search(descricao)

    if match:
        parcela_atual = int(match.group(1))
        total_parcelas = int(match.group(2))

        # Meses restantes = total - parcela_atual + 1
        duration = total_parcelas - parcela_atual + 1

        # Remove o trecho " - Parcela X/Y" da descrição
        descricao_limpa = PARCELA_PATTERN.sub("", descricao).strip(" -").strip()

        return descricao_limpa, "Monthly", duration

    return descricao, "One-time", 1


# ── Parser CSV do Nubank ──────────────────────────────────────

def parse_csv(conteudo: str) -> list[dict]:
    """
    Processa CSV do Nubank.
    Colunas esperadas: date, title, amount
    """
    df = pd.read_csv(StringIO(conteudo))
    df.columns = df.columns.str.strip().str.lower()

    required = {"date", "title", "amount"}
    if not required.issubset(set(df.columns)):
        raise ValueError(
            f"CSV inválido. Colunas esperadas: {required}. "
            f"Encontradas: {set(df.columns)}"
        )

    transacoes = []
    for _, row in df.iterrows():
        valor = abs(float(row["amount"]))
        if valor <= 0:
            continue

        descricao_raw = str(row["title"]).strip()
        data_raw      = str(row["date"]).strip()

        try:
            data = datetime.strptime(data_raw, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            data = data_raw

        # ✅ Detecta parcelas
        descricao, frequency, duration = processar_parcela(descricao_raw)

        transacoes.append({
            "description":     descricao,
            "amount":          valor,
            "startDate":       data,
            "category":        detectar_categoria(descricao),
            "frequency":       frequency,
            "durationInMonths": duration,
            "paymentType":     "Cartão de Crédito",
            "paid":            False,
        })

    return transacoes


# ── Parser OFX do Nubank ──────────────────────────────────────

def parse_ofx(conteudo: str) -> list[dict]:
    """
    Processa OFX do Nubank (formato SGML).
    Extrai DTPOSTED, TRNAMT e MEMO de cada <STMTTRN>.
    """
    transacoes = []
    blocos = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", conteudo, re.DOTALL)

    for bloco in blocos:
        dtposted = re.search(r"<DTPOSTED>(.*?)[\r\n<]", bloco)
        trnamt   = re.search(r"<TRNAMT>(.*?)[\r\n<]", bloco)
        memo     = re.search(r"<MEMO>(.*?)[\r\n<]", bloco)

        if not (dtposted and trnamt and memo):
            continue

        valor_raw = float(trnamt.group(1).strip())
        if valor_raw >= 0:
            continue  # ignora créditos/estornos

        valor         = abs(valor_raw)
        descricao_raw = memo.group(1).strip()

        data_raw = dtposted.group(1).strip()[:8]
        try:
            data = datetime.strptime(data_raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            data = data_raw

        # ✅ Detecta parcelas
        descricao, frequency, duration = processar_parcela(descricao_raw)

        transacoes.append({
            "description":      descricao,
            "amount":           valor,
            "startDate":        data,
            "category":         detectar_categoria(descricao),
            "frequency":        frequency,
            "durationInMonths": duration,
            "paymentType":      "Cartão de Crédito",
            "paid":             False,
        })

    return transacoes


# ── Endpoint principal ────────────────────────────────────────

@app.route("/processar-extrato", methods=["POST"])
def processar_extrato():
    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado. Use o campo 'file'."}), 400

    arquivo = request.files["file"]
    nome    = arquivo.filename.lower()

    if not nome:
        return jsonify({"erro": "Nome do arquivo inválido."}), 400

    try:
        conteudo = arquivo.read().decode("utf-8", errors="replace")

        if nome.endswith(".csv"):
            transacoes = parse_csv(conteudo)
        elif nome.endswith(".ofx"):
            transacoes = parse_ofx(conteudo)
        else:
            return jsonify({"erro": "Formato não suportado. Envie CSV ou OFX."}), 400

        return jsonify({
            "total":      len(transacoes),
            "transacoes": transacoes,
        }), 200

    except ValueError as e:
        return jsonify({"erro": str(e)}), 422
    except Exception as e:
        return jsonify({"erro": f"Erro interno: {str(e)}"}), 500


# ── Health check ──────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)