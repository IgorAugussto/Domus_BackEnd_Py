"""
================================================================
Domus — Microserviço de Processamento de Extratos
================================================================
Responsável por receber arquivos CSV/OFX do Nubank,
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
    "posto":        "Transportation",
    "combustivel":  "Transportation",
    "uber":         "Transportation",
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
    "oi ":          "Bills & Utilities",
    "energia":      "Bills & Utilities",
    "agua":         "Bills & Utilities",
    "light":        "Bills & Utilities",
    "netflix":      "Entertainment",
    "spotify":      "Entertainment",
    "amazon":       "Entertainment",
    "cinema":       "Entertainment",
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

def detectar_categoria(descricao: str) -> str:
    """Detecta a categoria com base em palavras-chave na descrição."""
    descricao_lower = descricao.lower()
    for keyword, category in CATEGORY_MAP.items():
        if keyword in descricao_lower:
            return category
    return "Other"


# ── Parser CSV do Nubank ──────────────────────────────────────

def parse_csv(conteudo: str) -> list[dict]:
    """
    Processa CSV do Nubank.
    Colunas esperadas: date, title, amount
    """
    df = pd.read_csv(StringIO(conteudo))
    df.columns = df.columns.str.strip().str.lower()

    # Valida colunas obrigatórias
    required = {"date", "title", "amount"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"CSV inválido. Colunas esperadas: {required}. Encontradas: {set(df.columns)}")

    transacoes = []
    for _, row in df.iterrows():
        valor = abs(float(row["amount"]))  # garante positivo
        if valor <= 0:
            continue  # ignora estornos ou entradas

        descricao = str(row["title"]).strip()
        data_raw  = str(row["date"]).strip()

        # Converte data para yyyy-MM-dd
        try:
            data = datetime.strptime(data_raw, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            data = data_raw  # mantém como está se não conseguir parsear

        transacoes.append({
            "description": descricao,
            "amount":      valor,
            "startDate":   data,
            "category":    detectar_categoria(descricao),
            "frequency":   "One-time",
            "paymentType": "Cartão de Crédito",
            "paid":        False,
        })

    return transacoes


# ── Parser OFX do Nubank ──────────────────────────────────────

def parse_ofx(conteudo: str) -> list[dict]:
    """
    Processa OFX do Nubank (formato SGML, não XML puro).
    Extrai DTPOSTED, TRNAMT e MEMO de cada <STMTTRN>.
    """
    transacoes = []

    # Divide em blocos de transação
    blocos = re.findall(r"<STMTTRN>(.*?)</STMTTRN>", conteudo, re.DOTALL)

    for bloco in blocos:
        # Extrai campos
        dtposted = re.search(r"<DTPOSTED>(.*?)[\r\n<]", bloco)
        trnamt   = re.search(r"<TRNAMT>(.*?)[\r\n<]", bloco)
        memo     = re.search(r"<MEMO>(.*?)[\r\n<]", bloco)

        if not (dtposted and trnamt and memo):
            continue

        valor_raw = float(trnamt.group(1).strip())

        # No OFX do Nubank os débitos vêm negativos — ignora créditos
        if valor_raw >= 0:
            continue

        valor     = abs(valor_raw)
        descricao = memo.group(1).strip()

        # Converte data OFX: "20251005000000[-3:BRT]" → "2025-10-05"
        data_raw = dtposted.group(1).strip()[:8]  # pega só "20251005"
        try:
            data = datetime.strptime(data_raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            data = data_raw

        transacoes.append({
            "description": descricao,
            "amount":      valor,
            "startDate":   data,
            "category":    detectar_categoria(descricao),
            "frequency":   "One-time",
            "paymentType": "Cartão de Crédito",
            "paid":        False,
        })

    return transacoes


# ── Endpoint principal ────────────────────────────────────────

@app.route("/processar-extrato", methods=["POST"])
def processar_extrato():
    """
    Recebe um arquivo CSV ou OFX via multipart/form-data.
    Campo esperado: 'file'
    Retorna: JSON com lista de transações processadas.
    """
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


# ── Health check (usado pelo Java para verificar se Python está vivo) ──

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)