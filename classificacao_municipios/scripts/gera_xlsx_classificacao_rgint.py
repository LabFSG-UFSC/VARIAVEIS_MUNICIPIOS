#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = ROOT / "classificacao_municipios"
PROCESSAMENTO_DIR = BASE_DIR / "processamento"
POR_RGINT_DIR = PROCESSAMENTO_DIR / "por_rgint"
OUTPUT_FILE = PROCESSAMENTO_DIR / "classificacao_municipios_fuzzy_rgint.xlsx"

ARQUIVOS_FIXOS = [
    ("municipios", PROCESSAMENTO_DIR / "classificacao_municipios_fuzzy_rgint.csv"),
    ("resumo_rgint", PROCESSAMENTO_DIR / "classificacao_rgint_resumo_fuzzy.csv"),
]


def sanitize_sheet_name(nome: str, usados: set[str]) -> str:
    proibidos = '[]:*?/\\'
    limpo = "".join("_" if ch in proibidos else ch for ch in nome).strip()
    limpo = limpo[:31] or "sheet"

    candidato = limpo
    contador = 1
    while candidato in usados:
        sufixo = f"_{contador}"
        candidato = f"{limpo[:31 - len(sufixo)]}{sufixo}"
        contador += 1

    usados.add(candidato)
    return candidato


def main() -> None:
    if not POR_RGINT_DIR.exists():
        raise FileNotFoundError(f"Pasta nao encontrada: {POR_RGINT_DIR}")

    arquivos_rgint = sorted(POR_RGINT_DIR.glob("*.csv"))
    if not arquivos_rgint:
        raise FileNotFoundError(f"Nenhum CSV encontrado em: {POR_RGINT_DIR}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    usados: set[str] = set()
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for sheet_name, csv_path in ARQUIVOS_FIXOS:
            df = pd.read_csv(csv_path)
            df.to_excel(
                writer,
                sheet_name=sanitize_sheet_name(sheet_name, usados),
                index=False,
            )

        for csv_path in arquivos_rgint:
            df = pd.read_csv(csv_path)
            sheet_name = sanitize_sheet_name(csv_path.stem, usados)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Total de abas de rgint: {len(arquivos_rgint)}")


if __name__ == "__main__":
    main()
