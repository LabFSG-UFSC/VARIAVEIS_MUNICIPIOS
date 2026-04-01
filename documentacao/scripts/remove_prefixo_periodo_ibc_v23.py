#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Remove o prefixo da fonte e o sufixo de ano das colunas do IBC na v23 e gera a v24.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v23.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v24.csv"

RENOMEAR_COLUNAS = {
    "ibc_indice_2024": "indice_conectividade",
    "ibc_cobertura_pop_4g5g_2024": "cobertura_pop_4g5g",
    "ibc_densidade_smp_2024": "densidade_smp",
    "ibc_hhi_smp_2024": "hhi_smp",
    "ibc_densidade_scm_2024": "densidade_scm",
    "ibc_hhi_scm_2024": "hhi_scm",
    "ibc_adensamento_estacoes_2024": "adensamento_estacoes",
    "ibc_fibra_2024": "fibra",
    "ibc_cobertura_area_agricultavel_2024": "cobertura_area_agricultavel",
}


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in RENOMEAR_COLUNAS if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas do bloco IBC ausentes na v23: {faltantes}")

    resultado = df.rename(columns=RENOMEAR_COLUNAS)
    duplicadas = resultado.columns[resultado.columns.duplicated()].tolist()
    if duplicadas:
        raise ValueError(f"Renomeacao gerou colunas duplicadas: {duplicadas}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Colunas renomeadas: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
