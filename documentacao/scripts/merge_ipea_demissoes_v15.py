#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da base anual de demissoes do Ipea com a base v15.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V15_FILE = BASE_DIR / "prata" / "processamento" / "merge_v15.csv"
IPEA_FILE = BASE_DIR / "prata" / "pre_merge" / "ipea_demissoes_municipais_2025.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v16.csv"
COLUNA_IPEA = "demissoes_2025"


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v15() -> pd.DataFrame:
    df = pd.read_csv(V15_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v15.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_ipea() -> pd.DataFrame:
    df = pd.read_csv(IPEA_FILE)

    colunas_necessarias = ["cod_mun", COLUNA_IPEA]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na base anual do Ipea: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["_codigo_merge"] = base["cod_mun"].map(normalizar_codigo)
    base[COLUNA_IPEA] = pd.to_numeric(base[COLUNA_IPEA], errors="coerce")
    base = base.drop(columns=["cod_mun"])

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na base anual do Ipea.")

    return base


def main() -> int:
    v15 = carregar_v15()
    ipea = carregar_ipea()

    resultado = v15.merge(ipea, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na base anual do Ipea: {correspondencias}")
    print(f"Municipios sem correspondencia na base anual do Ipea: {len(resultado) - correspondencias}")
    print(f"Coluna incorporada: {COLUNA_IPEA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
