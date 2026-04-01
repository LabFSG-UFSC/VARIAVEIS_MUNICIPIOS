#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 10330 com a base v24 e incorpora o tempo de deslocamento ao trabalho principal.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V24_FILE = BASE_DIR / "prata" / "processamento" / "merge_v24.csv"
TABELA10330_FILE = BASE_DIR / "bronze" / "tabela10330.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v25.csv"

RENOMEAR_COLUNAS = {
    "Total": "desloc_trab_ate_5min_2022",
    "Total.1": "desloc_trab_6a15min_2022",
    "Total.2": "desloc_trab_15a30min_2022",
    "Total.3": "desloc_trab_30a60min_2022",
    "Total.4": "desloc_trab_1a2h_2022",
    "Total.5": "desloc_trab_2a4h_2022",
    "Total.6": "desloc_trab_mais_4h_2022",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v24() -> pd.DataFrame:
    df = pd.read_csv(V24_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v24.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_tabela10330() -> pd.DataFrame:
    df = pd.read_csv(TABELA10330_FILE, skiprows=5)

    colunas_necessarias = [
        "Cód.",
        "Cor ou raça",
        "Local de exercício do trabalho principal",
        *RENOMEAR_COLUNAS.keys(),
    ]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 10330: {faltantes}")

    base = df.loc[
        df["Cor ou raça"].astype(str).eq("Total")
        & df["Local de exercício do trabalho principal"].astype(str).eq("Total"),
        colunas_necessarias,
    ].copy()

    base["_codigo_merge"] = base["Cód."].map(normalizar_codigo)

    for coluna in RENOMEAR_COLUNAS:
        base[coluna] = base[coluna].replace("-", 0)
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce")

    base = base.rename(columns=RENOMEAR_COLUNAS)
    base = base[["_codigo_merge", *RENOMEAR_COLUNAS.values()]].copy()

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 10330.")

    return base


def main() -> int:
    v24 = carregar_v24()
    tabela10330 = carregar_tabela10330()

    resultado = v24.merge(tabela10330, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 10330: {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 10330: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
