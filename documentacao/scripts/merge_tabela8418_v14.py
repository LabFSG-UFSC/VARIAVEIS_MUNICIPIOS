#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 8418 com a base v14 e incorpora as areas urbanizadas em km2.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V14_FILE = BASE_DIR / "prata" / "processamento" / "merge_v14.csv"
TABELA8418_FILE = BASE_DIR / "bronze" / "tabela8418.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v15.csv"

RENOMEAR_COLUNAS = {
    "Áreas urbanizadas densas (Quilômetros quadrados)": "area_urb_densa_km2",
    "Loteamento vazio (Quilômetros quadrados)": "loteamento_vazio_km2",
    "Vazios intraurbanos (Quilômetros quadrados)": "vazios_intraurbanos_km2",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v14() -> pd.DataFrame:
    df = pd.read_csv(V14_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v14.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_tabela8418() -> pd.DataFrame:
    df = pd.read_csv(TABELA8418_FILE, skiprows=3)

    colunas_necessarias = ["Cód."] + list(RENOMEAR_COLUNAS.keys())
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 8418: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["Cód."] = pd.to_numeric(base["Cód."], errors="coerce")
    base = base[base["Cód."].notna()].copy()
    base = base[base["Cód."].ne(0)].copy()

    for coluna in RENOMEAR_COLUNAS:
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce")

    base = base.rename(columns=RENOMEAR_COLUNAS)
    base["_codigo_merge"] = base["Cód."].map(normalizar_codigo)
    base = base.drop(columns=["Cód."])

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 8418.")

    return base


def main() -> int:
    v14 = carregar_v14()
    tabela8418 = carregar_tabela8418()

    resultado = v14.merge(tabela8418, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 8418: {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 8418: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas: {', '.join(RENOMEAR_COLUNAS.values())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
