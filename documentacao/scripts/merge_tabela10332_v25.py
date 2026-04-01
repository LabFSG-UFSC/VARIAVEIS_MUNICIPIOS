#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 10332 com a base v25 e incorpora o meio de transporte principal no deslocamento ao trabalho.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V25_FILE = BASE_DIR / "prata" / "processamento" / "merge_v25.csv"
TABELA10332_FILE = BASE_DIR / "bronze" / "tabela10332.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v26.csv"

RENOMEAR_COLUNAS = {
    "Total": "transp_trab_pe",
    "Total.1": "transp_trab_bicicleta",
    "Total.2": "transp_trab_motocicleta",
    "Total.3": "transp_trab_automovel",
    "Total.4": "transp_trab_onibus",
    "Total.5": "transp_trab_trem_metro",
    "Total.6": "transp_trab_outros",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v25() -> pd.DataFrame:
    df = pd.read_csv(V25_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v25.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_tabela10332() -> pd.DataFrame:
    df = pd.read_csv(TABELA10332_FILE, skiprows=5)

    colunas_necessarias = ["Cód.", "Cor ou raça", *RENOMEAR_COLUNAS.keys()]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 10332: {faltantes}")

    base = df.loc[
        df["Cor ou raça"].astype(str).eq("Total"),
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
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 10332.")

    return base


def main() -> int:
    v25 = carregar_v25()
    tabela10332 = carregar_tabela10332()

    resultado = v25.merge(tabela10332, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 10332: {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 10332: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
