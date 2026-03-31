#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge do CNES de ambulatorios SUS com a base v16.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V16_FILE = BASE_DIR / "prata" / "processamento" / "merge_v16.csv"
CNES_FILE = BASE_DIR / "bronze" / "cnes_cnv_atambbr131932200_135_70_71.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v17.csv"
COLUNA_SAIDA = "ambulatorios_sus_2026_02"


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v16() -> pd.DataFrame:
    df = pd.read_csv(V16_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v16.")

    df["_codigo_merge_6"] = df["cod_mun"].astype("string").str[:6].map(normalizar_codigo)
    return df


def extrair_periodo_cnes() -> str:
    with CNES_FILE.open("r", encoding="latin1", errors="replace") as f:
        linhas = [f.readline().strip() for _ in range(3)]

    if len(linhas) < 3:
        raise ValueError("Nao foi possivel ler o cabecalho do arquivo CNES.")

    match = re.search(r"Per[ií]odo:([A-Za-z]{3})/(\d{4})", linhas[2], flags=re.IGNORECASE)
    if not match:
        raise ValueError("Nao foi possivel identificar o periodo no cabecalho do arquivo CNES.")

    mes_txt = match.group(1).lower()
    ano = match.group(2)
    mapa_meses = {
        "jan": "01",
        "fev": "02",
        "mar": "03",
        "abr": "04",
        "mai": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "set": "09",
        "out": "10",
        "nov": "11",
        "dez": "12",
    }
    if mes_txt not in mapa_meses:
        raise ValueError(f"Mes nao reconhecido no cabecalho do CNES: {mes_txt}")

    return f"{ano}_{mapa_meses[mes_txt]}"


def carregar_cnes() -> pd.DataFrame:
    df = pd.read_csv(CNES_FILE, encoding="latin1", sep=";", skiprows=3)

    colunas_necessarias = ["Município", "SUS"]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo do CNES: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["_codigo_merge_6"] = base["Município"].astype("string").str.extract(r"^(\d{6})")[0]
    base[COLUNA_SAIDA] = pd.to_numeric(base["SUS"], errors="coerce")
    base = base[["_codigo_merge_6", COLUNA_SAIDA]].copy()
    base = base[base["_codigo_merge_6"].notna()].copy()

    duplicados = base["_codigo_merge_6"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos de 6 digitos duplicados no CNES.")

    return base


def main() -> int:
    periodo = extrair_periodo_cnes()
    if periodo != "2026_02":
        raise ValueError(
            f"O periodo detectado no CNES foi {periodo}, mas a coluna de saida esperada e {COLUNA_SAIDA}."
        )

    v16 = carregar_v16()
    cnes = carregar_cnes()

    resultado = v16.merge(cnes, on="_codigo_merge_6", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge_6", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia no CNES: {correspondencias}")
    print(f"Municipios sem correspondencia no CNES: {len(resultado) - correspondencias}")
    print(f"Coluna incorporada: {COLUNA_SAIDA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
