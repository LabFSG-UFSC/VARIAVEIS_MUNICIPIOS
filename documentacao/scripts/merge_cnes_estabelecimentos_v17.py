#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge do CNES de estabelecimentos por tipo com a base v17.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V17_FILE = BASE_DIR / "prata" / "processamento" / "merge_v17.csv"
CNES_FILE = BASE_DIR / "bronze" / "cnes_cnv_estabbr134413200_135_70_71.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v18.csv"


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def slugify(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.lower().replace("/", " ")
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return re.sub(r"_+", "_", texto).strip("_")


def extrair_periodo() -> str:
    with CNES_FILE.open("r", encoding="latin1", errors="replace") as f:
        linhas = [f.readline().strip() for _ in range(3)]

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


def nome_coluna_saida(coluna_original: str, periodo: str) -> str:
    return f"cnes_estab_{slugify(coluna_original)}_{periodo}"


def carregar_v17() -> pd.DataFrame:
    df = pd.read_csv(V17_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v17.")

    df["_codigo_merge_6"] = df["cod_mun"].astype("string").str[:6].map(normalizar_codigo)
    return df


def carregar_cnes() -> tuple[pd.DataFrame, str, list[str]]:
    periodo = extrair_periodo()
    df = pd.read_csv(CNES_FILE, encoding="latin1", sep=";", skiprows=3)

    if "Município" not in df.columns:
        raise ValueError("Coluna 'Município' nao encontrada no arquivo do CNES.")

    colunas_valor = [col for col in df.columns if col != "Município"]
    base = df[["Município", *colunas_valor]].copy()
    base["_codigo_merge_6"] = base["Município"].astype("string").str.extract(r"^(\d{6})")[0]
    base = base[base["_codigo_merge_6"].notna()].copy()

    for coluna in colunas_valor:
        base[coluna] = base[coluna].replace("-", 0)
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce").fillna(0)

    mascara_brasilia = base["_codigo_merge_6"].eq("530010")
    if mascara_brasilia.any():
        base.loc[mascara_brasilia, colunas_valor] = 0

    renomear = {coluna: nome_coluna_saida(coluna, periodo) for coluna in colunas_valor}
    base = base.rename(columns=renomear)
    colunas_saida = list(renomear.values())
    base = base[["_codigo_merge_6", *colunas_saida]].copy()

    duplicados = base["_codigo_merge_6"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos de 6 digitos duplicados no CNES.")

    return base, periodo, colunas_saida


def main() -> int:
    v17 = carregar_v17()
    cnes, periodo, colunas_saida = carregar_cnes()

    resultado = v17.merge(cnes, on="_codigo_merge_6", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge_6", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Periodo detectado no CNES: {periodo}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia no CNES: {correspondencias}")
    print(f"Municipios sem correspondencia no CNES: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas: {len(colunas_saida)}")
    print("Brasilia zerada nas colunas do CNES: sim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
