#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge das bases da ANA de demanda de agua e eventos de seca com a v21.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V21_FILE = BASE_DIR / "prata" / "processamento" / "merge_v21.csv"
AGUA_FILE = BASE_DIR / "bronze" / "Demanda_Total.csv"
SECA_FILE = BASE_DIR / "bronze" / "N%C3%BAmero_de_Registros_de_Secas_por_Munic%C3%ADpio_entre_2003_e_2015.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v22.csv"

RENOMEAR_AGUA = {
    "VZHURM3S": "demanda_agua_hum_urb_m3s",
    "VZHRUM3S": "demanda_agua_hum_rur_m3s",
    "VZINDM3S": "demanda_agua_ind_m3s",
    "VZMINM3S": "demanda_agua_min_m3s",
    "VZTERM3S": "demanda_agua_term_m3s",
    "VZANIM3S": "demanda_agua_animal_m3s",
    "VZIRRM3S": "demanda_agua_irr_m3s",
    "VZTOTM3S": "demanda_agua_total_m3s",
}

RENOMEAR_SECA = {
    "SECAS2003A": "registros_seca_2003_2015",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v21() -> pd.DataFrame:
    df = pd.read_csv(V21_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v21.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_agua() -> tuple[pd.DataFrame, int]:
    df = pd.read_csv(AGUA_FILE)
    colunas_necessarias = ["ANO", "CDMUN", *RENOMEAR_AGUA.keys()]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo de demanda de agua: {faltantes}")

    anos = sorted(pd.to_numeric(df["ANO"], errors="coerce").dropna().astype(int).unique().tolist())
    if len(anos) != 1:
        raise ValueError(f"O arquivo de demanda de agua deve ter um unico ano de referencia. Anos encontrados: {anos}")

    base = df[colunas_necessarias].copy()
    base["_codigo_merge"] = base["CDMUN"].map(normalizar_codigo)

    for coluna in RENOMEAR_AGUA:
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce")

    base = base.rename(columns=RENOMEAR_AGUA)
    base = base[["_codigo_merge", *RENOMEAR_AGUA.values()]].copy()

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados no arquivo de demanda de agua.")

    return base, anos[0]


def carregar_seca() -> pd.DataFrame:
    df = pd.read_csv(SECA_FILE)
    colunas_necessarias = ["CD_GEOCMU", *RENOMEAR_SECA.keys()]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo de seca: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["_codigo_merge"] = base["CD_GEOCMU"].map(normalizar_codigo)

    for coluna in RENOMEAR_SECA:
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce")

    base = base.rename(columns=RENOMEAR_SECA)
    base = base[["_codigo_merge", *RENOMEAR_SECA.values()]].copy()

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados no arquivo de seca.")

    return base


def main() -> int:
    v21 = carregar_v21()
    agua, ano_agua = carregar_agua()
    seca = carregar_seca()

    resultado = v21.merge(agua, on="_codigo_merge", how="left", indicator="_merge_agua")
    correspondencias_agua = resultado["_merge_agua"].eq("both").sum()
    resultado = resultado.drop(columns=["_merge_agua"])

    resultado = resultado.merge(seca, on="_codigo_merge", how="left", indicator="_merge_seca")
    correspondencias_seca = resultado["_merge_seca"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge_seca"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Ano identificado no arquivo de demanda de agua: {ano_agua}")
    print(f"Municipios com correspondencia na base de agua: {correspondencias_agua}")
    print(f"Municipios sem correspondencia na base de agua: {len(resultado) - correspondencias_agua}")
    print(f"Municipios com correspondencia na base de seca: {correspondencias_seca}")
    print(f"Municipios sem correspondencia na base de seca: {len(resultado) - correspondencias_seca}")
    print(f"Colunas incorporadas da ANA: {len(RENOMEAR_AGUA) + len(RENOMEAR_SECA)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
