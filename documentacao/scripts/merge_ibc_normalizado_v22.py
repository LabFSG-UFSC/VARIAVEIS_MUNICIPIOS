#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da base de indicadores normalizados do IBC com a v22.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V22_FILE = BASE_DIR / "prata" / "processamento" / "merge_v22.csv"
IBC_FILE = BASE_DIR / "bronze" / "IBC_municipios_indicadores_normalizados.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v23.csv"
PRE_MERGE_FILE = BASE_DIR / "prata" / "pre_merge" / "ibc_municipios_indicadores_normalizados_2024.csv"

RENOMEAR_COLUNAS = {
    "IBC": "ibc_indice_2024",
    "Cobertura Pop. 4G5G": "ibc_cobertura_pop_4g5g_2024",
    "Densidade SMP": "ibc_densidade_smp_2024",
    "HHI SMP": "ibc_hhi_smp_2024",
    "Densidade SCM": "ibc_densidade_scm_2024",
    "HHI SCM": "ibc_hhi_scm_2024",
    "Adensamento Estações": "ibc_adensamento_estacoes_2024",
    "Fibra": "ibc_fibra_2024",
    "Cobertura área agricultável": "ibc_cobertura_area_agricultavel_2024",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v22() -> pd.DataFrame:
    df = pd.read_csv(V22_FILE)
    if "cod_mun" not in df.columns:
        raise ValueError("Coluna 'cod_mun' nao encontrada na base v22.")

    df["_codigo_merge"] = df["cod_mun"].map(normalizar_codigo)
    return df


def carregar_ibc() -> tuple[pd.DataFrame, int]:
    df = pd.read_csv(IBC_FILE, sep=";", dtype={"Código Município": "string"})

    colunas_necessarias = ["Ano", "Código Município", *RENOMEAR_COLUNAS.keys()]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo do IBC: {faltantes}")

    anos = sorted(pd.to_numeric(df["Ano"], errors="coerce").dropna().astype(int).unique().tolist())
    if not anos:
        raise ValueError("Nenhum ano valido foi encontrado no arquivo do IBC.")

    ano_referencia = max(anos)
    base = df.loc[df["Ano"].eq(ano_referencia), colunas_necessarias].copy()
    base["_codigo_merge"] = base["Código Município"].map(normalizar_codigo)

    for coluna in RENOMEAR_COLUNAS:
        base[coluna] = (
            base[coluna]
            .astype("string")
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce")

    pre_merge = base.rename(
        columns={
            "Ano": "ano",
            "Código Município": "cod_mun",
            **RENOMEAR_COLUNAS,
        }
    ).copy()
    pre_merge["cod_mun"] = pre_merge["cod_mun"].map(normalizar_codigo)
    pre_merge = pre_merge[
        ["ano", "cod_mun", *RENOMEAR_COLUNAS.values()]
    ].copy()

    base = base.rename(columns=RENOMEAR_COLUNAS)
    base = base[["_codigo_merge", *RENOMEAR_COLUNAS.values()]].copy()

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados no arquivo do IBC para o ano {ano_referencia}.")

    PRE_MERGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    pre_merge.to_csv(PRE_MERGE_FILE, index=False, encoding="utf-8")

    return base, ano_referencia


def main() -> int:
    v22 = carregar_v22()
    ibc, ano_referencia = carregar_ibc()

    resultado = v22.merge(ibc, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Arquivo pre-merge gerado: {PRE_MERGE_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Ano utilizado no arquivo do IBC: {ano_referencia}")
    print(f"Municipios com correspondencia no IBC: {correspondencias}")
    print(f"Municipios sem correspondencia no IBC: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas do IBC: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
