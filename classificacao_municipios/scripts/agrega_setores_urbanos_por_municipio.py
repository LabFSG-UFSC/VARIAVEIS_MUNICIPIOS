#!/usr/bin/env python3
"""
Agrega metricas dos setores censitarios urbanos por municipio.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyogrio


ROOT = Path(__file__).resolve().parents[2]
SETORES_PADRAO = ROOT / "classificacao_municipios" / "BR_setores_CD2022 (1).gpkg"
SAIDA_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "setores_urbanos_metricas_municipio.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Agrega metricas de setores urbanos por municipio."
    )
    parser.add_argument("--setores", default=str(SETORES_PADRAO), help="GeoPackage dos setores censitarios.")
    parser.add_argument("--output", default=str(SAIDA_PADRAO), help="CSV de saida por municipio.")
    return parser.parse_args()


def carrega_setores(caminho: Path) -> pd.DataFrame:
    colunas = [
        "CD_SETOR",
        "SITUACAO",
        "CD_SIT",
        "AREA_KM2",
        "CD_MUN",
        "NM_MUN",
        "CD_BAIRRO",
        "CD_DIST",
        "CD_SUBDIST",
        "CD_NU",
        "CD_CONCURB",
    ]
    df = pyogrio.read_dataframe(caminho, columns=colunas, read_geometry=False)
    df["CD_MUN"] = pd.to_numeric(df["CD_MUN"], errors="coerce").astype("Int64")
    df["CD_SIT"] = pd.to_numeric(df["CD_SIT"], errors="coerce")
    df["AREA_KM2"] = pd.to_numeric(df["AREA_KM2"], errors="coerce")
    return df


def agrega_metricas(df: pd.DataFrame) -> pd.DataFrame:
    urbanos = df.loc[
        df["SITUACAO"].astype(str).str.strip().str.lower().eq("urbana")
        | df["CD_SIT"].isin([1, 2, 3])
    ].copy()

    urbanos["is_cd_sit_1"] = urbanos["CD_SIT"].eq(1).astype(int)
    urbanos["is_cd_sit_2"] = urbanos["CD_SIT"].eq(2).astype(int)
    urbanos["is_cd_sit_3"] = urbanos["CD_SIT"].eq(3).astype(int)

    resumo = (
        urbanos.groupby(["CD_MUN", "NM_MUN"], as_index=False)
        .agg(
            qtd_setores_urbanos=("CD_SETOR", "nunique"),
            area_urb_setores_km2=("AREA_KM2", "sum"),
            area_media_setor_urb_km2=("AREA_KM2", "mean"),
            area_mediana_setor_urb_km2=("AREA_KM2", "median"),
            area_dp_setor_urb_km2=("AREA_KM2", "std"),
            qtd_bairros_urbanos=("CD_BAIRRO", lambda s: s.dropna().astype(str).nunique()),
            qtd_distritos_urbanos=("CD_DIST", lambda s: s.dropna().astype(str).nunique()),
            qtd_subdistritos_urbanos=("CD_SUBDIST", lambda s: s.dropna().astype(str).nunique()),
            qtd_nucleos_urbanos=("CD_NU", lambda s: s.dropna().astype(str).nunique()),
            qtd_concurb_urbanos=("CD_CONCURB", lambda s: s.dropna().astype(str).nunique()),
            qtd_setores_cd_sit_1=("is_cd_sit_1", "sum"),
            qtd_setores_cd_sit_2=("is_cd_sit_2", "sum"),
            qtd_setores_cd_sit_3=("is_cd_sit_3", "sum"),
        )
        .rename(columns={"CD_MUN": "codigo_mun", "NM_MUN": "nome_municipio_setores"})
    )

    resumo["area_dp_setor_urb_km2"] = resumo["area_dp_setor_urb_km2"].fillna(0)
    resumo["cv_area_setor_urbano"] = resumo["area_dp_setor_urb_km2"] / resumo["area_media_setor_urb_km2"].replace(0, np.nan)
    resumo["pct_setores_cd_sit_1"] = 100 * resumo["qtd_setores_cd_sit_1"] / resumo["qtd_setores_urbanos"].replace(0, np.nan)
    resumo["pct_setores_cd_sit_2"] = 100 * resumo["qtd_setores_cd_sit_2"] / resumo["qtd_setores_urbanos"].replace(0, np.nan)
    resumo["pct_setores_cd_sit_3"] = 100 * resumo["qtd_setores_cd_sit_3"] / resumo["qtd_setores_urbanos"].replace(0, np.nan)
    resumo["densidade_setores_urbanos_km2"] = resumo["qtd_setores_urbanos"] / resumo["area_urb_setores_km2"].replace(0, np.nan)
    return resumo


def main() -> None:
    args = parse_args()
    setores = carrega_setores(Path(args.setores))
    resumo = agrega_metricas(setores)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    resumo.to_csv(output, index=False)

    print(f"CSV gerado: {output}")
    print(f"Municipios com setores urbanos: {len(resumo)}")
    print(resumo.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
