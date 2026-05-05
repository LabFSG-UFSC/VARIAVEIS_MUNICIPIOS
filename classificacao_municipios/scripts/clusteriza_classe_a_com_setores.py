#!/usr/bin/env python3
"""
Clusteriza a classe A incorporando metricas de setores censitarios urbanos.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from gera_csv_representacao_cnefe_capitais import (
    SHAPEFILE_PADRAO,
    carrega_malha_municipal,
    seleciona_capitais_uf,
)


ROOT = Path(__file__).resolve().parents[2]
ABC_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_municipios.csv"
MERGE_PADRAO = ROOT / "prata" / "processamento" / "merge_v26.csv"
OFERTAS_PADRAO = ROOT / "prata" / "cnefe domiclios ofertas-" / "ofertas_merge_cnefe_municipal.csv"
SETORES_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "setores_urbanos_metricas_municipio.csv"
SAIDA_BASE_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_setores_municipios.csv"
SAIDA_RESUMO_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_setores_resumo.csv"
SAIDA_CENTROIDES_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_setores_centroides.csv"
SAIDA_GPKG_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_setores_municipios.gpkg"

VARIAVEIS_CLUSTER = [
    "log_pop_total",
    "indice_conectividade",
    "log_total_geral",
    "domicilios_por_1k_hab",
    "regic_var61",
    "log_qtd_setores_urbanos",
    "log_area_urb_setores_km2",
    "cv_area_setor_urbano",
    "pct_setores_cd_sit_1",
    "densidade_setores_urbanos_km2",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clusteriza a classe A com metricas de setores urbanos."
    )
    parser.add_argument("--abc", default=str(ABC_PADRAO))
    parser.add_argument("--merge", default=str(MERGE_PADRAO))
    parser.add_argument("--ofertas", default=str(OFERTAS_PADRAO))
    parser.add_argument("--setores", default=str(SETORES_PADRAO))
    parser.add_argument("--shapefile", default=str(SHAPEFILE_PADRAO))
    parser.add_argument("--k", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-base", default=str(SAIDA_BASE_PADRAO))
    parser.add_argument("--output-resumo", default=str(SAIDA_RESUMO_PADRAO))
    parser.add_argument("--output-centroides", default=str(SAIDA_CENTROIDES_PADRAO))
    parser.add_argument("--output-gpkg", default=str(SAIDA_GPKG_PADRAO))
    return parser.parse_args()


def carrega_abc(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(
        caminho,
        usecols=["codigo_mun", "NOME", "classe_abc", "domicilios", "domicilios_por_1k_hab"],
    ).copy()
    df["codigo_mun"] = pd.to_numeric(df["codigo_mun"], errors="coerce").astype("Int64")
    for coluna in ["domicilios", "domicilios_por_1k_hab"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def carrega_merge(caminho: Path) -> pd.DataFrame:
    colunas = ["cod_mun", "municipio", "pop_total", "indice_conectividade", "regic_var61"]
    df = pd.read_csv(caminho, usecols=colunas).copy()
    df["codigo_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")
    df = df.drop(columns=["cod_mun"])
    for coluna in ["pop_total", "indice_conectividade", "regic_var61"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def carrega_ofertas(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho, usecols=["CD_MUN", "total_geral"]).copy()
    df["codigo_mun"] = pd.to_numeric(df["CD_MUN"], errors="coerce").astype("Int64")
    total_geral = df["total_geral"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    df["total_geral"] = pd.to_numeric(total_geral, errors="coerce")
    return df.drop(columns=["CD_MUN"])


def prepara_base(abc: pd.DataFrame, merge: pd.DataFrame, ofertas: pd.DataFrame, setores: pd.DataFrame) -> pd.DataFrame:
    df = abc.merge(merge, on="codigo_mun", how="left", validate="one_to_one")
    df = df.merge(ofertas, on="codigo_mun", how="left", validate="one_to_one")
    df = df.merge(setores, on="codigo_mun", how="left", validate="one_to_one")
    df = df.loc[df["classe_abc"] == "A"].copy()
    df["log_pop_total"] = np.log1p(df["pop_total"].clip(lower=0))
    df["log_total_geral"] = np.log1p(df["total_geral"].clip(lower=0))
    df["log_qtd_setores_urbanos"] = np.log1p(df["qtd_setores_urbanos"].clip(lower=0))
    df["log_area_urb_setores_km2"] = np.log1p(df["area_urb_setores_km2"].clip(lower=0))
    return df


def identifica_capitais_uf(caminho_shapefile: Path) -> set[int]:
    municipios = carrega_malha_municipal(caminho_shapefile)
    capitais = seleciona_capitais_uf(municipios)
    return set(capitais["codigo_mun"].dropna().astype(int))


def padroniza(df: pd.DataFrame, colunas: list[str]) -> tuple[np.ndarray, pd.Series, pd.Series, pd.Index]:
    trabalho = df[colunas].replace([np.inf, -np.inf], np.nan).dropna().copy()
    idx = trabalho.index
    medias = trabalho.mean()
    desvios = trabalho.std(ddof=0).replace(0, 1.0)
    z = ((trabalho - medias) / desvios).to_numpy(dtype=float)
    return z, medias, desvios, idx


def inicializa_kmeans_pp(X: np.ndarray, k: int, rng: np.random.Generator) -> np.ndarray:
    n = X.shape[0]
    centroides = np.empty((k, X.shape[1]), dtype=float)
    centroides[0] = X[rng.integers(0, n)]
    dist2 = np.sum((X - centroides[0]) ** 2, axis=1)
    for i in range(1, k):
        probs = dist2 / dist2.sum()
        escolhido = rng.choice(n, p=probs)
        centroides[i] = X[escolhido]
        dist2 = np.minimum(dist2, np.sum((X - centroides[i]) ** 2, axis=1))
    return centroides


def roda_kmeans(X: np.ndarray, k: int, seed: int, max_iter: int = 200) -> tuple[np.ndarray, float]:
    rng = np.random.default_rng(seed)
    centroides = inicializa_kmeans_pp(X, k, rng)
    for _ in range(max_iter):
        distancias = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
        labels = distancias.argmin(axis=1)
        novos = centroides.copy()
        for grupo in range(k):
            mascara = labels == grupo
            if mascara.any():
                novos[grupo] = X[mascara].mean(axis=0)
            else:
                novos[grupo] = X[rng.integers(0, X.shape[0])]
        if np.allclose(novos, centroides):
            centroides = novos
            break
        centroides = novos
    distancias = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
    labels = distancias.argmin(axis=1)
    inertia = float(np.sum(np.min(distancias, axis=1)))
    return labels, inertia


def reorganiza_clusters(base_valida: pd.DataFrame, labels: np.ndarray) -> pd.Series:
    trabalho = base_valida.copy()
    trabalho["cluster_id"] = labels
    ordem = (
        trabalho.groupby("cluster_id", as_index=False)["domicilios"]
        .mean()
        .sort_values("domicilios", ascending=False)
        .reset_index(drop=True)
    )
    ordem["cluster_ordem"] = np.arange(1, len(ordem) + 1)
    mapa = dict(zip(ordem["cluster_id"], ordem["cluster_ordem"]))
    return pd.Series(labels, index=base_valida.index).map(mapa).astype(int)


def monta_centroides(base_valida: pd.DataFrame, cluster_rank: pd.Series, medias: pd.Series, desvios: pd.Series) -> pd.DataFrame:
    trabalho = base_valida.copy()
    trabalho["cluster_setores"] = cluster_rank.values
    agrupado = trabalho.groupby("cluster_setores")[VARIAVEIS_CLUSTER].mean().reset_index()
    centroides = agrupado.copy()
    for var in VARIAVEIS_CLUSTER:
        centroides = centroides.rename(columns={var: f"{var}_original"})
        centroides[f"{var}_z"] = (centroides[f"{var}_original"] - medias[var]) / desvios[var]
    colunas = ["cluster_setores"]
    for var in VARIAVEIS_CLUSTER:
        colunas.append(f"{var}_z")
    for var in VARIAVEIS_CLUSTER:
        colunas.append(f"{var}_original")
    return centroides[colunas]


def monta_resumo(df: pd.DataFrame) -> pd.DataFrame:
    resumo = (
        df.groupby(["grupo_setores", "tipo_grupo"], as_index=False)
        .agg(
            quantidade_municipios=("codigo_mun", "nunique"),
            domicilios_totais=("domicilios", "sum"),
            domicilios_medios=("domicilios", "mean"),
            pop_total_media=("pop_total", "mean"),
            conectividade_media=("indice_conectividade", "mean"),
            ofertas_media=("total_geral", "mean"),
            setores_urbanos_medios=("qtd_setores_urbanos", "mean"),
            area_setores_urb_km2_media=("area_urb_setores_km2", "mean"),
            pct_cd_sit_1_medio=("pct_setores_cd_sit_1", "mean"),
        )
        .sort_values(["tipo_grupo", "domicilios_totais"], ascending=[True, False])
        .reset_index(drop=True)
    )
    resumo["percentual_domicilios_classe_a"] = 100 * resumo["domicilios_totais"] / resumo["domicilios_totais"].sum()
    return resumo


def monta_gpkg(caminho_shapefile: Path, base_saida: pd.DataFrame) -> gpd.GeoDataFrame:
    municipios = carrega_malha_municipal(caminho_shapefile).copy()
    gdf = municipios[
        ["codigo_mun", "NOME", "uf", "rgi", "nome_rgi", "rgint", "nome_rgint", "geometry"]
    ].merge(base_saida, on=["codigo_mun", "NOME"], how="inner", validate="one_to_one")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=municipios.crs)


def main() -> None:
    args = parse_args()
    abc = carrega_abc(Path(args.abc))
    merge = carrega_merge(Path(args.merge))
    ofertas = carrega_ofertas(Path(args.ofertas))
    setores = pd.read_csv(Path(args.setores))
    setores["codigo_mun"] = pd.to_numeric(setores["codigo_mun"], errors="coerce").astype("Int64")
    base = prepara_base(abc, merge, ofertas, setores)

    capitais_uf = identifica_capitais_uf(Path(args.shapefile))
    base["is_capital_uf"] = base["codigo_mun"].astype("Int64").isin(list(capitais_uf))

    capitais = base.loc[base["is_capital_uf"]].copy()
    capitais["grupo_setores"] = "capital_uf"
    capitais["tipo_grupo"] = "capital"
    capitais["cluster_setores"] = pd.NA

    restante = base.loc[~base["is_capital_uf"]].copy()
    X, medias, desvios, idx_validos = padroniza(restante, VARIAVEIS_CLUSTER)
    restante_valido = restante.loc[idx_validos].copy()
    labels, inertia = roda_kmeans(X, args.k, args.seed)
    cluster_rank = reorganiza_clusters(restante_valido, labels)
    restante_valido["cluster_setores"] = cluster_rank.values
    restante_valido["grupo_setores"] = restante_valido["cluster_setores"].map(lambda x: f"setores_a_{int(x)}")
    restante_valido["tipo_grupo"] = "cluster_setores"
    restante_valido["inercia_kmeans"] = inertia

    base_saida = pd.concat([capitais, restante_valido], ignore_index=True, sort=False)
    resumo = monta_resumo(base_saida)
    centroides = monta_centroides(restante_valido, cluster_rank, medias, desvios)
    gdf = monta_gpkg(Path(args.shapefile), base_saida)

    output_base = Path(args.output_base)
    output_resumo = Path(args.output_resumo)
    output_centroides = Path(args.output_centroides)
    output_gpkg = Path(args.output_gpkg)
    output_base.parent.mkdir(parents=True, exist_ok=True)
    output_resumo.parent.mkdir(parents=True, exist_ok=True)
    output_centroides.parent.mkdir(parents=True, exist_ok=True)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)

    base_saida.to_csv(output_base, index=False)
    resumo.to_csv(output_resumo, index=False)
    centroides.to_csv(output_centroides, index=False)
    gdf.to_file(output_gpkg, layer="clusters_classe_a_setores", driver="GPKG")

    print(f"Base gerada: {output_base}")
    print(f"Resumo gerado: {output_resumo}")
    print(f"Centroides gerados: {output_centroides}")
    print(f"GeoPackage gerado: {output_gpkg}")
    print()
    print(resumo.to_string(index=False))


if __name__ == "__main__":
    main()
