#!/usr/bin/env python3
"""
Agrupa os municipios da classe A da classificacao ABC da CNEFE em clusters
de perfil semelhante.

O script usa k-means implementado com numpy para evitar dependencia de
bibliotecas extras no ambiente.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from gera_csv_representacao_cnefe_capitais import SHAPEFILE_PADRAO, carrega_malha_municipal


ROOT = Path(__file__).resolve().parents[2]
ABC_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_municipios.csv"
MERGE_PADRAO = ROOT / "prata" / "processamento" / "merge_v26.csv"
OFERTAS_PADRAO = ROOT / "prata" / "cnefe domiclios ofertas-" / "ofertas_merge_cnefe_municipal.csv"
SAIDA_BASE_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_municipios.csv"
SAIDA_RESUMO_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_resumo.csv"
SAIDA_CENTROIDES_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_centroides.csv"
SAIDA_GPKG_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "clusters_classe_a_municipios.gpkg"

VARIAVEIS_PADRAO = [
    "log_domicilios",
    "log_pop_total",
    "log_empresas_total",
    "log_estab_total",
    "log_area_urb_densa_km2",
    "indice_conectividade",
    "log_total_geral",
    "domicilios_por_1k_hab",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clusteriza municipios da classe A da classificacao ABC da CNEFE."
    )
    parser.add_argument("--abc", default=str(ABC_PADRAO), help="CSV da classificacao ABC municipal.")
    parser.add_argument("--merge", default=str(MERGE_PADRAO), help="CSV merge_v26.")
    parser.add_argument("--ofertas", default=str(OFERTAS_PADRAO), help="CSV municipal de ofertas.")
    parser.add_argument("--shapefile", default=str(SHAPEFILE_PADRAO), help="Shapefile municipal.")
    parser.add_argument("--k", type=int, default=4, help="Numero de clusters. Padrao: 4.")
    parser.add_argument("--seed", type=int, default=42, help="Semente do k-means. Padrao: 42.")
    parser.add_argument(
        "--output-base",
        default=str(SAIDA_BASE_PADRAO),
        help="CSV com cluster por municipio da classe A.",
    )
    parser.add_argument(
        "--output-resumo",
        default=str(SAIDA_RESUMO_PADRAO),
        help="CSV resumo por cluster.",
    )
    parser.add_argument(
        "--output-centroides",
        default=str(SAIDA_CENTROIDES_PADRAO),
        help="CSV com centroides padronizados e originais dos clusters.",
    )
    parser.add_argument(
        "--output-gpkg",
        default=str(SAIDA_GPKG_PADRAO),
        help="GeoPackage com os municipios da classe A clusterizados.",
    )
    return parser.parse_args()


def carrega_abc(caminho: Path) -> pd.DataFrame:
    colunas = ["codigo_mun", "NOME", "classe_abc", "domicilios", "domicilios_por_1k_hab"]
    df = pd.read_csv(caminho, usecols=colunas).copy()
    df["codigo_mun"] = pd.to_numeric(df["codigo_mun"], errors="coerce").astype("Int64")
    for coluna in ["domicilios", "domicilios_por_1k_hab"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def carrega_merge(caminho: Path) -> pd.DataFrame:
    colunas = [
        "cod_mun",
        "municipio",
        "pop_total",
        "empresas_total",
        "estab_total",
        "area_urb_densa_km2",
        "indice_conectividade",
    ]
    df = pd.read_csv(caminho, usecols=colunas).copy()
    df["codigo_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")
    df = df.drop(columns=["cod_mun"])
    for coluna in ["pop_total", "empresas_total", "estab_total", "area_urb_densa_km2", "indice_conectividade"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def carrega_ofertas(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho, usecols=["CD_MUN", "total_geral"]).copy()
    df["codigo_mun"] = pd.to_numeric(df["CD_MUN"], errors="coerce").astype("Int64")
    total_geral = (
        df["total_geral"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    )
    df["total_geral"] = pd.to_numeric(total_geral, errors="coerce")
    return df.drop(columns=["CD_MUN"])


def prepara_base(abc: pd.DataFrame, merge: pd.DataFrame, ofertas: pd.DataFrame) -> pd.DataFrame:
    df = abc.merge(merge, on="codigo_mun", how="left", validate="one_to_one")
    df = df.merge(ofertas, on="codigo_mun", how="left", validate="one_to_one")
    df = df.loc[df["classe_abc"] == "A"].copy()
    df["log_domicilios"] = np.log1p(df["domicilios"].clip(lower=0))
    df["log_pop_total"] = np.log1p(df["pop_total"].clip(lower=0))
    df["log_empresas_total"] = np.log1p(df["empresas_total"].clip(lower=0))
    df["log_estab_total"] = np.log1p(df["estab_total"].clip(lower=0))
    df["log_area_urb_densa_km2"] = np.log1p(df["area_urb_densa_km2"].clip(lower=0))
    df["log_total_geral"] = np.log1p(df["total_geral"].clip(lower=0))
    return df


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
    primeiro = rng.integers(0, n)
    centroides[0] = X[primeiro]
    dist2 = np.sum((X - centroides[0]) ** 2, axis=1)

    for i in range(1, k):
        probs = dist2 / dist2.sum()
        escolhido = rng.choice(n, p=probs)
        centroides[i] = X[escolhido]
        nova_dist2 = np.sum((X - centroides[i]) ** 2, axis=1)
        dist2 = np.minimum(dist2, nova_dist2)

    return centroides


def roda_kmeans(X: np.ndarray, k: int, seed: int, max_iter: int = 200) -> tuple[np.ndarray, np.ndarray, float]:
    if k < 2:
        raise ValueError("O numero de clusters deve ser pelo menos 2.")
    if X.shape[0] < k:
        raise ValueError("Ha menos municipios validos do que clusters solicitados.")

    rng = np.random.default_rng(seed)
    centroides = inicializa_kmeans_pp(X, k, rng)

    for _ in range(max_iter):
        distancias = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
        labels = distancias.argmin(axis=1)

        novos_centroides = centroides.copy()
        for grupo in range(k):
            mascara = labels == grupo
            if mascara.any():
                novos_centroides[grupo] = X[mascara].mean(axis=0)
            else:
                novos_centroides[grupo] = X[rng.integers(0, X.shape[0])]

        if np.allclose(centroides, novos_centroides):
            centroides = novos_centroides
            break
        centroides = novos_centroides

    distancias_finais = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
    labels = distancias_finais.argmin(axis=1)
    inertia = float(np.sum(np.min(distancias_finais, axis=1)))
    return labels, centroides, inertia


def organiza_rotulos(base_valida: pd.DataFrame, labels: np.ndarray) -> pd.Series:
    trabalho = base_valida.copy()
    trabalho["cluster_id"] = labels
    ordem = (
        trabalho.groupby("cluster_id", as_index=False)["domicilios"]
        .mean()
        .sort_values("domicilios", ascending=False)
        .reset_index(drop=True)
    )
    ordem["cluster_rank"] = np.arange(1, len(ordem) + 1)
    mapa = dict(zip(ordem["cluster_id"], ordem["cluster_rank"]))
    return pd.Series(labels, index=base_valida.index).map(mapa).astype(int)


def monta_resumo(base_valida: pd.DataFrame, cluster_rank: pd.Series, variaveis: list[str], inertia: float) -> pd.DataFrame:
    trabalho = base_valida.copy()
    trabalho["cluster_a"] = cluster_rank.values
    resumo = (
        trabalho.groupby("cluster_a", as_index=False)
        .agg(
            quantidade_municipios=("codigo_mun", "nunique"),
            domicilios_totais=("domicilios", "sum"),
            domicilios_medios=("domicilios", "mean"),
            pop_total_media=("pop_total", "mean"),
            empresas_media=("empresas_total", "mean"),
            estab_media=("estab_total", "mean"),
            conectividade_media=("indice_conectividade", "mean"),
            ofertas_media=("total_geral", "mean"),
        )
        .sort_values("cluster_a")
        .reset_index(drop=True)
    )
    resumo["percentual_domicilios_classe_a"] = 100 * resumo["domicilios_totais"] / resumo["domicilios_totais"].sum()
    resumo["inercia_kmeans"] = inertia
    return resumo


def monta_centroides(
    variaveis: list[str],
    base_valida: pd.DataFrame,
    cluster_rank: pd.Series,
    medias: pd.Series,
    desvios: pd.Series,
) -> pd.DataFrame:
    trabalho = base_valida.copy()
    trabalho["cluster_a"] = cluster_rank.values
    agrupado = trabalho.groupby("cluster_a")[variaveis].mean().reset_index()

    centroides_df = agrupado.copy()
    for var in variaveis:
        centroides_df = centroides_df.rename(columns={var: f"{var}_original"})
        centroides_df[f"{var}_z"] = (
            centroides_df[f"{var}_original"] - medias[var]
        ) / desvios[var]

    colunas = ["cluster_a"]
    for var in variaveis:
        colunas.append(f"{var}_z")
    for var in variaveis:
        colunas.append(f"{var}_original")
    return centroides_df[colunas]


def monta_gpkg(caminho_shapefile: Path, base_clusters: pd.DataFrame) -> gpd.GeoDataFrame:
    municipios = carrega_malha_municipal(caminho_shapefile).copy()
    colunas_geom = ["codigo_mun", "NOME", "uf", "rgi", "nome_rgi", "rgint", "nome_rgint", "geometry"]
    gdf = municipios[colunas_geom].merge(
        base_clusters,
        on=["codigo_mun", "NOME"],
        how="inner",
        validate="one_to_one",
    )
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=municipios.crs)


def main() -> None:
    args = parse_args()
    abc = carrega_abc(Path(args.abc))
    merge = carrega_merge(Path(args.merge))
    ofertas = carrega_ofertas(Path(args.ofertas))
    base = prepara_base(abc, merge, ofertas)

    X, medias, desvios, idx_validos = padroniza(base, VARIAVEIS_PADRAO)
    base_valida = base.loc[idx_validos].copy()
    labels, centroides, inertia = roda_kmeans(X, args.k, args.seed)
    cluster_rank = organiza_rotulos(base_valida, labels)

    base_valida["cluster_a"] = cluster_rank.values
    resumo = monta_resumo(base_valida, cluster_rank, VARIAVEIS_PADRAO, inertia)
    centroides_df = monta_centroides(VARIAVEIS_PADRAO, base_valida, cluster_rank, medias, desvios)
    gdf = monta_gpkg(Path(args.shapefile), base_valida)

    output_base = Path(args.output_base)
    output_resumo = Path(args.output_resumo)
    output_centroides = Path(args.output_centroides)
    output_gpkg = Path(args.output_gpkg)
    output_base.parent.mkdir(parents=True, exist_ok=True)
    output_resumo.parent.mkdir(parents=True, exist_ok=True)
    output_centroides.parent.mkdir(parents=True, exist_ok=True)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)

    base_valida.sort_values(["cluster_a", "domicilios"], ascending=[True, False]).to_csv(output_base, index=False)
    resumo.to_csv(output_resumo, index=False)
    centroides_df.to_csv(output_centroides, index=False)
    gdf.to_file(output_gpkg, layer="clusters_classe_a", driver="GPKG")

    print(f"Base com clusters gerada: {output_base}")
    print(f"Resumo por cluster gerado: {output_resumo}")
    print(f"Centroides gerados: {output_centroides}")
    print(f"GeoPackage gerado: {output_gpkg}")
    print()
    print(resumo.to_string(index=False))


if __name__ == "__main__":
    main()
