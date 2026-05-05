#!/usr/bin/env python3
"""
Classifica os municipios brasileiros em A, B e C de acordo com a
participacao acumulada dos domicilios da CNEFE.

O criterio padrao segue um corte cumulativo transparente e reproduzivel:
- classe A: municipios ate 80% dos domicilios acumulados
- classe B: municipios entre 80% e 95%
- classe C: municipios acima de 95%
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

from gera_csv_representacao_cnefe_capitais import (
    CNEFE_PADRAO,
    SHAPEFILE_PADRAO,
    carrega_malha_municipal,
)

ROOT = Path(__file__).resolve().parents[2]
SAIDA_MUNICIPIOS_PADRAO = (
    ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_municipios.csv"
)
SAIDA_RESUMO_PADRAO = (
    ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_resumo.csv"
)
SAIDA_GRAFICO_PADRAO = (
    ROOT / "classificacao_municipios" / "processamento" / "grafico_abc_cnefe_municipios.svg"
)
SAIDA_GPKG_PADRAO = (
    ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_municipios.gpkg"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classifica municipios em A/B/C com base nos domicilios da CNEFE."
    )
    parser.add_argument(
        "--cnefe",
        default=str(CNEFE_PADRAO),
        help="CSV municipal consolidado da CNEFE.",
    )
    parser.add_argument(
        "--shapefile",
        default=str(SHAPEFILE_PADRAO),
        help="Shapefile municipal com vinculos territoriais.",
    )
    parser.add_argument(
        "--output-municipios",
        default=str(SAIDA_MUNICIPIOS_PADRAO),
        help="CSV detalhado por municipio.",
    )
    parser.add_argument(
        "--output-resumo",
        default=str(SAIDA_RESUMO_PADRAO),
        help="CSV resumo por classe ABC.",
    )
    parser.add_argument(
        "--output-plot",
        default=str(SAIDA_GRAFICO_PADRAO),
        help="Arquivo SVG de saida com a curva ABC.",
    )
    parser.add_argument(
        "--output-gpkg",
        default=str(SAIDA_GPKG_PADRAO),
        help="GeoPackage de saida com a classificacao ABC municipal.",
    )
    parser.add_argument(
        "--limite-a",
        type=float,
        default=80.0,
        help="Percentual acumulado maximo da classe A. Padrao: 80.",
    )
    parser.add_argument(
        "--limite-b",
        type=float,
        default=95.0,
        help="Percentual acumulado maximo da classe B. Padrao: 95.",
    )
    return parser.parse_args()


def valida_limites(limite_a: float, limite_b: float) -> None:
    if not 0 < limite_a < limite_b < 100:
        raise ValueError("Os limites devem obedecer a regra 0 < limite_a < limite_b < 100.")


def carrega_cnefe_abc(caminho: Path) -> pd.DataFrame:
    colunas_disponiveis = pd.read_csv(caminho, nrows=0).columns.tolist()
    colunas = ["codigo_mun", "domicilios"]
    if "populacao" in colunas_disponiveis:
        colunas.append("populacao")

    df = pd.read_csv(caminho, usecols=colunas).copy()
    df["codigo_mun"] = pd.to_numeric(df["codigo_mun"], errors="coerce").astype("Int64")
    df["domicilios"] = pd.to_numeric(df["domicilios"], errors="coerce")

    if "populacao" in df.columns:
        df["populacao"] = pd.to_numeric(df["populacao"], errors="coerce")
    else:
        df["populacao"] = pd.NA

    if df["codigo_mun"].isna().any():
        raise ValueError("O CSV da CNEFE possui codigo_mun invalido.")

    return df


def monta_base(caminho_cnefe: Path, caminho_shapefile: Path) -> pd.DataFrame:
    cnefe = carrega_cnefe_abc(caminho_cnefe)
    municipios = carrega_malha_municipal(caminho_shapefile)

    colunas_municipais = [
        "codigo_mun",
        "NOME",
        "uf",
        "rgi",
        "nome_rgi",
        "rgint",
        "nome_rgint",
    ]
    df = municipios[colunas_municipais].merge(cnefe, on="codigo_mun", how="left", validate="one_to_one")
    df["domicilios"] = pd.to_numeric(df["domicilios"], errors="coerce").fillna(0)
    return pd.DataFrame(df)


def monta_gpkg_municipios(caminho_shapefile: Path, df_municipios: pd.DataFrame) -> gpd.GeoDataFrame:
    municipios = carrega_malha_municipal(caminho_shapefile).copy()
    colunas_geom = [
        "codigo_mun",
        "NOME",
        "uf",
        "rgi",
        "nome_rgi",
        "rgint",
        "nome_rgint",
        "geometry",
    ]
    gdf = municipios[colunas_geom].merge(
        df_municipios,
        on=["codigo_mun", "NOME", "uf", "rgi", "nome_rgi", "rgint", "nome_rgint"],
        how="left",
        validate="one_to_one",
    )
    colunas_saida = [
        "ordem_abc",
        "classe_abc",
        "codigo_mun",
        "NOME",
        "uf",
        "rgi",
        "nome_rgi",
        "rgint",
        "nome_rgint",
        "domicilios",
        "participacao_pct",
        "participacao_acumulada_pct",
        "domicilios_acumulados",
        "domicilios_por_1k_hab",
        "geometry",
    ]
    return gpd.GeoDataFrame(gdf[colunas_saida], geometry="geometry", crs=municipios.crs)


def classifica_abc(df: pd.DataFrame, limite_a: float, limite_b: float) -> pd.DataFrame:
    total_brasil = float(df["domicilios"].sum())
    if total_brasil <= 0:
        raise ValueError("A soma de domicilios da CNEFE precisa ser positiva.")

    resultado = df.copy()
    resultado = resultado.sort_values(
        ["domicilios", "codigo_mun"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)
    resultado["ordem_abc"] = resultado.index + 1
    resultado["participacao_pct"] = resultado["domicilios"] / total_brasil * 100
    resultado["participacao_acumulada_pct"] = resultado["participacao_pct"].cumsum()
    resultado["domicilios_acumulados"] = resultado["domicilios"].cumsum()

    classe = pd.Series("C", index=resultado.index)
    classe = classe.mask(resultado["participacao_acumulada_pct"] <= limite_b, "B")
    classe = classe.mask(resultado["participacao_acumulada_pct"] <= limite_a, "A")
    if not resultado.empty:
        classe.iloc[0] = "A"

    resultado["classe_abc"] = classe
    resultado["domicilios_por_1k_hab"] = (
        1000 * resultado["domicilios"] / resultado["populacao"]
    )
    resultado["domicilios_por_1k_hab"] = resultado["domicilios_por_1k_hab"].where(
        resultado["populacao"].fillna(0) > 0
    )

    colunas_saida = [
        "ordem_abc",
        "classe_abc",
        "codigo_mun",
        "NOME",
        "uf",
        "rgi",
        "nome_rgi",
        "rgint",
        "nome_rgint",
        "domicilios",
        "participacao_pct",
        "participacao_acumulada_pct",
        "domicilios_acumulados",
        "domicilios_por_1k_hab",
    ]
    return resultado[colunas_saida]


def monta_resumo(df: pd.DataFrame) -> pd.DataFrame:
    total_brasil = float(df["domicilios"].sum())
    resumo = (
        df.groupby("classe_abc", as_index=False)
        .agg(
            quantidade_municipios=("codigo_mun", "nunique"),
            domicilios_cnefe=("domicilios", "sum"),
            menor_ordem=("ordem_abc", "min"),
            maior_ordem=("ordem_abc", "max"),
        )
        .sort_values("menor_ordem", kind="stable")
        .reset_index(drop=True)
    )
    resumo["percentual_domicilios"] = resumo["domicilios_cnefe"] / total_brasil * 100
    resumo["percentual_domicilios_acumulado"] = resumo["percentual_domicilios"].cumsum()
    return resumo[
        [
            "classe_abc",
            "quantidade_municipios",
            "domicilios_cnefe",
            "percentual_domicilios",
            "percentual_domicilios_acumulado",
            "menor_ordem",
            "maior_ordem",
        ]
    ]


def arredonda_saida(df_municipios: pd.DataFrame, df_resumo: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    municipios = df_municipios.copy()
    resumo = df_resumo.copy()

    for coluna in ["participacao_pct", "participacao_acumulada_pct", "domicilios_por_1k_hab"]:
        municipios[coluna] = municipios[coluna].round(4)

    for coluna in ["percentual_domicilios", "percentual_domicilios_acumulado"]:
        resumo[coluna] = resumo[coluna].round(4)

    return municipios, resumo


def gera_grafico_abc(
    df_municipios: pd.DataFrame,
    df_resumo: pd.DataFrame,
    limite_a: float,
    limite_b: float,
    caminho_saida: Path,
) -> None:
    graf = df_municipios.copy()
    total_municipios = len(graf)
    graf["municipios_acumulados_pct"] = 100 * graf["ordem_abc"] / total_municipios

    resumo_idx = df_resumo.set_index("classe_abc")
    ordem_a = int(resumo_idx.loc["A", "maior_ordem"]) if "A" in resumo_idx.index else 0
    ordem_b = int(resumo_idx.loc["B", "maior_ordem"]) if "B" in resumo_idx.index else ordem_a
    ordem_c = int(resumo_idx.loc["C", "maior_ordem"]) if "C" in resumo_idx.index else total_municipios
    pct_mun_a = 100 * ordem_a / total_municipios if total_municipios else 0
    pct_mun_b = 100 * ordem_b / total_municipios if total_municipios else 0
    pct_mun_c = 100 * ordem_c / total_municipios if total_municipios else 100

    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    width = 1100
    height = 760
    left = 110
    right = 70
    top = 90
    bottom = 100
    plot_width = width - left - right
    plot_height = height - top - bottom

    def x_map(valor: float) -> float:
        return left + (valor / 100.0) * plot_width

    def y_map(valor: float) -> float:
        return top + plot_height - (valor / 100.0) * plot_height

    pontos_todos = [
        (float(x), float(y))
        for x, y in zip(graf["municipios_acumulados_pct"], graf["participacao_acumulada_pct"])
    ]

    def monta_polyline(pontos_base: list[tuple[float, float]]) -> str:
        return " ".join(f"{x_map(x):.2f},{y_map(y):.2f}" for x, y in pontos_base)

    pontos_a = [(0.0, 0.0)] + [
        (x, y) for x, y in pontos_todos if x <= pct_mun_a + 1e-9
    ]
    pontos_b = [
        (x, y) for x, y in pontos_todos if pct_mun_a - 1e-9 <= x <= pct_mun_b + 1e-9
    ]
    pontos_c = [
        (x, y) for x, y in pontos_todos if pct_mun_b - 1e-9 <= x <= pct_mun_c + 1e-9
    ]

    if pontos_b and pontos_b[0][0] > pct_mun_a and len(pontos_a) >= 1:
        pontos_b = [pontos_a[-1]] + pontos_b
    if pontos_c and pontos_c[0][0] > pct_mun_b and len(pontos_b) >= 1:
        pontos_c = [pontos_b[-1]] + pontos_c

    linha_a = monta_polyline(pontos_a)
    linha_b = monta_polyline(pontos_b)
    linha_c = monta_polyline(pontos_c)
    linha_total = " ".join(
        f"{x_map(float(x)):.2f},{y_map(float(y)):.2f}"
        for x, y in pontos_todos
    )

    def linha_vertical(x_pct: float, cor: str) -> str:
        x = x_map(x_pct)
        return (
            f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_height}" '
            f'stroke="{cor}" stroke-width="1.5" stroke-dasharray="5,5" />'
        )

    def linha_horizontal(y_pct: float, cor: str) -> str:
        y = y_map(y_pct)
        return (
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" '
            f'stroke="{cor}" stroke-width="1.5" stroke-dasharray="5,5" />'
        )

    def rotulo_eixo_x(valor: int) -> str:
        x = x_map(valor)
        y = top + plot_height + 28
        return f'<text x="{x:.2f}" y="{y}" font-size="12" text-anchor="middle" fill="#334155">{valor}</text>'

    def rotulo_eixo_y(valor: int) -> str:
        x = left - 18
        y = y_map(valor) + 4
        return f'<text x="{x}" y="{y:.2f}" font-size="12" text-anchor="end" fill="#334155">{valor}</text>'

    ticks = range(0, 101, 10)
    grid_x = "\n".join(
        f'<line x1="{x_map(t):.2f}" y1="{top}" x2="{x_map(t):.2f}" y2="{top + plot_height}" stroke="#e2e8f0" stroke-width="1" />'
        for t in ticks
    )
    grid_y = "\n".join(
        f'<line x1="{left}" y1="{y_map(t):.2f}" x2="{left + plot_width}" y2="{y_map(t):.2f}" stroke="#e2e8f0" stroke-width="1" />'
        for t in ticks
    )
    labels_x = "\n".join(rotulo_eixo_x(t) for t in ticks)
    labels_y = "\n".join(rotulo_eixo_y(t) for t in ticks)

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="100%" height="100%" fill="#f8fafc" />
  <text x="{width / 2:.0f}" y="42" font-size="28" text-anchor="middle" fill="#0f172a" font-family="Arial, sans-serif">Curva ABC dos domicilios CNEFE por municipio</text>
  <text x="{width / 2:.0f}" y="68" font-size="14" text-anchor="middle" fill="#475569" font-family="Arial, sans-serif">Participacao acumulada dos municipios no total nacional de enderecos</text>

  <rect x="{left}" y="{top}" width="{plot_width}" height="{plot_height}" fill="white" stroke="#cbd5e1" stroke-width="1.2" />
  {grid_x}
  {grid_y}

  <rect x="{left}" y="{top}" width="{x_map(pct_mun_a) - left:.2f}" height="{plot_height}" fill="#dcfce7" opacity="0.45" />
  <rect x="{x_map(pct_mun_a):.2f}" y="{top}" width="{x_map(pct_mun_b) - x_map(pct_mun_a):.2f}" height="{plot_height}" fill="#fef3c7" opacity="0.45" />
  <rect x="{x_map(pct_mun_b):.2f}" y="{top}" width="{left + plot_width - x_map(pct_mun_b):.2f}" height="{plot_height}" fill="#fee2e2" opacity="0.45" />

  <line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top}" stroke="#94a3b8" stroke-width="1.8" stroke-dasharray="7,6" />
  <polyline fill="none" stroke="#14532d" stroke-width="5" points="{linha_a}" />
  <polyline fill="none" stroke="#b45309" stroke-width="5" points="{linha_b}" />
  <polyline fill="none" stroke="#b91c1c" stroke-width="5" points="{linha_c}" />
  <polyline fill="none" stroke="#0f172a" stroke-width="1.2" opacity="0.22" points="{linha_total}" />

  {linha_horizontal(limite_a, "#b45309")}
  {linha_horizontal(limite_b, "#7c2d12")}
  {linha_vertical(pct_mun_a, "#b45309")}
  {linha_vertical(pct_mun_b, "#7c2d12")}

  <circle cx="{x_map(pct_mun_a):.2f}" cy="{y_map(limite_a):.2f}" r="5" fill="#b45309" />
  <circle cx="{x_map(pct_mun_b):.2f}" cy="{y_map(limite_b):.2f}" r="5" fill="#7c2d12" />
  <circle cx="{x_map(pct_mun_c):.2f}" cy="{y_map(100):.2f}" r="5" fill="#b91c1c" />

  <text x="{min(width - 180, x_map(pct_mun_a) + 12):.2f}" y="{max(top + 18, y_map(limite_a) - 12):.2f}" font-size="13" fill="#92400e" font-family="Arial, sans-serif">A: {ordem_a} municipios</text>
  <text x="{min(width - 180, x_map(pct_mun_b) + 12):.2f}" y="{max(top + 18, y_map(limite_b) - 12):.2f}" font-size="13" fill="#7c2d12" font-family="Arial, sans-serif">B: ate {ordem_b} municipios</text>
  <text x="{width - 240}" y="{top + 28}" font-size="13" fill="#991b1b" font-family="Arial, sans-serif">C: {total_municipios - ordem_b} municipios finais</text>

  <text x="{left + 18}" y="{top + 28}" font-size="18" fill="#166534" font-family="Arial, sans-serif">A</text>
  <text x="{x_map(pct_mun_a) + 18:.2f}" y="{top + 28}" font-size="18" fill="#92400e" font-family="Arial, sans-serif">B</text>
  <text x="{x_map(pct_mun_b) + 18:.2f}" y="{top + 28}" font-size="18" fill="#991b1b" font-family="Arial, sans-serif">C</text>

  <line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#334155" stroke-width="1.5" />
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#334155" stroke-width="1.5" />
  {labels_x}
  {labels_y}

  <text x="{left + plot_width / 2:.2f}" y="{height - 35}" font-size="16" text-anchor="middle" fill="#0f172a" font-family="Arial, sans-serif">Percentual acumulado de municipios</text>
  <text x="28" y="{top + plot_height / 2:.2f}" font-size="16" text-anchor="middle" fill="#0f172a" font-family="Arial, sans-serif" transform="rotate(-90 28 {top + plot_height / 2:.2f})">Percentual acumulado de domicilios CNEFE</text>

  <rect x="{width - 285}" y="{height - 148}" width="230" height="108" rx="8" fill="white" stroke="#cbd5e1" />
  <line x1="{width - 268}" y1="{height - 122}" x2="{width - 228}" y2="{height - 122}" stroke="#14532d" stroke-width="4" />
  <text x="{width - 218}" y="{height - 117}" font-size="13" fill="#0f172a" font-family="Arial, sans-serif">Trecho A</text>
  <line x1="{width - 268}" y1="{height - 96}" x2="{width - 228}" y2="{height - 96}" stroke="#b45309" stroke-width="4" />
  <text x="{width - 218}" y="{height - 91}" font-size="13" fill="#0f172a" font-family="Arial, sans-serif">Trecho B</text>
  <line x1="{width - 268}" y1="{height - 70}" x2="{width - 228}" y2="{height - 70}" stroke="#b91c1c" stroke-width="4" />
  <text x="{width - 218}" y="{height - 65}" font-size="13" fill="#0f172a" font-family="Arial, sans-serif">Trecho C</text>
  <line x1="{width - 268}" y1="{height - 44}" x2="{width - 228}" y2="{height - 44}" stroke="#94a3b8" stroke-width="2" stroke-dasharray="7,6" />
  <text x="{width - 218}" y="{height - 39}" font-size="13" fill="#0f172a" font-family="Arial, sans-serif">Linha de igualdade</text>
</svg>
"""
    caminho_saida.write_text(svg, encoding="utf-8")


def exporta_gpkg_municipios(gdf: gpd.GeoDataFrame, caminho_saida: Path) -> None:
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(caminho_saida, layer="municipios_abc_cnefe", driver="GPKG")


def main() -> None:
    args = parse_args()
    valida_limites(args.limite_a, args.limite_b)

    base = monta_base(Path(args.cnefe), Path(args.shapefile))
    municipios = classifica_abc(base, args.limite_a, args.limite_b)
    resumo = monta_resumo(municipios)
    municipios, resumo = arredonda_saida(municipios, resumo)
    gdf_municipios = monta_gpkg_municipios(Path(args.shapefile), municipios)

    output_municipios = Path(args.output_municipios)
    output_resumo = Path(args.output_resumo)
    output_plot = Path(args.output_plot)
    output_gpkg = Path(args.output_gpkg)
    output_municipios.parent.mkdir(parents=True, exist_ok=True)
    output_resumo.parent.mkdir(parents=True, exist_ok=True)

    municipios.to_csv(output_municipios, index=False)
    resumo.to_csv(output_resumo, index=False)
    gera_grafico_abc(municipios, resumo, args.limite_a, args.limite_b, output_plot)
    exporta_gpkg_municipios(gdf_municipios, output_gpkg)

    print(f"CSV municipal gerado: {output_municipios}")
    print(f"CSV resumo gerado: {output_resumo}")
    print(f"Grafico gerado: {output_plot}")
    print(f"GeoPackage gerado: {output_gpkg}")
    print(resumo.to_string(index=False))


if __name__ == "__main__":
    main()
