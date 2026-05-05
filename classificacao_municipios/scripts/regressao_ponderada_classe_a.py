#!/usr/bin/env python3
"""
Roda uma regressao linear ponderada apenas nos municipios da classe A da
classificacao ABC da CNEFE.

O objetivo e modelar o volume de domicilios da CNEFE nos municipios mais
concentradores, permitindo que municipios maiores tenham mais influencia
no ajuste via pesos.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
ABC_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "classificacao_abc_cnefe_municipios.csv"
MERGE_PADRAO = ROOT / "prata" / "processamento" / "merge_v26.csv"
OFERTAS_PADRAO = ROOT / "prata" / "cnefe domiclios ofertas-" / "ofertas_merge_cnefe_municipal.csv"
SAIDA_BASE_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "regressao_ponderada_classe_a_base.csv"
SAIDA_COEF_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "regressao_ponderada_classe_a_coeficientes.csv"
SAIDA_RESUMO_PADRAO = ROOT / "classificacao_municipios" / "processamento" / "regressao_ponderada_classe_a_resumo.txt"

PREDITORES_PADRAO = [
    "log_pop_total",
    "log_empresas_total",
    "log_estab_total",
    "indice_conectividade",
    "log_area_urb_densa_km2",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Roda regressao ponderada na classe A da classificacao ABC da CNEFE."
    )
    parser.add_argument("--abc", default=str(ABC_PADRAO), help="CSV da classificacao ABC municipal.")
    parser.add_argument("--merge", default=str(MERGE_PADRAO), help="CSV municipal consolidado merge_v26.")
    parser.add_argument("--ofertas", default=str(OFERTAS_PADRAO), help="CSV municipal de ofertas.")
    parser.add_argument(
        "--peso",
        default="domicilios",
        choices=["domicilios", "pop_total", "total_geral", "uniforme"],
        help="Variavel de peso da regressao.",
    )
    parser.add_argument(
        "--output-base",
        default=str(SAIDA_BASE_PADRAO),
        help="CSV detalhado da base usada na regressao.",
    )
    parser.add_argument(
        "--output-coeficientes",
        default=str(SAIDA_COEF_PADRAO),
        help="CSV de coeficientes e estatisticas do modelo.",
    )
    parser.add_argument(
        "--output-resumo",
        default=str(SAIDA_RESUMO_PADRAO),
        help="TXT com resumo executivo da regressao.",
    )
    return parser.parse_args()


def carrega_abc(caminho: Path) -> pd.DataFrame:
    colunas = ["codigo_mun", "NOME", "classe_abc", "domicilios"]
    df = pd.read_csv(caminho, usecols=colunas).copy()
    df["codigo_mun"] = pd.to_numeric(df["codigo_mun"], errors="coerce").astype("Int64")
    df["domicilios"] = pd.to_numeric(df["domicilios"], errors="coerce")
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

    df["log_domicilios"] = np.log1p(df["domicilios"])
    df["log_pop_total"] = np.log1p(df["pop_total"].clip(lower=0))
    df["log_empresas_total"] = np.log1p(df["empresas_total"].clip(lower=0))
    df["log_estab_total"] = np.log1p(df["estab_total"].clip(lower=0))
    df["log_area_urb_densa_km2"] = np.log1p(df["area_urb_densa_km2"].clip(lower=0))
    df["log_total_geral"] = np.log1p(df["total_geral"].clip(lower=0))
    return df


def seleciona_pesos(df: pd.DataFrame, peso: str) -> pd.Series:
    if peso == "uniforme":
        pesos = pd.Series(1.0, index=df.index)
    else:
        pesos = pd.to_numeric(df[peso], errors="coerce")

    pesos = pesos.replace([np.inf, -np.inf], np.nan)
    pesos = pesos.fillna(0)
    pesos = pesos.where(pesos > 0, 0)
    return pesos


def ajusta_wls(df: pd.DataFrame, y_col: str, x_cols: list[str], peso_col: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float, float]:
    trabalho = df[[y_col, peso_col] + x_cols].replace([np.inf, -np.inf], np.nan).dropna().copy()
    trabalho = trabalho.loc[trabalho[peso_col] > 0].copy()
    if trabalho.empty:
        raise ValueError("Nao ha observacoes validas para a regressao ponderada.")

    y = trabalho[y_col].to_numpy(dtype=float)
    X = trabalho[x_cols].to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(X)), X])
    pesos = trabalho[peso_col].to_numpy(dtype=float)

    raiz_pesos = np.sqrt(pesos)
    Xw = X * raiz_pesos[:, None]
    yw = y * raiz_pesos

    beta, _, _, _ = np.linalg.lstsq(Xw, yw, rcond=None)
    y_hat = X @ beta
    resid = y - y_hat

    n = len(y)
    p = X.shape[1]
    if n <= p:
        raise ValueError("Ha poucas observacoes validas para estimar o modelo com os preditores atuais.")

    sse_w = float(np.sum(pesos * resid**2))
    y_bar_w = float(np.sum(pesos * y) / np.sum(pesos))
    sst_w = float(np.sum(pesos * (y - y_bar_w) ** 2))
    r2_w = 1.0 - sse_w / sst_w if sst_w > 0 else np.nan
    sigma2 = sse_w / (n - p)

    xtwx_inv = np.linalg.inv(X.T @ (X * pesos[:, None]))
    cov_beta = sigma2 * xtwx_inv
    se = np.sqrt(np.diag(cov_beta))
    t_stat = beta / se

    trabalho["pred_log_domicilios"] = y_hat
    trabalho["residuo_log"] = resid
    trabalho["pred_domicilios"] = np.expm1(y_hat)
    trabalho["residuo_domicilios"] = trabalho[y_col] - trabalho["pred_domicilios"]
    return beta, se, t_stat, r2_w, sigma2, float(np.sum(pesos)), trabalho


def monta_tabela_coeficientes(beta: np.ndarray, se: np.ndarray, t_stat: np.ndarray, x_cols: list[str]) -> pd.DataFrame:
    termos = ["intercepto"] + x_cols
    return pd.DataFrame(
        {
            "termo": termos,
            "coeficiente": beta,
            "erro_padrao": se,
            "estatistica_t": t_stat,
        }
    )


def salva_resumo(
    caminho: Path,
    peso: str,
    r2_w: float,
    sigma2: float,
    soma_pesos: float,
    n: int,
    tabela: pd.DataFrame,
) -> None:
    top = tabela.loc[tabela["termo"] != "intercepto"].copy()
    top["abs_t"] = top["estatistica_t"].abs()
    top = top.sort_values("abs_t", ascending=False).head(3)

    linhas = [
        "Regressao ponderada da classe A da classificacao ABC da CNEFE",
        "",
        f"Municipios usados no ajuste: {n}",
        f"Variavel resposta: log_domicilios",
        f"Peso do modelo: {peso}",
        f"R2 ponderado: {r2_w:.4f}",
        f"Variancia residual ponderada: {sigma2:.6f}",
        f"Soma dos pesos: {soma_pesos:.2f}",
        "",
        "Preditores com maior sinal no ajuste:",
    ]
    for _, linha in top.iterrows():
        linhas.append(
            f"- {linha['termo']}: coef={linha['coeficiente']:.4f}, t={linha['estatistica_t']:.2f}"
        )

    caminho.write_text("\n".join(linhas), encoding="utf-8")


def main() -> None:
    args = parse_args()
    abc = carrega_abc(Path(args.abc))
    merge = carrega_merge(Path(args.merge))
    ofertas = carrega_ofertas(Path(args.ofertas))
    base = prepara_base(abc, merge, ofertas)
    base["peso_modelo"] = seleciona_pesos(base, args.peso)

    beta, se, t_stat, r2_w, sigma2, soma_pesos, trabalho = ajusta_wls(
        base,
        y_col="log_domicilios",
        x_cols=PREDITORES_PADRAO,
        peso_col="peso_modelo",
    )

    saida_base = base.merge(
        trabalho[
            [
                "pred_log_domicilios",
                "residuo_log",
                "pred_domicilios",
                "residuo_domicilios",
            ]
        ],
        left_index=True,
        right_index=True,
        how="left",
    )
    tabela = monta_tabela_coeficientes(beta, se, t_stat, PREDITORES_PADRAO)

    output_base = Path(args.output_base)
    output_coef = Path(args.output_coeficientes)
    output_resumo = Path(args.output_resumo)
    output_base.parent.mkdir(parents=True, exist_ok=True)
    output_coef.parent.mkdir(parents=True, exist_ok=True)
    output_resumo.parent.mkdir(parents=True, exist_ok=True)

    saida_base.to_csv(output_base, index=False)
    tabela.to_csv(output_coef, index=False)
    salva_resumo(output_resumo, args.peso, r2_w, sigma2, soma_pesos, len(trabalho), tabela)

    print(f"Base detalhada gerada: {output_base}")
    print(f"Coeficientes gerados: {output_coef}")
    print(f"Resumo gerado: {output_resumo}")
    print(tabela.to_string(index=False))
    print()
    print(f"Municipios da classe A usados no modelo: {len(trabalho)}")
    print(f"R2 ponderado: {r2_w:.4f}")


if __name__ == "__main__":
    main()
