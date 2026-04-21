#!/usr/bin/env python3

"""
Gera uma amostra inicial de municipios para calibracao manual da
classificacao fuzzy por regiao intermediaria.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
INPUT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_municipios_fuzzy_rgint.csv"
)
OUTPUT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "referencias"
    / "amostra_calibracao_inicial.csv"
)

MUNICIPIOS_PRIORITARIOS = [
    "São Paulo (SP)",
    "Rio de Janeiro (RJ)",
    "Brasília (DF)",
    "Campinas (SP)",
    "Belo Horizonte (MG)",
    "Curitiba (PR)",
    "Fortaleza (CE)",
    "Recife (PE)",
    "Salvador (BA)",
    "Manaus (AM)",
    "Belém (PA)",
    "Goiânia (GO)",
    "Porto Alegre (RS)",
    "São Luís (MA)",
    "Ribeirão Preto (SP)",
    "Uberlândia (MG)",
    "Londrina (PR)",
    "Maringá (PR)",
    "Cascavel (PR)",
    "Chapecó (SC)",
    "Caxias do Sul (RS)",
    "Joinville (SC)",
    "Juazeiro do Norte (CE)",
    "Caruaru (PE)",
    "Petrolina (PE)",
    "Feira de Santana (BA)",
    "Vitória da Conquista (BA)",
    "Imperatriz (MA)",
    "Marabá (PA)",
    "Santarém (PA)",
    "Altamira (PA)",
    "Palmas (TO)",
    "Dourados (MS)",
    "Breves (PA)",
    "Tefé (AM)",
    "Lábrea (AM)",
    "Oiapoque (AP)",
    "São Raimundo Nonato (PI)",
    "Patos (PB)",
    "Barueri (SP)",
]

CLASSES_ORDENADAS = [
    "muito_alto",
    "alto",
    "medio",
    "baixo",
    "muito_baixo",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera uma amostra inicial para calibracao manual da logica fuzzy."
    )
    parser.add_argument(
        "--input",
        default=str(INPUT_PADRAO),
        help="CSV de classificacao municipal ja gerado.",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_PADRAO),
        help="CSV de saida da amostra de calibracao.",
    )
    parser.add_argument(
        "--alvo-por-classe",
        type=int,
        default=8,
        help="Quantidade total desejada por classe na amostra final.",
    )
    return parser.parse_args()


def seleciona_prioritarios(df: pd.DataFrame) -> pd.DataFrame:
    prioritarios = df[df["municipio"].isin(MUNICIPIOS_PRIORITARIOS)].copy()
    prioridade = {municipio: indice for indice, municipio in enumerate(MUNICIPIOS_PRIORITARIOS)}
    prioritarios["ordem_prioridade"] = prioritarios["municipio"].map(prioridade)
    prioritarios["origem_amostra"] = "prioritario"
    return prioritarios.sort_values("ordem_prioridade")


def seleciona_extras(
    df: pd.DataFrame,
    prioritarios: pd.DataFrame,
    alvo_por_classe: int,
) -> pd.DataFrame:
    usados = set(prioritarios["municipio"].tolist())
    selecionados: list[pd.DataFrame] = []

    for classe in CLASSES_ORDENADAS:
        ja_tem = int((prioritarios["classificacao_fuzzy"] == classe).sum())
        faltam = max(alvo_por_classe - ja_tem, 0)
        if faltam == 0:
            continue

        candidatos = (
            df[
                (df["classificacao_fuzzy"] == classe)
                & (~df["municipio"].isin(usados))
            ]
            .sort_values(
                ["confianca_classificacao", "centralidade_economica", "oferta_servicos"],
                ascending=[False, False, False],
            )
            .drop_duplicates(subset=["rgint"], keep="first")
            .head(faltam)
            .copy()
        )
        candidatos["origem_amostra"] = "extra_automatico"
        selecionados.append(candidatos)
        usados.update(candidatos["municipio"].tolist())

    if not selecionados:
        return df.iloc[0:0].copy()
    return pd.concat(selecionados, ignore_index=True)


def prepara_saida(df: pd.DataFrame) -> pd.DataFrame:
    saida = df.copy()
    saida["classe_referencia_esperada"] = ""
    saida["avaliacao_modelo"] = ""
    saida["ajuste_sugerido"] = ""
    saida["observacoes_calibracao"] = ""
    return saida[
        [
            "origem_amostra",
            "cod_mun",
            "municipio",
            "uf",
            "rgint",
            "nome_rgint",
            "classificacao_fuzzy",
            "confianca_classificacao",
            "centralidade_economica",
            "infraestrutura_urbana",
            "conectividade_digital",
            "oferta_servicos",
            "vulnerabilidade",
            "classe_referencia_esperada",
            "avaliacao_modelo",
            "ajuste_sugerido",
            "observacoes_calibracao",
        ]
    ]


def main() -> None:
    args = parse_args()
    input_file = Path(args.input).resolve()
    output_file = Path(args.output).resolve()

    df = pd.read_csv(input_file)
    prioritarios = seleciona_prioritarios(df)
    prioritarios = (
        prioritarios.groupby("classificacao_fuzzy", group_keys=False)
        .head(args.alvo_por_classe)
        .copy()
    )
    extras = seleciona_extras(df, prioritarios, args.alvo_por_classe)

    amostra = pd.concat([prioritarios, extras], ignore_index=True)
    amostra = amostra.drop_duplicates(subset=["cod_mun"], keep="first")
    amostra = prepara_saida(amostra)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    amostra.to_csv(output_file, index=False)

    print(f"Arquivo gerado: {output_file}")
    print("Distribuicao da amostra por classe atual:")
    print(amostra["classificacao_fuzzy"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
