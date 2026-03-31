#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Agrega os valores mensais de demissoes do Ipea em um total anual por municipio.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "bronze"
OUTPUT_DIR = BASE_DIR / "prata" / "pre_merge"
PROCESSAMENTO_DIR = BASE_DIR / "prata" / "processamento"
INPUT_PATTERN = "ipeadata*.csv"


def descobrir_arquivo_ipea() -> Path:
    arquivos = sorted(BRONZE_DIR.glob(INPUT_PATTERN), key=lambda p: p.stat().st_mtime)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo {INPUT_PATTERN} encontrado em {BRONZE_DIR}")
    return arquivos[-1]


def descobrir_colunas_mensais(colunas: list[str]) -> tuple[str, list[str]]:
    padrao = re.compile(r"^(\d{4})\.(\d{2})$")
    anos_encontrados: dict[str, list[str]] = {}

    for coluna in colunas:
        match = padrao.fullmatch(str(coluna))
        if not match:
            continue
        ano = match.group(1)
        anos_encontrados.setdefault(ano, []).append(coluna)

    if not anos_encontrados:
        raise ValueError("Nenhuma coluna mensal no formato AAAA.MM foi encontrada no arquivo do Ipea.")

    if len(anos_encontrados) > 1:
        raise ValueError(f"Foram encontrados multiplos anos no arquivo do Ipea: {sorted(anos_encontrados)}")

    ano, colunas_mensais = next(iter(anos_encontrados.items()))
    colunas_mensais = sorted(colunas_mensais)

    if len(colunas_mensais) != 12:
        raise ValueError(
            f"Esperadas 12 colunas mensais para o ano {ano}, mas foram encontradas {len(colunas_mensais)}."
        )

    return ano, colunas_mensais


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def descobrir_ultima_base_processada() -> Path | None:
    candidatos: list[tuple[int, Path]] = []
    for arquivo in PROCESSAMENTO_DIR.glob("merge_v*.csv"):
        match = re.fullmatch(r"merge_v(\d+)\.csv", arquivo.name)
        if match:
            candidatos.append((int(match.group(1)), arquivo))

    if not candidatos:
        return None

    return max(candidatos, key=lambda item: item[0])[1]


def carregar_ipea() -> tuple[pd.DataFrame, str, Path]:
    input_file = descobrir_arquivo_ipea()
    df = pd.read_csv(input_file, skiprows=1)

    colunas_excluir = [col for col in df.columns if str(col).startswith("Unnamed:")]
    if colunas_excluir:
        df = df.drop(columns=colunas_excluir)

    colunas_necessarias = ["Sigla", "Código", "Município"]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo do Ipea: {faltantes}")

    ano, colunas_mensais = descobrir_colunas_mensais(list(df.columns))

    base = df[colunas_necessarias + colunas_mensais].copy()
    base["Código"] = pd.to_numeric(base["Código"], errors="coerce")
    base = base[base["Código"].notna()].copy()

    for coluna in colunas_mensais:
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce").fillna(0)

    base["cod_mun"] = base["Código"].map(normalizar_codigo)
    base["demissoes_anuais"] = base[colunas_mensais].sum(axis=1)

    resultado = (
        base.groupby(["cod_mun", "Sigla", "Município"], as_index=False)["demissoes_anuais"]
        .sum()
        .copy()
    )
    resultado = resultado.rename(
        columns={
            "Sigla": "sigla_uf",
            "Município": "municipio",
            "demissoes_anuais": f"demissoes_{ano}",
        }
    )

    return resultado, ano, input_file


def filtrar_para_base_corrente(df: pd.DataFrame) -> tuple[pd.DataFrame, int, Path | None]:
    base_atual = descobrir_ultima_base_processada()
    if base_atual is None:
        return df, 0, None

    base_df = pd.read_csv(base_atual, usecols=["cod_mun"], dtype={"cod_mun": "string"})
    codigos_validos = set(base_df["cod_mun"].dropna().astype(str))

    antes = len(df)
    filtrado = df[df["cod_mun"].astype(str).isin(codigos_validos)].copy()
    removidos = antes - len(filtrado)
    return filtrado, removidos, base_atual


def main() -> int:
    resultado, ano, input_file = carregar_ipea()
    resultado, removidos, base_atual = filtrar_para_base_corrente(resultado)
    output_file = OUTPUT_DIR / f"ipea_demissoes_municipais_{ano}.csv"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(output_file, index=False, encoding="utf-8")

    print(f"Arquivo de origem: {input_file}")
    if base_atual is not None:
        print(f"Base de referencia para filtro municipal: {base_atual}")
        print(f"Codigos removidos por nao pertencerem a base corrente: {removidos}")
    print(f"Arquivo gerado: {output_file}")
    print(f"Ano consolidado: {ano}")
    print(f"Municipios agregados: {len(resultado)}")
    print(f"Coluna anual gerada: demissoes_{ano}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
