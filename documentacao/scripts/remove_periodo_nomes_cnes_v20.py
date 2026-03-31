#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Remove o sufixo de periodo das colunas de estabelecimentos na v20 e gera a v21.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v20.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v21.csv"

RENOMEAR_COLUNAS = {
    "posto_saude_2025_12": "posto_saude",
    "centro_saude_ubs_2025_12": "centro_saude_ubs",
    "policlinica_2025_12": "policlinica",
    "hospital_geral_2025_12": "hospital_geral",
    "hospital_esp_2025_12": "hospital_esp",
    "unidade_mista_2025_12": "unidade_mista",
    "pronto_socorro_geral_2025_12": "pronto_socorro_geral",
    "pronto_socorro_esp_2025_12": "pronto_socorro_esp",
    "consultorio_isolado_2025_12": "consultorio_isolado",
    "unidade_movel_fluvial_2025_12": "unidade_movel_fluvial",
    "clinica_centro_esp_2025_12": "clinica_centro_esp",
    "sadt_isolado_2025_12": "sadt_isolado",
    "unidade_movel_terrestre_2025_12": "unidade_movel_terrestre",
    "unidade_movel_pre_hosp_urg_2025_12": "unidade_movel_pre_hosp_urg",
    "farmacia_2025_12": "farmacia",
    "vigilancia_saude_2025_12": "vigilancia_saude",
    "coop_cessao_saude_2025_12": "coop_cessao_saude",
    "centro_parto_normal_isol_2025_12": "centro_parto_normal_isol",
    "hospital_dia_isol_2025_12": "hospital_dia_isol",
    "lacen_2025_12": "lacen",
    "central_gestao_saude_2025_12": "central_gestao_saude",
    "hemoterapia_hematologia_2025_12": "hemoterapia_hematologia",
    "caps_2025_12": "caps",
    "apoio_saude_familia_2025_12": "apoio_saude_familia",
    "saude_indigena_2025_12": "saude_indigena",
    "pronto_atendimento_2025_12": "pronto_atendimento",
    "polo_academia_saude_2025_12": "polo_academia_saude",
    "telessaude_2025_12": "telessaude",
    "regulacao_medica_urg_2025_12": "regulacao_medica_urg",
    "atencao_domiciliar_homecare_2025_12": "atencao_domiciliar_homecare",
    "atencao_regime_residencial_2025_12": "atencao_regime_residencial",
    "oficina_ortopedica_2025_12": "oficina_ortopedica",
    "laboratorio_saude_pub_2025_12": "laboratorio_saude_pub",
    "regulacao_acesso_2025_12": "regulacao_acesso",
    "notif_capt_distrib_orgaos_2025_12": "notif_capt_distrib_orgaos",
    "prev_agravos_prom_saude_2025_12": "prev_agravos_prom_saude",
    "central_abastecimento_2025_12": "central_abastecimento",
    "centro_imunizacao_2025_12": "centro_imunizacao",
    "estab_total_2025_12": "estab_total",
}


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in RENOMEAR_COLUNAS if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas do bloco CNES ausentes na v20: {faltantes}")

    resultado = df.rename(columns=RENOMEAR_COLUNAS)
    duplicadas = resultado.columns[resultado.columns.duplicated()].tolist()
    if duplicadas:
        raise ValueError(f"Renomeacao gerou colunas duplicadas: {duplicadas}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Colunas renomeadas: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
