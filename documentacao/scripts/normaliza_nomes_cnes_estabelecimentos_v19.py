#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Remove o prefixo cnes das colunas de estabelecimentos na v19 e gera a v20.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v19.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v20.csv"

RENOMEAR_COLUNAS = {
    "cnes_posto_saude_2025_12": "posto_saude_2025_12",
    "cnes_centro_saude_ubs_2025_12": "centro_saude_ubs_2025_12",
    "cnes_policlinica_2025_12": "policlinica_2025_12",
    "cnes_hosp_geral_2025_12": "hospital_geral_2025_12",
    "cnes_hosp_esp_2025_12": "hospital_esp_2025_12",
    "cnes_unid_mista_2025_12": "unidade_mista_2025_12",
    "cnes_ps_geral_2025_12": "pronto_socorro_geral_2025_12",
    "cnes_ps_esp_2025_12": "pronto_socorro_esp_2025_12",
    "cnes_cons_isol_2025_12": "consultorio_isolado_2025_12",
    "cnes_unid_mov_fluv_2025_12": "unidade_movel_fluvial_2025_12",
    "cnes_clin_centro_esp_2025_12": "clinica_centro_esp_2025_12",
    "cnes_sadt_isol_2025_12": "sadt_isolado_2025_12",
    "cnes_unid_mov_terr_2025_12": "unidade_movel_terrestre_2025_12",
    "cnes_unid_mov_pre_hosp_urg_2025_12": "unidade_movel_pre_hosp_urg_2025_12",
    "cnes_farmacia_2025_12": "farmacia_2025_12",
    "cnes_vig_saude_2025_12": "vigilancia_saude_2025_12",
    "cnes_coop_cessao_saude_2025_12": "coop_cessao_saude_2025_12",
    "cnes_cpn_isol_2025_12": "centro_parto_normal_isol_2025_12",
    "cnes_hosp_dia_isol_2025_12": "hospital_dia_isol_2025_12",
    "cnes_lacen_2025_12": "lacen_2025_12",
    "cnes_gestao_saude_2025_12": "central_gestao_saude_2025_12",
    "cnes_hemoterapia_hematol_2025_12": "hemoterapia_hematologia_2025_12",
    "cnes_caps_2025_12": "caps_2025_12",
    "cnes_apoio_saude_fam_2025_12": "apoio_saude_familia_2025_12",
    "cnes_saude_indigena_2025_12": "saude_indigena_2025_12",
    "cnes_pronto_atend_2025_12": "pronto_atendimento_2025_12",
    "cnes_polo_acad_saude_2025_12": "polo_academia_saude_2025_12",
    "cnes_telessaude_2025_12": "telessaude_2025_12",
    "cnes_reg_med_urg_2025_12": "regulacao_medica_urg_2025_12",
    "cnes_atend_dom_homecare_2025_12": "atencao_domiciliar_homecare_2025_12",
    "cnes_atend_reg_resid_2025_12": "atencao_regime_residencial_2025_12",
    "cnes_ofic_ortopedica_2025_12": "oficina_ortopedica_2025_12",
    "cnes_lab_saude_pub_2025_12": "laboratorio_saude_pub_2025_12",
    "cnes_reg_acesso_2025_12": "regulacao_acesso_2025_12",
    "cnes_notif_capt_distrib_orgaos_2025_12": "notif_capt_distrib_orgaos_2025_12",
    "cnes_prev_agravos_prom_saude_2025_12": "prev_agravos_prom_saude_2025_12",
    "cnes_abastecimento_2025_12": "central_abastecimento_2025_12",
    "cnes_imunizacao_2025_12": "centro_imunizacao_2025_12",
    "cnes_total_2025_12": "estab_total_2025_12",
}


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in RENOMEAR_COLUNAS if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas curtas do CNES ausentes na v19: {faltantes}")

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
