#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Encurta os nomes das colunas de estabelecimentos do CNES na v18 e gera a v19.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v18.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v19.csv"

RENOMEAR_COLUNAS = {
    "cnes_estab_posto_de_saude_2025_12": "cnes_posto_saude_2025_12",
    "cnes_estab_centro_de_saude_unidade_basica_2025_12": "cnes_centro_saude_ubs_2025_12",
    "cnes_estab_policlinica_2025_12": "cnes_policlinica_2025_12",
    "cnes_estab_hospital_geral_2025_12": "cnes_hosp_geral_2025_12",
    "cnes_estab_hospital_especializado_2025_12": "cnes_hosp_esp_2025_12",
    "cnes_estab_unidade_mista_2025_12": "cnes_unid_mista_2025_12",
    "cnes_estab_pronto_socorro_geral_2025_12": "cnes_ps_geral_2025_12",
    "cnes_estab_pronto_socorro_especializado_2025_12": "cnes_ps_esp_2025_12",
    "cnes_estab_consultorio_isolado_2025_12": "cnes_cons_isol_2025_12",
    "cnes_estab_unidade_movel_fluvial_2025_12": "cnes_unid_mov_fluv_2025_12",
    "cnes_estab_clinica_centro_de_especialidade_2025_12": "cnes_clin_centro_esp_2025_12",
    "cnes_estab_unidade_de_apoio_diagnose_e_terapia_sadt_isolado_2025_12": "cnes_sadt_isol_2025_12",
    "cnes_estab_unidade_movel_terrestre_2025_12": "cnes_unid_mov_terr_2025_12",
    "cnes_estab_unidade_movel_de_nivel_pre_hospitalar_na_area_de_urgencia_2025_12": "cnes_unid_mov_pre_hosp_urg_2025_12",
    "cnes_estab_farmacia_2025_12": "cnes_farmacia_2025_12",
    "cnes_estab_unidade_de_vigilancia_em_saude_2025_12": "cnes_vig_saude_2025_12",
    "cnes_estab_cooperativa_ou_empresa_de_cessao_de_trabalhadores_na_saude_2025_12": "cnes_coop_cessao_saude_2025_12",
    "cnes_estab_centro_de_parto_normal_isolado_2025_12": "cnes_cpn_isol_2025_12",
    "cnes_estab_hospital_dia_isolado_2025_12": "cnes_hosp_dia_isol_2025_12",
    "cnes_estab_laboratorio_central_de_saude_publica_lacen_2025_12": "cnes_lacen_2025_12",
    "cnes_estab_central_de_gestao_em_saude_2025_12": "cnes_gestao_saude_2025_12",
    "cnes_estab_centro_de_atencao_hemoterapia_e_ou_hematologica_2025_12": "cnes_hemoterapia_hematol_2025_12",
    "cnes_estab_centro_de_atencao_psicossocial_2025_12": "cnes_caps_2025_12",
    "cnes_estab_centro_de_apoio_a_saude_da_familia_2025_12": "cnes_apoio_saude_fam_2025_12",
    "cnes_estab_unidade_de_atencao_a_saude_indigena_2025_12": "cnes_saude_indigena_2025_12",
    "cnes_estab_pronto_atendimento_2025_12": "cnes_pronto_atend_2025_12",
    "cnes_estab_polo_academia_da_saude_2025_12": "cnes_polo_acad_saude_2025_12",
    "cnes_estab_telessaude_2025_12": "cnes_telessaude_2025_12",
    "cnes_estab_central_de_regulacao_medica_das_urgencias_2025_12": "cnes_reg_med_urg_2025_12",
    "cnes_estab_servico_de_atencao_domiciliar_isolado_home_care_2025_12": "cnes_atend_dom_homecare_2025_12",
    "cnes_estab_unidade_de_atencao_em_regime_residencial_2025_12": "cnes_atend_reg_resid_2025_12",
    "cnes_estab_oficina_ortopedica_2025_12": "cnes_ofic_ortopedica_2025_12",
    "cnes_estab_laboratorio_de_saude_publica_2025_12": "cnes_lab_saude_pub_2025_12",
    "cnes_estab_central_de_regulacao_do_acesso_2025_12": "cnes_reg_acesso_2025_12",
    "cnes_estab_central_de_notificacao_captacao_e_distrib_de_orgaos_estadual_2025_12": "cnes_notif_capt_distrib_orgaos_2025_12",
    "cnes_estab_polo_de_prevencao_de_doencas_e_agravos_e_promocao_da_saude_2025_12": "cnes_prev_agravos_prom_saude_2025_12",
    "cnes_estab_central_de_abastecimento_2025_12": "cnes_abastecimento_2025_12",
    "cnes_estab_centro_de_imunizacao_2025_12": "cnes_imunizacao_2025_12",
    "cnes_estab_total_2025_12": "cnes_total_2025_12",
}


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in RENOMEAR_COLUNAS if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas do CNES ausentes na v18: {faltantes}")

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
