#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera um dicionario de dados em CSV para a versao mais recente da base merge_v*.csv.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
PROCESSAMENTO_DIR = BASE_DIR / "prata" / "processamento"

TIPO_POR_VARIAVEL = {
    "cod_mun": "identificador",
    "municipio": "categorica",
    "pib_total": "numerica_absoluta",
    "impostos_sub": "numerica_absoluta",
    "pib_agro": "numerica_absoluta",
    "pib_industria": "numerica_absoluta",
    "pib_servicos": "numerica_absoluta",
    "pib_adm": "numerica_absoluta",
    "pop_total": "numerica_absoluta",
    "dom_total": "numerica_absoluta",
    "empresas_total": "numerica_absoluta",
    "fundeb_vaaf": "numerica_absoluta",
    "fundeb_vaar": "numerica_absoluta",
    "fundeb_vaat": "numerica_absoluta",
    "fundeb_fpe": "numerica_absoluta",
    "fundeb_fpm": "numerica_absoluta",
    "fundeb_fti": "numerica_absoluta",
    "fundeb_icms": "numerica_absoluta",
    "fundeb_ipi_exp": "numerica_absoluta",
    "fundeb_ipva": "numerica_absoluta",
    "fundeb_itcmd": "numerica_absoluta",
    "fundeb_itr": "numerica_absoluta",
    "esgoto_pop_total_rede": "percentual",
    "esgoto_pop_urb_rede": "percentual",
    "esgoto_dom_total_rede": "percentual",
    "esgoto_dom_urb_rede": "percentual",
    "esgoto_dom_total_trat": "percentual",
    "esgoto_dom_urb_trat": "percentual",
    "vitimas_homicidio_2022": "contagem",
    "via_pav_pct": "percentual",
    "via_pav_n": "contagem",
    "ilum_pub_pct": "percentual",
    "ilum_pub_n": "contagem",
    "calcada_pct": "percentual",
    "calcada_n": "contagem",
    "regic_var56": "indice_normalizado",
    "regic_var57": "indice_normalizado",
    "regic_var58": "indice_normalizado",
    "regic_var59": "indice_normalizado",
    "regic_var60": "indice_normalizado",
    "regic_var61": "indice_normalizado",
    "regic_var62": "indice_normalizado",
    "regic_var63": "indice_normalizado",
    "regic_var64": "indice_normalizado",
    "regic_var65": "indice_normalizado",
    "regic_var66": "indice_normalizado",
    "plano_diretor": "binaria",
    "area_urb_densa_km2": "area_km2",
    "loteamento_vazio_km2": "area_km2",
    "vazios_intraurbanos_km2": "area_km2",
    "demissoes_2025": "contagem",
    "ambulatorios_sus_2026_02": "contagem",
    "demanda_agua_hum_urb_m3s": "numerica_absoluta",
    "demanda_agua_hum_rur_m3s": "numerica_absoluta",
    "demanda_agua_ind_m3s": "numerica_absoluta",
    "demanda_agua_min_m3s": "numerica_absoluta",
    "demanda_agua_term_m3s": "numerica_absoluta",
    "demanda_agua_animal_m3s": "numerica_absoluta",
    "demanda_agua_irr_m3s": "numerica_absoluta",
    "demanda_agua_total_m3s": "numerica_absoluta",
    "registros_seca_2003_2015": "contagem",
}

FONTE_PADRAO = {
    "merge_completo.csv / IBGE": "IBGE - base preliminar merge_completo.csv",
    "merge_completo.csv / IBGE tabela5938": "IBGE - Tabela 5938",
    "merge_completo.csv / IBGE tabela9514": "IBGE - Tabela 9514",
    "merge_completo.csv / IBGE tabela4712": "IBGE - Tabela 4712",
    "bronze/tabela9582.csv": "IBGE - Tabela 9582",
    "bronze/transferências_para_municípios.csv": "Transferencias FUNDEB por municipio",
    "SINISA - Indicadores de Atendimento": "SINISA - Modulo Esgotamento Sanitario - Indicadores de Atendimento",
    "indicadoressegurancapublicamunic.xlsx": "Indicadores municipais de seguranca publica",
    "bronze/tabela9584_%.csv": "IBGE - Tabela 9584 percentual",
    "bronze/tabela9584.csv": "IBGE - Tabela 9584 absoluta",
    "REGIC 2018 - Descrição das variáveis": "IBGE - REGIC 2018",
    "bronze/tabela5882.csv": "IBGE - Tabela 5882",
    "bronze/tabela8418.csv": "IBGE - Tabela 8418",
    "prata/pre_merge/ipea_demissoes_municipais_2025.csv": "IpeaData - Novo Caged sem ajuste",
    "bronze/cnes_cnv_atambbr131932200_135_70_71.csv": "CNES - Ambulatorio SUS por municipio",
    "bronze/cnes_cnv_estabbr134413200_135_70_71.csv": "CNES - Estabelecimentos por tipo",
    "bronze/Demanda_Total.csv": "ANA - Demanda total de agua por municipio",
    "bronze/N%C3%BAmero_de_Registros_de_Secas_por_Munic%C3%ADpio_entre_2003_e_2015.csv": "ANA - Numero de registros de secas por municipio entre 2003 e 2015",
}

CNES_ESTAB_RENOMEAR = {
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

CNES_ESTAB_RENOMEAR_V20 = {
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

CNES_ESTAB_RENOMEAR_V21 = {
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

METADADOS = [
    {
        "variavel_v14": "cod_mun",
        "variavel_original": "Cód.",
        "descricao_original": "Codigo IBGE do municipio.",
        "ano_referencia": "",
        "fonte_original": "merge_completo.csv / IBGE",
        "observacoes": "Identificador municipal herdado da base preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "municipio",
        "variavel_original": "Município",
        "descricao_original": "Nome do municipio, apresentado com a UF entre parenteses.",
        "ano_referencia": "",
        "fonte_original": "merge_completo.csv / IBGE",
        "observacoes": "Identificacao textual do municipio herdada da base preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pib_total",
        "variavel_original": "pib_total",
        "descricao_original": "Produto Interno Bruto municipal a precos correntes.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "impostos_sub",
        "variavel_original": "impostos - sub",
        "descricao_original": "Impostos, liquidos de subsidios, sobre produtos no PIB municipal.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pib_agro",
        "variavel_original": "pib_agro",
        "descricao_original": "Valor adicionado bruto da agropecuaria.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pib_industria",
        "variavel_original": "pib_industria",
        "descricao_original": "Valor adicionado bruto da industria.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pib_servicos",
        "variavel_original": "pib_serviços",
        "descricao_original": "Valor adicionado bruto dos servicos.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pib_adm",
        "variavel_original": "pib_adm",
        "descricao_original": "Valor adicionado bruto da administracao publica.",
        "ano_referencia": "2021",
        "fonte_original": "merge_completo.csv / IBGE tabela5938",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "pop_total",
        "variavel_original": "pop",
        "descricao_original": "Populacao residente no municipio.",
        "ano_referencia": "2022",
        "fonte_original": "merge_completo.csv / IBGE tabela9514",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "dom_total",
        "variavel_original": "dom",
        "descricao_original": "Domicilios particulares permanentes ocupados no municipio.",
        "ano_referencia": "2022",
        "fonte_original": "merge_completo.csv / IBGE tabela4712",
        "observacoes": "Variavel herdada do merge preliminar anterior ao pipeline atual.",
    },
    {
        "variavel_v14": "empresas_total",
        "variavel_original": "Total",
        "descricao_original": "Numero de empresas e outras organizacoes no municipio.",
        "ano_referencia": "2023",
        "fonte_original": "bronze/tabela9582.csv",
        "observacoes": "Obtida da Tabela 9582 do IBGE, mantendo o recorte Total de ano de fundacao e de faixa de pessoal ocupado.",
    },
    {
        "variavel_v14": "fundeb_vaaf",
        "variavel_original": "FUNDEB - COUN VAAF",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - COUN VAAF para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_vaar",
        "variavel_original": "FUNDEB - COUN VAAR",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - COUN VAAR para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_vaat",
        "variavel_original": "FUNDEB - COUN VAAT",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - COUN VAAT para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_fpe",
        "variavel_original": "FUNDEB - FPE",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - FPE para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_fpm",
        "variavel_original": "FUNDEB - FPM",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - FPM para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_fti",
        "variavel_original": "FUNDEB - FUNDEB - FTI",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - FTI para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "O nome original aparece duplicado no arquivo fonte e foi encurtado no nome final da coluna.",
    },
    {
        "variavel_v14": "fundeb_icms",
        "variavel_original": "FUNDEB - ICMS",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - ICMS para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_ipi_exp",
        "variavel_original": "FUNDEB - IPI-EXP",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - IPI-EXP para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_ipva",
        "variavel_original": "FUNDEB - IPVA",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - IPVA para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_itcmd",
        "variavel_original": "FUNDEB - ITCMD",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - ITCMD para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "fundeb_itr",
        "variavel_original": "FUNDEB - ITR",
        "descricao_original": "Valor consolidado da transferencia FUNDEB - ITR para o municipio.",
        "ano_referencia": "2025",
        "fonte_original": "bronze/transferências_para_municípios.csv",
        "observacoes": "Derivada por pivot da coluna Transferência no arquivo original do Fundeb.",
    },
    {
        "variavel_v14": "esgoto_pop_total_rede",
        "variavel_original": "Atendimento da população total com rede coletora de esgoto",
        "descricao_original": "Percentual de atendimento da populacao total com rede coletora de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "esgoto_pop_urb_rede",
        "variavel_original": "Atendimento da população urbana com rede coletora de esgoto",
        "descricao_original": "Percentual de atendimento da populacao urbana com rede coletora de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "esgoto_dom_total_rede",
        "variavel_original": "Atendimento dos domicílios totais com rede coletora de esgoto",
        "descricao_original": "Percentual de atendimento dos domicilios totais com rede coletora de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "esgoto_dom_urb_rede",
        "variavel_original": "Atendimento dos domicílios urbanos com rede coletora de esgoto",
        "descricao_original": "Percentual de atendimento dos domicilios urbanos com rede coletora de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "esgoto_dom_total_trat",
        "variavel_original": "Atendimento dos domicílios totais com coleta e tratamento de esgoto",
        "descricao_original": "Percentual de atendimento dos domicilios totais com coleta e tratamento de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "esgoto_dom_urb_trat",
        "variavel_original": "Atendimento dos domicílios urbanos com coleta e tratamento de esgoto",
        "descricao_original": "Percentual de atendimento dos domicilios urbanos com coleta e tratamento de esgoto.",
        "ano_referencia": "2023",
        "fonte_original": "SINISA - Indicadores de Atendimento",
        "observacoes": "Obtida da aba Atendimento do modulo Esgotamento Sanitario do SINISA.",
    },
    {
        "variavel_v14": "vitimas_homicidio_2022",
        "variavel_original": "Vítimas",
        "descricao_original": "Numero total de vitimas de homicidio no municipio ao longo do ano.",
        "ano_referencia": "2022",
        "fonte_original": "indicadoressegurancapublicamunic.xlsx",
        "observacoes": "Agregada a partir dos CSVs por UF, somando as vitimas mensais de 2022 por municipio.",
    },
    {
        "variavel_v14": "via_pav_pct",
        "variavel_original": "Via pavimentada - Existe",
        "descricao_original": "Percentual de domicilios em setores censitarios selecionados com existencia de via pavimentada no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584_%.csv",
        "observacoes": "Percentual do total geral na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "via_pav_n",
        "variavel_original": "Via pavimentada - Existe",
        "descricao_original": "Numero de domicilios em setores censitarios selecionados com existencia de via pavimentada no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584.csv",
        "observacoes": "Valor absoluto de domicilios na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "ilum_pub_pct",
        "variavel_original": "Existência de iluminação pública - Existe",
        "descricao_original": "Percentual de domicilios em setores censitarios selecionados com existencia de iluminacao publica no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584_%.csv",
        "observacoes": "Percentual do total geral na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "ilum_pub_n",
        "variavel_original": "Existência de iluminação pública - Existe",
        "descricao_original": "Numero de domicilios em setores censitarios selecionados com existencia de iluminacao publica no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584.csv",
        "observacoes": "Valor absoluto de domicilios na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "calcada_pct",
        "variavel_original": "Existência de calçada / passeio - Existe",
        "descricao_original": "Percentual de domicilios em setores censitarios selecionados com existencia de calcada ou passeio no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584_%.csv",
        "observacoes": "Percentual do total geral na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "calcada_n",
        "variavel_original": "Existência de calçada / passeio - Existe",
        "descricao_original": "Numero de domicilios em setores censitarios selecionados com existencia de calcada ou passeio no entorno.",
        "ano_referencia": "2022",
        "fonte_original": "bronze/tabela9584.csv",
        "observacoes": "Valor absoluto de domicilios na Tabela 9584 do IBGE.",
    },
    {
        "variavel_v14": "regic_var56",
        "variavel_original": "VAR56",
        "descricao_original": "Indice de Atracao Geral.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var57",
        "variavel_original": "VAR57",
        "descricao_original": "Indice de atracao tematica para compra de vestuario e calcados.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var58",
        "variavel_original": "VAR58",
        "descricao_original": "Indice de atracao tematica para compra de moveis e eletroeletronicos.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var59",
        "variavel_original": "VAR59",
        "descricao_original": "Indice de atracao tematica para saude de baixa e media complexidades.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var60",
        "variavel_original": "VAR60",
        "descricao_original": "Indice de atracao tematica para saude de alta complexidade.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var61",
        "variavel_original": "VAR61",
        "descricao_original": "Indice de atracao tematica para ensino superior.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var62",
        "variavel_original": "VAR62",
        "descricao_original": "Indice de atracao tematica para atividades culturais.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var63",
        "variavel_original": "VAR63",
        "descricao_original": "Indice de atracao tematica para atividades esportivas.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var64",
        "variavel_original": "VAR64",
        "descricao_original": "Indice de atracao tematica para aeroporto.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var65",
        "variavel_original": "VAR65",
        "descricao_original": "Indice de atracao tematica para jornais.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "regic_var66",
        "variavel_original": "VAR66",
        "descricao_original": "Indice de atracao tematica para transporte publico.",
        "ano_referencia": "2018",
        "fonte_original": "REGIC 2018 - Descrição das variáveis",
        "observacoes": "Na v14, esta variavel esta normalizada para a faixa de 0 a 1.",
    },
    {
        "variavel_v14": "plano_diretor",
        "variavel_original": "Total",
        "descricao_original": "Indicador de existencia de plano diretor no municipio, derivado da categoria Com Plano Diretor.",
        "ano_referencia": "2021",
        "fonte_original": "bronze/tabela5882.csv",
        "observacoes": "Na tabela original, a coluna Total registra o numero de municipios com plano diretor; no nivel municipal ela foi convertida para indicador binario 1/0.",
    },
    {
        "variavel_v14": "area_urb_densa_km2",
        "variavel_original": "Áreas urbanizadas densas (Quilômetros quadrados)",
        "descricao_original": "Area de areas urbanizadas densas no municipio, em quilometros quadrados.",
        "ano_referencia": "2019",
        "fonte_original": "bronze/tabela8418.csv",
        "observacoes": "Obtida da Tabela 8418 do IBGE e renomeada para formato curto na base final.",
    },
    {
        "variavel_v14": "loteamento_vazio_km2",
        "variavel_original": "Loteamento vazio (Quilômetros quadrados)",
        "descricao_original": "Area de loteamentos vazios no municipio, em quilometros quadrados.",
        "ano_referencia": "2019",
        "fonte_original": "bronze/tabela8418.csv",
        "observacoes": "Obtida da Tabela 8418 do IBGE e renomeada para formato curto na base final.",
    },
    {
        "variavel_v14": "vazios_intraurbanos_km2",
        "variavel_original": "Vazios intraurbanos (Quilômetros quadrados)",
        "descricao_original": "Area de vazios intraurbanos no municipio, em quilometros quadrados.",
        "ano_referencia": "2019",
        "fonte_original": "bronze/tabela8418.csv",
        "observacoes": "Obtida da Tabela 8418 do IBGE e renomeada para formato curto na base final.",
    },
    {
        "variavel_v14": "demissoes_2025",
        "variavel_original": "Empregados - demissões - Novo Caged sem ajuste",
        "descricao_original": "Total anual de demissoes de empregados no municipio, obtido pela soma dos 12 meses do ano.",
        "ano_referencia": "2025",
        "fonte_original": "prata/pre_merge/ipea_demissoes_municipais_2025.csv",
        "observacoes": "Derivada do arquivo mensal do Ipea por agregacao dos meses de 2025 em um unico valor anual por municipio.",
    },
    {
        "variavel_v14": "ambulatorios_sus_2026_02",
        "variavel_original": "SUS",
        "descricao_original": "Numero de estabelecimentos com tipo de atendimento prestado ambulatorio no SUS, por municipio.",
        "ano_referencia": "2026-02",
        "fonte_original": "bronze/cnes_cnv_atambbr131932200_135_70_71.csv",
        "observacoes": "Derivada do arquivo do CNES por municipio. O merge com a base principal usa o codigo municipal reduzido de 6 digitos do CNES.",
    },
    {
        "variavel_v14": "demanda_agua_hum_urb_m3s",
        "variavel_original": "VZHURM3S",
        "descricao_original": "Demanda de agua para uso humano urbano no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_hum_rur_m3s",
        "variavel_original": "VZHRUM3S",
        "descricao_original": "Demanda de agua para uso humano rural no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_ind_m3s",
        "variavel_original": "VZINDM3S",
        "descricao_original": "Demanda de agua para uso industrial no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_min_m3s",
        "variavel_original": "VZMINM3S",
        "descricao_original": "Demanda de agua para mineracao no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_term_m3s",
        "variavel_original": "VZTERM3S",
        "descricao_original": "Demanda de agua para uso termeletrico no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_animal_m3s",
        "variavel_original": "VZANIM3S",
        "descricao_original": "Demanda de agua para dessedentacao animal no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_irr_m3s",
        "variavel_original": "VZIRRM3S",
        "descricao_original": "Demanda de agua para irrigacao no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "demanda_agua_total_m3s",
        "variavel_original": "VZTOTM3S",
        "descricao_original": "Demanda total de agua no municipio, em metros cubicos por segundo.",
        "ano_referencia": "2020",
        "fonte_original": "bronze/Demanda_Total.csv",
        "observacoes": "Derivada da base da ANA de demanda total por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
    {
        "variavel_v14": "registros_seca_2003_2015",
        "variavel_original": "SECAS2003A",
        "descricao_original": "Numero de registros de secas por municipio no periodo de 2003 a 2015.",
        "ano_referencia": "2003-2015",
        "fonte_original": "bronze/N%C3%BAmero_de_Registros_de_Secas_por_Munic%C3%ADpio_entre_2003_e_2015.csv",
        "observacoes": "Derivada da base da ANA de registros de secas por municipio. O merge e feito pelo codigo IBGE municipal.",
    },
]


def normalizar_ano_referencia(valor: object) -> str:
    texto = str(valor).strip()
    if not texto:
        return "nao_se_aplica"
    return texto


def padronizar_fonte(valor: str) -> str:
    return FONTE_PADRAO.get(valor, valor)


def slugify(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.lower().replace("/", " ")
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return re.sub(r"_+", "_", texto).strip("_")


def extrair_periodo_cnes_estabelecimentos() -> str | None:
    arquivo = BASE_DIR / "bronze" / "cnes_cnv_estabbr134413200_135_70_71.csv"
    if not arquivo.exists():
        return None

    with arquivo.open("r", encoding="latin1", errors="replace") as f:
        linhas = [f.readline().strip() for _ in range(3)]

    match = re.search(r"Per[ií]odo:([A-Za-z]{3})/(\d{4})", linhas[2], flags=re.IGNORECASE)
    if not match:
        return None

    mes_txt = match.group(1).lower()
    ano = match.group(2)
    mapa_meses = {
        "jan": "01",
        "fev": "02",
        "mar": "03",
        "abr": "04",
        "mai": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "set": "09",
        "out": "10",
        "nov": "11",
        "dez": "12",
    }
    if mes_txt not in mapa_meses:
        return None

    return f"{ano}_{mapa_meses[mes_txt]}"


def nome_coluna_cnes_estabelecimentos(coluna_original: str, periodo: str) -> str:
    return f"cnes_estab_{slugify(coluna_original)}_{periodo}"


def gerar_metadados_cnes_estabelecimentos(colunas_base: list[str]) -> list[dict[str, str]]:
    arquivo = BASE_DIR / "bronze" / "cnes_cnv_estabbr134413200_135_70_71.csv"
    if not arquivo.exists():
        return []

    periodo = extrair_periodo_cnes_estabelecimentos()
    if periodo is None:
        return []

    df = pd.read_csv(arquivo, encoding="latin1", sep=";", skiprows=3, nrows=0)
    colunas_origem = [col for col in df.columns if col != "Município"]
    ano_ref = periodo.replace("_", "-")

    metadados: list[dict[str, str]] = []
    for coluna in colunas_origem:
        variavel_longa = nome_coluna_cnes_estabelecimentos(coluna, periodo)
        variavel = CNES_ESTAB_RENOMEAR.get(variavel_longa, variavel_longa)
        variavel = CNES_ESTAB_RENOMEAR_V20.get(variavel, variavel)
        variavel = CNES_ESTAB_RENOMEAR_V21.get(variavel, variavel)
        if variavel not in colunas_base and variavel_longa not in colunas_base:
            continue

        metadados.append(
            {
                "variavel_v14": variavel,
                "variavel_original": coluna,
                "descricao_original": f"Numero de estabelecimentos do tipo {coluna.lower()} por municipio no CNES.",
                "ano_referencia": ano_ref,
                "fonte_original": "bronze/cnes_cnv_estabbr134413200_135_70_71.csv",
                "observacoes": "Derivada do arquivo do CNES por municipio. Os valores '-' sao convertidos para 0 e Brasilia e zerada em todas as colunas do bloco.",
            }
        )

    return metadados


def descobrir_ultima_base() -> tuple[Path, str]:
    arquivos = sorted(PROCESSAMENTO_DIR.glob("merge_v*.csv"))
    candidatos: list[tuple[int, Path, str]] = []

    for arquivo in arquivos:
        match = re.fullmatch(r"merge_v(\d+)\.csv", arquivo.name)
        if match:
            versao = int(match.group(1))
            candidatos.append((versao, arquivo, f"v{versao}"))

    if not candidatos:
        raise FileNotFoundError(f"Nenhum arquivo merge_v*.csv encontrado em {PROCESSAMENTO_DIR}")

    _, caminho, rotulo = max(candidatos, key=lambda item: item[0])
    return caminho, rotulo


def main() -> int:
    input_file, versao_base = descobrir_ultima_base()
    output_file = BASE_DIR / "documentacao" / "dicionario_dados.csv"

    df = pd.read_csv(input_file)
    colunas_base = df.columns.tolist()
    metadados = METADADOS + gerar_metadados_cnes_estabelecimentos(colunas_base)
    colunas_metadados = [item["variavel_v14"] for item in metadados]

    faltantes = [col for col in colunas_base if col not in colunas_metadados]
    extras = [col for col in colunas_metadados if col not in colunas_base]
    if faltantes or extras:
        raise ValueError(
            "Metadados inconsistentes com a merge_v14. "
            f"Faltantes no dicionario: {faltantes}. Extras no dicionario: {extras}."
        )

    dicionario = pd.DataFrame(metadados)
    dicionario["ordem_v14"] = dicionario["variavel_v14"].map({col: i + 1 for i, col in enumerate(colunas_base)})
    dicionario = dicionario.sort_values("ordem_v14").reset_index(drop=True)
    dicionario["tipo"] = dicionario["variavel_v14"].map(TIPO_POR_VARIAVEL)
    dicionario.loc[dicionario["variavel_v14"].str.startswith("cnes_estab_"), "tipo"] = "contagem"
    dicionario.loc[dicionario["variavel_v14"].str.startswith("cnes_"), "tipo"] = "contagem"
    dicionario.loc[
        ~dicionario["variavel_v14"].astype(str).str.startswith("cnes_")
        & dicionario["fonte_original"].eq("bronze/cnes_cnv_estabbr134413200_135_70_71.csv"),
        "tipo",
    ] = "contagem"
    if dicionario["tipo"].isna().any():
        faltantes_tipo = dicionario.loc[dicionario["tipo"].isna(), "variavel_v14"].tolist()
        raise ValueError(f"Tipo ausente para as variaveis: {faltantes_tipo}")

    dicionario["ano_referencia"] = dicionario["ano_referencia"].map(normalizar_ano_referencia)
    dicionario["fonte_original"] = dicionario["fonte_original"].map(padronizar_fonte)
    dicionario = dicionario[
        [
            "ordem_v14",
            "variavel_v14",
            "tipo",
            "variavel_original",
            "descricao_original",
            "ano_referencia",
            "fonte_original",
            "observacoes",
        ]
    ]

    output_file.parent.mkdir(parents=True, exist_ok=True)
    dicionario.to_csv(output_file, index=False, encoding="utf-8")

    print(f"Base usada: {input_file}")
    print(f"Versao detectada: {versao_base}")
    print(f"Arquivo gerado: {output_file}")
    print(f"Variaveis documentadas: {len(dicionario)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
