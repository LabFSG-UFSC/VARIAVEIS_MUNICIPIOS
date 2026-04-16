from __future__ import annotations

import csv
import shutil
import sqlite3
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT = Path("/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios")
PROCESSAMENTO = ROOT / "regioes_geograficas" / "processamento"

SQLITE_ORIG = PROCESSAMENTO / "geodados_dbeaver.sqlite"
SQLITE_CORRIGIDO = PROCESSAMENTO / "geodados_dbeaver_corrigido.sqlite"
SHAPE_INDEX = Path("/tmp/variaveis_postgres/export/municipios_shp_index_clean.csv")

RECON_CSV = PROCESSAMENTO / "reconstrucao_cod_mun_corrompidos.csv"
EXTRAS_CSV = PROCESSAMENTO / "municipios_extras_malha_2025.csv"
CSV_CORRIGIDO = PROCESSAMENTO / "variaveis_municipios_corrigido.csv"


def text_factory(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return value


def is_valid_cod_mun(value: object) -> bool:
    text = "" if value is None else str(value)
    return len(text) == 7 and text.isdigit()


def derived_municipio(nm_mun: str, sigla_uf: str) -> str:
    nm_mun = (nm_mun or "").strip()
    sigla_uf = (sigla_uf or "").strip()
    if nm_mun and sigla_uf:
        return f"{nm_mun} ({sigla_uf})"
    return nm_mun


def is_bad_municipio(value: object, nm_ref: str = "") -> bool:
    text = "" if value is None else str(value).strip()
    if not text:
        return True
    if "�" in text:
        return True
    if len(text) < 3:
        return True
    if nm_ref and nm_ref.strip() and nm_ref.strip() not in text:
        return True
    return False


def load_shape_index() -> list[dict[str, str]]:
    with SHAPE_INDEX.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_sqlite_rows() -> tuple[list[str], list[tuple]]:
    con = sqlite3.connect(SQLITE_ORIG)
    con.text_factory = text_factory
    cur = con.cursor()
    cur.execute("SELECT * FROM variaveis_municipios ORDER BY fid")
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    con.close()
    return cols, rows


def build_reconstruction(rows: list[tuple], shape_rows: list[dict[str, str]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    corrupt = []
    extras = []
    j = 0

    for row in rows:
        fid, cod_mun, municipio, nm_mun, cd_rgi, cd_rgint = row[0], row[16], row[17], row[2], row[3], row[5]
        cod_mun = "" if cod_mun is None else str(cod_mun)

        if not is_valid_cod_mun(cod_mun):
            target = shape_rows[j]
            corrupt.append(
                {
                    "fid": fid,
                    "cod_mun_atual": cod_mun,
                    "municipio_atual": "" if municipio is None else str(municipio),
                    "nm_mun_atual": "" if nm_mun is None else str(nm_mun),
                    "cd_rgi_atual": "" if cd_rgi is None else str(cd_rgi),
                    "cd_rgint_atual": "" if cd_rgint is None else str(cd_rgint),
                    "cod_mun_reconstruido": target["CD_MUN"],
                    "nm_mun_reconstruido": target["NM_MUN"],
                    "cd_rgi_reconstruido": target["CD_RGI"],
                    "cd_rgint_reconstruido": target["CD_RGINT"],
                }
            )
            j += 1
            continue

        if cod_mun == shape_rows[j]["CD_MUN"]:
            j += 1
            continue

        found = None
        for k in range(1, 8):
            if j + k < len(shape_rows) and cod_mun == shape_rows[j + k]["CD_MUN"]:
                found = k
                break

        if found is None:
            raise RuntimeError(f"Alinhamento nao resolvido no fid {fid}: {cod_mun} vs {shape_rows[j]['CD_MUN']}")

        for x in range(found):
            extras.append(
                {
                    "ordem_malha": j + 1 + x,
                    "cod_mun": shape_rows[j + x]["CD_MUN"],
                    "nm_mun": shape_rows[j + x]["NM_MUN"],
                    "cd_rgi": shape_rows[j + x]["CD_RGI"],
                    "cd_rgint": shape_rows[j + x]["CD_RGINT"],
                }
            )

        j += found + 1

    return corrupt, extras


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_corrected_outputs(cols: list[str], rows: list[tuple], recon_rows: list[dict[str, object]]) -> None:
    recon_map = {int(row["fid"]): row for row in recon_rows}
    idx = {c: i for i, c in enumerate(cols)}

    clean_rows = []
    for row in rows:
        row = list(row)
        fid = row[idx["fid"]]
        if fid in recon_map:
            rr = recon_map[fid]
            row[idx["cod_mun"]] = rr["cod_mun_reconstruido"]
            row[idx["NM_MUN"]] = rr["nm_mun_reconstruido"]
            row[idx["CD_RGI"]] = rr["cd_rgi_reconstruido"]
            row[idx["CD_RGINT"]] = rr["cd_rgint_reconstruido"]
            if is_bad_municipio(row[idx["municipio"]], rr["nm_mun_reconstruido"]):
                row[idx["municipio"]] = derived_municipio(rr["nm_mun_reconstruido"], row[idx["SIGLA_UF"]])
        clean_rows.append(row)

    with CSV_CORRIGIDO.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(clean_rows)

    if SQLITE_CORRIGIDO.exists():
        SQLITE_CORRIGIDO.unlink()
    shutil.copy2(SQLITE_ORIG, SQLITE_CORRIGIDO)

    con = sqlite3.connect(SQLITE_CORRIGIDO)
    con.text_factory = text_factory
    cur = con.cursor()

    for fid, rr in recon_map.items():
        sigla_uf, municipio_atual = cur.execute(
            "SELECT SIGLA_UF, municipio FROM variaveis_municipios WHERE fid = ?",
            (fid,),
        ).fetchone()
        municipio_novo = municipio_atual
        if is_bad_municipio(municipio_atual, rr["nm_mun_reconstruido"]):
            municipio_novo = derived_municipio(rr["nm_mun_reconstruido"], sigla_uf)

        cur.execute(
            """
            UPDATE variaveis_municipios
            SET cod_mun = ?,
                NM_MUN = ?,
                CD_RGI = ?,
                CD_RGINT = ?,
                municipio = ?
            WHERE fid = ?
            """,
            (
                rr["cod_mun_reconstruido"],
                rr["nm_mun_reconstruido"],
                rr["cd_rgi_reconstruido"],
                rr["cd_rgint_reconstruido"],
                municipio_novo,
                fid,
            ),
        )

    con.commit()
    con.close()


def main() -> None:
    shape_rows = load_shape_index()
    cols, rows = load_sqlite_rows()
    corrupt, extras = build_reconstruction(rows, shape_rows)

    write_csv(RECON_CSV, corrupt)
    write_csv(EXTRAS_CSV, extras)
    build_corrected_outputs(cols, rows, corrupt)

    print(f"reconstructed_rows={len(corrupt)}")
    print(f"extras_rows={len(extras)}")
    print(f"csv_corrigido={CSV_CORRIGIDO}")
    print(f"sqlite_corrigido={SQLITE_CORRIGIDO}")


if __name__ == "__main__":
    main()
