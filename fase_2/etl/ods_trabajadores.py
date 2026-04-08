"""
ETL csv_buenos → ODS (trabajadores, simulados, indicadores)
===========================================================
Fuentes (fase_2/output/csv_buenos/):
  Renglon_011.csv             → Perfil_Persona + Trabajador + Cargo + Contrato (Congreso, renglon 011)
  Renglon_022.csv             → Perfil_Persona + Trabajador + Cargo + Contrato (Congreso, renglon 022)
  Salario_Cargo_Diputados.csv → Perfil_Persona + Trabajador + Cargo + Contrato (Diputados)
  Salarios_OJ.csv             → Perfil_Persona + Trabajador + Cargo + Contrato (OJ)
  Casos.csv                   → Indicador_Externo (casos por departamento, PNC 2024)
  Procesos.csv                → Indicador_Externo (procesos judiciales por delito, OJ 2024)
  Proveedores.csv             → Already covered by 02_insert_proveedores.sql (skipped)

Simulados (para demostración BI — corrupción en sector justicia):
  Declaracion_Patrimonial     → 40% de los trabajadores
  Pagos_Realizados            → 3 meses (ene-mar 2026) × todos los trabajadores
  Denuncia                    → 20 casos sintéticos de corrupción
  Implicado                   → 2-3 personas por denuncia

Salidas (fase_2/output/sql/):
  04_insert_trabajadores_congreso.sql
  05_insert_trabajadores_oj.sql
  06_insert_simulados.sql
  07_insert_indicadores_casos.sql

ID Ranges (sin colisión con datos ya generados):
  Perfil_Persona  : empieza en 300001  (02_insert_proveedores usa 1..262574)
  Trabajador      : empieza en 1       (tabla nueva)
  Cargo           : empieza en 1       (tabla nueva)
  Indicador_Externo: empieza en 500    (03_insert_indicadores_ine usa 1..384)
  Tipo_Contrato 3 = Renglon 011, 4 = Renglon 022 (según 00_catalogos_base.sql)
  Institucion Congreso: id=10001 (OCDS usa 1..718; OJ ya tiene id=5)
"""

import csv
import os
import hashlib
from datetime import date

BASE    = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE, "..", "output", "csv_buenos")
OUT_SQL = os.path.join(BASE, "..", "output", "sql")
os.makedirs(OUT_SQL, exist_ok=True)

# ── Helpers ────────────────────────────────────────────────────────────────────
def esc(v) -> str:
    if v is None or (isinstance(v, str) and v.strip() in ("", "nan", "None", "NULL")):
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def read_csv(fname: str) -> list[dict]:
    with open(os.path.join(CSV_DIR, fname), encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def det_hash(s, mod: int) -> int:
    """Hash determinista para generar datos simulados reproducibles."""
    return int(hashlib.md5(str(s).encode()).hexdigest(), 16) % mod


def split_name(full: str) -> tuple[str, str]:
    """Últimas 2 palabras → apellidos; el resto → nombres."""
    parts = full.strip().strip('"').split()
    if len(parts) <= 1:
        return full.strip(), ""
    if len(parts) == 2:
        return parts[0], parts[1]
    return " ".join(parts[:-2]), " ".join(parts[-2:])


def build_inserts(table: str, cols: list[str], rows: list[dict],
                  chunk: int = 500) -> list[str]:
    if not rows:
        return []
    col_str = ", ".join(cols)
    stmts = []
    for i in range(0, len(rows), chunk):
        batch = rows[i: i + chunk]
        vals = ",\n".join(
            "  (" + ", ".join(esc(r.get(c)) for c in cols) + ")"
            for r in batch
        )
        stmts.append(f"INSERT INTO {table} ({col_str}) VALUES\n{vals};")
    return stmts


def write_sql(fname: str, title: str, sections: list[tuple[str, list[str]]]):
    lines = [
        f"-- {title}",
        f"-- Generado: {date.today()}",
        "",
        "USE ods_justicia;",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]
    for comment, stmts in sections:
        if comment:
            lines += [
                f"-- {'=' * 58}",
                f"-- {comment}",
                f"-- {'=' * 58}",
            ]
        lines += stmts + [""]
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    path = os.path.join(OUT_SQL, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    total_stmts = sum(len(s) for _, s in sections)
    print(f"  SQL → {fname}  ({total_stmts} sentencias)")


# ── Constantes de IDs ──────────────────────────────────────────────────────────
PP_BASE       = 300_001
TRAB_BASE     = 1
CARGO_BASE    = 1
IND_BASE      = 500
DECL_BASE     = 1
PAGO_BASE     = 1
DEN_BASE      = 1
IMP_BASE      = 1

CONGRESO_ID   = 10_001
OJ_ID         = 5       # ya existe en OCDS (id=5 → ORGANISMO JUDICIAL)

TC_011        = 3       # Renglon 011 — Personal Permanente
TC_022        = 4       # Renglon 022 — Personal por Contrato
EC_ACTIVO     = 1       # Estado_Contrato Activo

# Columnas fijas por tabla
PP_COLS   = ["id", "id_municipio_vivienda", "id_sexo", "id_estado_civil",
             "id_grupo_etnico", "dpi", "nit", "nombres", "apellidos",
             "fecha_nacimiento", "direccion", "telefono"]
TRAB_COLS = ["id", "id_perfil_persona", "esta_activo"]
CARGO_COLS = ["id", "id_institucion", "id_nivel_cargo", "id_cargo_jefe",
              "nombre", "descripcion", "sueldo_base", "renglon_presupuestario",
              "fecha_creacion"]
CONT_COLS = ["id_trabajador", "id_empresa", "id_institucion", "id_cargo",
             "id_tipo_contrato", "id_estado_contrato", "sueldo_real",
             "fecha_inicio", "fecha_fin"]
INST_COLS = ["id", "id_municipio", "id_tipo_institucion", "id_nivel_institucion",
             "nombre", "siglas", "direccion", "telefono"]
DECL_COLS = ["id", "id_trabajador", "id_tipo_declaracion", "ingresos", "egresos",
             "activos", "pasivos", "vehiculos", "observaciones", "fecha_presentacion"]
PAGO_COLS = ["id", "id_trabajador", "id_tipo_pago", "pago_realizado", "fecha_pago"]
DEN_COLS  = ["id", "id_tipo_denuncia", "id_estado_denuncia",
             "id_institucion", "descripcion"]
IMP_COLS  = ["id", "id_denuncia", "id_perfil_persona", "id_tipo_implicado"]
IND_COLS  = ["id", "anio", "fuente", "tipo_indicador", "valor", "unidad"]


# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("ETL csv_buenos → ODS")
print("=" * 60)

pp_rows    = []
trab_rows  = []
cargo_rows = []
cont_rows  = []
cargo_map  = {}          # (inst_id, puesto_key) → cargo_id
all_workers = []         # (trab_id, pp_id, inst_id, salario) — para simulación

pp_id    = PP_BASE
trab_id  = TRAB_BASE
cargo_id = CARGO_BASE
congreso_pp_start = PP_BASE   # marcador para partir Congreso vs OJ

RENGLON_TC = {"011": TC_011, "022": TC_022}


def get_or_create_cargo(inst_id: int, puesto: str, sueldo_base) -> int:
    global cargo_id
    key = (inst_id, puesto[:150])
    if key not in cargo_map:
        cargo_map[key] = cargo_id
        cargo_rows.append({
            "id":                   cargo_id,
            "id_institucion":       inst_id,
            "id_nivel_cargo":       None,
            "id_cargo_jefe":        None,
            "nombre":               puesto[:150],
            "descripcion":          None,
            "sueldo_base":          sueldo_base,
            "renglon_presupuestario": None,
            "fecha_creacion":       None,
        })
        cargo_id += 1
    return cargo_map[key]


def add_worker(nombre_raw: str, puesto: str, salario_raw,
               inst_id: int, tc_id: int):
    global pp_id, trab_id
    nombres, apellidos = split_name(nombre_raw)
    try:
        sal = round(float(str(salario_raw).replace(",", "")), 2)
    except (ValueError, TypeError):
        sal = 0.0

    pp_rows.append({
        "id":                   pp_id,
        "id_municipio_vivienda": None,
        "id_sexo":              None,
        "id_estado_civil":      None,
        "id_grupo_etnico":      None,
        "dpi":                  None,
        "nit":                  None,
        "nombres":              nombres,
        "apellidos":            apellidos,
        "fecha_nacimiento":     None,
        "direccion":            None,
        "telefono":             None,
    })
    trab_rows.append({
        "id":              trab_id,
        "id_perfil_persona": pp_id,
        "esta_activo":     1,
    })
    c_id = get_or_create_cargo(inst_id, puesto, sal)
    cont_rows.append({
        "id_trabajador":      trab_id,
        "id_empresa":         None,
        "id_institucion":     inst_id,
        "id_cargo":           c_id,
        "id_tipo_contrato":   tc_id,
        "id_estado_contrato": EC_ACTIVO,
        "sueldo_real":        sal,
        "fecha_inicio":       "2024-01-01",
        "fecha_fin":          None,
    })
    all_workers.append((trab_id, pp_id, inst_id, sal))
    pp_id   += 1
    trab_id += 1


# ══════════════════════════════════════════════════════════════════════════════
# FUENTE 1 — Congreso (Renglones 011/022 + Diputados)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[1] Congreso de la República")

rows_011 = read_csv("Renglon_011.csv")
rows_022 = read_csv("Renglon_022.csv")
rows_dip = read_csv("Salario_Cargo_Diputados.csv")

for r in rows_011:
    renglon = r.get("Renglon", "011").strip().lstrip("0") or "011"
    tc = RENGLON_TC.get(renglon.zfill(3), TC_011)
    add_worker(r["Nombre"], r["Puesto"], r["Salario"], CONGRESO_ID, tc)

for r in rows_022:
    renglon = r.get("Renglon", "022").strip().lstrip("0") or "022"
    tc = RENGLON_TC.get(renglon.zfill(3), TC_022)
    add_worker(r["Nombre"], r["Puesto"], r["Salario"], CONGRESO_ID, tc)

for r in rows_dip:
    add_worker(r["Nombre"], r["Cargo"], r["Sueldo"], CONGRESO_ID, TC_011)

congreso_count = len(rows_011) + len(rows_022) + len(rows_dip)
congreso_cargo_ids = {v for k, v in cargo_map.items() if k[0] == CONGRESO_ID}
print(f"  Empleados procesados : {congreso_count}")
print(f"  Cargos únicos        : {len(congreso_cargo_ids)}")

# ══════════════════════════════════════════════════════════════════════════════
# FUENTE 2 — Organismo Judicial (Salarios_OJ)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[2] Organismo Judicial")
oj_start = trab_id - TRAB_BASE   # índice para partir después

rows_oj = read_csv("Salarios_OJ.csv")
for r in rows_oj:
    # No hay columna de puesto; cada código diferente puede tener distinto nivel
    # Se usa puesto genérico para no explotar el catálogo de cargos
    add_worker(r["Nombre"], "EMPLEADO JUDICIAL", r["Salario"], OJ_ID, TC_011)

oj_count = len(rows_oj)
oj_cargo_ids = {v for k, v in cargo_map.items() if k[0] == OJ_ID}
print(f"  Empleados procesados : {oj_count}")
print(f"  Cargos únicos        : {len(oj_cargo_ids)}")

# ── Partir colecciones por institución ────────────────────────────────────────
pp_congreso  = pp_rows[:congreso_count]
pp_oj        = pp_rows[congreso_count:]
trab_congreso = trab_rows[:congreso_count]
trab_oj       = trab_rows[congreso_count:]
cont_congreso = cont_rows[:congreso_count]
cont_oj       = cont_rows[congreso_count:]
cargo_congreso = [c for c in cargo_rows if c["id_institucion"] == CONGRESO_ID]
cargo_oj       = [c for c in cargo_rows if c["id_institucion"] == OJ_ID]

# ── Institución Congreso (OJ ya existe con id=5 de OCDS) ──────────────────────
inst_congreso = [{
    "id":                   CONGRESO_ID,
    "id_municipio":         1,
    "id_tipo_institucion":  3,     # Organismo del Estado
    "id_nivel_institucion": 1,     # Nacional
    "nombre":               "CONGRESO DE LA REPÚBLICA DE GUATEMALA",
    "siglas":               "CONGRESO",
    "direccion":            "6a Avenida, Zona 1, Ciudad de Guatemala",
    "telefono":             "23184000",
}]

# ── Escribir SQL 04 (Congreso) ─────────────────────────────────────────────────
write_sql(
    "04_insert_trabajadores_congreso.sql",
    "INSERT Congreso → Institucion, Perfil_Persona, Trabajador, Cargo, Contrato",
    [
        ("Institucion — Congreso de la República (id=10001)",
         build_inserts("Institucion", INST_COLS, inst_congreso)),
        ("Perfil_Persona — empleados Congreso (Renglones 011, 022 y Diputados)",
         build_inserts("Perfil_Persona", PP_COLS, pp_congreso)),
        ("Trabajador — empleados Congreso",
         build_inserts("Trabajador", TRAB_COLS, trab_congreso)),
        ("Cargo — cargos únicos Congreso",
         build_inserts("Cargo", CARGO_COLS, cargo_congreso)),
        ("Contrato — vínculos laborales Congreso",
         build_inserts("Contrato", CONT_COLS, cont_congreso)),
    ],
)

# ── Escribir SQL 05 (OJ) ──────────────────────────────────────────────────────
write_sql(
    "05_insert_trabajadores_oj.sql",
    "INSERT OJ → Perfil_Persona, Trabajador, Cargo, Contrato",
    [
        ("AVISO: Institucion OJ (id=5) ya insertada en 01_insert_contratos_ocds.sql.", []),
        ("Perfil_Persona — empleados del Organismo Judicial",
         build_inserts("Perfil_Persona", PP_COLS, pp_oj)),
        ("Trabajador — empleados del OJ",
         build_inserts("Trabajador", TRAB_COLS, trab_oj)),
        ("Cargo — cargo genérico EMPLEADO JUDICIAL",
         build_inserts("Cargo", CARGO_COLS, cargo_oj)),
        ("Contrato — vínculos laborales OJ",
         build_inserts("Contrato", CONT_COLS, cont_oj)),
    ],
)

# ══════════════════════════════════════════════════════════════════════════════
# SIMULADOS: Declaracion_Patrimonial, Pagos_Realizados, Denuncia, Implicado
# ══════════════════════════════════════════════════════════════════════════════
print("\n[3] Datos simulados (BI — corrupción sector justicia)")

# ── Declaracion_Patrimonial (~40% de los trabajadores) ────────────────────────
decl_rows = []
decl_id   = DECL_BASE

for t_id, p_id, inst_id, sal in all_workers:
    if det_hash(t_id, 10) < 4:   # ~40%
        h = det_hash(f"{t_id}D", 1_000_000)
        anio    = 2022 + (h % 3)
        mes     = 1 + (h % 11)
        dia     = 1 + (h % 27)
        tipo_d  = (h % 3) + 1   # 1=Inicial 2=Anual 3=Final
        ingresos = round(sal * 12, 2)
        egresos  = round(sal * 12 * (0.55 + (h % 30) / 100), 2)
        activos  = round(sal * (18 + h % 20), 2)
        pasivos  = round(sal * (2  + h % 8), 2)
        vehs     = h % 3
        decl_rows.append({
            "id":                  decl_id,
            "id_trabajador":       t_id,
            "id_tipo_declaracion": tipo_d,
            "ingresos":            ingresos,
            "egresos":             egresos,
            "activos":             activos,
            "pasivos":             pasivos,
            "vehiculos":           vehs,
            "observaciones":       None,
            "fecha_presentacion":  f"{anio}-{mes:02d}-{dia:02d}",
        })
        decl_id += 1

# ── Pagos_Realizados (3 meses ene-mar 2026 + bono por mes) ────────────────────
pago_rows = []
pago_id   = PAGO_BASE
MESES_PAGO = ["2026-01-31", "2026-02-28", "2026-03-31"]

for t_id, _, _, sal in all_workers:
    for fecha in MESES_PAGO:
        pago_rows.append({
            "id":            pago_id,
            "id_trabajador": t_id,
            "id_tipo_pago":  1,        # Salario
            "pago_realizado": sal,
            "fecha_pago":    fecha,
        })
        pago_id += 1
    # Bonificación incentivo (250 GTQ — Dto. 78-89)
    pago_rows.append({
        "id":            pago_id,
        "id_trabajador": t_id,
        "id_tipo_pago":  2,            # Bonificación incentivo
        "pago_realizado": 250.00,
        "fecha_pago":    "2026-03-31",
    })
    pago_id += 1

# ── Denuncia — 20 casos sintéticos ────────────────────────────────────────────
TIPO_D_LIST   = [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 1, 4, 3, 2, 5, 1, 4, 3]
ESTADO_D_LIST = [2, 2, 1, 3, 1, 2, 4, 2, 1, 3, 2, 1, 2, 3, 1, 2, 4, 1, 2, 3]
INST_D_LIST   = [
    CONGRESO_ID, OJ_ID, 8, 5, CONGRESO_ID,
    OJ_ID, 8, CONGRESO_ID, OJ_ID, 8,
    5, CONGRESO_ID, OJ_ID, CONGRESO_ID, OJ_ID,
    8, CONGRESO_ID, OJ_ID, 5, CONGRESO_ID,
]
DESC_LIST = [
    "Adjudicación irregular de contratos de mantenimiento sin licitación.",
    "Nombramiento de familiares en cargos públicos sin concurso.",
    "Uso de recursos institucionales para beneficio personal.",
    "Incremento patrimonial no justificado en declaración anual.",
    "Cobro de comisiones irregulares por gestión de expedientes.",
    "Filtración de expedientes judiciales a terceros interesados.",
    "Manipulación de resoluciones a cambio de pagos.",
    "Contratación de empresa vinculada a funcionario en ejercicio.",
    "Doble cobro de viáticos y dietas sin respaldo documental.",
    "Pagos a proveedores fantasma registrados en RGAE.",
    "Alteración de registros de asistencia del personal.",
    "Sustracción de fondos del presupuesto de operaciones.",
    "Soborno a testigos en procesos judiciales activos.",
    "Uso indebido de vehículos institucionales para actividades privadas.",
    "Firma de contratos en período de veda presupuestaria.",
    "Irregularidades en proceso de precalificación de proveedores.",
    "Presión indebida a subalternos para actividades político-partidarias.",
    "Falsificación de facturas para justificar gastos no realizados.",
    "Tráfico de influencias en proceso de ascenso y promoción.",
    "Irregularidades en auditoría interna de planilla de sueldos.",
]

den_rows = []
imp_rows = []
den_id   = DEN_BASE
imp_id   = IMP_BASE

# Muestra de trabajadores para implicar (espaciado uniforme, 60 personas)
step       = max(1, len(all_workers) // 60)
sample_wks = all_workers[::step][:60]

for i in range(20):
    den_rows.append({
        "id":               den_id,
        "id_tipo_denuncia": TIPO_D_LIST[i],
        "id_estado_denuncia": ESTADO_D_LIST[i],
        "id_institucion":   INST_D_LIST[i],
        "descripcion":      DESC_LIST[i],
    })
    n_imp = 2 + (i % 2)
    for j in range(n_imp):
        t_idx = (i * 3 + j * 7) % len(sample_wks)
        _, p_id, _, _ = sample_wks[t_idx]
        imp_rows.append({
            "id":               imp_id,
            "id_denuncia":      den_id,
            "id_perfil_persona": p_id,
            "id_tipo_implicado": (j % 4) + 1,
        })
        imp_id += 1
    den_id += 1

print(f"  Declaracion_Patrimonial : {len(decl_rows)}")
print(f"  Pagos_Realizados        : {len(pago_rows)}")
print(f"  Denuncias               : {len(den_rows)}")
print(f"  Implicados              : {len(imp_rows)}")

write_sql(
    "06_insert_simulados.sql",
    "INSERT simulados → Declaracion_Patrimonial, Pagos_Realizados, Denuncia, Implicado",
    [
        ("Declaracion_Patrimonial — 40% de los trabajadores (simulado para BI)",
         build_inserts("Declaracion_Patrimonial", DECL_COLS, decl_rows)),
        ("Pagos_Realizados — ene-mar 2026, salario + bonificación incentivo",
         build_inserts("Pagos_Realizados", PAGO_COLS, pago_rows)),
        ("Denuncia — 20 casos sintéticos de corrupción en sector justicia",
         build_inserts("Denuncia", DEN_COLS, den_rows)),
        ("Implicado — 2-3 personas por denuncia",
         build_inserts("Implicado", IMP_COLS, imp_rows)),
    ],
)

# ══════════════════════════════════════════════════════════════════════════════
# INDICADORES desde Casos.csv y Procesos.csv
# ══════════════════════════════════════════════════════════════════════════════
print("\n[4] Indicadores desde Casos.csv y Procesos.csv")

ind_rows = []
ind_id   = IND_BASE

# Casos por departamento (PNC / Ministerio de Gobernación)
rows_casos = read_csv("Casos.csv")
for r in rows_casos:
    depto = r["departamento"].strip()
    for campo, sfx in [
        ("casos_totales",     "Total casos"),
        ("detenidos_hombres", "Detenidos hombres"),
        ("detenidos_mujeres", "Detenidos mujeres"),
    ]:
        try:
            val = round(float(r[campo]), 4)
        except (ValueError, TypeError, KeyError):
            continue
        ind_rows.append({
            "id":             ind_id,
            "anio":           2024,
            "fuente":         "PNC / Ministerio de Gobernación",
            "tipo_indicador": "Casos penales y detenidos por departamento",
            "valor":          val,
            "unidad":         f"{sfx} — {depto}",
        })
        ind_id += 1

# Procesos judiciales por tipo de delito (OJ)
rows_procesos = read_csv("Procesos.csv")
for r in rows_procesos:
    delito = r.get("delito", "").strip()
    if not delito:
        continue
    try:
        val = round(float(r.get("total", "0") or "0"), 4)
    except (ValueError, TypeError):
        continue
    ind_rows.append({
        "id":             ind_id,
        "anio":           2024,
        "fuente":         "Organismo Judicial — Procesos Judiciales",
        "tipo_indicador": "Procesos judiciales por tipo de delito",
        "valor":          val,
        "unidad":         delito,
    })
    ind_id += 1

ind_casos    = [r for r in ind_rows if "Casos penales"     in r["tipo_indicador"]]
ind_procesos = [r for r in ind_rows if "Procesos judiciales" in r["tipo_indicador"]]
print(f"  Indicadores (Casos)   : {len(ind_casos)}")
print(f"  Indicadores (Procesos): {len(ind_procesos)}")

write_sql(
    "07_insert_indicadores_casos.sql",
    "INSERT indicadores → Indicador_Externo (Casos PNC + Procesos OJ 2024)",
    [
        ("Indicador_Externo — Casos PNC 2024 por departamento",
         build_inserts("Indicador_Externo", IND_COLS, ind_casos)),
        ("Indicador_Externo — Procesos OJ 2024 por tipo de delito",
         build_inserts("Indicador_Externo", IND_COLS, ind_procesos)),
    ],
)

# ══════════════════════════════════════════════════════════════════════════════
# RESUMEN
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("RESUMEN")
print("=" * 60)
print(f"  Perfil_Persona          : {len(pp_rows):>7,}")
print(f"  Trabajador              : {len(trab_rows):>7,}")
print(f"  Cargo                   : {len(cargo_rows):>7,}")
print(f"  Contrato                : {len(cont_rows):>7,}")
print(f"  Declaracion_Patrimonial : {len(decl_rows):>7,}")
print(f"  Pagos_Realizados        : {len(pago_rows):>7,}")
print(f"  Denuncia                : {len(den_rows):>7,}")
print(f"  Implicado               : {len(imp_rows):>7,}")
print(f"  Indicador_Externo       : {len(ind_rows):>7,}")
print(f"\nArchivos generados en output/sql/:")
for f in [
    "04_insert_trabajadores_congreso.sql",
    "05_insert_trabajadores_oj.sql",
    "06_insert_simulados.sql",
    "07_insert_indicadores_casos.sql",
]:
    print(f"  {f}")
