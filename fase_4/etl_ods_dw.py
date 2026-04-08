"""
ETL: ods_justicia → dw_justicia
================================
Ejecutar con:  python fase_3/etl_ods_dw.py
Dependencias:  pip install mysql-connector-python
"""

import mysql.connector
from mysql.connector import Error
from datetime import date, timedelta

# ── Configuración de conexiones ────────────────────────────────────────────────
ODS_CFG = dict(host="localhost", user="root", password="12345",
               database="ods_justicia", charset="utf8mb4")
DW_CFG  = dict(host="localhost", user="root", password="12345",
               database="dw_justicia",  charset="utf8mb4")

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo",  6: "Junio",   7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def bulk_insert(cursor, table: str, columns: list[str],
                rows: list[tuple], chunk: int = 500) -> int:
    """INSERT IGNORE en lotes. Devuelve el total de filas insertadas."""
    if not rows:
        return 0
    col_str   = ", ".join(columns)
    ph        = ", ".join(["%s"] * len(columns))
    sql       = f"INSERT IGNORE INTO {table} ({col_str}) VALUES ({ph})"
    inserted  = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i: i + chunk]
        cursor.executemany(sql, batch)
        inserted += cursor.rowcount
    return inserted


def run_step(label: str, fn, ods_conn, dw_conn):
    print(f"\n>>> Cargando {label}...")
    try:
        n = fn(ods_conn, dw_conn)
        print(f"[OK] {label}: {n} filas insertadas")
    except Exception as e:
        print(f"[ERROR] {label}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PASO 1 — Dim_Tiempo
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_tiempo(ods_conn, dw_conn) -> int:
    cur = dw_conn.cursor()
    rows = []
    current = date(2018, 1, 1)
    end     = date(2026, 12, 31)
    while current <= end:
        sk        = int(current.strftime("%Y%m%d"))
        trimestre = (current.month - 1) // 3 + 1
        semana    = current.isocalendar()[1]
        fin_sem   = current.weekday() >= 5
        rows.append((
            sk, current.isoformat(), current.year, trimestre,
            current.month, MESES_ES[current.month],
            semana, fin_sem,
        ))
        current += timedelta(days=1)
    n = bulk_insert(cur, "Dim_Tiempo",
                    ["sk_tiempo", "fecha", "anio", "trimestre", "mes",
                     "nombre_mes", "semana", "es_fin_semana"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 2 — Dim_Institucion
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_institucion(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            i.id                     AS id_ods,
            i.nombre,
            i.siglas,
            ti.nombre                AS tipo_institucion,
            ni.nombre                AS nivel_institucion,
            d.nombre                 AS departamento,
            m.nombre                 AS municipio
        FROM Institucion i
        LEFT JOIN Tipo_Institucion   ti ON i.id_tipo_institucion  = ti.id
        LEFT JOIN Nivel_Institucion  ni ON i.id_nivel_institucion = ni.id
        LEFT JOIN Municipio          m  ON i.id_municipio         = m.id
        LEFT JOIN Departamento       d  ON m.id_departamento      = d.id
    """)
    rows_src = ods.fetchall()
    ods.close()

    cur = dw_conn.cursor()
    rows = [
        (r["id_ods"], r["id_ods"], r["nombre"], r["siglas"],
         r["tipo_institucion"], r["nivel_institucion"],
         r["departamento"], r["municipio"])
        for r in rows_src
    ]
    n = bulk_insert(cur, "Dim_Institucion",
                    ["sk_institucion", "id_institucion_ods", "nombre", "siglas",
                     "tipo_institucion", "nivel_institucion",
                     "departamento", "municipio"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 3 — Dim_Cargo
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_cargo(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            c.id           AS id_ods,
            c.nombre,
            c.descripcion,
            c.sueldo_base,
            c.renglon_presupuestario,
            ni.nombre      AS nivel_cargo
        FROM Cargo c
        LEFT JOIN Nivel_Institucion ni ON c.id_nivel_cargo = ni.id
    """)
    rows_src = ods.fetchall()
    ods.close()

    cur = dw_conn.cursor()
    rows = [
        (r["id_ods"], r["id_ods"], r["nombre"], r["descripcion"],
         r["sueldo_base"], r["renglon_presupuestario"], r["nivel_cargo"])
        for r in rows_src
    ]
    n = bulk_insert(cur, "Dim_Cargo",
                    ["sk_cargo", "id_cargo_ods", "nombre", "descripcion",
                     "sueldo_base", "renglon_presupuestario", "nivel_cargo"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 4 — Dim_Trabajador
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_trabajador(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            t.id                                        AS id_trabajador_ods,
            CONCAT(pp.nombres,' ',pp.apellidos)         AS nombres_completos,
            c.nombre                                    AS cargo_nombre,
            ni.nombre                                   AS nivel_cargo,
            NULL                                        AS rol_proceso,
            i.nombre                                    AS institucion_nombre,
            ti.nombre                                   AS tipo_institucion,
            d.nombre                                    AS departamento,
            t.esta_activo
        FROM Trabajador t
        JOIN Perfil_Persona pp ON t.id_perfil_persona = pp.id
        LEFT JOIN (
            SELECT ct.id_trabajador, ct.id_cargo, ct.id_institucion,
                   ROW_NUMBER() OVER (PARTITION BY ct.id_trabajador
                                      ORDER BY ct.fecha_inicio DESC) AS rn
            FROM Contrato ct
        ) latest ON latest.id_trabajador = t.id AND latest.rn = 1
        LEFT JOIN Cargo              c  ON latest.id_cargo       = c.id
        LEFT JOIN Nivel_Institucion  ni ON c.id_nivel_cargo      = ni.id
        LEFT JOIN Institucion        i  ON latest.id_institucion  = i.id
        LEFT JOIN Tipo_Institucion   ti ON i.id_tipo_institucion  = ti.id
        LEFT JOIN Municipio          m  ON i.id_municipio         = m.id
        LEFT JOIN Departamento       d  ON m.id_departamento      = d.id
    """)
    rows_src = ods.fetchall()
    ods.close()

    cur = dw_conn.cursor()
    rows = [
        (r["id_trabajador_ods"], r["id_trabajador_ods"],
         r["nombres_completos"], r["cargo_nombre"], r["nivel_cargo"],
         r["rol_proceso"], r["institucion_nombre"], r["tipo_institucion"],
         r["departamento"], bool(r["esta_activo"]))
        for r in rows_src
    ]
    n = bulk_insert(cur, "Dim_Trabajador",
                    ["sk_trabajador", "id_trabajador_ods", "nombres_completos",
                     "cargo_nombre", "nivel_cargo", "rol_proceso",
                     "institucion_nombre", "tipo_institucion",
                     "departamento", "esta_activo"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 5 — Dim_Empresa
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_empresa(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            e.id                           AS id_empresa_ods,
            COALESCE(e.nit,
                     CONCAT('EMP-', e.id)) AS nombre,
            e.nit,
            te.nombre                      AS tipo_empresa,
            CONCAT(COALESCE(pp.nombres,''),
                   ' ',
                   COALESCE(pp.apellidos,''))  AS nombre_representante,
            e.fecha_registro               AS fecha_constitucion
        FROM Empresa e
        LEFT JOIN Tipo_Empresa   te ON e.id_tipo_empresa       = te.id
        LEFT JOIN Perfil_Persona pp ON e.id_perfil_persona_repr = pp.id
    """)
    rows_src = ods.fetchall()
    ods.close()

    cur = dw_conn.cursor()
    rows = [
        (r["id_empresa_ods"], r["id_empresa_ods"], r["nombre"], r["nit"],
         r["tipo_empresa"],
         r["nombre_representante"].strip() if r["nombre_representante"] else None,
         r["fecha_constitucion"], None)
        for r in rows_src
    ]
    n = bulk_insert(cur, "Dim_Empresa",
                    ["sk_empresa", "id_empresa_ods", "nombre", "nit",
                     "tipo_empresa", "nombre_representante",
                     "fecha_constitucion", "departamento"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 6 — Catálogos pequeños
# ══════════════════════════════════════════════════════════════════════════════
def load_catalogos(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    dw  = dw_conn.cursor()
    total = 0

    # 6a — Dim_Tipo_Delito
    ods.execute("SELECT id, nombre FROM Tipo_Denuncia")
    rows = [(r["id"], r["nombre"], None, False, False, None)
            for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Tipo_Delito",
                         ["sk_tipo_delito", "nombre_delito", "categoria",
                          "es_corrupto", "es_alto_impacto", "descripcion"], rows)

    # 6b — Dim_Tipo_Implicado
    ods.execute("SELECT id, nombre, descripcion FROM Tipo_Implicado")
    rows = [(r["id"], r["nombre"], r["descripcion"]) for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Tipo_Implicado",
                         ["sk_tipo_implicado", "nombre", "descripcion"], rows)

    # 6c — Dim_Estado_Proceso
    # La tabla Estado_Proceso puede no existir en el ODS; se usan Estado_Denuncia
    # como proxy con los mismos IDs.
    try:
        ods.execute("SELECT id, nombre FROM Estado_Proceso")
        rows = [(r["id"], r["nombre"]) for r in ods.fetchall()]
    except Error:
        ods.execute("SELECT id, nombre FROM Estado_Denuncia")
        rows = [(r["id"], r["nombre"]) for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Estado_Proceso",
                         ["sk_tipo_implicado", "nombre"], rows)

    # 6d — Dim_Tipo_Declaracion
    ods.execute("SELECT id, nombre FROM Tipo_Declaracion")
    rows = [(r["id"], r["nombre"]) for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Tipo_Declaracion",
                         ["sk_trabajo", "nombre"], rows)

    # 6e — Dim_Relacion
    ods.execute("SELECT id, nombre FROM Relacion")
    rows = [(r["id"], r["nombre"]) for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Relacion",
                         ["sk_relacion", "nombre"], rows)

    # 6f — Dim_Tipo_Contrato
    ods.execute("SELECT id, nombre FROM Tipo_Contrato")
    rows = [(r["id"], r["nombre"]) for r in ods.fetchall()]
    total += bulk_insert(dw, "Dim_Tipo_Contrato",
                         ["sk_tipo_contrato", "nombre"], rows)

    dw_conn.commit()
    ods.close()
    dw.close()
    return total


# ══════════════════════════════════════════════════════════════════════════════
# PASO 7 — Dim_Trabajador_Familiar
# ══════════════════════════════════════════════════════════════════════════════
def load_dim_trabajador_familiar(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            t.id                                   AS sk_relacion,
            t.id                                   AS id_trabajador_ods,
            CONCAT(pp.nombres,' ',pp.apellidos)    AS nombres_completos,
            c.nombre                               AS cargo_nombre,
            i.nombre                               AS institucion_nombre,
            d.nombre                               AS departamento
        FROM Trabajador t
        JOIN Perfil_Persona pp ON t.id_perfil_persona = pp.id
        LEFT JOIN (
            SELECT id_trabajador, id_cargo, id_institucion,
                   ROW_NUMBER() OVER (PARTITION BY id_trabajador
                                      ORDER BY fecha_inicio DESC) AS rn
            FROM Contrato
        ) lc ON lc.id_trabajador = t.id AND lc.rn = 1
        LEFT JOIN Cargo        c ON lc.id_cargo       = c.id
        LEFT JOIN Institucion  i ON lc.id_institucion  = i.id
        LEFT JOIN Municipio    m ON i.id_municipio     = m.id
        LEFT JOIN Departamento d ON m.id_departamento  = d.id
    """)
    rows_src = ods.fetchall()
    ods.close()

    cur = dw_conn.cursor()
    rows = [
        (r["sk_relacion"], r["id_trabajador_ods"], r["nombres_completos"],
         r["cargo_nombre"], r["institucion_nombre"], r["departamento"])
        for r in rows_src
    ]
    n = bulk_insert(cur, "Dim_Trabajador_Familiar",
                    ["sk_relacion", "id_trabajador_ods", "nombres_completos",
                     "cargo_nombre", "institucion_nombre", "departamento"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 8 — Hecho_Contrato
# ══════════════════════════════════════════════════════════════════════════════
def load_hecho_contrato(ods_conn, dw_conn) -> int:
    # Cargar lookup de apellidos de trabajadores
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT t.id, pp.apellidos
        FROM Trabajador t
        JOIN Perfil_Persona pp ON t.id_perfil_persona = pp.id
    """)
    trab_apellidos: dict[int, set[str]] = {}
    for r in ods.fetchall():
        parts = (r["apellidos"] or "").split()
        trab_apellidos[r["id"]] = {p for p in parts if len(p) >= 4}

    # Cargar apellidos del representante legal por empresa
    ods.execute("""
        SELECT e.id AS emp_id,
               COALESCE(pp.apellidos, '') AS apellidos_repr
        FROM Empresa e
        LEFT JOIN Perfil_Persona pp ON e.id_perfil_persona_repr = pp.id
    """)
    emp_repr: dict[int, set[str]] = {}
    for r in ods.fetchall():
        parts = (r["apellidos_repr"] or "").split()
        emp_repr[r["emp_id"]] = {p for p in parts if len(p) >= 4}

    # Métricas acumuladas por empresa
    ods.execute("""
        SELECT id_empresa,
               COUNT(*)           AS cantidad,
               SUM(sueldo_real)   AS monto_acum
        FROM Contrato
        WHERE id_empresa IS NOT NULL
        GROUP BY id_empresa
    """)
    emp_stats: dict[int, tuple] = {
        r["id_empresa"]: (r["cantidad"], r["monto_acum"])
        for r in ods.fetchall()
    }

    # Contrato principal
    ods.execute("""
        SELECT
            ct.id                AS sk_contrato,
            ct.fecha_inicio      AS fecha,
            ct.id_trabajador     AS sk_trabajador,
            ct.id_institucion    AS sk_institucion,
            ct.id_cargo          AS sk_cargo,
            ct.id_empresa        AS sk_empresa,
            ct.id_tipo_contrato  AS sk_tipo_contrato,
            ct.sueldo_real       AS monto_contrato,
            DATEDIFF(COALESCE(ct.fecha_fin, CURDATE()),
                     ct.fecha_inicio)  AS duracion_dias,
            ct.id_estado_contrato
        FROM Contrato ct
        WHERE ct.id_empresa IS NOT NULL
          AND ct.fecha_inicio IS NOT NULL
    """)
    rows_src = ods.fetchall()
    ods.close()

    # Cargar sk_empresa existentes en Dim_Empresa del DW
    dw_check = dw_conn.cursor()
    dw_check.execute("SELECT sk_empresa FROM Dim_Empresa")
    valid_emp = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_trabajador FROM Dim_Trabajador")
    valid_trab = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_cargo FROM Dim_Cargo")
    valid_cargo = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_institucion FROM Dim_Institucion")
    valid_inst = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_tipo_contrato FROM Dim_Tipo_Contrato")
    valid_tc = {r[0] for r in dw_check.fetchall()}
    dw_check.close()

    rows = []
    for r in rows_src:
        sk_emp  = r["sk_empresa"]
        sk_trab = r["sk_trabajador"]
        sk_inst = r["sk_institucion"]
        sk_carg = r["sk_cargo"]
        sk_tc   = r["sk_tipo_contrato"]

        if sk_emp not in valid_emp:
            continue
        if sk_trab not in valid_trab:
            continue
        if sk_inst not in valid_inst:
            sk_inst = next(iter(valid_inst), 1)
        if sk_carg not in valid_cargo:
            sk_carg = next(iter(valid_cargo), 1)
        if sk_tc not in valid_tc:
            sk_tc = next(iter(valid_tc), 1)

        sk_tiempo = int(r["fecha"].strftime("%Y%m%d")) if r["fecha"] else 20230101

        # Vinculación empresa-trabajador por apellidos
        t_aps  = trab_apellidos.get(sk_trab, set())
        e_aps  = emp_repr.get(sk_emp, set())
        common = t_aps & e_aps
        grado  = min(len(common), 2)
        vinc   = 1 if grado > 0 else 0

        cant, monto = emp_stats.get(sk_emp, (0, 0.0))

        rows.append((
            r["sk_contrato"], sk_tiempo, sk_trab, sk_inst, sk_carg,
            sk_emp, sk_tc, None,
            r["monto_contrato"], r["duracion_dias"],
            vinc, grado, cant, float(monto or 0),
        ))

    cur = dw_conn.cursor()
    n = bulk_insert(cur, "Hecho_Contrato",
                    ["sk_contrato", "sk_tiempo", "sk_trabajador", "sk_institucion",
                     "sk_cargo", "sk_empresa", "sk_tipo_contrato", "sk_relacion",
                     "monto_contrato", "duracion_dias",
                     "es_empresa_vinculada", "grado_vinculacion",
                     "cantidad_contratos_emp", "monto_acumulado_emp"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 9 — Hecho_Patrimonio
# ══════════════════════════════════════════════════════════════════════════════
def load_hecho_patrimonio(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)
    ods.execute("""
        SELECT
            dp.id                    AS sk_patrimonio,
            dp.fecha_presentacion    AS fecha,
            dp.id_trabajador         AS sk_trabajador,
            dp.id_tipo_declaracion   AS sk_tipo_declaracion,
            dp.ingresos              AS ingresos_declarados,
            dp.egresos               AS egresos_declarados,
            dp.activos               AS activos_declarados,
            dp.pasivos,
            dp.vehiculos             AS vehiculos_declarados,
            pr.pago_realizado,
            lc.id_institucion        AS sk_institucion,
            lc.id_cargo              AS sk_cargo,
            lc.sueldo_real
        FROM Declaracion_Patrimonial dp
        LEFT JOIN (
            SELECT id_trabajador, id_institucion, id_cargo,
                   AVG(sueldo_real) AS sueldo_real,
                   ROW_NUMBER() OVER (PARTITION BY id_trabajador
                                      ORDER BY MAX(fecha_inicio) DESC) AS rn
            FROM Contrato
            GROUP BY id_trabajador, id_institucion, id_cargo
        ) lc ON lc.id_trabajador = dp.id_trabajador AND lc.rn = 1
        LEFT JOIN (
            SELECT id_trabajador, AVG(pago_realizado) AS pago_realizado
            FROM Pagos_Realizados
            GROUP BY id_trabajador
        ) pr ON pr.id_trabajador = dp.id_trabajador
    """)
    rows_src = ods.fetchall()
    ods.close()

    # Cargar FKs válidas del DW
    dw_check = dw_conn.cursor()
    dw_check.execute("SELECT sk_trabajador FROM Dim_Trabajador")
    valid_trab = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_institucion FROM Dim_Institucion")
    valid_inst = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_cargo FROM Dim_Cargo")
    valid_cargo = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_trabajo FROM Dim_Tipo_Declaracion")
    valid_td = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_tiempo FROM Dim_Tiempo")
    valid_tiempo = {r[0] for r in dw_check.fetchall()}
    dw_check.close()

    fallback_inst  = next(iter(valid_inst),  1)
    fallback_cargo = next(iter(valid_cargo), 1)
    fallback_td    = next(iter(valid_td),    1)

    rows = []
    for r in rows_src:
        sk_trab = r["sk_trabajador"]
        if sk_trab not in valid_trab:
            continue

        sk_inst = r["sk_institucion"] if r["sk_institucion"] in valid_inst else fallback_inst
        sk_carg = r["sk_cargo"]       if r["sk_cargo"]       in valid_cargo else fallback_cargo
        sk_td   = r["sk_tipo_declaracion"] if r["sk_tipo_declaracion"] in valid_td else fallback_td

        if r["fecha"]:
            sk_tiempo = int(r["fecha"].strftime("%Y%m%d"))
        else:
            sk_tiempo = 20230101
        if sk_tiempo not in valid_tiempo:
            sk_tiempo = 20230101

        sueldo  = float(r["sueldo_real"] or 0)
        activos = float(r["activos_declarados"] or 0)
        pasivos = float(r["pasivos"] or 0)
        pat_neto    = activos - pasivos
        enriquec    = activos - (sueldo * 12)

        rows.append((
            r["sk_patrimonio"], sk_tiempo, sk_trab, sk_inst, sk_carg, sk_td,
            sueldo, r["pago_realizado"],
            r["ingresos_declarados"], r["egresos_declarados"],
            r["activos_declarados"], r["vehiculos_declarados"],
            pat_neto, enriquec,
        ))

    cur = dw_conn.cursor()
    n = bulk_insert(cur, "Hecho_Patrimonio",
                    ["sk_patrimonio", "sk_tiempo", "sk_trabajador",
                     "sk_institucion", "sk_cargo", "sk_tipo_declaracion",
                     "sueldo", "pago_realizado",
                     "ingresos_declarados", "egresos_declarados",
                     "activos_declarados", "vehiculos_declarados",
                     "patrimonio_neto", "enriquecimiento"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 10 — Hecho_Nepotismo
# ══════════════════════════════════════════════════════════════════════════════
def load_hecho_nepotismo(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)

    # Verificar si Familia tiene datos
    ods.execute("SELECT COUNT(*) AS cnt FROM Familia")
    fam_cnt = ods.fetchone()["cnt"]

    rows_out = []
    sk_nep   = 1
    hoy_sk   = int(date.today().strftime("%Y%m%d"))
    INST_TARGET = (5, 8, 10001)

    # Cargar FKs válidas del DW
    dw_check = dw_conn.cursor()
    dw_check.execute("SELECT sk_trabajador FROM Dim_Trabajador")
    valid_trab = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_relacion FROM Dim_Trabajador_Familiar")
    valid_fam = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_institucion FROM Dim_Institucion")
    valid_inst = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_cargo FROM Dim_Cargo")
    valid_cargo = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_relacion FROM Dim_Relacion")
    valid_rel = {r[0] for r in dw_check.fetchall()}
    dw_check.close()

    fallback_inst  = next(iter(valid_inst),  1)
    fallback_cargo = next(iter(valid_cargo), 1)

    if fam_cnt > 0:
        # Rama A — desde Familia del ODS
        ods.execute("""
            SELECT f.id, f.id_trabajador_uno, f.id_relacion, f.id_perfil_persona,
                   lc1.id_institucion, lc1.id_cargo, lc1.sueldo_real,
                   lc2.sueldo_real AS sueldo_fam
            FROM Familia f
            LEFT JOIN (
                SELECT id_trabajador, id_institucion, id_cargo, AVG(sueldo_real) AS sueldo_real,
                       ROW_NUMBER() OVER (PARTITION BY id_trabajador ORDER BY fecha_inicio DESC) AS rn
                FROM Contrato GROUP BY id_trabajador, id_institucion, id_cargo
            ) lc1 ON lc1.id_trabajador = f.id_trabajador_uno AND lc1.rn = 1
            LEFT JOIN (
                SELECT t.id AS t_id, AVG(ct.sueldo_real) AS sueldo_real
                FROM Trabajador t
                JOIN Contrato ct ON ct.id_trabajador = t.id
                JOIN Perfil_Persona pp ON t.id_perfil_persona = pp.id
                GROUP BY t.id
            ) lc2 ON lc2.t_id = f.id_trabajador_uno
        """)
        for r in ods.fetchall():
            t1   = r["id_trabajador_uno"]
            rel  = r["id_relacion"] if r["id_relacion"] in valid_rel else 1
            inst = r["id_institucion"] if r["id_institucion"] in valid_inst else fallback_inst
            carg = r["id_cargo"]       if r["id_cargo"]       in valid_cargo else fallback_cargo
            if t1 not in valid_trab:
                continue
            rows_out.append((
                sk_nep, hoy_sk, t1, inst, carg, t1, rel,
                0, 0, 0,
                float(r["sueldo_real"] or 0),
                float(r["sueldo_fam"]  or 0), 1,
            ))
            sk_nep += 1
    else:
        # Rama B — cruce por segundo apellido
        ods.execute("""
            SELECT t.id, pp.apellidos, pp.nombres,
                   lc.id_institucion, lc.id_cargo,
                   AVG(ct.sueldo_real) AS sueldo_real,
                   MIN(ct.fecha_inicio) AS fecha_inicio
            FROM Trabajador t
            JOIN Perfil_Persona pp ON t.id_perfil_persona = pp.id
            LEFT JOIN (
                SELECT id_trabajador, id_institucion, id_cargo,
                       ROW_NUMBER() OVER (PARTITION BY id_trabajador ORDER BY fecha_inicio DESC) AS rn
                FROM Contrato
            ) lc ON lc.id_trabajador = t.id AND lc.rn = 1
            LEFT JOIN Contrato ct ON ct.id_trabajador = t.id
            WHERE lc.id_institucion IN %s
            GROUP BY t.id, pp.apellidos, pp.nombres, lc.id_institucion, lc.id_cargo
        """, (INST_TARGET,))
        trab_data = ods.fetchall()

        # Indexar por segundo apellido (último token de apellidos)
        apellido2_idx: dict[str, list] = {}
        for r in trab_data:
            parts = (r["apellidos"] or "").split()
            if parts:
                ap2 = parts[-1]
                if len(ap2) >= 4:
                    apellido2_idx.setdefault(ap2, []).append(r)

        for ap2, grupo in apellido2_idx.items():
            if len(grupo) < 2:
                continue
            for i in range(len(grupo)):
                for j in range(i + 1, len(grupo)):
                    t1 = grupo[i]
                    t2 = grupo[j]
                    if t1["id"] not in valid_trab or t2["id"] not in valid_trab:
                        continue
                    if t2["id"] not in valid_fam:
                        continue
                    misma = (t1["id_institucion"] == t2["id_institucion"])
                    inst  = t1["id_institucion"] if t1["id_institucion"] in valid_inst else fallback_inst
                    carg  = t1["id_cargo"]       if t1["id_cargo"]       in valid_cargo else fallback_cargo
                    rel   = 1  # "Familiar" genérico
                    dias  = None
                    if t1["fecha_inicio"] and t2["fecha_inicio"]:
                        dias = abs((t1["fecha_inicio"] - t2["fecha_inicio"]).days)
                    rows_out.append((
                        sk_nep, hoy_sk, t1["id"], inst, carg, t2["id"], rel,
                        dias, bool(misma), 0,
                        float(t1["sueldo_real"] or 0),
                        float(t2["sueldo_real"] or 0), 1,
                    ))
                    sk_nep += 1

    ods.close()

    cur = dw_conn.cursor()
    n = bulk_insert(cur, "Hecho_Nepotismo",
                    ["sk_nepotismo", "sk_tiempo", "sk_trabajador",
                     "sk_institucion", "sk_cargo", "sk_trabajador_familiar",
                     "sk_relacion",
                     "dias_diferencia_ingreso", "misma_institucion",
                     "mismo_departamento", "sueldo_titular", "sueldo_familiar",
                     "cantidad_familiares_sis"], rows_out)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PASO 11 — Hecho_Proceso
# ══════════════════════════════════════════════════════════════════════════════
def load_hecho_proceso(ods_conn, dw_conn) -> int:
    ods = ods_conn.cursor(dictionary=True)

    # Verificar tablas opcionales
    def table_exists(cur, name: str) -> bool:
        cur.execute(
            "SELECT COUNT(*) AS c FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s", (name,)
        )
        return cur.fetchone()["c"] > 0

    has_pj  = table_exists(ods, "Proceso_Judicial")
    has_den = table_exists(ods, "Denuncia")

    if not has_pj or not has_den:
        ods.close()
        return 0

    # Generar hecho sintético a partir de Denuncia + Implicado
    ods.execute("""
        SELECT
            d.id                  AS sk_proceso,
            d.id_tipo_denuncia    AS sk_tipo_delito,
            COALESCE(im.id_tipo_implicado, 1) AS sk_tipo_implicado,
            COALESCE(d.id_estado_denuncia, 1) AS sk_estado_proceso,
            im.id_perfil_persona  AS pp_id,
            d.id_institucion      AS sk_institucion,
            d.id_tipo_denuncia    AS sk_tipo_contrato
        FROM Denuncia d
        LEFT JOIN Implicado im ON im.id_denuncia = d.id
        GROUP BY d.id, d.id_tipo_denuncia, im.id_tipo_implicado,
                 d.id_estado_denuncia, im.id_perfil_persona,
                 d.id_institucion, d.id_tipo_denuncia
    """)
    rows_src = ods.fetchall()

    # Buscar trabajador desde perfil_persona
    ods.execute("""
        SELECT t.id AS trab_id, t.id_perfil_persona
        FROM Trabajador t
    """)
    pp_to_trab: dict[int, int] = {r["id_perfil_persona"]: r["trab_id"]
                                   for r in ods.fetchall()}
    ods.close()

    # Cargar FKs válidas del DW
    dw_check = dw_conn.cursor()
    dw_check.execute("SELECT sk_trabajador FROM Dim_Trabajador")
    valid_trab = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_institucion FROM Dim_Institucion")
    valid_inst = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_tipo_delito FROM Dim_Tipo_Delito")
    valid_delito = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_tipo_implicado FROM Dim_Tipo_Implicado")
    valid_timp = {r[0] for r in dw_check.fetchall()}
    dw_check.execute("SELECT sk_tipo_implicado FROM Dim_Estado_Proceso")
    valid_ep = {r[0] for r in dw_check.fetchall()}
    dw_check.close()

    fallback_trab  = next(iter(valid_trab),  1)
    fallback_inst  = next(iter(valid_inst),  1)
    fallback_delito = next(iter(valid_delito), 1)
    fallback_timp  = next(iter(valid_timp),  1)
    fallback_ep    = next(iter(valid_ep),    1)

    sk_tiempo_hoy = int(date(2023, 6, 1).strftime("%Y%m%d"))

    rows = []
    seen = set()
    for r in rows_src:
        sk_proc = r["sk_proceso"]
        if sk_proc in seen:
            continue
        seen.add(sk_proc)

        sk_td   = r["sk_tipo_delito"]  if r["sk_tipo_delito"]  in valid_delito else fallback_delito
        sk_timp = r["sk_tipo_implicado"] if r["sk_tipo_implicado"] in valid_timp else fallback_timp
        sk_ep   = r["sk_estado_proceso"] if r["sk_estado_proceso"] in valid_ep else fallback_ep
        sk_inst = r["sk_institucion"]  if r["sk_institucion"]  in valid_inst else fallback_inst
        sk_trab = pp_to_trab.get(r["pp_id"], fallback_trab)
        if sk_trab not in valid_trab:
            sk_trab = fallback_trab

        rows.append((
            sk_proc, sk_tiempo_hoy, sk_trab, sk_inst,
            sk_td, sk_timp, sk_ep,
            None, 0,
            False, False, False, False,
        ))

    cur = dw_conn.cursor()
    n = bulk_insert(cur, "Hecho_Proceso",
                    ["sk_proceso", "sk_tiempo", "sk_trabajador",
                     "sk_institucion", "sk_tipo_delito", "sk_tipo_implicado",
                     "sk_estado_proceso",
                     "dias_proceso", "cantidad_audiencias",
                     "fue_condenado", "fue_sobreseido",
                     "fue_archivado", "fue_apelado"], rows)
    dw_conn.commit()
    cur.close()
    return n


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("ETL ODS → DW  |  dw_justicia")
    print("=" * 60)

    try:
        ods_conn = mysql.connector.connect(**ODS_CFG)
        dw_conn  = mysql.connector.connect(**DW_CFG)
        print("[OK] Conexiones establecidas")
    except Error as e:
        print(f"[FATAL] No se pudo conectar a MySQL: {e}")
        raise SystemExit(1)

    PASOS = [
        ("Dim_Tiempo",              load_dim_tiempo),
        ("Dim_Institucion",         load_dim_institucion),
        ("Dim_Cargo",               load_dim_cargo),
        ("Dim_Trabajador",          load_dim_trabajador),
        ("Dim_Empresa",             load_dim_empresa),
        ("Catálogos pequeños",      load_catalogos),
        ("Dim_Trabajador_Familiar", load_dim_trabajador_familiar),
        ("Hecho_Contrato",          load_hecho_contrato),
        ("Hecho_Patrimonio",        load_hecho_patrimonio),
        ("Hecho_Nepotismo",         load_hecho_nepotismo),
        ("Hecho_Proceso",           load_hecho_proceso),
    ]

    for label, fn in PASOS:
        run_step(label, fn, ods_conn, dw_conn)

    ods_conn.close()
    dw_conn.close()
    print("\n" + "=" * 60)
    print("ETL completado.")
    print("=" * 60)
