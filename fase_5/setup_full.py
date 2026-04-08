"""
setup_full.py — Carga completa del pipeline Justicia GT
========================================================
Ejecutar desde la raíz del proyecto:
    & "C:/Program Files/Python312/python.exe" setup_full.py

Pasos que realiza:
  1. Crea y estructura la base ods_justicia (DDL)
  2. Carga los catálogos e inserts en orden (SQL files)
  3. Ejecuta el ETL de fuentes OCDS/RGAE/INE  (etl_ods_completo.py)
  4. Ejecuta el ETL de trabajadores CSV        (ods_trabajadores.py)
  5. Crea la base dw_justicia (DDL)
  6. Ejecuta el ETL ODS → DW                  (etl_ods_dw.py)
"""

import subprocess
import sys
import os
import mysql.connector
from mysql.connector import Error

# ── Configuración ──────────────────────────────────────────────────────────────
PYTHON   = sys.executable          # mismo intérprete que ejecuta este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_CFG = dict(host="localhost", user="root", password="12345", charset="utf8mb4")

# ── Helpers ────────────────────────────────────────────────────────────────────
def titulo(texto: str):
    linea = "=" * 60
    print(f"\n{linea}\n  {texto}\n{linea}")

def ok(msg: str):   print(f"  [OK]  {msg}")
def err(msg: str):  print(f"  [ERR] {msg}")
def info(msg: str): print(f"  ...   {msg}")


def ejecutar_sql_file(filepath: str, database: str | None = None):
    """Ejecuta un archivo .sql usando mysql CLI si está disponible,
    o leyendo y ejecutando sentencias con el conector Python."""
    if not os.path.exists(filepath):
        err(f"Archivo no encontrado: {filepath}")
        return False

    cfg = {**DB_CFG}
    if database:
        cfg["database"] = database

    try:
        conn = mysql.connector.connect(**cfg)
        cursor = conn.cursor()
        with open(filepath, encoding="utf-8") as f:
            sql_raw = f.read()

        # Separar por ";" respetando multi-línea; omitir vacíos
        sentencias = [s.strip() for s in sql_raw.split(";") if s.strip()]
        for stmt in sentencias:
            try:
                cursor.execute(stmt)
                conn.commit()
            except Error as e:
                # Ignorar errores de duplicado o "ya existe"
                code = e.errno
                if code in (1007, 1050, 1062, 1060):   # DB existe, tabla existe, dup key, col existe
                    continue
                err(f"SQL warning ({filepath}): {e}")

        cursor.close()
        conn.close()
        ok(f"{os.path.basename(filepath)}")
        return True
    except Error as e:
        err(f"Conexión fallida al ejecutar {filepath}: {e}")
        return False


def ejecutar_script(ruta_relativa: str) -> bool:
    """Ejecuta un script Python con el intérprete actual."""
    abs_path = os.path.join(BASE_DIR, ruta_relativa)
    if not os.path.exists(abs_path):
        err(f"Script no encontrado: {abs_path}")
        return False

    info(f"Ejecutando {ruta_relativa} ...")
    result = subprocess.run(
        [PYTHON, abs_path],
        cwd=os.path.dirname(abs_path),
    )
    if result.returncode == 0:
        ok(ruta_relativa)
        return True
    else:
        err(f"{ruta_relativa} terminó con código {result.returncode}")
        return False


def probar_conexion() -> bool:
    try:
        conn = mysql.connector.connect(**DB_CFG)
        conn.close()
        ok("Conexión a MySQL exitosa (root@localhost)")
        return True
    except Error as e:
        err(f"No se pudo conectar a MySQL: {e}")
        print("\n  Verifica que MySQL esté corriendo y que las credenciales sean correctas.")
        print("  Configuración actual: host=localhost  user=root  password=12345\n")
        return False


# ── Pipeline ───────────────────────────────────────────────────────────────────
def main():
    print("\n╔══════════════════════════════════════════════════════════╗")
    print("║        PIPELINE COMPLETO — Proyecto Justicia GT         ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # ── 0. Verificar conexión ──────────────────────────────────────────────────
    titulo("0. Verificar conexión a MySQL")
    if not probar_conexion():
        sys.exit(1)

    # ── 1. DDL ODS ─────────────────────────────────────────────────────────────
    titulo("1. Crear estructura ODS (ods_justicia)")
    ejecutar_sql_file(os.path.join(BASE_DIR, "fase_2", "create_ods.sql"))

    # ── 2. Catálogos e inserts SQL ODS ─────────────────────────────────────────
    titulo("2. Cargar catálogos e inserts ODS")
    sql_files = [
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "00_catalogos_base.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "00b_geografia.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "01_insert_contratos_ocds.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "02_insert_proveedores.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "03_insert_indicadores_ine.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "04_insert_trabajadores_congreso.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "05_insert_trabajadores_oj.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "06_insert_simulados.sql"),
        os.path.join(BASE_DIR, "fase_2", "output", "sql", "07_insert_indicadores_casos.sql"),
    ]
    for f in sql_files:
        ejecutar_sql_file(f, database="ods_justicia")

    # ── 3. ETL fuentes (OCDS / RGAE / INE) ────────────────────────────────────
    titulo("3. ETL fuentes OCDS / RGAE / INE")
    info("Este paso puede tardar varios minutos con archivos grandes.")
    ejecutar_script(os.path.join("fase_2", "etl", "etl_ods_completo.py"))

    # Cargar los SQL generados por etl_ods_completo si existen
    for fname in ["01_insert_contratos_ocds.sql", "02_insert_proveedores.sql",
                  "03_insert_indicadores_ine.sql"]:
        ruta = os.path.join(BASE_DIR, "fase_2", "output", "sql", fname)
        if os.path.exists(ruta):
            ejecutar_sql_file(ruta, database="ods_justicia")

    # ── 4. ETL trabajadores (csv_buenos) ──────────────────────────────────────
    titulo("4. ETL trabajadores (Congreso / OJ / simulados)")
    ejecutar_script(os.path.join("fase_2", "etl", "ods_trabajadores.py"))

    # Cargar los SQL generados por ods_trabajadores si existen
    for fname in ["04_insert_trabajadores_congreso.sql", "05_insert_trabajadores_oj.sql",
                  "06_insert_simulados.sql", "07_insert_indicadores_casos.sql"]:
        ruta = os.path.join(BASE_DIR, "fase_2", "output", "sql", fname)
        if os.path.exists(ruta):
            ejecutar_sql_file(ruta, database="ods_justicia")

    # ── 5. DDL DW ──────────────────────────────────────────────────────────────
    titulo("5. Crear estructura DW (dw_justicia)")
    # Primero crear la base de datos
    try:
        conn = mysql.connector.connect(**DB_CFG)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS dw_justicia CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        conn.commit()
        cursor.close()
        conn.close()
        ok("Base de datos dw_justicia creada/verificada")
    except Error as e:
        err(f"No se pudo crear dw_justicia: {e}")

    ejecutar_sql_file(os.path.join(BASE_DIR, "fase_3", "create_dw.sql"), database="dw_justicia")

    # ── 6. ETL ODS → DW ────────────────────────────────────────────────────────
    titulo("6. ETL ODS → DW")
    ejecutar_script(os.path.join("fase_3", "etl_ods_dw.py"))

    # ── Resumen final ──────────────────────────────────────────────────────────
    titulo("PIPELINE COMPLETADO")
    print("  Para iniciar el dashboard ejecuta:")
    print(f'\n  & "{PYTHON}" -m streamlit run fase_5/dashboard.py\n')


if __name__ == "__main__":
    main()
