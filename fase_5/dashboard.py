"""
Dashboard — Proyecto Justicia GT
Ejecutar con:  streamlit run fase_5/dashboard.py
Dependencias:  pip install streamlit plotly pandas mysql-connector-python
"""

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

# ── Configuración de página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Proyecto Justicia GT",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer    {visibility: hidden;}
    body, [class*="css"] { font-family: Arial, sans-serif; }

    .header-box {
        background: #1A365D;
        padding: 28px 36px;
        border-radius: 10px;
        margin-bottom: 24px;
        text-align: center;
    }
    .header-box h1  { color: #FFFFFF; font-size: 2.2rem; margin: 0 0 6px; }
    .header-box p   { color: #BEE3F8; font-size: 1.0rem; margin: 0; }
    .header-box small { color: #90CDF4; font-size: 0.82rem; }

    [data-testid="stMetric"] {
        background: #EBF4FF;
        border-left: 5px solid #E53E3E;
        border-radius: 8px;
        padding: 12px 16px;
    }
</style>
""", unsafe_allow_html=True)


# ── Conexión con caché ─────────────────────────────────────────────────────────
@st.cache_resource
def get_dw_engine():
    return create_engine(
        "mysql+mysqlconnector://root:12345@localhost/dw_justicia?charset=utf8mb4"
    )


@st.cache_data(ttl=300)
def query(_engine, sql: str, params=None) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"Error en query: {e}")
        return pd.DataFrame()


try:
    conn = get_dw_engine()
except Exception as e:
    st.error(f"No se pudo conectar a dw_justicia: {e}")
    st.stop()


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="header-box">
  <h1>⚖️ Estado de la Justicia en Guatemala</h1>
  <p>Sistema de Business Intelligence — Análisis de corrupción, impunidad y redes de cooptación institucional</p>
  <small>Datos: Guatecompras OCDS &nbsp;|&nbsp; Nómina OJ &nbsp;|&nbsp; Nómina Congreso
         &nbsp;|&nbsp; INE Hechos Delictivos &nbsp;|&nbsp; Indicadores WJP</small>
</div>
""", unsafe_allow_html=True)


# ── Sidebar — Filtros globales ─────────────────────────────────────────────────
with st.sidebar:
    st.header("🔧 Filtros")

    anio_min, anio_max = st.slider("Período", 2018, 2026, (2020, 2026))

    # Solo instituciones del sector justicia y entidades del Estado relevantes
    df_inst = query(conn, """
        SELECT DISTINCT nombre FROM Dim_Institucion
        WHERE nombre REGEXP
            'JUDICIAL|MINISTERIO P|CONGRESO|PROCURADUR|FORENSES|RENAP|POLICIA|
             TRIBUNAL|CORTE|FISCALIA|GOBERNACI|DERECHOS HUMANOS|INACIF|
             MINISTERIO DE GOBERN|SEGURIDAD SOCIAL'
        ORDER BY nombre
    """)
    inst_opts = ["Todas"] + df_inst["nombre"].tolist() if not df_inst.empty else ["Todas"]
    inst_sel = st.multiselect("Institución (sector justicia)", inst_opts, default=["Todas"])

    df_delito = query(conn, "SELECT DISTINCT nombre_delito FROM Dim_Tipo_Delito ORDER BY nombre_delito")
    del_opts = ["Todos"] + df_delito["nombre_delito"].tolist() if not df_delito.empty else ["Todos"]
    del_sel = st.multiselect("Tipo de delito", del_opts, default=["Todos"])


def inst_where(alias: str = "i") -> str:
    if "Todas" in inst_sel or not inst_sel:
        return ""
    names = ", ".join(f"'{n}'" for n in inst_sel if n != "Todas")
    return f"AND {alias}.nombre IN ({names})"


def del_where(alias: str = "td") -> str:
    if "Todos" in del_sel or not del_sel:
        return ""
    names = ", ".join(f"'{n}'" for n in del_sel if n != "Todos")
    return f"AND {alias}.nombre_delito IN ({names})"


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Panorama General",
    "⚖️ Impunidad y Resoluciones",
    "💰 Patrimonio",
    "👥 Nepotismo",
    "📋 Contratos",
])

FOOTER_TEXT = (
    "**Datos reales:** Guatecompras OCDS (2020-2025), Nómina OJ (12,796 empleados), "
    "Nómina Congreso (929 empleados), INE Hechos Delictivos (2022-2024).  \n"
    "**Datos simulados:** Declaraciones patrimoniales y denuncias (generados de forma "
    "determinista para demostración).  \n"
    "Análisis de nepotismo basado en cruce de apellidos por restricción de acceso al RENAP "
    "(Ley 90-2005).  \n"
    "_Toda persona que figure es inocente hasta sentencia firme ejecutoriada._"
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Panorama General
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    col1, col2, col3, col4 = st.columns(4)

    # KPI 1 — Tasa de Impunidad
    df_imp = query(conn, """
        SELECT ROUND((SUM(fue_sobreseido)+SUM(fue_archivado))*100.0
               / NULLIF(COUNT(*),0), 1) AS val,
               COUNT(*) AS total
        FROM dw_justicia.Hecho_Proceso hp
        JOIN dw_justicia.Dim_Tiempo t ON hp.sk_tiempo = t.sk_tiempo
        WHERE t.anio BETWEEN %s AND %s
    """, (anio_min, anio_max))
    _tiene_procesos = not df_imp.empty and int(df_imp["total"].iloc[0] or 0) > 0
    val_imp = float(df_imp["val"].iloc[0]) if _tiene_procesos and df_imp["val"].iloc[0] is not None else 0.0
    col1.metric("Tasa de Impunidad", f"{val_imp}%" if _tiene_procesos else "Sin datos", delta="objetivo <30%")

    # KPI 2 — Contratos con Vinculación
    df_vinc = query(conn, """
        SELECT COUNT(*) AS val
        FROM dw_justicia.Hecho_Contrato hc
        JOIN dw_justicia.Dim_Tiempo t ON hc.sk_tiempo = t.sk_tiempo
        WHERE hc.es_empresa_vinculada = 1
          AND t.anio BETWEEN %s AND %s
    """, (anio_min, anio_max))
    _tiene_contratos = not df_vinc.empty and int(df_vinc["val"].iloc[0] or 0) >= 0
    val_vinc = int(df_vinc["val"].iloc[0] or 0) if _tiene_contratos else 0
    col2.metric("Contratos Vinculados", f"{val_vinc:,}")

    # KPI 3 — Ratio de Enriquecimiento Promedio
    df_enr = query(conn, """
        SELECT ROUND(AVG(enriquecimiento / NULLIF(sueldo*12, 0)), 2) AS val
        FROM dw_justicia.Hecho_Patrimonio hp
        JOIN dw_justicia.Dim_Tiempo t ON hp.sk_tiempo = t.sk_tiempo
        WHERE t.anio BETWEEN %s AND %s
          AND hp.sueldo > 0
    """, (anio_min, anio_max))
    val_enr = df_enr["val"].iloc[0] if not df_enr.empty and df_enr["val"].iloc[0] is not None else 0.0
    col3.metric("Ratio Enriquecimiento Prom.", f"{val_enr}x", delta="alerta >3x")

    # KPI 4 — Familiares en Nómina
    df_nep = query(conn, """
        SELECT COUNT(*) AS val
        FROM dw_justicia.Hecho_Nepotismo
        WHERE misma_institucion = 1
    """)
    val_nep = int(df_nep["val"].iloc[0]) if not df_nep.empty else 0
    col4.metric("Familiares en Nómina", f"{val_nep:,}")

    st.divider()

    # Gráfico IPC histórico
    ipc = {
        1998: 3.1, 1999: 3.2, 2000: 2.8, 2001: 2.9, 2002: 2.5,
        2003: 2.4, 2004: 2.2, 2005: 2.5, 2006: 2.6, 2007: 2.8,
        2008: 3.1, 2009: 3.4, 2010: 3.2, 2011: 2.7, 2012: 3.3,
        2013: 2.9, 2014: 3.2, 2015: 2.8, 2016: 2.8, 2017: 2.8,
        2018: 2.7, 2019: 2.6, 2020: 2.5, 2021: 2.5, 2022: 2.4,
        2023: 2.3, 2024: 2.5,
    }
    df_ipc = pd.DataFrame(ipc.items(), columns=["Año", "IPC"])
    fig_ipc = px.line(
        df_ipc, x="Año", y="IPC",
        title="Índice de Percepción de Corrupción — Guatemala (1998-2024)",
        template="plotly_white", height=420,
        color_discrete_sequence=["#E53E3E"],
    )
    for x_val, txt, color in [
        (2007, "Creación CICIG",      "#2B6CB0"),
        (2015, "Caso La Línea",       "#DD6B20"),
        (2019, "Expulsión CICIG",     "#E53E3E"),
        (2021, "Destitución Sandoval","#744210"),
    ]:
        fig_ipc.add_vline(x=x_val, line_dash="dash", line_color=color,
                          annotation_text=txt, annotation_position="top right")
    st.plotly_chart(fig_ipc, width='stretch')

    with st.expander("ℹ️ Fuentes y limitaciones"):
        st.markdown(FOOTER_TEXT)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Impunidad y Resoluciones
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader(
        "¿Existe un sesgo estadístico en las resoluciones judiciales "
        "según el tipo de implicado y el operador de justicia?"
    )

    df_check_p = query(conn, "SELECT COUNT(*) AS n FROM dw_justicia.Hecho_Proceso")
    _procesos_ok = not df_check_p.empty and int(df_check_p["n"].iloc[0] or 0) > 0

    if not _procesos_ok:
        st.warning(
            "⚠️ **Hecho_Proceso** no tiene registros.  \n"
            "Los 42 indicadores de Procesos.csv son valores agregados, no casos individuales.  \n"
            "Se muestran los indicadores disponibles en el ODS como alternativa."
        )
        df_ind = query(conn, """
            SELECT ie.anio,
                   CONCAT(IFNULL(ie.tipo_indicador,'Sin tipo'), ' — ', IFNULL(ie.fuente,'')) AS nombre_indicador,
                   ie.valor,
                   IFNULL(i.nombre, 'Sin institución') AS institución
            FROM ods_justicia.Indicador_Externo ie
            LEFT JOIN ods_justicia.Institucion i ON ie.id_institucion = i.id
            ORDER BY ie.anio DESC, ie.valor DESC
            LIMIT 50
        """)
        if not df_ind.empty:
            st.subheader("Indicadores disponibles en el ODS")
            fig_ind = px.bar(
                df_ind, x="nombre_indicador", y="valor", color="institución",
                barmode="group",
                title="Indicadores judiciales / delictivos (fuente: ODS)",
                template="plotly_white", height=460,
            )
            fig_ind.update_layout(xaxis_tickangle=-40)
            st.plotly_chart(fig_ind, width="stretch")
            st.dataframe(df_ind, width="stretch")
        else:
            st.info("No hay indicadores disponibles en el ODS para mostrar.")
        with st.expander("ℹ️ Fuentes y limitaciones"):
            st.markdown(FOOTER_TEXT)

    else:
        # Barras apiladas por año
        df_res = query(conn, """
            SELECT t.anio,
                   SUM(p.fue_condenado)  AS Condenados,
                   SUM(p.fue_sobreseido) AS Sobreseídos,
                   SUM(p.fue_archivado)  AS Archivados,
                   SUM(p.fue_apelado)    AS Apelados
            FROM dw_justicia.Hecho_Proceso p
            JOIN dw_justicia.Dim_Tiempo t ON p.sk_tiempo = t.sk_tiempo
            WHERE t.anio BETWEEN %s AND %s
            GROUP BY t.anio ORDER BY t.anio
        """, (anio_min, anio_max))

        if not df_res.empty:
            df_long = df_res.melt(
                id_vars="anio",
                value_vars=["Condenados", "Sobreseídos", "Archivados", "Apelados"],
                var_name="Resultado", value_name="Casos",
            )
            fig_res = px.bar(
                df_long, x="anio", y="Casos", color="Resultado",
                barmode="stack", title="Resoluciones judiciales por año",
                template="plotly_white", height=420,
                color_discrete_map={
                    "Condenados": "#2F855A", "Sobreseídos": "#DD6B20",
                    "Archivados": "#C53030", "Apelados":    "#2B6CB0",
                },
                labels={"anio": "Año"},
            )
            st.plotly_chart(fig_res, width="stretch")
        else:
            st.info("Sin datos de resoluciones para el período seleccionado.")

        # Ranking por institución
        df_rank = query(conn, """
            SELECT i.nombre AS Institución,
                   ROUND(SUM(p.fue_sobreseido)*100.0 / NULLIF(COUNT(*),0), 1) AS Tasa_Sobreseimiento,
                   COUNT(*) AS Total_Casos
            FROM dw_justicia.Hecho_Proceso p
            JOIN dw_justicia.Dim_Institucion i ON p.sk_institucion = i.sk_institucion
            JOIN dw_justicia.Dim_Tiempo t ON p.sk_tiempo = t.sk_tiempo
            WHERE t.anio BETWEEN %s AND %s
            GROUP BY i.nombre
            HAVING Total_Casos >= 2
            ORDER BY Tasa_Sobreseimiento DESC
            LIMIT 15
        """, (anio_min, anio_max))

        if not df_rank.empty:
            fig_rank = px.bar(
                df_rank, x="Tasa_Sobreseimiento", y="Institución",
                orientation="h",
                title="Sector justicia — tasa de sobreseimiento por institución (%)",
                template="plotly_white", height=480,
                color="Tasa_Sobreseimiento", color_continuous_scale="Reds",
                labels={"Tasa_Sobreseimiento": "% Sobreseimiento"},
            )
            st.plotly_chart(fig_rank, width="stretch")
        else:
            st.info("Sin datos de resoluciones por institución.")

        # Comparativa pre/post CICIG
        st.subheader("Comparativa: impunidad Pre vs. Post expulsión CICIG")
        col_pre, col_post = st.columns(2)

        def tasa_impunidad(a_min: int, a_max: int) -> float:
            df_ = query(conn, """
                SELECT ROUND((SUM(fue_sobreseido)+SUM(fue_archivado))*100.0
                       / NULLIF(COUNT(*), 0), 1) AS val
                FROM dw_justicia.Hecho_Proceso hp
                JOIN dw_justicia.Dim_Tiempo t ON hp.sk_tiempo = t.sk_tiempo
                WHERE t.anio BETWEEN %s AND %s
            """, (a_min, a_max))
            if df_.empty or df_["val"].iloc[0] is None:
                return 0.0
            return float(df_["val"].iloc[0])

        v_pre  = tasa_impunidad(2015, 2018)
        v_post = tasa_impunidad(2019, 2024)
        col_pre.metric("Tasa Impunidad Pre-CICIG (2015-18)",   f"{v_pre}%")
        col_post.metric("Tasa Impunidad Post-CICIG (2019-24)", f"{v_post}%",
                        delta=f"{v_post - v_pre:+.1f}%", delta_color="inverse")

        with st.expander("ℹ️ Fuentes y limitaciones"):
            st.markdown(FOOTER_TEXT)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Patrimonio
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader(
        "¿Qué funcionarios tienen crecimiento patrimonial desproporcionado "
        "respecto a su ingreso salarial?"
    )

    df_pat = query(conn, f"""
        SELECT w.nombres_completos,
               w.cargo_nombre,
               w.institucion_nombre,
               AVG(h.enriquecimiento / NULLIF(h.sueldo*12, 0)) AS ratio,
               AVG(h.sueldo)                                     AS sueldo_promedio,
               AVG(h.patrimonio_neto)                            AS patrimonio
        FROM dw_justicia.Hecho_Patrimonio h
        JOIN dw_justicia.Dim_Trabajador w ON h.sk_trabajador  = w.sk_trabajador
        JOIN dw_justicia.Dim_Tiempo t     ON h.sk_tiempo      = t.sk_tiempo
        WHERE t.anio BETWEEN %s AND %s
          AND h.sueldo > 0
        GROUP BY w.sk_trabajador, w.nombres_completos, w.cargo_nombre, w.institucion_nombre
    """, (anio_min, anio_max))

    if not df_pat.empty:
        df_pat["nivel_riesgo"] = df_pat["ratio"].apply(
            lambda r: "Alto (>3x)" if r > 3 else ("Medio (1-3x)" if r > 1 else "Normal (<1x)")
        )
        color_risk = {
            "Alto (>3x)":   "#C53030",
            "Medio (1-3x)": "#DD6B20",
            "Normal (<1x)": "#2F855A",
        }
        fig_scat = px.scatter(
            df_pat, x="sueldo_promedio", y="patrimonio",
            color="nivel_riesgo", color_discrete_map=color_risk,
            hover_data=["nombres_completos", "cargo_nombre",
                        "institucion_nombre", "ratio"],
            title="Patrimonio vs. Sueldo Mensual por Funcionario",
            labels={"sueldo_promedio": "Sueldo mensual (Q)",
                    "patrimonio":      "Patrimonio neto (Q)"},
            template="plotly_white", height=500,
        )
        fig_scat.add_vline(x=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_scat, width='stretch')

        # Top 10
        st.subheader("Top 10 — Mayor ratio de enriquecimiento")
        top10 = df_pat.nlargest(10, "ratio")[
            ["nombres_completos", "cargo_nombre", "institucion_nombre",
             "sueldo_promedio", "patrimonio", "ratio"]
        ].rename(columns={
            "nombres_completos": "Funcionario",
            "cargo_nombre":      "Cargo",
            "institucion_nombre":"Institución",
            "sueldo_promedio":   "Sueldo mensual (Q)",
            "patrimonio":        "Patrimonio neto (Q)",
            "ratio":             "Ratio enriquecimiento",
        })
        st.dataframe(
            top10.style.background_gradient(subset=["Ratio enriquecimiento"],
                                             cmap="RdYlGn_r"),
        )
    else:
        st.info("Sin datos de patrimonio para el período seleccionado.")

    with st.expander("ℹ️ Fuentes y limitaciones"):
        st.markdown(FOOTER_TEXT)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Nepotismo
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader(
        "¿Qué instituciones concentran mayor proporción de empleados "
        "con vínculos familiares dentro del sector justicia?"
    )

    df_nep2 = query(conn, f"""
        SELECT i.nombre                            AS Institución,
               COUNT(DISTINCT n.sk_trabajador)     AS Empleados_con_vínculo,
               SUM(n.misma_institucion)            AS Misma_institución,
               SUM(n.mismo_departamento)           AS Mismo_departamento,
               SUM(n.sueldo_titular + n.sueldo_familiar) AS Costo_total
        FROM dw_justicia.Hecho_Nepotismo n
        JOIN dw_justicia.Dim_Institucion i ON n.sk_institucion = i.sk_institucion
        GROUP BY i.nombre
        ORDER BY Empleados_con_vínculo DESC
        LIMIT 15
    """)

    if df_nep2.empty or df_nep2["Empleados_con_vínculo"].sum() == 0:
        st.info("""ℹ️ La tabla Hecho_Nepotismo está vacía o tiene pocos datos.
        Esto puede ocurrir porque:
        1. La tabla Familia del ODS no tiene registros reales (datos de parentesco
           requieren RENAP, el cual tiene acceso restringido por Ley 90-2005).
        2. El cruce por apellidos no encontró coincidencias suficientes.
        Consulta la documentación de la Fase I para más detalles sobre esta limitación.""")
    else:
        fig_nep = px.bar(
            df_nep2, x="Institución",
            y=["Misma_institución", "Mismo_departamento"],
            barmode="group", title="Vínculos familiares por institución",
            template="plotly_white", height=450,
            color_discrete_sequence=["#2C5282", "#E53E3E"],
            labels={"value": "Personas", "variable": "Ámbito"},
        )
        st.plotly_chart(fig_nep, width='stretch')

        # Costo de red familiar
        df_costo = df_nep2.sort_values("Costo_total", ascending=True).tail(15)
        df_costo["Costo_fmt"] = df_costo["Costo_total"].apply(
            lambda v: f"Q {v:,.0f}" if v else "Q 0"
        )
        fig_costo = px.bar(
            df_costo, x="Costo_total", y="Institución",
            orientation="h",
            title="Costo mensual estimado de la red familiar por institución",
            template="plotly_white", height=450,
            color_discrete_sequence=["#744210"],
            labels={"Costo_total": "Q (salarios acumulados)"},
        )
        st.plotly_chart(fig_costo, width='stretch')

    with st.expander("ℹ️ Fuentes y limitaciones"):
        st.markdown(FOOTER_TEXT)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Contratos
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader(
        "¿Cuánto dinero público fue adjudicado a empresas con posible "
        "conflicto de interés con funcionarios del sector justicia?"
    )

    # Verificar si hay datos de contratos en el DW
    df_check_c = query(conn, "SELECT COUNT(*) AS n FROM dw_justicia.Hecho_Contrato")
    _contratos_ok = not df_check_c.empty and int(df_check_c["n"].iloc[0] or 0) > 0

    if not _contratos_ok:
        st.warning(
            "⚠️ La tabla **Hecho_Contrato** no tiene registros vinculados a trabajadores.  \n"
            "Esto ocurre porque el cruce apellido-empresa entre los 13,725 trabajadores "
            "y las 272,952 empresas OCDS no encontró coincidencias en este ciclo de datos.  \n"
            "Se muestran a continuación los contratos cargados en el ODS directamente."
        )
        # Mostrar resumen de contratos ODS como alternativa
        df_ods_c = query(conn, """
            SELECT i.nombre AS Institución,
                   COUNT(*) AS Num_contratos,
                   SUM(IFNULL(c.sueldo_real, 0)) AS Monto_total
            FROM ods_justicia.Contrato c
            JOIN ods_justicia.Institucion i ON c.id_institucion = i.id
            WHERE i.nombre REGEXP 'JUDICIAL|CONGRESO|PROCURADUR|GOBERNACI|MINISTERIO|POLICIA|RENAP'
            GROUP BY i.nombre
            ORDER BY Monto_total DESC
            LIMIT 20
        """)
        if not df_ods_c.empty:
            df_ods_c["Monto_total"] = df_ods_c["Monto_total"].fillna(0)
            fig_ods = px.bar(
                df_ods_c, x="Monto_total", y="Institución",
                orientation="h",
                title="Contratos ODS — Top 20 instituciones sector justicia (monto total Q)",
                template="plotly_white", height=500,
                color="Monto_total", color_continuous_scale="Blues",
                text="Num_contratos",
            )
            st.plotly_chart(fig_ods, width="stretch")
            st.dataframe(df_ods_c, width="stretch")
        else:
            st.info("No se encontraron contratos para instituciones del sector justicia en el ODS.")
        with st.expander("ℹ️ Fuentes y limitaciones"):
            st.markdown(FOOTER_TEXT)

    else:  # _contratos_ok
        # Métricas resumen
        df_totales = query(conn, f"""
            SELECT
                COUNT(*)                                             AS total_contratos,
                SUM(monto_contrato)                                  AS monto_total,
                SUM(CASE WHEN es_empresa_vinculada=1 THEN 1 ELSE 0 END) AS vinculados
            FROM dw_justicia.Hecho_Contrato hc
            JOIN dw_justicia.Dim_Tiempo t ON hc.sk_tiempo = t.sk_tiempo
            WHERE t.anio BETWEEN %s AND %s
        """, (anio_min, anio_max))

        if not df_totales.empty:
            total_c = int(df_totales["total_contratos"].iloc[0] or 0)
            monto_t = float(df_totales["monto_total"].iloc[0] or 0)
            vinc_c  = int(df_totales["vinculados"].iloc[0] or 0)
            pct_r   = round(vinc_c / total_c * 100, 1) if total_c else 0.0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total contratos",    f"{total_c:,}")
            c2.metric("Monto total",        f"Q {monto_t:,.0f}")
            c3.metric("Contratos vinculados", f"{vinc_c:,}")
            c4.metric("% en riesgo",        f"{pct_r}%")

        st.divider()

        # Treemap
        df_tree = query(conn, f"""
            SELECT e.nombre                  AS Empresa,
                   e.tipo_empresa            AS Tipo,
                   SUM(c.monto_contrato)     AS Monto_total,
                   COUNT(*)                  AS Num_contratos,
                   MAX(c.grado_vinculacion)  AS Vinculacion
            FROM dw_justicia.Hecho_Contrato c
            JOIN dw_justicia.Dim_Empresa e ON c.sk_empresa = e.sk_empresa
            JOIN dw_justicia.Dim_Tiempo  t ON c.sk_tiempo  = t.sk_tiempo
            WHERE t.anio BETWEEN %s AND %s
            GROUP BY e.nombre, e.tipo_empresa, e.sk_empresa
            ORDER BY Monto_total DESC
            LIMIT 50
        """, (anio_min, anio_max))

        if not df_tree.empty:
            df_tree["nivel_vinculacion"] = df_tree["Vinculacion"].apply(
                lambda v: (
                    "Vínculo fuerte" if v == 2
                    else "Vínculo débil" if v == 1
                    else "Sin vínculo" if v == 0
                    else "Sin datos"
                )
            )
            color_vinc = {
                "Sin vínculo":    "#718096",
                "Vínculo débil":  "#D69E2E",
                "Vínculo fuerte": "#C53030",
                "Sin datos":      "#CBD5E0",
            }
            fig_tree = px.treemap(
                df_tree, path=["Tipo", "Empresa"], values="Monto_total",
                color="nivel_vinculacion", color_discrete_map=color_vinc,
                title="Distribución de contratos por empresa y grado de vinculación",
                template="plotly_white", height=550,
            )
            st.plotly_chart(fig_tree, width="stretch")

        # Serie temporal trimestral
        df_trim = query(conn, f"""
            SELECT CONCAT(t.anio,'-T',t.trimestre)                     AS Periodo,
                   SUM(c.monto_contrato)                               AS Monto_total,
                   SUM(CASE WHEN c.es_empresa_vinculada=1
                            THEN c.monto_contrato ELSE 0 END)          AS Monto_vinculado
            FROM dw_justicia.Hecho_Contrato c
            JOIN dw_justicia.Dim_Tiempo t ON c.sk_tiempo = t.sk_tiempo
            WHERE t.anio BETWEEN %s AND %s
            GROUP BY t.anio, t.trimestre
            ORDER BY t.anio, t.trimestre
        """, (anio_min, anio_max))

        if not df_trim.empty:
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=df_trim["Periodo"], y=df_trim["Monto_total"],
                name="Total adjudicado", line=dict(color="#2C5282"),
            ))
            fig_line.add_trace(go.Scatter(
                x=df_trim["Periodo"], y=df_trim["Monto_vinculado"],
                name="Con conflicto de interés",
                line=dict(color="#E53E3E", dash="dash"),
                fill="tozeroy", fillcolor="rgba(229,62,62,0.1)",
            ))
            fig_line.update_layout(
                title="Monto total vs. monto con conflicto de interés por trimestre",
                template="plotly_white", height=420,
                yaxis_title="Quetzales (Q)", xaxis_title="Período",
            )
            st.plotly_chart(fig_line, width="stretch")

        # Top 20 contratos vinculados
        df_top20 = query(conn, f"""
            SELECT e.nombre AS Empresa, i.nombre AS Institución,
                   c.monto_contrato AS Monto, c.duracion_dias AS Días,
                   c.grado_vinculacion AS Grado_vinculación, t.anio AS Año
            FROM dw_justicia.Hecho_Contrato c
            JOIN dw_justicia.Dim_Empresa     e ON c.sk_empresa    = e.sk_empresa
            JOIN dw_justicia.Dim_Institucion i ON c.sk_institucion = i.sk_institucion
            JOIN dw_justicia.Dim_Tiempo      t ON c.sk_tiempo      = t.sk_tiempo
            WHERE c.es_empresa_vinculada = 1
              AND t.anio BETWEEN %s AND %s
            ORDER BY c.monto_contrato DESC
            LIMIT 20
        """, (anio_min, anio_max))

        if not df_top20.empty:
            st.subheader("Top 20 contratos con mayor monto y vinculación")
            st.dataframe(df_top20, width="stretch")

        with st.expander("ℹ️ Fuentes y limitaciones"):
            st.markdown(FOOTER_TEXT)


if __name__ == "__main__":
    # Ejecutar con:  streamlit run fase_5/dashboard.py
    pass
