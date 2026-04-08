USE dw_justicia;

-- ================================================================
-- KPI 1 (ajustado)
-- ================================================================
SELECT
    IFNULL(dt.anio, 0) AS anio,
    IFNULL(di.nombre, 'TOTAL') AS institucion,
    COUNT(*) AS total_contratos,
    ROUND(SUM(hc.monto_contrato), 2) AS monto_total_GTQ,
    SUM(hc.es_empresa_vinculada) AS contratos_vinculados,
    ROUND(100.0 * SUM(hc.es_empresa_vinculada) / COUNT(*), 1) AS pct_vinculado,
    ROUND(
        SUM(CASE WHEN hc.es_empresa_vinculada = 1
                 THEN hc.monto_contrato ELSE 0 END), 2
    ) AS monto_vinculado_GTQ
FROM Hecho_Contrato hc
JOIN Dim_Tiempo dt ON dt.sk_tiempo = hc.sk_tiempo
JOIN Dim_Institucion di ON di.sk_institucion = hc.sk_institucion
GROUP BY dt.anio, di.nombre WITH ROLLUP;

-- ================================================================
-- KPI 2 (OK en MySQL 8+)
-- ================================================================
SELECT
    de.nombre AS empresa,
    de.tipo_empresa,
    de.departamento,
    COUNT(*) AS total_contratos,
    SUM(hc.es_empresa_vinculada) AS contratos_con_vinculo,
    ROUND(SUM(hc.monto_contrato), 2) AS monto_total_GTQ,
    ROUND(SUM(hc.monto_acumulado_emp), 2) AS monto_acumulado_GTQ,
    ROUND(AVG(hc.grado_vinculacion), 2) AS grado_vinculacion_promedio,
    RANK() OVER (ORDER BY SUM(hc.monto_acumulado_emp) DESC) AS ranking_monto,
    NTILE(4) OVER (ORDER BY SUM(hc.monto_acumulado_emp) DESC) AS cuartil_riesgo
FROM Hecho_Contrato hc
JOIN Dim_Empresa de ON de.sk_empresa = hc.sk_empresa
WHERE hc.es_empresa_vinculada = 1
GROUP BY de.sk_empresa, de.nombre, de.tipo_empresa, de.departamento
LIMIT 20;

-- ================================================================
-- KPI 3 (ajustado)
-- ================================================================
SELECT
    IFNULL(td.nombre_delito, 'TOTAL') AS tipo_delito,
    IFNULL(dt.anio, 0) AS anio,
    COUNT(*) AS total_procesos,
    SUM(hp.fue_condenado) AS condenados,
    SUM(hp.fue_sobreseido) AS sobreseidos,
    SUM(hp.fue_archivado) AS archivados,
    SUM(hp.fue_apelado) AS apelados,
    ROUND(100.0 * (SUM(hp.fue_sobreseido) + SUM(hp.fue_archivado)) / COUNT(*), 1) AS tasa_impunidad_pct,
    ROUND(AVG(hp.dias_proceso), 0) AS dias_promedio_proceso,
    ROUND(AVG(hp.cantidad_audiencias), 1) AS audiencias_promedio
FROM Hecho_Proceso hp
JOIN Dim_Tipo_Delito td ON td.sk_tipo_delito = hp.sk_tipo_delito
JOIN Dim_Tiempo dt ON dt.sk_tiempo = hp.sk_tiempo
GROUP BY td.nombre_delito, dt.anio WITH ROLLUP;

-- ================================================================
-- KPI 4 (SIMULACIÓN de GROUPING SETS)
-- ================================================================
-- MySQL no soporta GROUPING SETS → usar UNION ALL

SELECT td.nombre_delito, di.nombre, ep.nombre, COUNT(*) AS procesos
FROM Hecho_Proceso hp
JOIN Dim_Tipo_Delito td ON td.sk_tipo_delito = hp.sk_tipo_delito
JOIN Dim_Institucion di ON di.sk_institucion = hp.sk_institucion
JOIN Dim_Estado_Proceso ep ON ep.sk_tipo_implicado = hp.sk_estado_proceso
GROUP BY td.nombre_delito, di.nombre, ep.nombre

UNION ALL

SELECT td.nombre_delito, di.nombre, NULL, COUNT(*)
FROM Hecho_Proceso hp
JOIN Dim_Tipo_Delito td ON td.sk_tipo_delito = hp.sk_tipo_delito
JOIN Dim_Institucion di ON di.sk_institucion = hp.sk_institucion
GROUP BY td.nombre_delito, di.nombre;

-- ================================================================
-- KPI 5 (ajustado)
-- ================================================================
SELECT
    IFNULL(di.nombre, 'TOTAL') AS institucion,
    IFNULL(dc.nombre, 'TOTAL') AS cargo,
    COUNT(*) AS declaraciones,
    ROUND(AVG(pat.sueldo), 2) AS sueldo_promedio,
    ROUND(AVG(pat.patrimonio_neto), 2) AS patrimonio_promedio,
    ROUND(AVG(pat.enriquecimiento), 2) AS enriquecimiento_promedio,
    ROUND(
        AVG(CASE WHEN pat.sueldo > 0
                 THEN pat.patrimonio_neto / pat.sueldo ELSE 0 END), 2
    ) AS ratio_pat_sueldo,
    SUM(CASE WHEN pat.enriquecimiento > 500000 THEN 1 ELSE 0 END) AS alertas
FROM Hecho_Patrimonio pat
JOIN Dim_Institucion di ON di.sk_institucion = pat.sk_institucion
JOIN Dim_Cargo dc ON dc.sk_cargo = pat.sk_cargo
GROUP BY di.nombre, dc.nombre WITH ROLLUP;

-- ================================================================
-- KPI 6 (ajustado)
-- ================================================================
SELECT
    dt.nombres_completos AS funcionario,
    dt.cargo_nombre,
    dt.institucion_nombre,
    dt.departamento,
    COUNT(*) AS num_declaraciones,
    ROUND(MAX(pat.sueldo), 2) AS sueldo_max,
    ROUND(MAX(pat.patrimonio_neto), 2) AS patrimonio_max,
    ROUND(MAX(pat.enriquecimiento), 2) AS enriquecimiento_max,
    ROUND(MAX(pat.patrimonio_neto) / NULLIF(MAX(pat.sueldo), 0), 1) AS ratio,
    PERCENT_RANK() OVER (ORDER BY MAX(pat.enriquecimiento)) AS percentil,
    CASE
        WHEN MAX(pat.enriquecimiento) > 1000000 THEN 'ROJO'
        WHEN MAX(pat.enriquecimiento) > 500000 THEN 'NARANJA'
        WHEN MAX(pat.enriquecimiento) > 100000 THEN 'AMARILLO'
        ELSE 'VERDE'
    END AS nivel_alerta
FROM Hecho_Patrimonio pat
JOIN Dim_Trabajador dt ON dt.sk_trabajador = pat.sk_trabajador
GROUP BY dt.sk_trabajador
LIMIT 15;

-- ================================================================
-- KPI 7 (ajustado)
-- ================================================================
SELECT
    IFNULL(di.nombre, 'TOTAL') AS institucion,
    IFNULL(dt.anio, 0) AS anio,
    COUNT(DISTINCT hn.sk_trabajador) AS titulares,
    COUNT(*) AS vinculos,
    SUM(hn.cantidad_familiares_sis) AS familiares,
    ROUND(SUM(hn.sueldo_titular), 2) AS costo_titular,
    ROUND(SUM(hn.sueldo_familiar), 2) AS costo_familiar,
    ROUND(SUM(hn.sueldo_titular + hn.sueldo_familiar), 2) AS costo_total
FROM Hecho_Nepotismo hn
JOIN Dim_Institucion di ON di.sk_institucion = hn.sk_institucion
JOIN Dim_Tiempo dt ON dt.sk_tiempo = hn.sk_tiempo
GROUP BY di.nombre, dt.anio WITH ROLLUP;

-- ================================================================
-- KPI 8 (SIMULADO con UNION)
-- ================================================================
SELECT
    dr.nombre,
    di.nombre,
    dtb.departamento,
    COUNT(*) AS vinculos,
    ROUND(SUM(hn.sueldo_titular + hn.sueldo_familiar), 2) AS costo_total
FROM Hecho_Nepotismo hn
JOIN Dim_Relacion dr ON dr.sk_relacion = hn.sk_relacion
JOIN Dim_Institucion di ON di.sk_institucion = hn.sk_institucion
JOIN Dim_Trabajador dtb ON dtb.sk_trabajador = hn.sk_trabajador
GROUP BY dr.nombre, di.nombre, dtb.departamento;