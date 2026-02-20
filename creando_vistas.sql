-- Crear schema bi si no existe
CREATE SCHEMA IF NOT EXISTS bi;

--1 Crear vista de resumen general
CREATE OR REPLACE VIEW bi.vw_dashboard_resumen AS
SELECT
    COALESCE(ds.nombre_sede, '>> TOTAL CONSOLIDADO <<') as cuenta,
    COUNT(DISTINCT fv.id_transaccion) as transacciones,
    SUM(fv.precio_bruto) as venta_bruta,
    COALESCE(SUM(ft.comision), 0) as comisiones_sumup,
    SUM(fv.precio_bruto) - COALESCE(SUM(ft.comision), 0) as liquido_a_recibir,
    ROUND(SUM(fv.precio_bruto) / NULLIF(COUNT(DISTINCT fv.id_transaccion), 0), 0) as ticket_promedio,
    SUM(fv.precio_neto) - COALESCE(SUM(ft.comision), 0) as margen_operativo_real
FROM dw.fact_ventas fv
JOIN dw.dim_sede ds ON fv.sede_sk = ds.sede_sk
LEFT JOIN dw.fact_transacciones ft ON fv.id_transaccion = ft.id_transaccion
GROUP BY ROLLUP(ds.nombre_sede)
ORDER BY (ds.nombre_sede IS NULL) ASC, venta_bruta DESC;


-- Probar la vista resumen general
SELECT * FROM bi.vw_dashboard_resumen LIMIT 10;

--2 analisis de propinas crear vector_spherical_distance
-- Crear vista para análisis de propinas por sede
CREATE OR REPLACE VIEW bi.vw_analisis_propinas AS
WITH propinas_base AS (
    SELECT 
        iv."ID_de_transacción",
        CASE
            WHEN iv."Sede_Normalizada" ILIKE '%plaza%bolsillo%' THEN 'Plaza Bolsillo'
            WHEN iv."Sede_Normalizada" ILIKE '%merced%' THEN 'Merced'
            WHEN iv."Sede_Normalizada" ILIKE '%tajamar%' THEN 'Tajamar'
            WHEN iv."Sede_Normalizada" ILIKE '%persa%' OR iv."Sede_Normalizada" ILIKE '%victor%manuel%' THEN 'Persa Victor Manuel'
            ELSE COALESCE(iv."Sede_Normalizada", 'Sede No Identificada')
        END AS sede,
        CAST(COALESCE(NULLIF(iv."Precio_Neto", ''), '0') AS NUMERIC) AS monto_propina
    FROM public.informe_ventas iv
    WHERE LOWER(TRIM(iv."Descripción")) = 'tip'
      AND iv."ID_de_transacción" IS NOT NULL
      AND iv."ID_de_transacción" != ''
),
transacciones_exitosas AS (
    SELECT DISTINCT
        t."ID_de_transacción",
        CASE
            WHEN LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%plaza.bolsillo%' OR LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%plaza%bolsillo%' THEN 'Plaza Bolsillo'
            WHEN LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%merced%' THEN 'Merced'
            WHEN LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%tajamar%' THEN 'Tajamar'
            WHEN LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%persa%' OR LOWER(COALESCE(t."Correo_electrónico", '')) ILIKE '%victor%manuel%' THEN 'Persa Victor Manuel'
            ELSE 'Sede No Identificada'
        END AS sede_transaccion
    FROM public.transacciones t
    WHERE LOWER(COALESCE(t."Estado", '')) = 'exitosa'
      AND t."ID_de_transacción" IS NOT NULL
      AND t."ID_de_transacción" != ''
)
SELECT
    COALESCE(pb.sede, te.sede_transaccion, 'No Identificada') AS sede_unificada,
    COUNT(DISTINCT te."ID_de_transacción") AS transacciones_totales,
    COUNT(DISTINCT pb."ID_de_transacción") AS transacciones_con_propina,
    ROUND(
        COUNT(DISTINCT pb."ID_de_transacción")::NUMERIC / 
        NULLIF(COUNT(DISTINCT te."ID_de_transacción"), 0) * 100, 
        2
    ) AS tasa_conversion_propina_pct,
    COALESCE(SUM(pb.monto_propina), 0) AS propinas_totales,
    ROUND(
        COALESCE(SUM(pb.monto_propina), 0) / 
        NULLIF(COUNT(DISTINCT pb."ID_de_transacción"), 0), 
        0
    ) AS propina_promedio
FROM transacciones_exitosas te
LEFT JOIN propinas_base pb ON te."ID_de_transacción" = pb."ID_de_transacción"
GROUP BY COALESCE(pb.sede, te.sede_transaccion, 'No Identificada')
ORDER BY tasa_conversion_propina_pct DESC;


-- Probar la vista
SELECT * FROM bi.vw_analisis_propinas LIMIT 10;

--3 vista de horas peak

-- Crear vista para análisis de horas pico
CREATE OR REPLACE VIEW bi.vw_horas_pico AS
SELECT
    EXTRACT(HOUR FROM CAST(iv."Fecha" AS TIMESTAMP)) AS hora_del_dia,
    COUNT(DISTINCT CASE 
        WHEN iv."Sede_Normalizada" ILIKE '%plaza%bolsillo%' 
        THEN iv."ID_de_transacción" 
    END) AS sede_plaza_bolsillo,
    COUNT(DISTINCT CASE 
        WHEN iv."Sede_Normalizada" ILIKE '%merced%' 
        THEN iv."ID_de_transacción" 
    END) AS sede_merced,
    COUNT(DISTINCT CASE 
        WHEN iv."Sede_Normalizada" ILIKE '%tajamar%' 
        THEN iv."ID_de_transacción" 
    END) AS sede_tajamar,
    COUNT(DISTINCT CASE 
        WHEN iv."Sede_Normalizada" ILIKE '%persa%victor%manuel%' 
        THEN iv."ID_de_transacción" 
    END) AS sede_persa_victor_manuel,
    COUNT(DISTINCT iv."ID_de_transacción") AS total_transacciones
FROM public.informe_ventas iv
WHERE iv."Descripción" NOT ILIKE '%tip%'
  AND iv."Descripción" NOT ILIKE '%importe personalizado%'
  AND iv."Fecha" IS NOT NULL
  AND iv."Fecha" != ''
  AND iv."Precio_Bruto" IS NOT NULL
  AND CAST(iv."Precio_Bruto" AS NUMERIC) > 0
GROUP BY EXTRACT(HOUR FROM CAST(iv."Fecha" AS TIMESTAMP))
ORDER BY hora_del_dia ASC;

-- Probar la vista
SELECT * FROM bi.vw_horas_pico ORDER BY hora_del_dia;

-- 4 Crear vista para análisis de fidelidad de clientes
CREATE OR REPLACE VIEW bi.vw_fidelidad_clientes AS
WITH ventas_limpias AS (
    SELECT 
        CASE 
            WHEN t."Correo_electrónico" ILIKE '%plaza%bolsillo%' THEN 'Plaza Bolsillo'
            WHEN t."Correo_electrónico" ILIKE '%merced%' THEN 'Merced'
            WHEN t."Correo_electrónico" ILIKE '%tajamar%' THEN 'Tajamar'
            WHEN t."Correo_electrónico" ILIKE '%persa%victor%manuel%' THEN 'Persa Victor Manuel'
            ELSE COALESCE(ds.nombre_sede, 'No Identificada')
        END AS nombre_sede,
        t."Últimos_4_dígitos" AS id_tarjeta,
        TO_CHAR(CAST(t."Fecha" AS DATE), 'YYYY-MM') AS mes_operacion,
        CAST(t."Fecha" AS DATE) AS fecha_dia
    FROM public.transacciones t
    LEFT JOIN public.informe_ventas iv ON t."ID_de_transacción" = iv."ID_de_transacción"
    LEFT JOIN dw.dim_sede ds ON ds.nombre_sede = iv."Sede_Normalizada"
    WHERE LOWER(COALESCE(t."Estado", '')) = 'exitosa' 
      AND t."Últimos_4_dígitos" IS NOT NULL 
      AND t."Últimos_4_dígitos" != ''
      AND t."Fecha" IS NOT NULL
),
comportamiento_mensual AS (
    SELECT 
        nombre_sede, 
        mes_operacion, 
        id_tarjeta, 
        COUNT(DISTINCT fecha_dia) AS dias_visitados_al_mes
    FROM ventas_limpias 
    GROUP BY nombre_sede, mes_operacion, id_tarjeta
)
SELECT 
    nombre_sede, 
    mes_operacion,
    COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 1 THEN id_tarjeta END) AS clientes_un_solo_dia,
    COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 2 THEN id_tarjeta END) AS clientes_recurrentes_2_veces,
    COUNT(DISTINCT CASE WHEN dias_visitados_al_mes > 2 THEN id_tarjeta END) AS clientes_fans_3_o_mas,
    ROUND(
        (COUNT(DISTINCT CASE WHEN dias_visitados_al_mes >= 2 THEN id_tarjeta END)::numeric / 
         NULLIF(COUNT(DISTINCT id_tarjeta), 0)) * 100, 
        2
    ) AS tasa_fidelidad_mes_pct
FROM comportamiento_mensual 
GROUP BY nombre_sede, mes_operacion 
ORDER BY mes_operacion DESC, nombre_sede;


-- Probar la vista
SELECT * FROM bi.vw_fidelidad_clientes ORDER BY mes_operacion DESC LIMIT 20;

--4 -- Crear vista para análisis de comportamiento de pago
-- Crear vista para comportamiento de pago (corregida)
CREATE OR REPLACE VIEW bi.vw_comportamiento_pago AS
WITH transacciones_validas AS (
    SELECT DISTINCT ON ("ID_de_transacción") 
        "ID_de_transacción",
        "Ejecutar_como",
        COALESCE(CAST(NULLIF("Comisión", '') AS NUMERIC), 0) AS comision
    FROM public.transacciones 
    WHERE LOWER(COALESCE("Estado", '')) IN ('exitosa', 'pagado')
      AND "ID_de_transacción" IS NOT NULL
      AND "ID_de_transacción" != ''
    ORDER BY "ID_de_transacción", 
             CASE WHEN "Ejecutar_como" IS NOT NULL THEN 0 ELSE 1 END,
             "Ejecutar_como" NULLS LAST
),
ventas_por_ticket AS (
    SELECT
        "ID_de_transacción",
        SUM(COALESCE(CAST(NULLIF("Precio_Bruto", '') AS NUMERIC), 0)) AS venta_total_bruta
    FROM public.informe_ventas
    WHERE LOWER(COALESCE("Descripción", '')) NOT ILIKE '%tip%' 
      AND LOWER(COALESCE("Descripción", '')) NOT ILIKE '%propina%'
      AND COALESCE(CAST(NULLIF("Precio_Bruto", '') AS NUMERIC), 0) > 0
    GROUP BY "ID_de_transacción"
),
consolidado AS (
    SELECT
        CASE
            WHEN UPPER(COALESCE(t."Ejecutar_como", '')) IN ('DEBIT', 'DEBITO') THEN 'Débito'
            WHEN UPPER(COALESCE(t."Ejecutar_como", '')) IN ('CREDIT', 'CREDITO') THEN 'Crédito'
            ELSE 'Efectivo'
        END AS medio_pago_limpio,
        v.venta_total_bruta,
        COALESCE(t.comision, 0) AS comision
    FROM ventas_por_ticket v
    INNER JOIN transacciones_validas t ON v."ID_de_transacción" = t."ID_de_transacción"
    WHERE v.venta_total_bruta > 0  -- Solo transacciones con venta
),
agrupacion_final AS (
    SELECT
        medio_pago_limpio,
        COUNT(*) AS total_transacciones,
        SUM(venta_total_bruta) AS ventas_totales,
        SUM(comision) AS comision_total
    FROM consolidado
    GROUP BY ROLLUP(medio_pago_limpio)
),
totales AS (
    SELECT 
        MAX(CASE WHEN medio_pago_limpio IS NULL THEN total_transacciones END) AS total_transacciones_global,
        MAX(CASE WHEN medio_pago_limpio IS NULL THEN ventas_totales END) AS ventas_totales_global,
        MAX(CASE WHEN medio_pago_limpio IS NULL THEN comision_total END) AS comision_total_global
    FROM agrupacion_final
)
SELECT
    COALESCE(medio_pago_limpio, 'TOTAL GENERAL') AS medio_de_pago,
    COALESCE(total_transacciones, 0) AS total_transacciones,
    CASE 
        WHEN COALESCE((SELECT total_transacciones_global FROM totales), 0) > 0 THEN
            ROUND(
                COALESCE(total_transacciones, 0)::numeric * 100.0 / 
                (SELECT total_transacciones_global FROM totales), 
                2
            )
        ELSE 0
    END AS participacion_transacciones_pct,
    COALESCE(ventas_totales, 0) AS ventas_totales,
    CASE 
        WHEN COALESCE((SELECT ventas_totales_global FROM totales), 0) > 0 THEN
            ROUND(
                COALESCE(ventas_totales, 0) * 100.0 / 
                (SELECT ventas_totales_global FROM totales), 
                2
            )
        ELSE 0
    END AS participacion_ventas_pct,
    COALESCE(comision_total, 0) AS comision_total,
    CASE 
        WHEN COALESCE(ventas_totales, 0) > 0 THEN
            ROUND(
                COALESCE(comision_total, 0) * 100.0 / COALESCE(ventas_totales, 1), 
                2
            )
        ELSE 0
    END AS tasa_comision_pct
FROM agrupacion_final
CROSS JOIN totales
ORDER BY (medio_pago_limpio IS NULL) ASC, COALESCE(ventas_totales, 0) DESC;


-- Probar la vista
SELECT * FROM bi.vw_comportamiento_pago;


-- 4 Crear vista para análisis de productos top (corregida)
-- Crear vista para análisis de productos top (con normalización corregida)
-- Eliminar la vista existente primero
DROP VIEW IF EXISTS bi.vw_productos_top;

-- Crear la nueva vista con la estructura correcta
CREATE OR REPLACE VIEW bi.vw_productos_top AS
WITH total_real_empresa AS (
    SELECT 
        SUM(fv.precio_bruto) as gran_total_dinero,
        COUNT(DISTINCT fv.id_transaccion) as gran_total_tickets
    FROM dw.fact_ventas fv
    WHERE fv.precio_bruto > 0
),
productos_agrupados AS (
    SELECT 
        dp.descripcion AS producto,
        dp.categoria,
        SUM(fv.cantidad) AS unidades_vendidas,
        SUM(fv.precio_bruto) AS ventas_brutas,
        COUNT(DISTINCT fv.id_transaccion) AS tickets_unicos,
        CASE 
            WHEN SUM(fv.cantidad) > 0 THEN
                ROUND(SUM(fv.precio_bruto) / SUM(fv.cantidad), 2)
            ELSE 0
        END AS precio_promedio_unitario
    FROM dw.fact_ventas fv
    JOIN dw.dim_producto dp ON fv.producto_sk = dp.producto_sk
    WHERE fv.precio_bruto > 0
      AND LOWER(dp.descripcion) NOT ILIKE '%tip%'
      AND LOWER(dp.descripcion) NOT ILIKE '%propina%'
      AND LOWER(dp.descripcion) NOT ILIKE '%Importe personalizado%'
    GROUP BY dp.descripcion, dp.categoria
)
SELECT 
    pa.producto,
    pa.categoria,
    pa.unidades_vendidas,
    pa.ventas_brutas,
    pa.precio_promedio_unitario,
    CASE 
        WHEN (SELECT COALESCE(gran_total_dinero, 1) FROM total_real_empresa) > 0 THEN
            ROUND((pa.ventas_brutas / (SELECT gran_total_dinero FROM total_real_empresa)) * 100, 2)
        ELSE 0
    END AS share_ventas_pct,
    CASE 
        WHEN (SELECT COALESCE(gran_total_tickets, 1) FROM total_real_empresa) > 0 THEN
            ROUND((pa.tickets_unicos::numeric / (SELECT gran_total_tickets FROM total_real_empresa)) * 100, 2)
        ELSE 0
    END AS tasa_penetracion_pct,
    pa.tickets_unicos
FROM productos_agrupados pa
ORDER BY pa.ventas_brutas DESC
LIMIT 100;

-- Probar la vista
SELECT * FROM bi.vw_productos_top LIMIT 10;
