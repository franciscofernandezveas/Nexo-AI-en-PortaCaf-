-- ticket promedio hoy vs ayer, total vendido hoy vs ayer.


WITH fechas_con_actividad AS (
  -- 1. Buscamos las fechas que realmente tienen datos (evitamos el "hoy" vacío)
  SELECT 
    fecha_key
  FROM dw.fact_ventas
  GROUP BY fecha_key
  -- Filtro de seguridad: Solo días con más de 10 transacciones (ajustable)
  -- Esto evita que si hoy recién abren, se tome como un día de tendencia a la baja
  HAVING COUNT(DISTINCT id_transaccion) > 10 
     AND SUM(precio_bruto) > 0
  ORDER BY fecha_key DESC
  LIMIT 2
),
metricas_base AS (
  -- 2. Calculamos métricas solo para esas 2 fechas detectadas
  SELECT
    COALESCE(ds.nombre_sede, '>> TOTAL CONSOLIDADO <<') as cuenta,
    fv.fecha_key,
    DENSE_RANK() OVER (ORDER BY fv.fecha_key DESC) as rango_fecha, -- 1 es lo más reciente, 2 es el anterior
    SUM(fv.precio_bruto) as ventas_totales,
    COUNT(DISTINCT fv.id_transaccion) as transacciones
  FROM dw.fact_ventas fv
  JOIN dw.dim_sede ds ON fv.sede_sk = ds.sede_sk
  WHERE fv.fecha_key IN (SELECT fecha_key FROM fechas_con_actividad)
  GROUP BY ROLLUP(ds.nombre_sede), fv.fecha_key
),
pivot_final AS (
  -- 3. Pivotamos los datos para tener hoy y ayer en la misma fila
  SELECT 
    cuenta,
    MAX(CASE WHEN rango_fecha = 1 THEN (ventas_totales / NULLIF(transacciones, 0)) END) as ticket_hoy,
    MAX(CASE WHEN rango_fecha = 2 THEN (ventas_totales / NULLIF(transacciones, 0)) END) as ticket_ayer,
    MAX(CASE WHEN rango_fecha = 1 THEN ventas_totales END) as total_vendido_hoy,
    MAX(CASE WHEN rango_fecha = 2 THEN ventas_totales END) as total_vendido_ayer
  FROM metricas_base
  GROUP BY cuenta
)
SELECT 
    cuenta,
    ROUND(ticket_hoy, 0) as ticket_hoy,
    ROUND(ticket_ayer, 0) as ticket_ayer,
    total_vendido_hoy,
    total_vendido_ayer,
    ROUND(((ticket_hoy - ticket_ayer) / NULLIF(ticket_ayer, 0)) * 100, 2) as variacion_ticket,
    CASE 
        WHEN ticket_ayer IS NULL OR ticket_hoy IS NULL THEN 'Datos insuficientes'
        WHEN ((ticket_hoy - ticket_ayer) / NULLIF(ticket_ayer, 0)) * 100 > 5 THEN '🔼 Creciendo'
        WHEN ((ticket_hoy - ticket_ayer) / NULLIF(ticket_ayer, 0)) * 100 < -5 THEN '🔽 Disminuyendo'
        ELSE '⚖️ Estable'
    END as estado_tendencia
FROM pivot_final
ORDER BY (cuenta = '>> TOTAL CONSOLIDADO <<') DESC, ticket_hoy DESC;



-- query 2 Ventas mes vs meta

WITH parametros AS (
  -- Parámetros de configuración
  SELECT 
    20000000 AS meta_mensual,  -- Ajusta según tu meta real
    EXTRACT(DAY FROM CURRENT_DATE) AS dia_actual,
    EXTRACT(DAY FROM DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day') AS dias_del_mes
),

ventas_mes_actual AS (
  SELECT 
    COALESCE(ds.nombre_sede, '>> TOTAL CONSOLIDADO <<') as cuenta,
    SUM(fv.precio_bruto) as ventas_mes_actual
  FROM dw.fact_ventas fv
  JOIN dw.dim_sede ds ON fv.sede_sk = ds.sede_sk
  WHERE DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY ROLLUP(ds.nombre_sede)
),

calculo_cumplimiento AS (
  SELECT 
    v.cuenta,
    v.ventas_mes_actual,
    p.meta_mensual,
    p.dia_actual,
    p.dias_del_mes,
    CASE 
      WHEN p.meta_mensual * (p.dia_actual::DECIMAL / p.dias_del_mes::DECIMAL) > 0 
      THEN v.ventas_mes_actual / (p.meta_mensual * (p.dia_actual::DECIMAL / p.dias_del_mes::DECIMAL))
      ELSE 0 
    END as ratio_cumplimiento
  FROM ventas_mes_actual v
  CROSS JOIN parametros p
  WHERE v.ventas_mes_actual IS NOT NULL
)

SELECT 
  cc.cuenta,
  ROUND(cc.ventas_mes_actual, 0) as ventas_mes_actual,
  cc.meta_mensual,
  cc.dia_actual::INTEGER as dia_actual,
  cc.dias_del_mes::INTEGER as dias_del_mes,
  ROUND(cc.ratio_cumplimiento, 2) as ratio_cumplimiento,
  CASE
    WHEN cc.ratio_cumplimiento >= 1.10 THEN '🟢 SOBRE_META'
    WHEN cc.ratio_cumplimiento >= 0.90 THEN '🟡 EN_LINEA'
    WHEN cc.ratio_cumplimiento < 0.90 THEN '🔴 BAJO_META'
    ELSE 'Sin datos'
  END as estado_cumplimiento
FROM calculo_cumplimiento cc
ORDER BY 
  (cc.cuenta = '>> TOTAL CONSOLIDADO <<') DESC,
  cc.ventas_mes_actual DESC;






--regla 5

WITH ventas_mensuales AS (
  SELECT 
    DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) as mes,
    SUM(fv.precio_bruto) as ventas_mes
  FROM dw.fact_ventas fv
  WHERE DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
    AND DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
  GROUP BY DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD'))
  ORDER BY mes
),

estadisticas_mensuales AS (
  SELECT
    mes,
    ventas_mes,
    AVG(ventas_mes) OVER () as promedio_historico,
    AVG(ventas_mes) OVER () as promedio_sin_actual
  FROM ventas_mensuales
),

mes_actual_vs_promedio AS (
  SELECT
    mes,
    ventas_mes,
    promedio_historico,
    CASE 
      WHEN promedio_historico > 0 
      THEN ROUND(((ventas_mes - promedio_historico) / promedio_historico) * 100, 2)
      ELSE NULL 
    END as variacion_vs_promedio_pct
  FROM estadisticas_mensuales
  WHERE mes = DATE_TRUNC('month', CURRENT_DATE)
)

SELECT 
  TO_CHAR(ma.mes, 'Month YYYY') as mes_actual,
  ROUND(ma.ventas_mes, 0) as ventas_mes_actual,
  ROUND(ma.promedio_historico, 0) as promedio_mensual_12meses,
  ma.variacion_vs_promedio_pct,
  CASE
    WHEN ma.variacion_vs_promedio_pct > 5 THEN '📈 CRECIMIENTO'
    WHEN ma.variacion_vs_promedio_pct BETWEEN -5 AND 5 THEN '⚖️ ESTABLE'
    WHEN ma.variacion_vs_promedio_pct < -5 THEN '📉 DESACELERACION'
    ELSE 'Sin datos'
  END as estado_tendencia
FROM mes_actual_vs_promedio ma
ORDER BY ma.mes DESC;



--4=============== este indicador nos muestra las ventas totales en cada mes del año en un historico. muestra la variacion de el mes en funcion del promedio historico del mes.

WITH ventas_mensuales AS (
  SELECT 
    DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) as mes,
    SUM(fv.precio_bruto) as ventas_mes
  FROM dw.fact_ventas fv
  WHERE DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
    AND DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD')) < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
  GROUP BY DATE_TRUNC('month', TO_DATE(fv.fecha_key::TEXT, 'YYYYMMDD'))
  ORDER BY mes
),

estadisticas_mensuales AS (
  SELECT
    mes,
    ventas_mes,
    AVG(ventas_mes) OVER () as promedio_historico_12meses
  FROM ventas_mensuales
),

comparacion_tendencia AS (
  SELECT
    mes,
    ventas_mes,
    promedio_historico_12meses,
    CASE 
      WHEN promedio_historico_12meses > 0 
      THEN ROUND(((ventas_mes - promedio_historico_12meses) / promedio_historico_12meses) * 100, 2)
      ELSE NULL 
    END as variacion_vs_promedio_pct
  FROM estadisticas_mensuales
)

SELECT 
  TO_CHAR(ct.mes, 'Month YYYY') as periodo,
  ROUND(ct.ventas_mes, 0) as ventas_mes,
  ROUND(ct.promedio_historico_12meses, 0) as promedio_historico,
  ct.variacion_vs_promedio_pct,
  CASE
    WHEN ct.variacion_vs_promedio_pct > 5 THEN '📈 CRECIMIENTO'
    WHEN ct.variacion_vs_promedio_pct BETWEEN -5 AND 5 THEN '⚖️ ESTABLE'
    WHEN ct.variacion_vs_promedio_pct < -5 THEN '📉 DESACELERACION'
    ELSE 'Sin datos'
  END as estado_tendencia
FROM comparacion_tendencia ct
ORDER BY ct.mes DESC;



--probar cada vista creada:
SELECT * FROM semantic.ticket_tendencia_hoy_ayer;
SELECT * FROM semantic.ventas_mes_vs_meta;
SELECT * FROM semantic.tendencia_mensual;
SELECT * FROM semantic.ventas_mensuales_historico;


-- Muestra de transacciones individuales de ayer en Tajamar
--SELECT 
  --fv.id_transaccion,
  --fv.precio_bruto,
  --fv.fecha_key,
 -- ds.nombre_sede
--FROM dw.fact_ventas fv
--JOIN dw.dim_sede ds ON fv.sede_sk = ds.sede_sk
--WHERE ds.nombre_sede = 'Tajamar' 
--  AND fv.fecha_key = TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYYMMDD')::INTEGER
--ORDER BY fv.precio_bruto DESC
--LIMIT 1000;


