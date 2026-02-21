-- Crear schema semantic si no existe
CREATE SCHEMA IF NOT EXISTS semantic;

-- Migrar todas las vistas existentes al schema semantic (si existen)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_dashboard_resumen') THEN
        ALTER VIEW bi.vw_dashboard_resumen SET SCHEMA semantic;
        ALTER VIEW semantic.vw_dashboard_resumen RENAME TO sales_overview;
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_analisis_propinas') THEN
        ALTER VIEW bi.vw_analisis_propinas SET SCHEMA semantic;
        ALTER VIEW semantic.vw_analisis_propinas RENAME TO tips_analysis;
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_horas_pico') THEN
        ALTER VIEW bi.vw_horas_pico SET SCHEMA semantic;
        ALTER VIEW semantic.vw_horas_pico RENAME TO peak_hours;
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_fidelidad_clientes') THEN
        ALTER VIEW bi.vw_fidelidad_clientes SET SCHEMA semantic;
        ALTER VIEW semantic.vw_fidelidad_clientes RENAME TO customer_loyalty;
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_comportamiento_pago') THEN
        ALTER VIEW bi.vw_comportamiento_pago SET SCHEMA semantic;
        ALTER VIEW semantic.vw_comportamiento_pago RENAME TO payment_methods;
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'bi' AND viewname = 'vw_productos_top') THEN
        ALTER VIEW bi.vw_productos_top SET SCHEMA semantic;
        ALTER VIEW semantic.vw_productos_top RENAME TO top_products;
    END IF;
END $$;

-- Crear vistas para endpoints que aún usan SQL embebido

-- Vista para purchase_behavior (corregida con nombres de columnas adecuados)
CREATE OR REPLACE VIEW semantic.purchase_behavior AS
WITH ventas_consolidadas AS (
    SELECT
        CASE
            WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
            WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
            WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
            ELSE iv."Cuenta"
        END AS sede_unificada,
        t."ID_de_transacción",
        COUNT(*) AS total_items,
        SUM(CAST(iv."Precio_Bruto" AS NUMERIC)) AS monto_boleta
    FROM public.transacciones t
    INNER JOIN public.informe_ventas iv ON t."ID_de_transacción" = iv."ID_de_transacción"
    WHERE t."Estado" = 'Exitosa' 
      AND LOWER(COALESCE(iv."Descripción", '')) NOT IN ('tip', 'importe personalizado') 
      AND COALESCE(CAST(NULLIF(iv."Precio_Bruto", '') AS NUMERIC), 0) > 0
    GROUP BY 1, 2
)
SELECT
    sede_unificada,
    COUNT(CASE WHEN total_items = 1 THEN 1 END) AS ventas_solitarias,
    COUNT(CASE WHEN total_items > 1 THEN 1 END) AS ventas_con_acompanamiento,
    ROUND(COUNT(CASE WHEN total_items > 1 THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS tasa_de_sugestion_exito_pct,
    ROUND(AVG(CASE WHEN total_items = 1 THEN monto_boleta END), 0) AS ticket_promedio_solo,
    ROUND(AVG(CASE WHEN total_items > 1 THEN monto_boleta END), 0) AS ticket_promedio_acompanado
FROM ventas_consolidadas 
GROUP BY sede_unificada;

-- Vista para top_products_alternative
CREATE OR REPLACE VIEW semantic.top_products_alternative AS
WITH ventas_sede_producto AS (
    SELECT
        CASE
            WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
            WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
            WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
            ELSE "Cuenta"
        END AS sede_unificada,
        "Descripción" AS producto,
        SUM(COALESCE(CAST(NULLIF("Precio_Bruto", '') AS NUMERIC), 0)) AS ingresos_producto
    FROM public.informe_ventas
    WHERE "Descripción" NOT ILIKE '%Tip%' 
      AND "Descripción" NOT ILIKE '%Importe personalizado%' 
      AND COALESCE(CAST(NULLIF("Precio_Bruto", '') AS NUMERIC), 0) > 0
    GROUP BY 1, 2
),
ranking_productos AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY sede_unificada ORDER BY ingresos_producto DESC) AS ranking
    FROM ventas_sede_producto
)
SELECT * 
FROM ranking_productos 
WHERE ranking <= 5 
ORDER BY sede_unificada, ranking;

-- Vista para hourly_sales (corregida)
CREATE OR REPLACE VIEW semantic.hourly_sales AS
WITH transacciones_limpias AS (
    SELECT DISTINCT 
        t."ID_de_transacción", 
        COALESCE(CAST(NULLIF(t."Total", '') AS NUMERIC), 0) AS venta_bruta, 
        COALESCE(CAST(NULLIF(t."Comisión", '') AS NUMERIC), 0) AS comision,
        EXTRACT(HOUR FROM TO_TIMESTAMP(t."Fecha", 'YYYY-MM-DD HH24:MI:SS')) AS hora
    FROM public.transacciones t 
    WHERE t."Estado" = 'Exitosa' 
      AND t."Fecha" IS NOT NULL
      AND t."Fecha" != ''
),
sede_por_transaccion AS (
    SELECT DISTINCT 
        iv."ID_de_transacción",
        CASE
            WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
            WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
            WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
            ELSE TRIM(COALESCE(iv."Cuenta", ''))
        END AS sede
    FROM public.informe_ventas iv
    WHERE iv."ID_de_transacción" IS NOT NULL
      AND iv."ID_de_transacción" != ''
)
SELECT 
    s.sede, 
    t.hora, 
    COUNT(*) AS transacciones, 
    SUM(venta_bruta) AS ventas_brutas
FROM transacciones_limpias t
INNER JOIN sede_por_transaccion s ON t."ID_de_transacción" = s."ID_de_transacción"
GROUP BY 1, 2 
ORDER BY 1, 2;

-- Vista para busy_hours (corregida)
CREATE OR REPLACE VIEW semantic.busy_hours AS
SELECT
    DATE(TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS dia,
    CASE EXTRACT(DOW FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI'))
        WHEN 1 THEN 'Lunes'
        WHEN 2 THEN 'Martes'
        WHEN 3 THEN 'Miércoles'
        WHEN 4 THEN 'Jueves'
        WHEN 5 THEN 'Viernes'
        WHEN 6 THEN 'Sábado'
        WHEN 0 THEN 'Domingo'
    END AS dia_semana,
    EXTRACT(HOUR FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS hora_del_dia,
    COUNT(DISTINCT CASE 
        WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') 
        THEN "ID_de_transacción" END) AS plaza_bolsillo,
    COUNT(DISTINCT CASE 
        WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') 
        THEN "ID_de_transacción" END) AS merced,
    COUNT(DISTINCT CASE 
        WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') 
        THEN "ID_de_transacción" END) AS tajamar
FROM public.informe_ventas
WHERE "Descripción" NOT ILIKE '%Tip%'
  AND "Descripción" NOT ILIKE '%Importe personalizado%'
  AND "Fecha" IS NOT NULL
  AND "Fecha" != ''
  AND "ID_de_transacción" IS NOT NULL
  AND "ID_de_transacción" != ''
GROUP BY 1,2,3
ORDER BY 1,3;

-- Vista para products_global (ya existía como vw_productos_top)
CREATE OR REPLACE VIEW semantic.products_global AS
SELECT * FROM semantic.top_products;

-- Crear vista para overview si no existe
CREATE OR REPLACE VIEW semantic.sales_overview AS
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


--2 script metric diccionary

-- Crear tabla de diccionario semántico
CREATE TABLE IF NOT EXISTS semantic.metric_dictionary (
    metric_name VARCHAR(100) PRIMARY KEY,
    business_description TEXT,
    sql_definition TEXT,
    grain_level VARCHAR(50),
    owner VARCHAR(50) DEFAULT 'BI'
);

-- Insertar métricas derivadas de las vistas existentes
INSERT INTO semantic.metric_dictionary 
(metric_name, business_description, sql_definition, grain_level, owner)
VALUES
-- Métricas de sales_overview
('transacciones', 'Número total de transacciones únicas realizadas', 'COUNT(DISTINCT fv.id_transaccion)', 'ticket', 'BI'),
('venta_bruta', 'Suma total de ingresos brutos antes de comisiones', 'SUM(fv.precio_bruto)', 'ticket', 'BI'),
('comisiones_sumup', 'Total de comisiones cobradas por SumUp', 'SUM(ft.comision)', 'ticket', 'BI'),
('liquido_a_recibir', 'Ingresos netos después de deducir comisiones', 'venta_bruta - comisiones_sumup', 'ticket', 'BI'),
('ticket_promedio', 'Promedio de venta por transacción', 'venta_bruta / transacciones', 'ticket', 'BI'),
('margen_operativo_real', 'Margen operativo considerando comisiones', 'precio_neto - comisiones_sumup', 'ticket', 'BI'),

-- Métricas de tips_analysis
('transacciones_con_propina', 'Transacciones que incluyen propina', 'COUNT(DISTINCT pb."ID_de_transacción")', 'ticket', 'BI'),
('tasa_conversion_propina_pct', 'Porcentaje de transacciones con propina', '(transacciones_con_propina / transacciones_totales) * 100', 'ticket', 'BI'),
('propinas_totales', 'Monto total de propinas recibidas', 'SUM(pb.monto_propina)', 'ticket', 'BI'),
('propina_promedio', 'Promedio de propina por transacción', 'propinas_totales / transacciones_con_propina', 'ticket', 'BI'),

-- Métricas de peak_hours
('total_transacciones_hora', 'Transacciones por hora del día', 'COUNT(DISTINCT iv."ID_de_transacción")', 'hora', 'BI'),

-- Métricas de customer_loyalty
('clientes_un_solo_dia', 'Clientes que visitaron solo un día en el mes', 'COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 1 THEN id_tarjeta END)', 'mes', 'BI'),
('clientes_recurrentes_2_veces', 'Clientes que visitaron exactamente 2 días en el mes', 'COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 2 THEN id_tarjeta END)', 'mes', 'BI'),
('clientes_fans_3_o_mas', 'Clientes que visitaron 3 o más días en el mes', 'COUNT(DISTINCT CASE WHEN dias_visitados_al_mes > 2 THEN id_tarjeta END)', 'mes', 'BI'),
('tasa_fidelidad_mes_pct', 'Porcentaje de clientes recurrentes (2+ visitas)', '(clientes_recurrentes_2_veces + clientes_fans_3_o_mas) / total_clientes * 100', 'mes', 'BI'),

-- Métricas de payment_methods
('total_transacciones_medio', 'Transacciones por método de pago', 'COUNT(*)', 'ticket', 'BI'),
('ventas_totales_medio', 'Ventas totales por método de pago', 'SUM(venta_total_bruta)', 'ticket', 'BI'),
('participacion_transacciones_pct', 'Participación porcentual de transacciones por método', '(total_transacciones_medio / total_general) * 100', 'ticket', 'BI'),
('participacion_ventas_pct', 'Participación porcentual de ventas por método', '(ventas_totales_medio / ventas_totales_general) * 100', 'ticket', 'BI'),
('tasa_comision_pct', 'Tasa promedio de comisión por método', '(comision_total / ventas_totales_medio) * 100', 'ticket', 'BI'),

-- Métricas de top_products
('unidades_vendidas', 'Cantidad total de unidades vendidas del producto', 'SUM(fv.cantidad)', 'producto', 'BI'),
('ventas_brutas_producto', 'Ventas totales brutas del producto', 'SUM(fv.precio_bruto)', 'producto', 'BI'),
('share_ventas_pct', 'Participación porcentual en ventas totales', '(ventas_brutas_producto / total_ventas_empresa) * 100', 'producto', 'BI'),
('tasa_penetracion_pct', 'Penetración del producto en transacciones', '(tickets_unicos / total_tickets_empresa) * 100', 'producto', 'BI'),

-- Métricas de purchase_behavior
('ventas_solitarias', 'Transacciones con un solo item', 'COUNT(CASE WHEN total_items = 1 THEN 1 END)', 'ticket', 'BI'),
('ventas_con_acompanamiento', 'Transacciones con múltiples items', 'COUNT(CASE WHEN total_items > 1 THEN 1 END)', 'ticket', 'BI'),
('tasa_de_sugestion_exito_pct', 'Tasa de éxito en sugerencias de venta', '(ventas_con_acompanamiento / total_transacciones) * 100', 'ticket', 'BI'),
('ticket_promedio_solo', 'Ticket promedio de compras individuales', 'AVG(CASE WHEN total_items = 1 THEN monto_boleta END)', 'ticket', 'BI'),
('ticket_promedio_acompanado', 'Ticket promedio de compras combinadas', 'AVG(CASE WHEN total_items > 1 THEN monto_boleta END)', 'ticket', 'BI'),

-- Métricas de hourly_sales
('transacciones_por_hora', 'Transacciones agrupadas por hora y sede', 'COUNT(*)', 'hora-sede', 'BI'),
('ventas_brutas_hora', 'Ventas totales por hora y sede', 'SUM(venta_bruta)', 'hora-sede', 'BI'),

-- Métricas de busy_hours
('transacciones_por_hora_sede', 'Transacciones por hora del día separadas por sede', 'COUNT(DISTINCT "ID de transacción")', 'hora-sede-dia', 'BI')
ON CONFLICT (metric_name) DO UPDATE SET
    business_description = EXCLUDED.business_description,
    sql_definition = EXCLUDED.sql_definition,
    grain_level = EXCLUDED.grain_level,
    owner = EXCLUDED.owner;
