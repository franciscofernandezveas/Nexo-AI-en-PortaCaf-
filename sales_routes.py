from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from auth import get_current_user, User

load_dotenv()

router = APIRouter(prefix="/api/sales", tags=["sales"])

def get_db_connection():
    """Crea una conexión directa usando DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL no configurada")
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    return psycopg2.connect(database_url)

# --- Query 1: Resumen de ventas por sede ---
@router.get("/overview", response_model=List[Dict[str, Any]])
async def get_sales_overview(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """

WITH TransaccionesUnicas AS (
    -- 1. OBTENER COMISIÓN ÚNICA POR ID
    -- Agrupamos por ID para asegurar que solo tomamos el costo una vez
    -- y filtramos duplicados de estado (Exitosa/Pagado).
    SELECT 
        "ID de transacción",
        MAX("Comisión") AS costo_comision, -- Asumimos que la comisión viene como valor positivo del costo
        MAX("Total") AS total_pos -- Usamos esto para validar contra la venta detallada
    FROM transacciones
    WHERE "Estado" IN ('Exitosa', 'Pagado')
    GROUP BY "ID de transacción"
),

VentasPorTicket AS (
    -- 2. AGRUPAR ÍTEMS EN UN SOLO TICKET (Pre-agregación)
    -- Esto convierte las N filas de productos en 1 fila por Ticket con su Sede.
    SELECT
        iv."ID de transacción",
        
        -- Lógica de Sede (Tomamos la máxima coincidencia para el ticket)
        MAX(CASE
            WHEN iv."Cuenta" ILIKE '%plaza.bolsillo%' OR iv."Cuenta" ILIKE '%Plaza bolsillo%' THEN 'Plaza Bolsillo'
            WHEN iv."Cuenta" ILIKE '%merced%' THEN 'Merced'
            WHEN iv."Cuenta" ILIKE '%tajamar%' THEN 'Tajamar'
            ELSE COALESCE(iv."Cuenta", 'Sede No Identificada')
        END) AS sede,

        -- Sumamos los ítems del ticket
        SUM(iv."Precio (Bruto)") AS ticket_bruto,
        SUM(iv."Precio (Neto)") AS ticket_neto

    FROM informe_ventas iv
    WHERE 
        iv."Descripción" NOT ILIKE '%Tip%' 
        AND iv."Descripción" NOT ILIKE '%Propina%'
        AND iv."Precio (Bruto)" > 0
    GROUP BY iv."ID de transacción"
)

SELECT
    COALESCE(vt.sede, '>> TOTAL CONSOLIDADO <<') AS cuenta,

    -- KPI 1: Transacciones
    COUNT(vt."ID de transacción") AS transacciones,

    -- KPI 2: Venta Bruta (Lo que paga el cliente)
    SUM(vt.ticket_bruto) AS venta_bruta,

    -- KPI 3: Costo SumUp (Lo que se queda la plataforma)
    SUM(tu.costo_comision) AS comisiones_sumup,

    -- KPI 4: A DEPOSITAR (Caja - Comisión)
    -- Este es el dinero que efectivamente entra al banco
    (SUM(vt.ticket_bruto) - SUM(tu.costo_comision)) AS liquido_a_recibir,

    -- KPI 5: Ticket Promedio
    ROUND(SUM(vt.ticket_bruto) / NULLIF(COUNT(vt."ID de transacción"), 0), 0) AS ticket_promedio,
    
    -- KPI 6: Margen Real Operativo (Venta Neta - Comisiones)
    -- Importante: Dinero sin IVA y sin Comisión (Ganancia real antes de costos de insumos)
    (SUM(vt.ticket_neto) - SUM(tu.costo_comision)) AS margen_operativo_real

FROM VentasPorTicket vt
INNER JOIN TransaccionesUnicas tu ON vt."ID de transacción" = tu."ID de transacción"
GROUP BY ROLLUP(vt.sede)
ORDER BY (vt.sede IS NULL) ASC, venta_bruta DESC;

        """

        cursor.execute(query)
        results = cursor.fetchall()
        return [dict(row) for row in results]
    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    finally:
        if conn: conn.close()

# --- Query 2: Análisis de propinas por sede ---
@router.get("/tips-analysis", response_model=List[Dict[str, Any]])
async def get_tips_analysis(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        WITH transacciones_base AS (
            SELECT DISTINCT
                t."ID de transacción",
                CASE
                    WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
                    WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
                    WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
                    ELSE iv."Cuenta"
                END AS sede_unificada
            FROM transacciones t
            INNER JOIN informe_ventas iv ON t."ID de transacción" = iv."ID de transacción"
            WHERE t."Estado" = 'Exitosa'
        ),
        propinas AS (
            SELECT iv."ID de transacción", SUM(iv."Precio (Neto)") AS monto_propina
            FROM informe_ventas iv
            WHERE LOWER(iv."Descripción") = 'tip'
            GROUP BY iv."ID de transacción"
        )
        SELECT
            tb.sede_unificada,
            COUNT(DISTINCT tb."ID de transacción") AS transacciones_totales,
            COUNT(DISTINCT p."ID de transacción") AS transacciones_con_propina,
            ROUND(COUNT(DISTINCT p."ID de transacción")::NUMERIC / NULLIF(COUNT(DISTINCT tb."ID de transacción"), 0) * 100, 2) AS tasa_conversion_propina_pct,
            SUM(p.monto_propina) AS propinas_totales,
            ROUND(SUM(p.monto_propina) / NULLIF(COUNT(DISTINCT p."ID de transacción"), 0), 0) AS propina_promedio
        FROM transacciones_base tb
        LEFT JOIN propinas p ON tb."ID de transacción" = p."ID de transacción"
        GROUP BY tb.sede_unificada
        ORDER BY tasa_conversion_propina_pct DESC;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 3: Horas pico por sede ---
@router.get("/peak-hours", response_model=List[Dict[str, Any]])
async def get_peak_hours(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        SELECT
            EXTRACT(HOUR FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS hora_del_dia,
            COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN "ID de transacción" END) AS sede_plaza_bolsillo,
            COUNT(DISTINCT CASE WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN "ID de transacción" END) AS sede_merced,
            COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN "ID de transacción" END) AS sede_tajamar
        FROM informe_ventas
        WHERE "Descripción" NOT ILIKE '%Tip%'
          AND "Descripción" NOT ILIKE '%Importe personalizado%'
          AND "Fecha" IS NOT NULL
          AND "Fecha" ~ '^\\d{2}-\\d{2}-\\d{4}, \\d{2}:\\d{2}'
        GROUP BY 1 ORDER BY 1 ASC;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 4: Fidelidad de clientes ---
@router.get("/customer-loyalty", response_model=List[Dict[str, Any]])
async def get_customer_loyalty(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        WITH ventas_limpias AS (
            SELECT 
                CASE 
                    WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
                    WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
                    WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
                    ELSE iv."Cuenta" 
                END AS nombre_sede,
                t."Últimos 4 dígitos" AS id_tarjeta,
                TO_CHAR(CAST(SUBSTRING(t."Fecha" FROM 1 FOR 10) AS DATE), 'YYYY-MM') AS mes_operacion,
                CAST(SUBSTRING(t."Fecha" FROM 1 FOR 10) AS DATE) AS fecha_dia
            FROM transacciones t
            INNER JOIN (SELECT "ID de transacción", "Cuenta" FROM informe_ventas GROUP BY 1, 2) iv ON t."ID de transacción" = iv."ID de transacción"
            WHERE t."Estado" = 'Exitosa' AND t."Últimos 4 dígitos" IS NOT NULL 
        ),
        comportamiento_mensual AS (
            SELECT nombre_sede, mes_operacion, id_tarjeta, COUNT(DISTINCT fecha_dia) AS dias_visitados_al_mes
            FROM ventas_limpias GROUP BY 1, 2, 3
        )
        SELECT 
            nombre_sede, mes_operacion,
            COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 1 THEN id_tarjeta END) AS clientes_un_solo_dia,
            COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 2 THEN id_tarjeta END) AS clientes_recurrentes_2_veces,
            COUNT(DISTINCT CASE WHEN dias_visitados_al_mes > 2 THEN id_tarjeta END) AS clientes_fans_3_o_mas,
            ROUND((COUNT(DISTINCT CASE WHEN dias_visitados_al_mes >= 2 THEN id_tarjeta END)::numeric / NULLIF(COUNT(DISTINCT id_tarjeta), 0)) * 100, 2) AS tasa_fidelidad_mes_pct
        FROM comportamiento_mensual GROUP BY 1, 2 ORDER BY mes_operacion DESC;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 5: Comportamiento de compra ---
@router.get("/purchase-behavior", response_model=List[Dict[str, Any]])
async def get_purchase_behavior(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        WITH ventas_consolidadas AS (
            SELECT
                CASE
                    WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
                    WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
                    WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
                    ELSE iv."Cuenta"
                END AS sede_unificada,
                t."ID de transacción",
                COUNT(*) AS total_items,
                SUM(iv."Precio (Bruto)") AS monto_boleta
            FROM transacciones t
            INNER JOIN informe_ventas iv ON t."ID de transacción" = iv."ID de transacción"
            WHERE t."Estado" = 'Exitosa' AND LOWER(iv."Descripción") NOT IN ('tip', 'importe personalizado') AND iv."Precio (Bruto)" > 0
            GROUP BY 1, 2
        )
        SELECT
            sede_unificada,
            COUNT(CASE WHEN total_items = 1 THEN 1 END) AS ventas_solitarias,
            COUNT(CASE WHEN total_items > 1 THEN 1 END) AS ventas_con_acompanamiento,
            ROUND(COUNT(CASE WHEN total_items > 1 THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS tasa_de_sugestion_exito_pct,
            ROUND(AVG(CASE WHEN total_items = 1 THEN monto_boleta END), 0) AS ticket_promedio_solo,
            ROUND(AVG(CASE WHEN total_items > 1 THEN monto_boleta END), 0) AS ticket_promedio_acompanado
        FROM ventas_consolidadas GROUP BY sede_unificada;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 6: Top 5 productos ---
@router.get("/top-products", response_model=List[Dict[str, Any]])
async def get_top_products(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        WITH ventas_sede_producto AS (
            SELECT
                CASE
                    WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
                    WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
                    WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
                    ELSE "Cuenta"
                END AS sede_unificada,
                "Descripción" AS producto,
                SUM("Precio (Bruto)") AS ingresos_producto
            FROM informe_ventas
            WHERE "Descripción" NOT ILIKE '%Tip%' AND "Descripción" NOT ILIKE '%Importe personalizado%' AND "Precio (Bruto)" > 0
            GROUP BY 1, 2
        ),
        ranking_productos AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY sede_unificada ORDER BY ingresos_producto DESC) AS ranking
            FROM ventas_sede_producto
        )
        SELECT * FROM ranking_productos WHERE ranking <= 5 ORDER BY sede_unificada, ranking;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 7: Medios de pago ---
@router.get("/payment-methods", response_model=List[Dict[str, Any]])
async def get_payment_methods(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
   WITH TransaccionesValidas AS (
            -- 1. BASE DE TRANSACCIONES (Igual al Overview pero inteligente)
            -- El problema del 55% efectivo es que tomábamos filas 'Pagado' que no dicen 'DEBIT'.
            -- Solución: Usamos DISTINCT ON para tomar 1 fila por ID.
            -- El ORDER BY ... NULLS LAST prioriza la fila que SÍ tiene dato (DEBIT/CREDIT).
            SELECT DISTINCT ON ("ID de transacción") 
                "ID de transacción",
                "Ejecutar como", -- Aquí viene DEBIT, CREDIT o NULL
                "Comisión"
            FROM transacciones 
            WHERE "Estado" IN ('Exitosa', 'Pagado')
            ORDER BY "ID de transacción", "Ejecutar como" NULLS LAST
        ),
        VentasPorTicket AS (
            -- 2. SUMA DE VENTA BRUTA (Igual al Overview)
            -- Agrupamos los items de informe_ventas por ticket.
            SELECT
                iv."ID de transacción",
                SUM(iv."Precio (Bruto)") AS venta_total_bruta
            FROM informe_ventas iv
            WHERE 
                iv."Descripción" NOT ILIKE '%Tip%' 
                AND iv."Descripción" NOT ILIKE '%Propina%'
                AND iv."Precio (Bruto)" > 0 
            GROUP BY iv."ID de transacción"
        ),
        Consolidado AS (
            -- 3. UNIÓN FINAL (MATCH EXACTO)
            -- Hacemos INNER JOIN igual que en el Overview.
            -- Si la venta está en el Overview, estará aquí.
            SELECT
                -- Clasificación corregida
                CASE
                    WHEN t."Ejecutar como" = 'DEBIT' THEN 'Débito'
                    WHEN t."Ejecutar como" = 'CREDIT' THEN 'Crédito'
                    -- Solo si realmente no hay dato de tarjeta, asumimos Efectivo
                    ELSE 'Efectivo'
                END AS medio_pago_limpio,
                
                v.venta_total_bruta,
                COALESCE(t."Comisión", 0) AS comision
                
            FROM VentasPorTicket v
            INNER JOIN TransaccionesValidas t ON v."ID de transacción" = t."ID de transacción"
        ),
        AgrupacionFinal AS (
            -- 4. AGRUPACIÓN
            SELECT
                medio_pago_limpio,
                COUNT(*) AS total_transacciones,
                SUM(venta_total_bruta) AS ventas_totales,
                SUM(comision) AS comision_total
            FROM Consolidado
            GROUP BY ROLLUP(medio_pago_limpio)
        )

        SELECT
            COALESCE(medio_pago_limpio, 'TOTAL GENERAL') AS medio_de_pago,
            
            total_transacciones,
            -- % Transacciones
            ROUND(
                total_transacciones::numeric / 
                NULLIF(MAX(CASE WHEN medio_pago_limpio IS NULL THEN total_transacciones END) OVER (), 0) * 100, 
                2
            ) AS participacion_transacciones_pct,
            
            ventas_totales,
            -- % Ventas
            ROUND(
                ventas_totales / 
                NULLIF(MAX(CASE WHEN medio_pago_limpio IS NULL THEN ventas_totales END) OVER (), 0) * 100, 
                2
            ) AS participacion_ventas_pct,
            
            comision_total,
            -- Tasa Comisión
            ROUND(
                comision_total / NULLIF(ventas_totales, 0) * 100, 
                2
            ) AS tasa_comision_pct
            
        FROM AgrupacionFinal
        ORDER BY (medio_pago_limpio IS NULL) ASC, ventas_totales DESC;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- Query 8: Resumen Horario ---
@router.get("/hourly-sales", response_model=List[Dict[str, Any]])
async def get_hourly_sales(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        WITH transacciones_limpias AS (
            SELECT DISTINCT t."ID de transacción", t."Total" AS venta_bruta, t."Comisión" AS comision,
            EXTRACT(HOUR FROM TO_TIMESTAMP(t."Fecha", 'YYYY-MM-DD HH24:MI:SS')) AS hora
            FROM transacciones t WHERE t."Estado" = 'Exitosa' AND t."Fecha" IS NOT NULL
        ),
        sede_por_transaccion AS (
            SELECT DISTINCT iv."ID de transacción",
            CASE
                WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo'
                WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced'
                WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar'
                ELSE TRIM(iv."Cuenta")
            END AS sede
            FROM informe_ventas iv
        )
        SELECT s.sede, t.hora, COUNT(*) AS transacciones, SUM(venta_bruta) AS ventas_brutas
        FROM transacciones_limpias t
        INNER JOIN sede_por_transaccion s ON t."ID de transacción" = s."ID de transacción"
        GROUP BY 1, 2 ORDER BY 1, 2;
        """
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()








@router.get("/products-global", response_model=List[Dict[str, Any]])
async def get_top_products_kpi(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
      WITH TotalRealEmpresa AS (
            -- 1. CALCULAMOS EL TOTAL VERDADERO (~27M)
            -- Incluimos TODO (incluso importe personalizado) para que el % Share sea honesto.
            SELECT 
                SUM("Precio (Bruto)") as gran_total_dinero,
                COUNT(DISTINCT "ID de transacción") as gran_total_tickets
            FROM informe_ventas
            WHERE 
                "Descripción" NOT ILIKE '%tip%' 
                AND "Descripción" NOT ILIKE '%propina%'
                AND "Precio (Bruto)" > 0
        ),
        BaseProductos AS (
            -- 2. LISTA LIMPIA (Aquí SÍ filtramos 'Importe personalizado')
            SELECT 
                -- Normalización: Mayúscula inicial y quitamos espacios
                CASE 
                    WHEN "Descripción" IS NULL OR TRIM("Descripción") = '' THEN 'Producto Sin Nombre'
                    ELSE TRIM(INITCAP("Descripción"))
                END AS producto_normalizado,
                
                "Cantidad",
                "Precio (Bruto)" AS monto_bruto,
                "ID de transacción"
            FROM informe_ventas
            WHERE 
                "Descripción" NOT ILIKE '%tip%'
                AND "Descripción" NOT ILIKE '%propina%'
                -- FILTRO SOLICITADO: Eliminamos la venta manual
                AND "Descripción" NOT ILIKE '%Importe personalizado%'
                AND "Precio (Bruto)" > 0
        )

        SELECT 
            bp.producto_normalizado AS producto,
            
            -- Unidades
            SUM(bp."Cantidad") AS unidades_vendidas,
            
            -- Ventas ($)
            SUM(bp.monto_bruto) AS ventas_brutas,
            
            -- Precio Promedio
            ROUND(SUM(bp.monto_bruto) / NULLIF(SUM(bp."Cantidad"), 0), 0) AS precio_promedio,
            
            -- Share de Ventas (%)
            -- Se compara contra el TOTAL DE LA EMPRESA (incluyendo lo manual)
            ROUND(
                (SUM(bp.monto_bruto) / 
                 NULLIF((SELECT gran_total_dinero FROM TotalRealEmpresa), 0)) * 100, 
                2
            ) as share_ventas_pct,
            
            -- Tasa de Penetración (%)
            ROUND(
                (COUNT(DISTINCT bp."ID de transacción")::numeric / 
                 NULLIF((SELECT gran_total_tickets FROM TotalRealEmpresa), 0)) * 100, 
                2
            ) as tasa_penetracion_pct

        FROM BaseProductos bp
        GROUP BY 1
        ORDER BY ventas_brutas DESC
        LIMIT 50;
        """
        
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()
# --- Query 9: Productos más vendidos (Global) ---
# --- Query 10: Horas del día más concurridas por sede (NUEVA) ---
@router.get("/busy-hours", response_model=List[Dict[str, Any]])
async def get_busy_hours(user: User = Depends(get_current_user)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
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
                THEN "ID de transacción" END) AS plaza_bolsillo,

            COUNT(DISTINCT CASE 
                WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') 
                THEN "ID de transacción" END) AS merced,

            COUNT(DISTINCT CASE 
                WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') 
                THEN "ID de transacción" END) AS tajamar

        FROM informe_ventas
        WHERE "Descripción" NOT ILIKE '%Tip%'
          AND "Descripción" NOT ILIKE '%Importe personalizado%'
          AND "Fecha" IS NOT NULL
        GROUP BY 1,2,3
        ORDER BY 1,3;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        # Convertimos objetos date a string para que FastAPI pueda serializarlos a JSON
        formatted_results = []
        for row in results:
            row_dict = dict(row)
            if row_dict['dia']:
                row_dict['dia'] = row_dict['dia'].isoformat()
            formatted_results.append(row_dict)
            
        return formatted_results

    except psycopg2.Error as e:
        print(f"❌ Error de base de datos (Query 10): {e}")
        raise HTTPException(status_code=500, detail="Error al consultar horas más concurridas")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    finally:
        if conn:
            conn.close()
