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
        SELECT * FROM bi.vw_analisis_propinas LIMIT 10;
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
       -- Probar la vista
SELECT * FROM bi.vw_horas_pico ORDER BY hora_del_dia;

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
    SELECT * FROM bi.vw_fidelidad_clientes ORDER BY mes_operacion DESC LIMIT 20;
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
   SELECT * FROM bi.vw_comportamiento_pago;
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
      
-- Probar la vista
SELECT * FROM bi.vw_productos_top LIMIT 100;
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
