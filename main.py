import logging
import os
import sys
import json
import traceback
from typing import TypedDict, List, Dict, Optional, Any, Union
from enum import Enum
from datetime import datetime

# Agrega esto a tus imports existentes
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field  # <-- Agregar Field aquí
from langgraph.graph import MessagesState


# LangChain imports
try:
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.runnables import RunnablePassthrough
    from langchain_core.prompts import PromptTemplate
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
    from langchain_core.tools import tool
    from langchain_core.utils.function_calling import convert_to_openai_function
except ImportError:
    print("❌ ERROR: LangChain desactualizado")
    print("Ejecuta: pip install --upgrade langchain-core langchain-community langchain-openai")
    sys.exit(1)

# LangGraph essentials
from langgraph.graph import StateGraph, END, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition
from langchain_openai import ChatOpenAI

# Configuración inicial
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.environment import setup_environment, verify_openai_connection
from database import create_database_connection

# ========================================================================
# KPI REGISTRY (CATÁLOGO OFICIAL DE MÉTRICAS) - VERSIONES LIMPIAS
# ========================================================================
### ✅ ADD

KPI_REGISTRY = {
    "ventas_por_sede": {
        "description": "Ventas totales por sede excluyendo propinas",
        "sql_template": '''WITH TransaccionesValidas AS (SELECT DISTINCT "ID de transacción" FROM transacciones WHERE "Estado" IN ('Exitosa', 'Pagado')), ventas_limpias AS (SELECT CASE WHEN iv."Cuenta" ILIKE '%plaza.bolsillo%' OR iv."Cuenta" ILIKE '%Plaza bolsillo%' THEN 'Plaza Bolsillo' WHEN iv."Cuenta" ILIKE '%merced%' THEN 'Merced' WHEN iv."Cuenta" ILIKE '%tajamar%' THEN 'Tajamar' ELSE COALESCE(iv."Cuenta", 'Desconocido') END AS sede, iv."ID de transacción", iv."Precio (Bruto)" AS venta_valor FROM informe_ventas iv INNER JOIN TransaccionesValidas tv ON iv."ID de transacción" = tv."ID de transacción" WHERE iv."Descripción" NOT ILIKE 'Tip' AND iv."Descripción" NOT ILIKE 'Propina' AND iv."Precio (Bruto)" > 0 ) SELECT COALESCE(sede, 'TOTAL GENERAL') AS cuenta, SUM(venta_valor) AS ventas_totales, COUNT(DISTINCT "ID de transacción") AS transacciones, ROUND(SUM(venta_valor) / NULLIF(COUNT(DISTINCT "ID de transacción"), 0), 0) AS ticket_promedio FROM ventas_limpias GROUP BY ROLLUP(sede) ORDER BY (sede IS NULL) ASC, ventas_totales DESC;''',
        "keywords": ["ventas por sede", "ventas totales sede", "total ventas por ubicacion"]
    },
    "top_productos": {
        "description": "Top 5 productos más vendidos por sede",
        "sql_template": '''WITH ventas_sede_producto AS (SELECT CASE WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo' WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced' WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar' ELSE "Cuenta" END AS sede_unificada, "Descripción" AS producto, SUM("Precio (Bruto)") AS ingresos_producto FROM informe_ventas WHERE "Descripción" NOT ILIKE '%Tip%' AND "Descripción" NOT ILIKE '%Importe personalizado%' AND "Precio (Bruto)" > 0 GROUP BY 1, 2), ranking_productos AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY sede_unificada ORDER BY ingresos_producto DESC) AS ranking FROM ventas_sede_producto) SELECT * FROM ranking_productos WHERE ranking <= 5 ORDER BY sede_unificada, ranking;''',
        "keywords": ["top productos", "productos mas vendidos", "mejores ventas productos"]
    },
    "medios_pago": {
        "description": "Distribución de medios de pago",
        "sql_template": '''WITH TransaccionesValidas AS (SELECT DISTINCT ON ("ID de transacción") "ID de transacción", "Ejecutar como", "Comisión" FROM transacciones WHERE "Estado" IN ('Exitosa', 'Pagado') ORDER BY "ID de transacción", "Ejecutar como" NULLS LAST), VentasPorTicket AS (SELECT iv."ID de transacción", SUM(iv."Precio (Bruto)") AS venta_total_bruta FROM informe_ventas iv WHERE iv."Descripción" NOT ILIKE '%Tip%' AND iv."Descripción" NOT ILIKE '%Propina%' AND iv."Precio (Bruto)" > 0 GROUP BY iv."ID de transacción"), Consolidado AS (SELECT CASE WHEN t."Ejecutar como" = 'DEBIT' THEN 'Débito' WHEN t."Ejecutar como" = 'CREDIT' THEN 'Crédito' ELSE 'Efectivo' END AS medio_pago_limpio, v.venta_total_bruta, COALESCE(t."Comisión", 0) AS comision FROM VentasPorTicket v INNER JOIN TransaccionesValidas t ON v."ID de transacción" = t."ID de transacción"), AgrupacionFinal AS (SELECT medio_pago_limpio, COUNT(*) AS total_transacciones, SUM(venta_total_bruta) AS ventas_totales, SUM(comision) AS comision_total FROM Consolidado GROUP BY ROLLUP(medio_pago_limpio)) SELECT COALESCE(medio_pago_limpio, 'TOTAL GENERAL') AS medio_de_pago, total_transacciones, ROUND(total_transacciones::numeric / NULLIF(MAX(CASE WHEN medio_pago_limpio IS NULL THEN total_transacciones END) OVER (), 0) * 100, 2) AS participacion_transacciones_pct, ventas_totales, ROUND(ventas_totales / NULLIF(MAX(CASE WHEN medio_pago_limpio IS NULL THEN ventas_totales END) OVER (), 0) * 100, 2) AS participacion_ventas_pct, comision_total, ROUND(comision_total / NULLIF(ventas_totales, 0) * 100, 2) AS tasa_comision_pct FROM AgrupacionFinal ORDER BY (medio_pago_limpio IS NULL) ASC, ventas_totales DESC;''',
        "keywords": ["medios de pago", "distribucion pagos", "metodos pago", "metodos de pago mas usados"]
    },
    "analisis_propinas": {
        "description": "Análisis de propinas por sede",
        "sql_template": '''WITH transacciones_base AS (SELECT DISTINCT t."ID de transacción", CASE WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo' WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced' WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar' ELSE iv."Cuenta" END AS sede_unificada FROM transacciones t INNER JOIN informe_ventas iv ON t."ID de transacción" = iv."ID de transacción" WHERE t."Estado" = 'Exitosa'), propinas AS (SELECT iv."ID de transacción", SUM(iv."Precio (Neto)") AS monto_propina FROM informe_ventas iv WHERE LOWER(iv."Descripción") = 'tip' GROUP BY iv."ID de transacción") SELECT tb.sede_unificada, COUNT(DISTINCT tb."ID de transacción") AS transacciones_totales, COUNT(DISTINCT p."ID de transacción") AS transacciones_con_propina, ROUND(COUNT(DISTINCT p."ID de transacción")::NUMERIC / NULLIF(COUNT(DISTINCT tb."ID de transacción"), 0) * 100, 2) AS tasa_conversion_propina_pct, SUM(p.monto_propina) AS propinas_totales, ROUND(SUM(p.monto_propina) / NULLIF(COUNT(DISTINCT p."ID de transacción"), 0), 0) AS propina_promedio FROM transacciones_base tb LEFT JOIN propinas p ON tb."ID de transacción" = p."ID de transacción" GROUP BY tb.sede_unificada ORDER BY tasa_conversion_propina_pct DESC;''',
        "keywords": ["analisis propinas", "propinas por sede", "tasa conversion propina"]
    },
    "horas_pico": {
        "description": "Horas pico por sede",
        "sql_template": '''SELECT EXTRACT(HOUR FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS hora_del_dia, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN "ID de transacción" END) AS sede_plaza_bolsillo, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN "ID de transacción" END) AS sede_merced, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN "ID de transacción" END) AS sede_tajamar FROM informe_ventas WHERE "Descripción" NOT ILIKE '%Tip%' AND "Descripción" NOT ILIKE '%Importe personalizado%' AND "Fecha" IS NOT NULL AND "Fecha" ~ '^\\d{2}-\\d{2}-\\d{4}, \\d{2}:\\d{2}' GROUP BY 1 ORDER BY 1 ASC;''',
        "keywords": ["horas pico", "peak hours", "horarios mas concurridos"]
    },
    "fidelidad_clientes": {
        "description": "Fidelidad de clientes por sede",
        "sql_template": '''WITH ventas_limpias AS (SELECT CASE WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo' WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced' WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar' ELSE iv."Cuenta" END AS nombre_sede, t."Últimos 4 dígitos" AS id_tarjeta, TO_CHAR(CAST(SUBSTRING(t."Fecha" FROM 1 FOR 10) AS DATE), 'YYYY-MM') AS mes_operacion, CAST(SUBSTRING(t."Fecha" FROM 1 FOR 10) AS DATE) AS fecha_dia FROM transacciones t INNER JOIN (SELECT "ID de transacción", "Cuenta" FROM informe_ventas GROUP BY 1, 2) iv ON t."ID de transacción" = iv."ID de transacción" WHERE t."Estado" = 'Exitosa' AND t."Últimos 4 dígitos" IS NOT NULL), comportamiento_mensual AS (SELECT nombre_sede, mes_operacion, id_tarjeta, COUNT(DISTINCT fecha_dia) AS dias_visitados_al_mes FROM ventas_limpias GROUP BY 1, 2, 3) SELECT nombre_sede, mes_operacion, COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 1 THEN id_tarjeta END) AS clientes_un_solo_dia, COUNT(DISTINCT CASE WHEN dias_visitados_al_mes = 2 THEN id_tarjeta END) AS clientes_recurrentes_2_veces, COUNT(DISTINCT CASE WHEN dias_visitados_al_mes > 2 THEN id_tarjeta END) AS clientes_fans_3_o_mas, ROUND((COUNT(DISTINCT CASE WHEN dias_visitados_al_mes >= 2 THEN id_tarjeta END)::numeric / NULLIF(COUNT(DISTINCT id_tarjeta), 0)) * 100, 2) AS tasa_fidelidad_mes_pct FROM comportamiento_mensual GROUP BY 1, 2 ORDER BY mes_operacion DESC;''',
        "keywords": ["fidelidad clientes", "clientes recurrentes", "tasa fidelidad"]
    },
    "comportamiento_compra": {
        "description": "Comportamiento de compra por sede",
        "sql_template": '''WITH ventas_consolidadas AS (SELECT CASE WHEN iv."Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN 'Sede Plaza Bolsillo' WHEN iv."Cuenta" IN ('merced', 'merced.158@gmail.com') THEN 'Sede Merced' WHEN iv."Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN 'Sede Tajamar' ELSE iv."Cuenta" END AS sede_unificada, t."ID de transacción", COUNT(*) AS total_items, SUM(iv."Precio (Bruto)") AS monto_boleta FROM transacciones t INNER JOIN informe_ventas iv ON t."ID de transacción" = iv."ID de transacción" WHERE t."Estado" = 'Exitosa' AND LOWER(iv."Descripción") NOT IN ('tip', 'importe personalizado') AND iv."Precio (Bruto)" > 0 GROUP BY 1, 2), SELECT sede_unificada, COUNT(CASE WHEN total_items = 1 THEN 1 END) AS ventas_solitarias, COUNT(CASE WHEN total_items > 1 THEN 1 END) AS ventas_con_acompanamiento, ROUND(COUNT(CASE WHEN total_items > 1 THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS tasa_de_sugestion_exito_pct, ROUND(AVG(CASE WHEN total_items = 1 THEN monto_boleta END), 0) AS ticket_promedio_solo, ROUND(AVG(CASE WHEN total_items > 1 THEN monto_boleta END), 0) AS ticket_promedio_acompanado FROM ventas_consolidadas GROUP BY sede_unificada;''',
        "keywords": ["comportamiento compra", "ventas solitarias", "ticket promedio"]
    },
    "productos_global": {
        "description": "Top 50 productos más vendidos globalmente",
        "sql_template": '''WITH TotalRealEmpresa AS (SELECT SUM("Precio (Bruto)") as gran_total_dinero, COUNT(DISTINCT "ID de transacción") as gran_total_tickets FROM informe_ventas WHERE "Descripción" NOT ILIKE '%tip%' AND "Descripción" NOT ILIKE '%propina%' AND "Precio (Bruto)" > 0), BaseProductos AS (SELECT CASE WHEN "Descripción" IS NULL OR TRIM("Descripción") = '' THEN 'Producto Sin Nombre' ELSE TRIM(INITCAP("Descripción")) END AS producto_normalizado, "Cantidad", "Precio (Bruto)" AS monto_bruto, "ID de transacción" FROM informe_ventas WHERE "Descripción" NOT ILIKE '%tip%' AND "Descripción" NOT ILIKE '%propina%' AND "Descripción" NOT ILIKE '%Importe personalizado%' AND "Precio (Bruto)" > 0) SELECT bp.producto_normalizado AS producto, SUM(bp."Cantidad") AS unidades_vendidas, SUM(bp.monto_bruto) AS ventas_brutas, ROUND(SUM(bp.monto_bruto) / NULLIF(SUM(bp."Cantidad"), 0), 0) AS precio_promedio, ROUND((SUM(bp.monto_bruto) / NULLIF((SELECT gran_total_dinero FROM TotalRealEmpresa), 0)) * 100, 2) as share_ventas_pct, ROUND((COUNT(DISTINCT bp."ID de transacción")::numeric / NULLIF((SELECT gran_total_tickets FROM TotalRealEmpresa), 0)) * 100, 2) as tasa_penetracion_pct FROM BaseProductos bp GROUP BY 1 ORDER BY ventas_brutas DESC LIMIT 50;''',
        "keywords": ["productos global", "top productos mundial", "share ventas productos"]
    },
    "horas_concurridas": {
        "description": "Horas del día más concurridas por sede",
        "sql_template": '''SELECT DATE(TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS dia, CASE EXTRACT(DOW FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) WHEN 1 THEN 'Lunes' WHEN 2 THEN 'Martes' WHEN 3 THEN 'Miércoles' WHEN 4 THEN 'Jueves' WHEN 5 THEN 'Viernes' WHEN 6 THEN 'Sábado' WHEN 0 THEN 'Domingo' END AS dia_semana, EXTRACT(HOUR FROM TO_TIMESTAMP("Fecha", 'DD-MM-YYYY, HH24:MI')) AS hora_del_dia, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Plaza bolsillo', 'plaza.bolsillo@gmail.com') THEN "ID de transacción" END) AS plaza_bolsillo, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('merced', 'merced.158@gmail.com') THEN "ID de transacción" END) AS merced, COUNT(DISTINCT CASE WHEN "Cuenta" IN ('Tajamar', 'providencia.tajamar@gmail.com') THEN "ID de transacción" END) AS tajamar FROM informe_ventas WHERE "Descripción" NOT ILIKE '%Tip%' AND "Descripción" NOT ILIKE '%Importe personalizado%' AND "Fecha" IS NOT NULL GROUP BY 1,2,3 ORDER BY 1,3;''',
        "keywords": ["horas concurridas", "traffic hours", "peak times"]
    }
}

# ========================================================================
# SCHEMA REAL PARA VALIDACIÓN
# ========================================================================
REAL_SCHEMA = {
    "transacciones": [
        "ID de transacción", "Fecha", "Hora", "Cuenta", "Estado", "Ejecutar como", "Comisión"
    ],
    "informe_ventas": [
        "ID de transacción", "Fecha", "Hora", "Cuenta", "Descripción", "Cantidad", 
        "Precio (Bruto)", "Precio (Neto)", "Últimos 4 dígitos"
    ]
}

# ========================================================================
# COMPONENTES GLOBALES
# ========================================================================
_lazy_components = {}

def get_database_schema(db):
    """Obtiene esquema técnico REAL de PostgreSQL"""
    try:
        raw_schema = db.get_table_info()
        print(f"\n✅ Esquema cargado ({len(raw_schema)} chars)")
        return raw_schema
    except Exception as e:
        print(f"⚠️ Error obteniendo esquema: {e}")
        return "Esquema no disponible"

# ========================================================================
# NUEVA IMPLEMENTACIÓN: TOOL DE EJECUCIÓN SQL REAL
# ========================================================================
@tool
def execute_sql(query: str) -> str:
    """
    Ejecuta una consulta SQL contra la base de datos.
    
    Args:
        query: Consulta SQL a ejecutar
        
    Returns:
        Resultados serializados o mensaje de error como string
    """
    db = _lazy_components.get('db')
    
    if not db:
        return "ERROR: No hay conexión a base de datos disponible"
    
    # 🔒 VALIDACIÓN DE SEGURIDAD - SOLO SELECT
    query_clean = query.strip().lower()
    
    # Verificar que empiece con SELECT o WITH
    if not query_clean.startswith("select") and not query_clean.startswith("with"):
        return "SQL_SECURITY_ERROR: Solo se permiten consultas SELECT"
    
    # Bloquear palabras clave peligrosas
    forbidden_keywords = [
        "insert",
        "update", 
        "delete",
        "drop",
        "alter", 
        "truncate",
        "grant",
        "revoke",
        "create",
        "execute"
    ]
    
    if any(keyword in query_clean for keyword in forbidden_keywords):
        return "SQL_SECURITY_ERROR: Operación SQL no permitida"
    
    try:
        # 🔧 Usar la instancia única creada en _lazy_components
        execute_query = _lazy_components.get("sql_tool")
        if not execute_query:
            return "ERROR: Herramienta SQL no inicializada"
        
        # 🔍 LOGGING AVANZADO - Indicador visual claro
        print("\n" + "="*50)
        print("🔍 CONSULTA SQL DETECTADA")
        print("="*50)
        print("🔥 EJECUTANDO SQL REAL:")
        print(query)
        print("-" * 50)
        
        # ⏱️ Marca de tiempo para medir duración
        import time
        start_time = time.time()
            
        result = execute_query.invoke({"query": query})
        
        # ⏱️ Calcular duración
        end_time = time.time()
        duration = end_time - start_time
        
        # 📊 LOGGING DE RESULTADOS
        print(f"✅ CONSULTA COMPLETADA en {duration:.2f} segundos")
        
        # Parsear resultado
        if isinstance(result, str):
            if "error" in result.lower() or "syntax" in result.lower():
                print(f"❌ ERROR EN CONSULTA: {result}")
                return result  # Devolvemos el error directamente
            else:
                try:
                    parsed_result = json.loads(result)
                    if isinstance(parsed_result, list) and len(parsed_result) == 0:
                        print("📝 Resultado: EMPTY_RESULT (0 filas)")
                        return "EMPTY_RESULT"
                    else:
                        row_count = len(parsed_result) if isinstance(parsed_result, list) else "N/A"
                        print(f"📊 Resultado: {row_count} filas obtenidas")
                        return json.dumps(parsed_result[:50], default=str)  # Limitamos resultados
                except:
                    if not result.strip():
                        print("📝 Resultado: EMPTY_RESULT (contenido vacío)")
                        return "EMPTY_RESULT"
                    print(f"📄 Resultado: Texto plano ({len(result)} caracteres)")
                    return result[:1000]  # Limitamos tamaño
        else:
            if isinstance(result, list) and len(result) == 0:
                print("📝 Resultado: EMPTY_RESULT (lista vacía)")
                return "EMPTY_RESULT"
            row_count = len(result) if isinstance(result, list) else "N/A"
            print(f"📊 Resultado: {row_count} elementos obtenidos")
            return json.dumps(result[:50], default=str)
            
    except Exception as e:
        # Devolvemos el error real como string
        print(f"💥 ERROR FATAL EN CONSULTA: {str(e)}")
        return f"SQL_ERROR: {str(e)}"



# ========================================================================
# NUEVA IMPLEMENTACIÓN: TOOL PARA OBTENER KPI SQL
# ========================================================================
@tool
def get_kpi_sql(kpi_name: str) -> str:
    """
    Obtiene la consulta SQL predefinida para un KPI específico.
    
    Args:
        kpi_name: Nombre del KPI registrado
        
    Returns:
        Consulta SQL como string
    """
    kpi = KPI_REGISTRY.get(kpi_name.lower())
    if not kpi:
        available_kpis = ", ".join(KPI_REGISTRY.keys())
        return f"KPI_NO_ENCONTRADO: KPI '{kpi_name}' no existe. KPIs disponibles: {available_kpis}"
    
    return kpi["sql_template"]


# ========================================================================
# ESTADO DEL AGENTE REACT REAL (basado en MessagesState)
# ========================================================================
class AgentState(MessagesState):
    """Estado extendido con tracking completo de RAG"""
    question: str
    sql_query: Optional[str] = None
    execution_success: Optional[bool] = None
    result_rows: Optional[List[Dict]] = None
    final_response: Optional[str] = None
    insights_analysis: Optional[str] = None
    attempt_count: int = 0
    rag_context_used: bool = False
    rag_queries_history: List[str] = Field(default_factory=list)  # Corrección
    rag_attempt_count: int = 0
# ========================================================================
# NODOS DEL GRAFO REACT REAL
# ========================================================================
def assistant_node(state: AgentState) -> Dict[str, Any]:
    """Nodo Assistant mejorado con contexto de uso RAG"""
    llm = _lazy_components.get('llm')
    
    # Crear contexto operacional efímero
    intentos = state.get("attempt_count", 0)
    exito = state.get("execution_success", None)
    sql_query = state.get("sql_query", None)
    result_rows = state.get("result_rows", None)
    rag_context_used = state.get("rag_context_used", False)
    rag_queries_history = state.get("rag_queries_history", [])
    rag_attempt_count = state.get("rag_attempt_count", 0)
    
    # Construir mensaje de estado operacional
    estado_info = f"""
ESTADO OPERACIONAL ACTUAL:
- Intentos SQL ejecutados: {intentos}
- Última ejecución exitosa: {exito}
- Contexto RAG utilizado: {rag_context_used}
- Intentos RAG: {rag_attempt_count}/3 (límite)
"""
    
    if rag_queries_history:
        estado_info += f"- Últimas búsquedas RAG: {rag_queries_history[-2:]}"
    
    if sql_query:
        estado_info += f"- Última query ejecutada: {sql_query[:100]}..."
    
    if result_rows is not None:
        if isinstance(result_rows, list) and len(result_rows) == 0:
            estado_info += "- Resultado: EMPTY_RESULT"
        else:
            estado_info += f"- Filas obtenidas: {len(result_rows) if isinstance(result_rows, list) else 'N/A'}"
    
    estado_info += """

PLANIFICACIÓN SEMÁNTICA:
1. Para métricas desconocidas: usar retrieve_documents primero
2. Para datos estructurados: usar execute_sql o get_kpi_sql
3. Las reglas encontradas en documentos deben guiar las consultas SQL
"""
    
    state_context = SystemMessage(content=estado_info)
    
    # Bind tools al LLM (todas las tools disponibles)
    tools_to_bind = [execute_sql, get_kpi_sql]
    if _lazy_components.get("retriever_tool"):
        tools_to_bind.append(_lazy_components["retriever_tool"])
    
    bound_llm = llm.bind_tools(tools_to_bind)
    
    # Invocar LLM con contexto efímero + historial
    response = bound_llm.invoke([state_context] + state["messages"])
    
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    """Decide si continuar con tools o terminar"""
    messages = state["messages"]
    last_message = messages[-1]
    
    # Si el último mensaje tiene tool_calls, ir al ToolNode
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tools"
    
    # Si llegamos aquí, el LLM ha terminado de pensar
    return "end"


def observer_node(state: AgentState) -> Dict[str, Any]:
    """Observer Node con tracking avanzado de RAG"""
    messages = state["messages"]
    
    if not messages:
        return {}
    
    last_message = messages[-1]
    
    # Estados actuales
    attempt_count = state.get("attempt_count", 0)
    rag_context_used = state.get("rag_context_used", False)
    rag_queries_history = state.get("rag_queries_history", [])
    rag_attempt_count = state.get("rag_attempt_count", 0)
    
    # Solo procesamos ToolMessages
    if not isinstance(last_message, ToolMessage):
        return {}
    
    # Detectar éxito/fracaso
    content = last_message.content
    execution_success = True
    if "ERROR" in content or "SQL_SECURITY_ERROR" in content or "SQL_ERROR" in content:
        execution_success = False
    
    # Extraer SQL ejecutado si está disponible
    sql_query = None
    if (hasattr(last_message, 'tool_call') and 
        last_message.tool_call and 
        isinstance(last_message.tool_call, dict)):
        
        tool_call = last_message.tool_call
        if tool_call.get('name') == 'execute_sql' and 'args' in tool_call:
            args = tool_call['args']
            if isinstance(args, dict) and 'query' in args:
                sql_query = args['query']
    
    # Parsear resultados
    result_rows = None
    if execution_success:
        if content == "EMPTY_RESULT":
            result_rows = []
        else:
            try:
                result_rows = json.loads(content)
                if not isinstance(result_rows, list):
                    result_rows = None
            except:
                result_rows = None
    
    # Detectar uso de RAG y actualizar tracking
    rag_used_in_this_call = False
    if (hasattr(last_message, 'tool_call') and 
        last_message.tool_call and 
        isinstance(last_message.tool_call, dict) and
        last_message.tool_call.get('name') == 'retrieve_documents'):
        
        rag_used_in_this_call = True
        rag_context_used = True
        rag_attempt_count += 1
        
        # Extraer query RAG para historial
        if 'args' in last_message.tool_call and 'query' in last_message.tool_call['args']:
            query = last_message.tool_call['args']['query']
            rag_queries_history.append(query)
    
    # Prevenir loops RAG excesivos (límite 3 intentos)
    if rag_attempt_count > 3:
        print("⚠️ Límite de intentos RAG alcanzado, evitando loops")
    
    return {
        "attempt_count": attempt_count + 1,
        "execution_success": execution_success,
        "sql_query": sql_query,
        "result_rows": result_rows,
        "rag_context_used": rag_context_used,
        "rag_queries_history": rag_queries_history[-5:],  # Mantener últimas 5
        "rag_attempt_count": min(rag_attempt_count, 3)  # Limitar a 3 intentos
    }

def create_react_graph_real(llm, db) -> StateGraph:
    """Crea el grafo ReAct real con arquitectura RAG profesional"""
    _lazy_components['llm'] = llm
    _lazy_components['db'] = db
    
    # 🔧 Inicializar QuerySQLDatabaseTool (sin cambios)
    from langchain_community.tools import QuerySQLDatabaseTool
    _lazy_components["sql_tool"] = QuerySQLDatabaseTool(db=db)
    
    # 📚 INICIALIZACIÓN RAG - SOLO CONEXIÓN (sin indexación)
    try:
        print("🔌 Conectando a sistema RAG persistente...")
        
        # Importar tool de RAG optimizada
        from rag.retriever_tool import get_rag_tool
        retriever_tool = get_rag_tool()
        
        if retriever_tool:
            _lazy_components["retriever_tool"] = retriever_tool
            print("✅ Sistema RAG conectado correctamente")
        else:
            print("⚠️ Sistema RAG no disponible")
            _lazy_components["retriever_tool"] = None
            
    except Exception as e:
        print(f"⚠️ Error conectando RAG: {e}")
        _lazy_components["retriever_tool"] = None
    
    # Crear workflow
    workflow = StateGraph(AgentState)
    
    # Crear ToolNode con todas las tools disponibles
    tools_list = [execute_sql, get_kpi_sql]
    if _lazy_components.get("retriever_tool"):
        tools_list.append(_lazy_components["retriever_tool"])
    
    tool_node = ToolNode(tools_list)
    
    # Agregar nodos
    workflow.add_node("assistant", assistant_node)
    workflow.add_node("tools", tool_node)
    workflow.add_node("observer", observer_node)
    
    # Set entry point
    workflow.set_entry_point("assistant")
    
    # Add conditional edges
    workflow.add_conditional_edges(
        "assistant",
        should_continue,
        {
            "tools": "tools",
            "end": END
        }
    )
    
    # Flujo: tools -> observer -> assistant
    workflow.add_edge("tools", "observer")
    workflow.add_edge("observer", "assistant")
    
    # Compilar grafo
    compiled_graph = workflow.compile()
    compiled_graph.max_iterations = 5  # Prevenir loops infinitos
    
    return compiled_graph


# ========================================================================
# FUNCIÓN PRINCIPAL DE PROCESAMIENTO
# ========================================================================
def process_question_react(question: str, graph) -> Dict[str, Any]:
    """Procesa una pregunta usando el grafo ReAct real"""
    
    # Preparar mensaje inicial con contexto
    kpi_descriptions = "\n".join([f"- {name}: {info['description']}" for name, info in KPI_REGISTRY.items()])
    initial_system_message = f"""Eres un experto analista de datos para cafeterías como Bolsillo Coffee.
Tu tarea es responder preguntas sobre datos usando consultas SQL cuando sea necesario.

REGLAS IMPORTANTES:
1. SIEMPRE usa el tool execute_sql para ejecutar consultas SQL
2. Puedes usar get_kpi_sql para obtener consultas predefinidas para métricas comunes
3. Las tablas disponibles son: transacciones, informe_ventas
4. Columnas válidas en transacciones: ID de transacción, Fecha, Hora, Cuenta, Estado, Ejecutar como, Comisión
5. Columnas válidas en informe_ventas: ID de transacción, Fecha, Hora, Cuenta, Descripción, Cantidad, Precio (Bruto), Precio (Neto), Últimos 4 dígitos
6. Excluir propinas con WHERE "Descripción" NOT ILIKE '%Tip%'
7. Si obtienes un error, analízalo y genera una nueva consulta corregida
8. Responde siempre en español y de forma clara para dueños de negocio

KPIs PREDEFINIDOS DISPONIBLES:
{kpi_descriptions}

Pregunta del usuario: """ + question

    initial_state = {
        "question": question,
        "messages": [
            SystemMessage(content=initial_system_message),
            HumanMessage(content=question)
        ],
        "sql_query": None,
        "execution_success": None,
        "result_rows": None,
        "final_response": None,
        "insights_analysis": None,
        "attempt_count": 0
    }
    
    print(f"\n💬 '{question}'")
    print("-" * 60)
    print("🔄 Iniciando ciclo ReAct REAL con tool calling...")
    
    try:
        # Aplicar límite de iteraciones manualmente si es necesario
        final_state = graph.invoke(initial_state)
        
        # Extraer la respuesta final del último mensaje
        messages = final_state.get("messages", [])
        if messages and hasattr(messages[-1], 'content'):
            final_response = messages[-1].content
        else:
            final_response = "No se pudo generar respuesta."
        
        return {
            'response': final_response,
            'messages': messages,
            'attempts': final_state.get('attempt_count', 0)
        }
    except Exception as e:
        error_msg = f"Error en proceso ReAct: {str(e)}"
        print(f"❌ {error_msg}")
        traceback.print_exc()
        return {
            'response': error_msg,
            'messages': [],
            'attempts': 0
        }

# ========================================================================
# FUNCIÓN MAIN ACTUALIZADA
# ========================================================================

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
    
    try:
        api_key = setup_environment()
        if not verify_openai_connection(api_key):
            raise ConnectionError("OpenAI no disponible")
        
        llm = ChatOpenAI(model="gpt-4o", temperature=0, max_tokens=1500)
        db = create_database_connection()
        
        print("\n" + "="*70)
        print("🚀 AGENTE SQL LANGGRAPH v5.0 - REACT REAL CON TOOL CALLING")
        print("="*70)
        print("✅ Tool Calling real con bind_tools")
        print("✅ ToolNode automático")
        print("✅ Reacción a ToolMessages")
        print("✅ Ciclo completo LLM ↔ Tools")
        print("✅ Sin routing determinista")
        print("="*70)
        
        # Mostrar KPIs disponibles
        print("\n📊 KPIs OFICIALES DISPONIBLES:")
        for kpi_name, kpi_info in KPI_REGISTRY.items():
            print(f"   • {kpi_name}: {kpi_info['description']}")
        
        # Crear grafo ReAct real
        graph = create_react_graph_real(llm, db)
        print("\n⚡ Listo para consultas SQL con arquitectura ReAct REAL")
        
        while True:
            q = input("\n💬 Bolsillo > ").strip()
            if q.lower() in ['salir', 'exit', 'q']: 
                break
            if not q: 
                continue
            
            if q.lower() == '/kpi':
                print("\n📊 KPIs OFICIALES:")
                for kpi_name, kpi_info in KPI_REGISTRY.items():
                    print(f"\n   {kpi_name.upper()}:")
                    print(f"     Descripción: {kpi_info['description']}")
                    print(f"     Palabras clave: {', '.join(kpi_info['keywords'])}")
                continue
            
            if q.lower() == '/schema':
                schema = get_database_schema(db)
                print(f"\n🗃️ ESQUEMA TÉCNICO:\n{schema}")
                continue
            
            result = process_question_react(q, graph)
            print("\n" + "="*70)
            print("🤖 RESPUESTA FINAL")
            print("="*70)
            print(result['response'])
            
            # Mostrar detalles si se desea
            show_details = input("\n¿Ver detalles del proceso? (s/n): ").strip().lower()
            if show_details in ['s', 'y']:
                print(f"\n⚡ Mensajes intercambiados ({len(result['messages'])}):")
                for i, msg in enumerate(result['messages']):
                    role = getattr(msg, 'type', 'unknown') if hasattr(msg, 'type') else type(msg).__name__
                    content = getattr(msg, 'content', str(msg))[:200]
                    print(f"   {i+1}. {role}: {content}")
            
            if result.get('attempts', 0) > 0:
                print(f"\n🔄 Intentos: {result['attempts']}")
    
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        traceback.print_exc(limit=5)

if __name__ == "__main__":
    main()
