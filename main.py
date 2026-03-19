import logging
import os
import sys
import json
import traceback
import re
from typing import List, Dict, Optional, Any
from pydantic import Field
from langgraph.graph import MessagesState

# Conteo de tokens - AGREGADO SIN ROMPER ARQUITECTURA
try:
    import tiktoken
    def count_tokens_simple(text: str) -> int:
        try:
            encoder = tiktoken.encoding_for_model("gpt-4o")
            return len(encoder.encode(str(text)))
        except:
            return len(str(text)) // 4
except ImportError:
    def count_tokens_simple(text: str) -> int:
        return len(str(text)) // 4

# LangChain imports
try:
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
    from langchain_core.tools import tool
except ImportError:
    print("❌ ERROR: LangChain desactualizado")
    print("Ejecuta: pip install --upgrade langchain-core langchain-community langchain-openai")
    sys.exit(1)

from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition
from langchain_openai import ChatOpenAI

# Configuración inicial
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.environment import setup_environment, verify_openai_connection
from database import create_database_connection

# ========================================================================
# KPI REGISTRY (CATÁLOGO OFICIAL DE MÉTRICAS)
# ========================================================================

from config.metrics_loader import metrics_loader

# Reemplazar las secciones antiguas con:
# ========================================================================
# KPI REGISTRY Y ESQUEMA CARGADOS DINÁMICAMENTE
# ========================================================================

KPI_REGISTRY = metrics_loader.load_kpi_registry()
REAL_SCHEMA_VIEWS = metrics_loader.load_schema_views()


# ========================================================================
# FUNCIONES DE EXTRACCIÓN Y UTILIDADES
# ========================================================================

def extract_sql_result(messages):
    """Extrae el resultado SQL más reciente de los mensajes"""
    for msg in reversed(messages):
        if isinstance(msg, ToolMessage):
            if msg.name == "execute_sql":
                try:
                    # Intentar parsear JSON si es posible
                    if msg.content.startswith('{') or msg.content.startswith('['):
                        return json.loads(msg.content)
                    else:
                        return msg.content
                except:
                    return msg.content
    return None

def extract_user_question(messages):
    """Extrae la pregunta original del usuario"""
    for msg in messages:
        if isinstance(msg, HumanMessage):
            return msg.content
    return "Consulta de datos"

# ========================================================================
# PROMPT DEL SISTEMA OPTIMIZADO
# ========================================================================
# ========================================================================
# PROMPT DEL SISTEMA JERÁRQUICO (MULTI-ROL)
# ========================================================================
SYSTEM_PROMPT = """
ERES UN SISTEMA DE INTELIGENCIA DE NEGOCIOS PARA PORTACAFÉ CON DOS MODOS DE OPERACIÓN:

========================
MODO 1: ANALISTA (USO DE HERRAMIENTAS)
========================
Tu objetivo es obtener datos precisos.

REGLAS:
- Usa herramientas cuando sea necesario
- Prioriza KPIs existentes (get_kpi_query)
- Ejecuta consultas con execute_sql
- NO generes conclusiones finales aún
- NO escribas respuestas largas al usuario

========================
MODO 2: CONSULTOR (SÍNTESIS FINAL)
========================
Tu objetivo es entregar insights accionables.

REGLAS:
- Interpreta datos, no los repitas
- Explica causas y consecuencias
- Propón acciones concretas

========================
REGLAS GLOBALES
========================
- Nunca inventes datos
- Prioriza precisión sobre creatividad
- Mantén coherencia entre modos
- Habla siempre como experto en cafeterías

KPIs DISPONIBLES:
""" + "\n".join([f"- {kpi}: {info['description']}" for kpi, info in list(KPI_REGISTRY.items())[:10]]) + """

VISTAS DISPONIBLES:
""" + "\n".join([f"- {view}" for view in list(REAL_SCHEMA_VIEWS.keys())[:10]])


# ========================================================================
# COMPONENTES GLOBALES
# ========================================================================
_lazy_components = {}

# ========================================================================
# HERRAMIENTAS DEL AGENTE (SIMPLIFICADAS)
# ========================================================================

@tool
def get_kpi_query(kpi_name: str) -> str:
    """
    Obtiene la consulta SQL para un KPI específico.
    
    Args:
        kpi_name: Nombre del KPI registrado
        
    Returns:
        Consulta SQL como string
    """
    kpi_info = KPI_REGISTRY.get(kpi_name.lower())
    if not kpi_info:
        available_kpis = ", ".join(KPI_REGISTRY.keys())
        return f"KPI_ERROR: '{kpi_name}' no encontrado. Disponibles: {available_kpis}"
    
    return kpi_info["sql_template"]

@tool
def get_available_schema() -> str:
    """
    Devuelve información sobre las vistas y columnas disponibles en la base de datos.
    
    Returns:
        Descripción del esquema disponible
    """
    schema_info = "ESQUEMA DISPONIBLE:\n"
    for view, columns in REAL_SCHEMA_VIEWS.items():
        schema_info += f"- {view}: {', '.join(columns)}\n"
    return schema_info

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
    
    query_clean = query.strip().lower()
    if not query_clean.startswith(("select", "with")):
        return "SQL_SECURITY_ERROR: Solo se permiten consultas SELECT"
    
    forbidden_keywords = [
        "insert", "update", "delete", "drop", "alter", 
        "truncate", "grant", "revoke", "create", "execute"
    ]
    if any(keyword in query_clean for keyword in forbidden_keywords):
        return "SQL_SECURITY_ERROR: Operación SQL no permitida"
    
    allowed_patterns = ['semantic.', 'bi.', 'dw.', 'public.']
    schema_validation_passed = any(pattern in query_clean for pattern in allowed_patterns)
    if not schema_validation_passed:
        registered_views = list(REAL_SCHEMA_VIEWS.keys())
        view_found = any(view in query_clean for view in registered_views)
        if not view_found:
            available_views = ", ".join(registered_views)
            return f"SQL_ERROR: Vista no encontrada. Vistas disponibles: {available_views}"
    
    try:
        execute_query = _lazy_components.get("sql_tool")
        if not execute_query:
            return "ERROR: Herramienta SQL no inicializada"
        
        print("\n" + "="*50)
        print("🔍 CONSULTA SQL DETECTADA")
        print("="*50)
        print(query)
        print("-" * 50)
        
        import time
        start_time = time.time()
        result = execute_query.invoke({"query": query})
        end_time = time.time()
        
        print(f"✅ CONSULTA COMPLETADA en {end_time - start_time:.2f} segundos")
        
        if isinstance(result, str):
            if "error" in result.lower() or "syntax" in result.lower():
                print(f"❌ ERROR EN CONSULTA: {result}")
                return result
            try:
                parsed_result = json.loads(result)
                if isinstance(parsed_result, list) and len(parsed_result) == 0:
                    print("📝 Resultado: EMPTY_RESULT (0 filas)")
                    return "EMPTY_RESULT"
                else:
                    row_count = len(parsed_result)
                    print(f"📊 Resultado: {row_count} filas obtenidas")
                    return json.dumps(parsed_result, default=str)
            except json.JSONDecodeError:
                if not result.strip():
                    print("📝 Resultado: EMPTY_RESULT (contenido vacío)")
                    return "EMPTY_RESULT"
                print(f"📄 Resultado: Texto plano ({len(result)} caracteres)")
                return result
        else:
            if isinstance(result, list) and len(result) == 0:
                print("📝 Resultado: EMPTY_RESULT (lista vacía)")
                return "EMPTY_RESULT"
            row_count = len(result)
            print(f"📊 Resultado: {row_count} elementos obtenidos")
            return json.dumps(result, default=str)
            
    except Exception as e:
        error_msg = f"SQL_ERROR: {str(e)}"
        print(f"💥 ERROR FATAL EN CONSULTA: {error_msg}")
        return error_msg

# ========================================================================
# ESTADO DEL AGENTE (SIMPLIFICADO)
# ========================================================================
AgentState = MessagesState

def agent_node(state: AgentState) -> Dict[str, Any]:
    """
    Nodo principal del agente que razona y decide qué herramientas usar.
    """
    llm = _lazy_components.get("llm")
    if not llm:
        raise ValueError("LLM no inicializado")

    # Solo las herramientas esenciales
    tools_to_bind = [get_kpi_query, execute_sql, get_available_schema]

    # Agregar RAG si está disponible
    retriever = _lazy_components.get("retriever_tool")
    if retriever:
        tools_to_bind.append(retriever)

    # Bind tools al LLM
    bound_llm = llm.bind_tools(tools_to_bind)

    # ✅ CORREGIDO: DEFINIR SYSTEM PROMPT SOLO AQUÍ
    contexto_para_llm = [
        SystemMessage(content=SYSTEM_PROMPT),  # ✅ Solo SYSTEM_PROMPT
        *state["messages"]  # ✅ Usar state original sin modificaciones
    ]
    
    # CONTEO DE TOKENS - AGREGADO SIN ROMPER FLUJO
    tokens_input = sum(count_tokens_simple(str(getattr(msg, 'content', ''))) for msg in state["messages"])
    tokens_total = sum(count_tokens_simple(str(getattr(msg, 'content', ''))) for msg in contexto_para_llm)
    print(f"📊 Tokens input: {tokens_input} | Total: {tokens_total}")

    response = bound_llm.invoke(contexto_para_llm)

    # Logging
    if getattr(response, "tool_calls", None):
        print(f"🛠️ Tool calls → {[tc['name'] for tc in response.tool_calls]}")
    else:
        tokens_respuesta = count_tokens_simple(response.content)
        print(f"🧠 Respuesta directa → {response.content[:120]}... (okens: {tokens_respuesta})")

    # ✅ CORREGIDO: NO duplicar historial
    return {"messages": [response]}



def response_node(state: AgentState) -> Dict[str, Any]:
    """
    Nodo especializado en generar respuestas ejecutivas como consultor BI senior.
    """
    llm = _lazy_components.get("llm")
    
    # CONTEO DE TOKENS - AGREGADO SIN ROMPER FLUJO
    tokens_contexto = sum(count_tokens_simple(str(getattr(msg, 'content', ''))) for msg in state["messages"])
    print(f"📊 Tokens contexto respuesta: {tokens_contexto}")

    # Extraer contexto completo
    user_question = extract_user_question(state["messages"])
    sql_result = extract_sql_result(state["messages"])
    
    # Convertir resultado a string legible
    if isinstance(sql_result, (dict, list)):
        result_str = json.dumps(sql_result, indent=2, default=str)
    else:
        result_str = str(sql_result) if sql_result else "No hay datos disponibles"
    
    # Prompt profesional de consultor BI con contexto estructurado
    consultor_prompt = f"""
MODO: CONSULTOR

Pregunta del usuario:
{user_question}

Datos obtenidos:
{result_str}

Genera una respuesta ejecutiva siguiendo esta estructura:

---
RESUMEN EJECUTIVO
2-3 frases clave para el dueño del negocio.

---
HALLAZGOS CLAVE
• Formato: Métrica: Valor, Variación/Tendencia
• Ej: Ventas hoy: $50,000, ↓5% vs ayer

---
INTERPRETACIÓN DEL NEGOCIO
Qué implica esto para las operaciones cafetería.

---
RECOMENDACIONES
1. Acción específica con impacto medible
2. Medida operativa inmediata

REGLAS:
- No inventes datos ni extrapolaciones
- No repitas tablas completas
- Habla como consultor senior, no como técnico
- Sé conciso y accionable
"""

    # ✅ CORREGIDO: NO duplicar SYSTEM_PROMPT
    contexto_para_respuesta = [
        *state["messages"],  # ✅ Usar state original
        HumanMessage(content=consultor_prompt)
    ]
    
    # CONTEO DE TOKENS TOTAL
    tokens_total_respuesta = sum(count_tokens_simple(str(getattr(msg, 'content', ''))) for msg in contexto_para_respuesta)
    print(f"📈 Tokens totales respuesta: {tokens_total_respuesta}")

    response = llm.invoke(contexto_para_respuesta)

    # CONTEO DE TOKENS DE RESPUESTA
    tokens_respuesta_final = count_tokens_simple(response.content)
    print(f"📝 RESPUESTA SINTETIZADA: {response.content[:120]}... (okens: {tokens_respuesta_final})")
    
    # ✅ CORREGIDO: NO modificar state
    return {"messages": [response]}


def process_question_react(question: str, graph) -> Dict[str, Any]:
    """
    Procesa una pregunta con el agente ReAct optimizado.
    """
    # ✅ CORREGIDO: LIMPIAR initial_state
    initial_state = {
        "messages": [
            HumanMessage(content=question)  # ✅ Solo el mensaje del usuario
        ]
    }
    
    print(f"\n💬 '{question}'")
    print("-" * 60)
    
    try:
        final_state = graph.invoke(initial_state)
        
        # Extraer respuesta final (último mensaje no-tool)
        messages = final_state.get("messages", [])
        final_response = "No se pudo generar respuesta."
        
        for msg in reversed(messages):
            if hasattr(msg, 'content') and msg.content and not getattr(msg, 'tool_calls', None):
                final_response = msg.content
                break
        
        return {
            'response': final_response,
            'messages': messages
        }
    except Exception as e:
        error_msg = f"Error en proceso: {str(e)}"
        print(f"❌ {error_msg}")
        traceback.print_exc()
        return {
            'response': error_msg,
            'messages': []
        }

# ========================================================================
# CREAR GRAFO (OPTIMIZADO)
# ========================================================================
def create_react_graph_real(llm, db) -> StateGraph:
    """
    Crea el grafo ReAct optimizado con arquitectura limpia.
    """
    # Inicializar componentes
    _lazy_components['llm'] = llm
    _lazy_components['db'] = db
    
    from langchain_community.tools import QuerySQLDatabaseTool
    _lazy_components["sql_tool"] = QuerySQLDatabaseTool(db=db)
    
    # Inicializar RAG si existe
    try:
        from rag.chroma_retriever import get_retriever_tool
        _lazy_components["retriever_tool"] = get_retriever_tool()
        print("✅ Chroma retriever inicializado correctamente")
    except Exception as e:
        print(f"⚠️ Error inicializando Chroma retriever: {e}")
        _lazy_components["retriever_tool"] = None
    
    # Construir grafo
    builder = StateGraph(AgentState)

    # Agregar nodos
    builder.add_node("agent", agent_node)
    
    # Herramientas disponibles
    tools = [get_kpi_query, execute_sql, get_available_schema]
    if _lazy_components.get("retriever_tool"):
        tools.append(_lazy_components["retriever_tool"])
    
    tool_node = ToolNode(tools)
    builder.add_node("tools", tool_node)
    builder.add_node("response", response_node)

    # Configurar flujo
    builder.set_entry_point("agent")
    
    builder.add_conditional_edges(
        "agent",
        tools_condition,
        {
            "tools": "tools",
            END: "response",
        },
    )

    builder.add_edge("tools", "agent")
    builder.add_edge("response", END)

    graph = builder.compile()
    graph.max_iterations = 15
    
    return graph

# ========================================================================
# FUNCIÓN PRINCIPAL
# ========================================================================
def main():
    """
    Función principal del agente BI refactorizado.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
    
    try:
        api_key = setup_environment()
        if not verify_openai_connection(api_key):
            raise ConnectionError("OpenAI no disponible")
        
        # Modelo optimizado para análisis
        llm = ChatOpenAI(model="gpt-4o", temperature=0, max_tokens=2000)
        db = create_database_connection()

        print("\n" + "="*70)
        print("🚀 AGENTE SQL LANGGRAPH v6.0 - REFACTORIZADO PRO")
        print("="*70)
        
        # Mostrar capacidades
        print("\n📋 VISTAS DISPONIBLES:")
        for vista in list(REAL_SCHEMA_VIEWS.keys())[:5]:  # Mostrar solo primeras 5
            print(f"  • {vista}")
        
        print(f"\n📊 KPIs DISPONIBLES ({len(KPI_REGISTRY)}):")
        for kpi_name in list(KPI_REGISTRY.keys())[:5]:  # Mostrar solo primeros 5
            print(f"  • {kpi_name}")
        
        # Crear grafo optimizado
        graph = create_react_graph_real(llm, db)
        print("\n⚡ Listo para consultas")
        
        while True:
            q = input("\n💬 Bolsillo > ").strip()
            if q.lower() in ['salir', 'exit', 'q']: 
                break
            if not q: 
                continue
            
            result = process_question_react(q, graph)
            print("\n" + "="*70)
            print("🤖 RESPUESTA FINAL")
            print("="*70)
            print(result['response'])
            
            # Opción para ver detalles técnicos
            show_details = input("\n¿Ver detalles del proceso? (s/n): ").strip().lower()
            if show_details in ['s', 'y']:
                print(f"\n📝 MENSAJES COMPLETOS:")
                for i, msg in enumerate(result['messages']):
                    role = type(msg).__name__
                    content = getattr(msg, 'content', str(msg))[:200] if hasattr(msg, 'content') else str(msg)[:200]
                    tool_calls = getattr(msg, 'tool_calls', None)
                    print(f"   {i+1}. {role}: {content}")
                    if tool_calls:
                        print(f"      🛠️  Tool calls: {tool_calls}")
    
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        traceback.print_exc(limit=5)

if __name__ == "__main__":
    main()
