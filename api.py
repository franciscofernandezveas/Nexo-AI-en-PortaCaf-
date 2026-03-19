# api.py - FastAPI con Streaming, Email, PDF Proactivo y JWT Authentication
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, Any
from contextlib import asynccontextmanager
from pathlib import Path
import json

from fastapi import FastAPI, Depends, HTTPException, status, Header
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from supabase import create_client, Client
from langchain_openai import ChatOpenAI

# Importaciones del agente
from config.environment import setup_environment, verify_openai_connection
from database import create_database_connection
from main import QueryOrchestrator, process_question_with_router, CAPABILITIES, QueryResult

# ========================================================================
# CONFIGURACIÓN
# ========================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")  # ✅ Usar ANON_KEY para autenticación

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("⚠️ Supabase no configurado. Autenticación deshabilitada.")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Cliente de Supabase inicializado")
    except Exception as e:
        logger.error(f"❌ Error inicializando Supabase: {e}")

SESSIONS: Dict[str, QueryOrchestrator] = {}
PENDING_ACTIONS: Dict[str, Dict[str, Any]] = {}

# Crear directorios para archivos estáticos
os.makedirs("files/charts", exist_ok=True)
os.makedirs("files/reports", exist_ok=True)
os.makedirs("visualizations", exist_ok=True)

# ========================================================================
# LIFECYCLE
# ========================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Iniciando servidor FastAPI...")
    api_key = setup_environment()
    if not verify_openai_connection(api_key):
        raise ConnectionError("OpenAI API no disponible")
    logger.info("✅ Servidor listo con autenticación JWT")
    yield
    logger.info("🔄 Cerrando sesiones...")
    for user_id, orchestrator in SESSIONS.items():
        if hasattr(orchestrator, 'executor'):
            orchestrator.executor.shutdown(wait=False)
    SESSIONS.clear()
    PENDING_ACTIONS.clear()
    logger.info("👋 Servidor detenido")

# ========================================================================
# APP
# ========================================================================

app = FastAPI(
    title="Agente SQL Inteligente API",
    version="3.2.0",
    description="API con autenticación JWT via Supabase",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción: ["https://tu-dominio.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/files", StaticFiles(directory="files"), name="files")

if os.path.exists("visualizations"):
    app.mount("/visualizations", StaticFiles(directory="visualizations"), name="visualizations")
    logger.info("✅ Directorio de visualizaciones montado")

# ========================================================================
# MODELOS
# ========================================================================

class QueryRequest(BaseModel):
    question: str = Field(..., description="Pregunta del usuario")

class User(BaseModel):
    id: str
    email: Optional[str] = None
    metadata: Dict = {}

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class EmailReportRequest(BaseModel):
    report_type: str
    summary: str
    generate_pdf: bool = True

class ExportPDFRequest(BaseModel):
    report_type: str
    content: str

class TokenRefreshResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    user: Dict

# ========================================================================
# AUTH - JWT VALIDATION
# ========================================================================

async def get_current_user(authorization: Optional[str] = Header(None)) -> User:
    """
    Valida el token JWT de Supabase y retorna el usuario
    Header esperado: Authorization: Bearer <token>
    """
    # Modo demo si Supabase no está configurado
    if not supabase:
        logger.warning("⚠️ Modo demo: Supabase no configurado")
        return User(id="demo-user", email="demo@example.com")
    
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autorización requerido",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        # Extraer token del header "Bearer TOKEN"
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise ValueError("Esquema de autorización inválido")
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Formato de autorización inválido. Use: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        # Validar token con Supabase
        user_response = supabase.auth.get_user(token)
        user_data = user_response.user
        
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido o expirado",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        logger.info(f"✅ Usuario autenticado: {user_data.email}")
        
        return User(
            id=user_data.id,
            email=user_data.email,
            metadata=user_data.user_metadata or {}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error validando token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Error de autenticación: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ========================================================================
# SESIONES
# ========================================================================

def get_user_orchestrator(user_id: str) -> QueryOrchestrator:
    """Crea o retorna el orchestrator del usuario"""
    if user_id not in SESSIONS:
        logger.info(f"🔄 Creando sesión para: {user_id}")
        
        llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.1,
            max_tokens=600,
            request_timeout=20,
            streaming=False
        )
        
        db = create_database_connection()
        viz_agent = None
        if CAPABILITIES['visualization']:
            viz_agent = CAPABILITIES['viz_agent_class'](llm)
        
        SESSIONS[user_id] = QueryOrchestrator(
            llm, db, viz_agent, 
            enable_memory=CAPABILITIES['memory']
        )
        logger.info(f"✅ Sesión creada para {user_id}")
    
    return SESSIONS[user_id]

# ========================================================================
# UTILIDADES
# ========================================================================

def detect_yes_no_response(text: str) -> Optional[bool]:
    """Detecta respuestas afirmativas/negativas"""
    text_lower = text.lower().strip()
    
    yes_words = ['sí', 'si', 'yes', 'y', 'ok', 'dale', 'claro', 'por supuesto', 
                 'obvio', 'desde luego', 'adelante', 'confirmo', 'afirmativo']
    no_words = ['no', 'nop', 'nope', 'negativo', 'mejor no', 'no gracias',
                'paso', 'cancel', 'cancelar']
    
    if any(word == text_lower or text_lower.startswith(word) for word in yes_words):
        return True
    if any(word == text_lower or text_lower.startswith(word) for word in no_words):
        return False
    
    return None

# ========================================================================
# ENDPOINTS PRINCIPALES
# ========================================================================

@app.get("/")
async def root():
    """Endpoint raíz con información de la API"""
    return {
        "service": "Agente SQL Inteligente API",
        "version": "3.2.0",
        "status": "operational",
        "authentication": "JWT via Supabase",
        "capabilities": CAPABILITIES
    }

@app.get("/api/v1/system/status")
async def system_status():
    """Estado del sistema (público)"""
    return {
        "timestamp": datetime.now().isoformat(),
        "capabilities": CAPABILITIES,
        "active_sessions": len(SESSIONS),
        "pending_actions": len(PENDING_ACTIONS),
        "authentication_enabled": supabase is not None
    }

# ========================================================================
# ENDPOINTS DE AUTENTICACIÓN
# ========================================================================

@app.post("/api/v1/auth/refresh")
async def refresh_token(authorization: Optional[str] = Header(None)):
    """
    Endpoint para refrescar el token JWT
    """
    if not supabase or not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token requerido para refrescar"
        )
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise ValueError("Esquema inválido")
        
        # Refrescar sesión en Supabase
        session_response = supabase.auth.refresh_session(token)
        
        if not session_response.session:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No se pudo refrescar el token"
            )
        
        logger.info(f"✅ Token refrescado para: {session_response.user.email}")
        
        return {
            "access_token": session_response.session.access_token,
            "refresh_token": session_response.session.refresh_token,
            "expires_in": session_response.session.expires_in,
            "user": {
                "id": session_response.user.id,
                "email": session_response.user.email,
                "metadata": session_response.user.user_metadata
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error refrescando token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )

@app.get("/api/v1/auth/verify")
async def verify_token(user: User = Depends(get_current_user)):
    """
    Verifica si el token es válido
    """
    return {
        "valid": True,
        "user": {
            "id": user.id,
            "email": user.email,
            "metadata": user.metadata
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/v1/auth/me")
async def get_me(user: User = Depends(get_current_user)):
    """
    Retorna información del usuario actual
    """
    return {
        "id": user.id,
        "email": user.email,
        "metadata": user.metadata,
        "session_active": user.id in SESSIONS
    }

# ========================================================================
# ENDPOINTS DE SESIÓN
# ========================================================================

@app.post("/api/v1/session/clear")
async def clear_session(user: User = Depends(get_current_user)):
    """Limpia la sesión del usuario"""
    orchestrator = get_user_orchestrator(user.id)
    
    # Limpiar acciones pendientes
    if user.id in PENDING_ACTIONS:
        del PENDING_ACTIONS[user.id]
        logger.info(f"🧹 Acciones pendientes limpiadas para {user.email}")
    
    # Limpiar memoria
    if orchestrator.memory_manager:
        try:
            if hasattr(orchestrator.memory_manager, 'clear_memory'):
                orchestrator.memory_manager.clear_memory()
            logger.info(f"🧹 Memoria limpiada para {user.email}")
            return {"success": True, "message": "Sesión limpiada"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    return {"success": False, "message": "Memoria no disponible"}

@app.delete("/api/v1/session/close")
async def close_session(user: User = Depends(get_current_user)):
    """Cierra y elimina la sesión del usuario"""
    if user.id in SESSIONS:
        orchestrator = SESSIONS[user.id]
        if hasattr(orchestrator, 'executor'):
            orchestrator.executor.shutdown(wait=False)
        del SESSIONS[user.id]
        logger.info(f"🚪 Sesión cerrada para {user.email}")
    
    if user.id in PENDING_ACTIONS:
        del PENDING_ACTIONS[user.id]
    
    return {"success": True, "message": "Sesión cerrada"}

# ========================================================================
# ENDPOINT DE CHAT CON STREAMING
# ========================================================================

@app.post("/api/v1/chat/stream")
async def stream_chat(
    request: QueryRequest,
    user: User = Depends(get_current_user)
):
    """Endpoint con streaming y preguntas proactivas - Requiere JWT"""
    
    async def event_generator():
        orchestrator = get_user_orchestrator(user.id)
        
        try:
            yield f"data: {json.dumps({'type': 'start'})}\n\n"
            
            # ==== VERIFICAR SI HAY ACCIÓN PENDIENTE ====
            if user.id in PENDING_ACTIONS:
                pending = PENDING_ACTIONS[user.id]
                yes_no = detect_yes_no_response(request.question)
                
                if yes_no is not None:
                    action_type = pending['action_type']
                    result_data = pending['result']
                    
                    if action_type == 'ask_email':
                        if yes_no:
                            # Usuario dijo SÍ a enviar email
                            yield f"data: {json.dumps({'type': 'token', 'content': '📧 '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'Perfecto, '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'enviando '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'reporte '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'por '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'email...\n\n'})}\n\n"
                            await asyncio.sleep(0.5)
                            
                            try:
                                if CAPABILITIES['email'] and orchestrator.email_automation:
                                    pdf_path = None
                                    if hasattr(result_data, 'pdf_ready') and result_data.pdf_ready:
                                        report_processor = orchestrator.get_processor('REPORT')
                                        if report_processor:
                                            pdf_data = result_data.pdf_data
                                            pdf_path = report_processor.export_to_pdf(
                                                question=pdf_data['question'],
                                                report_result=pdf_data['report'],
                                                template_name=pdf_data['template_name'],
                                                visualization_path=pdf_data.get('visualization_path')
                                            )
                                    
                                    email_result = orchestrator.email_automation.send_report(
                                        report_type=result_data.intent_type.replace('REPORT_', '').title(),
                                        summary=result_data.response[:500],
                                        pdf_path=Path(pdf_path) if pdf_path else None
                                    )
                                    
                                    if email_result.success:
                                        message = f"✅ Reporte enviado exitosamente por email a: {', '.join(email_result.recipients)}"
                                    else:
                                        message = f"❌ Error enviando email: {email_result.error}"
                                else:
                                    message = "❌ Sistema de emails no disponible"
                                
                                for word in message.split(' '):
                                    yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                    await asyncio.sleep(0.02)
                                
                            except Exception as e:
                                error_msg = f"❌ Error: {str(e)}"
                                for word in error_msg.split(' '):
                                    yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                    await asyncio.sleep(0.02)
                            
                            del PENDING_ACTIONS[user.id]
                            yield f"data: {json.dumps({'type': 'complete', 'intent': 'EMAIL_SENT', 'success': True})}\n\n"
                            return
                        
                        else:
                            message = "📄 Entendido. ¿Quieres exportar el reporte a PDF?"
                            for word in message.split(' '):
                                yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                await asyncio.sleep(0.02)
                            
                            PENDING_ACTIONS[user.id] = {
                                'action_type': 'ask_pdf',
                                'result': result_data,
                                'timestamp': datetime.now().isoformat()
                            }
                            
                            yield f"data: {json.dumps({'type': 'complete', 'intent': 'WAITING_PDF_RESPONSE', 'success': True, 'awaiting_response': True})}\n\n"
                            return
                    
                    elif action_type == 'ask_pdf':
                        if yes_no:
                            yield f"data: {json.dumps({'type': 'token', 'content': '📄 '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'Generando '})}\n\n"
                            await asyncio.sleep(0.02)
                            yield f"data: {json.dumps({'type': 'token', 'content': 'PDF...\n\n'})}\n\n"
                            await asyncio.sleep(0.5)
                            
                            try:
                                report_processor = orchestrator.get_processor('REPORT')
                                if report_processor and hasattr(result_data, 'pdf_ready') and result_data.pdf_ready:
                                    pdf_data = result_data.pdf_data
                                    pdf_path = report_processor.export_to_pdf(
                                        question=pdf_data['question'],
                                        report_result=pdf_data['report'],
                                        template_name=pdf_data['template_name'],
                                        visualization_path=pdf_data.get('visualization_path')
                                    )
                                    
                                    if pdf_path:
                                        import shutil
                                        filename = Path(pdf_path).name
                                        new_path = f"files/reports/{filename}"
                                        shutil.copy(pdf_path, new_path)
                                        
                                        message = f"✅ PDF generado exitosamente. Descárgalo desde: /files/reports/{filename}"
                                        pdf_url = f"/files/reports/{filename}"
                                    else:
                                        message = "❌ Error generando PDF"
                                        pdf_url = None
                                else:
                                    message = "❌ No se pudo generar el PDF"
                                    pdf_url = None
                                
                                for word in message.split(' '):
                                    yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                    await asyncio.sleep(0.02)
                                
                            except Exception as e:
                                error_msg = f"❌ Error: {str(e)}"
                                for word in error_msg.split(' '):
                                    yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                    await asyncio.sleep(0.02)
                                pdf_url = None
                            
                            del PENDING_ACTIONS[user.id]
                            
                            final_data = {
                                'type': 'complete',
                                'intent': 'PDF_GENERATED',
                                'success': True,
                                'pdf_url': pdf_url
                            }
                            yield f"data: {json.dumps(final_data)}\n\n"
                            return
                        
                        else:
                            message = "👍 Perfecto, si necesitas algo más avísame."
                            for word in message.split(' '):
                                yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                                await asyncio.sleep(0.02)
                            
                            del PENDING_ACTIONS[user.id]
                            yield f"data: {json.dumps({'type': 'complete', 'intent': 'CONVERSATION_END', 'success': True})}\n\n"
                            return
                
                else:
                    del PENDING_ACTIONS[user.id]
            
            # ==== PROCESAR PREGUNTA NORMAL ====
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                process_question_with_router,
                request.question,
                orchestrator
            )
            
            # Streaming de la respuesta
            if result.success and result.response:
                words = result.response.split(' ')
                for i, word in enumerate(words):
                    token = word if i == len(words) - 1 else word + ' '
                    yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"
                    await asyncio.sleep(0.02)
            
            # ==== SI ES REPORTE, PREGUNTAR AUTOMÁTICAMENTE ====
            if result.intent_type.startswith('REPORT_') and result.success and CAPABILITIES['email']:
                await asyncio.sleep(0.3)
                
                question = "\n\n💡 ¿Quieres que envíe este reporte por email?"
                for word in question.split(' '):
                    yield f"data: {json.dumps({'type': 'token', 'content': word + ' '})}\n\n"
                    await asyncio.sleep(0.02)
                
                PENDING_ACTIONS[user.id] = {
                    'action_type': 'ask_email',
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
                
                viz_url = None
                if result.visualization and result.visualization.get('success'):
                    img_path = result.visualization.get('image_path', '')
                    if img_path:
                        filename = img_path.replace('\\', '/').split('/')[-1]
                        viz_url = f"/visualizations/{filename}"
                
                final = {
                    'type': 'complete',
                    'intent': result.intent_type,
                    'success': result.success,
                    'confidence': result.confidence,
                    'has_visualization': bool(viz_url),
                    'visualization_url': viz_url,
                    'awaiting_response': True,
                    'timestamp': datetime.now().isoformat()
                }
                yield f"data: {json.dumps(final)}\n\n"
            
            else:
                viz_url = None
                if result.visualization and result.visualization.get('success'):
                    img_path = result.visualization.get('image_path', '')
                    if img_path:
                        filename = img_path.replace('\\', '/').split('/')[-1]
                        viz_url = f"/visualizations/{filename}"
                
                final = {
                    'type': 'complete',
                    'intent': result.intent_type,
                    'success': result.success,
                    'confidence': result.confidence,
                    'has_visualization': bool(viz_url),
                    'visualization_url': viz_url,
                    'timestamp': datetime.now().isoformat()
                }
                yield f"data: {json.dumps(final)}\n\n"
            
        except Exception as e:
            logger.error(f"❌ Error en streaming: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ========================================================================
# ENDPOINTS DE EMAIL Y PDF
# ========================================================================

@app.post("/api/v1/email/send-report")
async def send_report_email(
    request: EmailReportRequest,
    user: User = Depends(get_current_user)
):
    """Envía reporte por email - Requiere JWT"""
    if not CAPABILITIES['email']:
        raise HTTPException(status_code=503, detail="Sistema de emails no disponible")
    
    orchestrator = get_user_orchestrator(user.id)
    
    if not orchestrator.email_automation:
        raise HTTPException(status_code=503, detail="Email automation no inicializado")
    
    try:
        email_result = orchestrator.email_automation.send_report(
            report_type=request.report_type,
            summary=request.summary,
            pdf_path=None
        )
        
        return {
            "success": email_result.success,
            "recipients": email_result.recipients,
            "error": email_result.error,
            "timestamp": email_result.timestamp.isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error enviando email: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/reports/export-pdf")
async def export_pdf(
    request: ExportPDFRequest,
    user: User = Depends(get_current_user)
):
    """Exporta reporte a PDF - Requiere JWT"""
    raise HTTPException(status_code=501, detail="Implementación pendiente")

# ========================================================================
# MANEJO DE ERRORES
# ========================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"❌ Error no manejado: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

# ========================================================================
# MAIN
# ========================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
