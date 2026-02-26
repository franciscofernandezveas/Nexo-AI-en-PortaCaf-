import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import {
  ArrowLeft,
  LogIn,
  TrendingUp,
  CreditCard,
  Clock,
  DollarSign,
  Users,
  Coffee,
  BarChart2,
  Percent,
  ShoppingCart,
  Eye,
  RefreshCw,
  ChevronDown,
  ChevronUp
} from "lucide-react";

import { colors, spacing, typography } from "../components/dashboardStyles";
import { GlobalStyles } from "../components/DashboardComponents";
import BusinessStateIndicator from "../components/dashboard/states/BusinessStateIndicator"; // Importar el componente

import PeakHoursKPI from "../components/dashboard/kpis/PeakHoursKPI";
import PaymentMethodsKPI from "../components/dashboard/kpis/PaymentMethodsKPI";
import OverviewTableKPI from "../components/dashboard/kpis/OverviewTableKPI";
import TipsAnalysisTable from "../components/dashboard/kpis/TipsAnalysisTable";
import CustomerLoyaltyKPI from "../components/dashboard/kpis/CustomerLoyaltyKPI";
import TopProductsKPI from "../components/dashboard/kpis/TopProductsKPI";

// ===== ESTILOS PARA EL NUEVO LAYOUT =====
const LAYOUT_CONFIG = {
  // Quick Stats - Cards pequeñas
  QUICK_STATS_CARD: {
    background: "#FFFFFF",
    border: "1px solid #E5E7EB",
    borderRadius: "16px",
    boxShadow: "0 2px 8px rgba(0, 0, 0, 0.04)",
    padding: "20px",
    width: "100%",
    boxSizing: "border-box",
    minHeight: "150px",
    display: "flex",
    flexDirection: "column"
  },
  
  // Deep Dive - Cards grandes
  DEEP_DIVE_CARD: {
    background: "#FFFFFF",
    border: "1px solid #E5E7EB",
    borderRadius: "16px",
    boxShadow: "0 3px 12px rgba(0, 0, 0, 0.05)",
    padding: "24px",
    width: "100%",
    boxSizing: "border-box"
  },
  
  // Botón de acción
  ACTION_BUTTON: (isActive = false) => ({
    background: isActive ? "linear-gradient(135deg, #3b82f6, #6366f1)" : "#f3f4f6",
    color: isActive ? "#FFFFFF" : "#4b5563",
    border: "none",
    borderRadius: "12px",
    padding: "14px 20px",
    cursor: "pointer",
    fontWeight: 600,
    fontSize: "14px",
    display: "flex",
    alignItems: "center",
    gap: "10px",
    transition: "all 0.2s ease",
    boxShadow: isActive ? "0 4px 12px rgba(59, 130, 246, 0.3)" : "none",
    flex: 1,
    minHeight: "60px",
    "&:hover": {
      transform: "translateY(-2px)",
      boxShadow: "0 6px 16px rgba(59, 130, 246, 0.4)"
    }
  }),
  
  // Botón de recarga
  RELOAD_BUTTON: {
    background: "#10B981",
    color: "#FFFFFF",
    border: "none",
    borderRadius: "10px",
    padding: "10px 20px",
    cursor: "pointer",
    fontWeight: 600,
    fontSize: "13px",
    display: "flex",
    alignItems: "center",
    gap: "6px",
    transition: "all 0.2s ease",
    "&:hover": {
      background: "#059669",
      transform: "translateY(-1px)"
    }
  }
};

const HEADER_TITLE_STYLE = {
  ...typography.h2,
  color: "#111827",
  fontWeight: 700,
  margin: 0,
  fontSize: "24px"
};

const BACK_BUTTON_STYLE = {
  display: "flex",
  alignItems: "center",
  gap: "6px",
  background: "transparent",
  border: "none",
  color: "#6B7280",
  cursor: "pointer",
  fontSize: "14px",
  fontWeight: 500,
  padding: "8px 16px",
  borderRadius: "10px",
  transition: "background 0.2s",
  "&:hover": {
    background: "#F3F4F6"
  }
};

const SECTION_TITLE_STYLE = {
  color: "#111827",
  fontWeight: 600,
  fontSize: "18px",
  marginBottom: "16px",
  display: "flex",
  alignItems: "center",
  gap: "10px"
};

const BUTTONS_CONTAINER_STYLE = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
  gap: "16px",
  marginBottom: "24px",
  marginTop: "16px"
};

const EXPANDED_SECTION_STYLE = {
  background: "#FFFFFF",
  borderRadius: "16px",
  border: "1px solid #E5E7EB",
  boxShadow: "0 4px 20px rgba(0, 0, 0, 0.08)",
  marginBottom: "24px",
  overflow: "hidden"
};

const EXPANDED_HEADER_STYLE = {
  padding: "16px 24px",
  borderBottom: "1px solid #E5E7EB",
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  background: "linear-gradient(135deg, #f9fafb 0%, #f3f4f6 100%)"
};

// Componente para Quick Stats Card
const QuickStatsCard = ({ title, children, icon: Icon, loading }) => (
  <div style={LAYOUT_CONFIG.QUICK_STATS_CARD}>
    <div style={{
      display: "flex",
      alignItems: "center",
      gap: "10px",
      marginBottom: "14px"
    }}>
      {Icon && (
        <div style={{
          width: "36px",
          height: "36px",
          background: "linear-gradient(135deg, #f0f9ff, #dbeafe)",
          borderRadius: "10px",
          display: "flex",
          alignItems: "center",
          justifyContent: "center"
        }}>
          <Icon size={18} color="#3b82f6" />
        </div>
      )}
      <h3 style={{
        color: "#111827",
        fontWeight: 600,
        fontSize: "15px",
        margin: 0
      }}>
        {title}
      </h3>
    </div>
    {loading ? (
      <div style={{
        flex: 1,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#9CA3AF"
      }}>
        <div style={{
          width: "24px",
          height: "24px",
          border: "3px solid #cbd5e1",
          borderTopColor: "#3b82f6",
          borderRadius: "50%",
          animation: "spin 1s linear infinite"
        }} />
      </div>
    ) : (
      <div style={{ flex: 1 }}>
        {children}
      </div>
    )}
  </div>
);

// Componente para botón de acción
const ActionButton = ({ icon: Icon, label, onClick, isActive, loading }) => (
  <button
    onClick={onClick}
    style={LAYOUT_CONFIG.ACTION_BUTTON(isActive)}
    disabled={loading}
  >
    {loading ? (
      <>
        <div style={{
          width: "18px",
          height: "18px",
          border: "2px solid rgba(255, 255, 255, 0.3)",
          borderTopColor: "#FFFFFF",
          borderRadius: "50%",
          animation: "spin 1s linear infinite"
        }} />
        Cargando...
      </>
    ) : (
      <>
        <Icon size={20} />
        {label}
      </>
    )}
  </button>
);

export default function SalesOverview() {
  const { user, session } = useAuth();
  const navigate = useNavigate();
  
  // Estados para controlar qué secciones están expandidas
  const [expandedSection, setExpandedSection] = useState(null); // 'peakHours' | 'paymentMethods' | 'loyalty' | 'products'
  const [peakHoursLoading, setPeakHoursLoading] = useState(false);
  const [paymentMethodsLoading, setPaymentMethodsLoading] = useState(false);
  const [loyaltyLoading, setLoyaltyLoading] = useState(false);
  const [productsLoading, setProductsLoading] = useState(false);

  // Estado de datos
  const [data, setData] = useState({
    overview: [],
    paymentMethods: [],
    peakHours: [],
    tipsAnalysis: [],
    customerLoyalty: [],
    topProducts: []
  });

  const [loading, setLoading] = useState({
    overview: true,
    paymentMethods: true,
    peakHours: true,
    tipsAnalysis: true,
    customerLoyalty: true,
    topProducts: true
  });

  const [errors, setErrors] = useState({});

  /* ===============================
     FORMAT HELPERS
  =============================== */
  const safeRender = (value) => {
    if (value === null || value === undefined) return "—";
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  };

  const formatCurrency = (value) => {
    if (!value || isNaN(value)) return "—";
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
      minimumFractionDigits: 0
    }).format(Number(value));
  };

  const formatNumber = (value) => {
    if (!value || isNaN(value)) return "—";
    return new Intl.NumberFormat("es-CL").format(Number(value));
  };

  const formatPercentage = (value) => {
    if (!value || isNaN(value)) return "—";
    return `${Number(value)}%`;
  };

  /* ===============================
     DATA TRANSFORMS
  =============================== */
  const peakHoursChartData = (() => {
    if (!Array.isArray(data.peakHours)) return [];

    const map = {
      sede_plaza_bolsillo: "plaza",
      sede_merced: "merced",
      sede_tajamar: "tajamar"
    };

    return data.peakHours.map(row => {
      const hour = parseInt(row.hora_del_dia) || 0;
      const obj = { hora: `${hour.toString().padStart(2, "0")}:00` };
      
      Object.entries(map).forEach(([k, v]) => {
        obj[v] = parseInt(row[k]) || 0;
      });
      return obj;
    });
  })();

  const paymentMethodsChartData = (() => {
    if (!Array.isArray(data.paymentMethods)) return [];
    return data.paymentMethods.map(m => ({
      name: m.medio_de_pago || "—",
      value: Number(m.participacion_ventas_pct) || 0,
      transacciones: m.total_transacciones,
      ventas: m.ventas_brutas
    }));
  })();

  /* ===============================
     FETCH SYSTEM
  =============================== */
  const fetchKPI = async (endpoint, key, setLoadingState = null) => {
    try {
      if (setLoadingState) {
        setLoadingState(true);
      } else {
        setLoading(l => ({ ...l, [key]: true }));
      }
      
      const token = session?.access_token;
      if (!token) throw new Error("No autenticado");

      const res = await fetch(
        `http://localhost:8000/api/sales/${endpoint}`,
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      if (!res.ok) throw new Error(`Error ${res.status}`);
      const json = await res.json();
      setData(d => ({ ...d, [key]: Array.isArray(json) ? json : json.data || [] }));
      setErrors(e => ({ ...e, [key]: null }));
    } catch (err) {
      console.error(key, err);
      setErrors(e => ({ ...e, [key]: err.message }));
    } finally {
      if (setLoadingState) {
        setLoadingState(false);
      } else {
        setLoading(l => ({ ...l, [key]: false }));
      }
    }
  };

  // Funciones específicas para cada sección expandida
  const loadPeakHours = () => {
    if (data.peakHours.length === 0) {
      fetchKPI("peak-hours", "peakHours", setPeakHoursLoading);
    }
    setExpandedSection(expandedSection === 'peakHours' ? null : 'peakHours');
  };

  const loadPaymentMethods = () => {
    if (data.paymentMethods.length === 0) {
      fetchKPI("payment-methods", "paymentMethods", setPaymentMethodsLoading);
    }
    setExpandedSection(expandedSection === 'paymentMethods' ? null : 'paymentMethods');
  };

  const loadLoyalty = () => {
    if (data.customerLoyalty.length === 0) {
      fetchKPI("customer-loyalty", "customerLoyalty", setLoyaltyLoading);
    }
    setExpandedSection(expandedSection === 'loyalty' ? null : 'loyalty');
  };

  const loadProducts = () => {
    if (data.topProducts.length === 0) {
      fetchKPI("products-global", "topProducts", setProductsLoading);
    }
    setExpandedSection(expandedSection === 'products' ? null : 'products');
  };

  useEffect(() => {
    if (!user || !session) return;
    // Cargar solo los datos esenciales al inicio
    fetchKPI("overview", "overview");
    fetchKPI("tips-analysis", "tipsAnalysis");
  }, [user, session]);

  /* ===============================
     AUTH SCREEN
  =============================== */
  if (!user || !session) {
    return (
      <div style={{
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
        gap: spacing.lg
      }}>
        <LogIn size={48} color={colors.gray[400]} />
        <h2 style={typography.h2}>Inicia sesión</h2>
        <button
          onClick={() => navigate("/login")}
          style={{
            padding: "12px 24px",
            background: colors.primary[600],
            color: "#fff",
            border: "none",
            borderRadius: "8px",
            cursor: "pointer"
          }}
        >
          Ir a Login
        </button>
      </div>
    );
  }

  /* ===============================
     UI REFACTORIZADO
  =============================== */
  return (
    <>
      <GlobalStyles />
      <div style={{
        padding: "24px",
        background: "#F9FAFB",
        minHeight: "100vh",
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif"
      }}>
        {/* ===== HEADER ===== */}
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: "16px",
          marginBottom: "32px",
          flexWrap: "wrap"
        }}>
          <button
            onClick={() => navigate(-1)}
            style={BACK_BUTTON_STYLE}
          >
            <ArrowLeft size={16} /> Volver
          </button>
          <h1 style={HEADER_TITLE_STYLE}>Dashboard Ventas - PortaCafé</h1>
        </div>

        {/* ===== ESTADO DEL NEGOCIO ===== */}
        <div style={{ marginBottom: "32px" }}>
          <h2 style={SECTION_TITLE_STYLE}>
            Estado del Negocio
          </h2>
          <div style={{
            background: "#FFFFFF",
            borderRadius: "16px",
            border: "1px solid #E5E7EB",
            boxShadow: "0 2px 8px rgba(0, 0, 0, 0.04)",
            padding: "24px"
          }}>
            <BusinessStateIndicator />
          </div>
        </div>

        {/* ===== SECCIÓN 1: QUICK STATS (SIEMPRE VISIBLE) ===== */}
        <div style={{ marginBottom: "32px" }}>
          <h2 style={SECTION_TITLE_STYLE}>
            <BarChart2 size={20} />
            Resumen Rápido
          </h2>
          
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
            gap: "24px"
          }}>
            {/* Card 1: Resumen General */}
            <QuickStatsCard
              title="Ventas Totales"
              icon={DollarSign}
              loading={loading.overview}
            >
              <OverviewTableKPI
                data={data.overview}
                loading={loading.overview}
                error={errors.overview}
                safeRender={safeRender}
                formatCurrency={formatCurrency}
                formatNumber={formatNumber}
                formatPercentage={formatPercentage}
                compactView={true}
              />
            </QuickStatsCard>

            {/* Card 2: Análisis de Propinas */}
            <QuickStatsCard
              title="Propinas por Sede"
              icon={Percent}
              loading={loading.tipsAnalysis}
            >
              <TipsAnalysisTable
                data={data.tipsAnalysis}
                loading={loading.tipsAnalysis}
                error={errors.tipsAnalysis}
                formatCurrency={formatCurrency}
                formatNumber={formatNumber}
                formatPercentage={formatPercentage}
                compactView={true}
              />
            </QuickStatsCard>
          </div>
        </div>

        {/* ===== SECCIÓN 2: BOTONES DE ACCIÓN ===== */}
        <div style={{ marginBottom: "32px" }}>
          <h2 style={SECTION_TITLE_STYLE}>
            <Eye size={20} />
            Análisis Detallado
          </h2>
          
          <div style={BUTTONS_CONTAINER_STYLE}>
            <ActionButton
              icon={Clock}
              label="Análisis de Horas Pico"
              onClick={loadPeakHours}
              isActive={expandedSection === 'peakHours'}
              loading={peakHoursLoading && expandedSection === 'peakHours'}
            />
            
            <ActionButton
              icon={CreditCard}
              label="Métodos de Pago"
              onClick={loadPaymentMethods}
              isActive={expandedSection === 'paymentMethods'}
              loading={paymentMethodsLoading && expandedSection === 'paymentMethods'}
            />
            
            <ActionButton
              icon={Users}
              label="Fidelidad de Clientes"
              onClick={loadLoyalty}
              isActive={expandedSection === 'loyalty'}
              loading={loyaltyLoading && expandedSection === 'loyalty'}
            />
            
            <ActionButton
              icon={ShoppingCart}
              label="Productos Top"
              onClick={loadProducts}
              isActive={expandedSection === 'products'}
              loading={productsLoading && expandedSection === 'products'}
            />
          </div>
        </div>

        {/* ===== SECCIÓN 3: CONTENIDO EXPANDIBLE ===== */}
        
        {/* Peak Hours */}
        {expandedSection === 'peakHours' && (
          <div style={EXPANDED_SECTION_STYLE}>
            <div style={EXPANDED_HEADER_STYLE}>
              <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                <div style={{
                  width: "36px",
                  height: "36px",
                  background: "linear-gradient(135deg, #3b82f6, #6366f1)",
                  borderRadius: "10px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center"
                }}>
                  <Clock size={18} color="#FFFFFF" />
                </div>
                <h3 style={{ color: "#111827", fontWeight: 600, fontSize: "16px" }}>
                  Análisis de Demanda Horaria
                </h3>
              </div>
              <button
                onClick={() => setExpandedSection(null)}
                style={{
                  background: "#ef4444",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: "8px",
                  padding: "8px 16px",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "13px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px"
                }}
              >
                <ChevronUp size={16} />
                Cerrar
              </button>
            </div>
            
            <div style={{ padding: "24px" }}>
              <PeakHoursKPI
                data={peakHoursChartData}
                loading={peakHoursLoading}
                error={errors.peakHours}
                formatNumber={formatNumber}
              />
            </div>
          </div>
        )}

        {/* Payment Methods */}
        {expandedSection === 'paymentMethods' && (
          <div style={EXPANDED_SECTION_STYLE}>
            <div style={EXPANDED_HEADER_STYLE}>
              <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                <div style={{
                  width: "36px",
                  height: "36px",
                  background: "linear-gradient(135deg, #8b5cf6, #6366f1)",
                  borderRadius: "10px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center"
                }}>
                  <CreditCard size={18} color="#FFFFFF" />
                </div>
                <h3 style={{ color: "#111827", fontWeight: 600, fontSize: "16px" }}>
                  Análisis de Métodos de Pago
                </h3>
              </div>
              <button
                onClick={() => setExpandedSection(null)}
                style={{
                  background: "#ef4444",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: "8px",
                  padding: "8px 16px",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "13px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px"
                }}
              >
                <ChevronUp size={16} />
                Cerrar
              </button>
            </div>
            
            <div style={{ padding: "24px" }}>
              <PaymentMethodsKPI
                chartData={paymentMethodsChartData}
                tableData={data.paymentMethods}
                loading={paymentMethodsLoading}
                error={errors.paymentMethods}
                safeRender={safeRender}
                formatCurrency={formatCurrency}
                formatNumber={formatNumber}
                formatPercentage={formatPercentage}
              />
            </div>
          </div>
        )}

        {/* Customer Loyalty */}
        {expandedSection === 'loyalty' && (
          <div style={EXPANDED_SECTION_STYLE}>
            <div style={EXPANDED_HEADER_STYLE}>
              <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                <div style={{
                  width: "36px",
                  height: "36px",
                  background: "linear-gradient(135deg, #10b981, #059669)",
                  borderRadius: "10px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center"
                }}>
                  <Users size={18} color="#FFFFFF" />
                </div>
                <h3 style={{ color: "#111827", fontWeight: 600, fontSize: "16px" }}>
                  Fidelidad de Clientes
                </h3>
              </div>
              <button
                onClick={() => setExpandedSection(null)}
                style={{
                  background: "#ef4444",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: "8px",
                  padding: "8px 16px",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "13px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px"
                }}
              >
                <ChevronUp size={16} />
                Cerrar
              </button>
            </div>
            
            <div style={{ padding: "24px" }}>
              <CustomerLoyaltyKPI
                data={data.customerLoyalty}
                loading={loyaltyLoading}
                error={errors.customerLoyalty}
                formatNumber={formatNumber}
                formatPercentage={formatPercentage}
              />
            </div>
          </div>
        )}

        {/* Top Products */}
        {expandedSection === 'products' && (
          <div style={EXPANDED_SECTION_STYLE}>
            <div style={EXPANDED_HEADER_STYLE}>
              <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                <div style={{
                  width: "36px",
                  height: "36px",
                  background: "linear-gradient(135deg, #f59e0b, #d97706)",
                  borderRadius: "10px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center"
                }}>
                  <ShoppingCart size={18} color="#FFFFFF" />
                </div>
                <h3 style={{ color: "#111827", fontWeight: 600, fontSize: "16px" }}>
                  Productos Más Vendidos
                </h3>
              </div>
              <button
                onClick={() => setExpandedSection(null)}
                style={{
                  background: "#ef4444",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: "8px",
                  padding: "8px 16px",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "13px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px"
                }}
              >
                <ChevronUp size={16} />
                Cerrar
              </button>
            </div>
            
            <div style={{ padding: "24px" }}>
              <TopProductsKPI
                data={data.topProducts}
                loading={productsLoading}
                error={errors.topProducts}
                formatNumber={formatNumber}
                formatCurrency={formatCurrency}
              />
            </div>
          </div>
        )}
      </div>
    </>
  );
}
