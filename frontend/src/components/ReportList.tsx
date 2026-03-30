import { useState } from 'react';
import { BarChart2, ChevronLeft, ChevronRight, Loader2, AlertCircle } from 'lucide-react';
import { useReportStore } from '../stores/reportStore';

// ─── Design tokens ────────────────────────────────────────────────────────────
const BG          = '#162633';
const ACCENT      = '#244C5A';
const INDICATOR   = '#F2C811';
const BORDER      = 'rgba(36,76,90,0.5)';

const TEXT_MUTED  = 'rgba(255,255,255,0.3)';
const TEXT_DIM    = 'rgba(255,255,255,0.7)';
const ICON_IDLE   = 'rgba(255,255,255,0.35)';
const TOGGLE_IDLE = 'rgba(255,255,255,0.3)';

// ─── Inline style helpers ─────────────────────────────────────────────────────
const asideStyle = (collapsed: boolean): React.CSSProperties => ({
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  width: collapsed ? '56px' : '220px',
  height: '100%',
  background: BG,
  borderRight: `1px solid ${BORDER}`,
  transition: 'width 260ms cubic-bezier(0.4,0,0.2,1)',
  overflow: 'hidden',
});

const headerStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  flexShrink: 0,
  height: '48px',
  borderBottom: `1px solid ${BORDER}`,
  padding: '0 8px',
};

const toggleBtnStyle = (hovered: boolean): React.CSSProperties => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  flexShrink: 0,
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  padding: '4px',
  borderRadius: '4px',
  color: hovered ? 'rgba(255,255,255,0.6)' : TOGGLE_IDLE,
  transition: 'color 150ms ease',
  outline: 'none',
});

const listStyle: React.CSSProperties = {
  flex: 1,
  overflowY: 'auto',
  overflowX: 'hidden',
  padding: '6px 0',
  // thin scrollbar
  scrollbarWidth: 'thin',
  scrollbarColor: `rgba(255,255,255,0.1) transparent`,
};

// ─── Component ─────────────────────────────────────────────────────────────
export default function ReportList() {
  const { reports, selectedReport, loading, error, selectReport, fetchReports } = useReportStore();
  const [collapsed, setCollapsed]     = useState(false);
  const [toggleHovered, setToggleHovered] = useState(false);

  return (
    <aside style={asideStyle(collapsed)}>

      {/* ── Header ── */}
      <div style={headerStyle}>
        {!collapsed && (
          <span
            style={{
              flex: 1,
              fontSize: '10px',
              fontWeight: 600,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: TEXT_MUTED,
              userSelect: 'none',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              paddingLeft: '4px',
            }}
          >
            Relatórios
          </span>
        )}

        <button
          onClick={() => setCollapsed((v) => !v)}
          onMouseEnter={() => setToggleHovered(true)}
          onMouseLeave={() => setToggleHovered(false)}
          title={collapsed ? 'Expandir painel' : 'Recolher painel'}
          style={{
            ...toggleBtnStyle(toggleHovered),
            marginLeft: collapsed ? 'auto' : undefined,
            marginRight: collapsed ? 'auto' : undefined,
          }}
          aria-label={collapsed ? 'Expandir painel' : 'Recolher painel'}
        >
          {collapsed
            ? <ChevronRight size={14} />
            : <ChevronLeft  size={14} />}
        </button>
      </div>

      {/* ── List ── */}
      <div style={listStyle}>

        {/* Loading */}
        {loading && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: collapsed ? 'center' : 'flex-start',
              gap: '8px',
              padding: collapsed ? '16px 0' : '14px 14px',
              color: TEXT_MUTED,
            }}
          >
            <Loader2 size={13} className="animate-spin" style={{ flexShrink: 0 }} />
            {!collapsed && (
              <span style={{ fontSize: '11px' }}>Carregando...</span>
            )}
          </div>
        )}

        {/* Error */}
        {error && !loading && (
          collapsed ? (
            <div
              title="Erro ao carregar — clique para tentar novamente"
              onClick={fetchReports}
              style={{
                display: 'flex', justifyContent: 'center',
                padding: '14px 0',
                color: 'rgba(248,113,113,0.8)',
                cursor: 'pointer',
              }}
            >
              <AlertCircle size={13} />
            </div>
          ) : (
            <div
              style={{
                margin: '6px 8px',
                padding: '8px 10px',
                borderRadius: '6px',
                background: 'rgba(239,68,68,0.08)',
                border: '1px solid rgba(239,68,68,0.2)',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: '7px' }}>
                <AlertCircle
                  size={12}
                  style={{ color: 'rgba(248,113,113,0.9)', flexShrink: 0, marginTop: '1px' }}
                />
                <p style={{ fontSize: '11px', color: 'rgba(248,113,113,0.8)', lineHeight: '1.5', margin: 0 }}>
                  Falha ao carregar relatórios
                </p>
              </div>
              <button
                onClick={fetchReports}
                style={{
                  width: '100%',
                  padding: '6px',
                  borderRadius: '5px',
                  border: '1px solid rgba(239,68,68,0.25)',
                  background: 'rgba(239,68,68,0.1)',
                  color: 'rgba(248,113,113,0.9)',
                  fontSize: '11px',
                  fontWeight: 500,
                  cursor: 'pointer',
                  transition: 'background 0.15s',
                }}
                onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(239,68,68,0.18)'; }}
                onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(239,68,68,0.1)'; }}
              >
                Tentar novamente
              </button>
            </div>
          )
        )}

        {/* Report items */}
        {!loading && reports.map((report) => {
          const isSelected = selectedReport?.id === report.id;
          return (
            <ReportItem
              key={report.id}
              name={report.name}
              selected={isSelected}
              collapsed={collapsed}
              onClick={() => selectReport(report)}
            />
          );
        })}

        {/* Empty state */}
        {!loading && !error && reports.length === 0 && !collapsed && (
          <p
            style={{
              fontSize: '11px',
              color: TEXT_MUTED,
              textAlign: 'center',
              padding: '28px 12px',
              margin: 0,
              lineHeight: '1.5',
            }}
          >
            Nenhum relatório configurado
          </p>
        )}
      </div>
    </aside>
  );
}

// ─── ReportItem ──────────────────────────────────────────────────────────────
interface ReportItemProps {
  name: string;
  selected: boolean;
  collapsed: boolean;
  onClick: () => void;
}

function ReportItem({ name, selected, collapsed, onClick }: ReportItemProps) {
  const [hovered, setHovered] = useState(false);

  const bg = selected
    ? 'rgba(242,200,17,0.07)'
    : hovered
    ? 'rgba(255,255,255,0.05)'
    : 'transparent';

  const leftBorder = selected ? `3px solid ${INDICATOR}` : '3px solid transparent';
  const boxShadow = selected ? 'inset 2px 0 8px rgba(242,200,17,0.06)' : 'none';

  const iconColor = selected ? INDICATOR : hovered ? 'rgba(255,255,255,0.55)' : ICON_IDLE;
  const textColor = selected ? '#ffffff'  : TEXT_DIM;

  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      title={collapsed ? name : undefined}
      aria-pressed={selected}
      style={{
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: collapsed ? 'center' : 'flex-start',
        gap: collapsed ? 0 : '9px',
        padding: collapsed ? '10px 0' : '10px 12px',
        background: bg,
        border: 'none',
        borderLeft: leftBorder,
        boxShadow,
        cursor: 'pointer',
        outline: 'none',
        transition: 'background 150ms ease, border-color 150ms ease, box-shadow 150ms ease',
        textAlign: 'left',
        minWidth: 0,
      }}
    >
      <BarChart2
        size={14}
        style={{
          color: iconColor,
          flexShrink: 0,
          transition: 'color 150ms ease',
        }}
      />
      {!collapsed && (
        <span
          style={{
            fontSize: '12px',
            fontWeight: selected ? 500 : 400,
            color: textColor,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            transition: 'color 150ms ease',
            minWidth: 0,
          }}
        >
          {name}
        </span>
      )}
    </button>
  );
}
