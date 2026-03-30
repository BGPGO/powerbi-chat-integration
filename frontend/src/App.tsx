import { useEffect, useState } from 'react';
import ReportList from './components/ReportList';
import ReportEmbed from './components/ReportEmbed';
import ChatSidebar from './components/ChatSidebar';
import { useReportStore } from './stores/reportStore';
import { MessageSquare, X } from 'lucide-react';

function App() {
  const { fetchReports, selectedReport } = useReportStore();
  const [chatOpen, setChatOpen] = useState(false);

  useEffect(() => {
    fetchReports();
  }, []);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        overflow: 'hidden',
        background: '#0d1b2a',
        fontFamily: "'Inter', sans-serif",
      }}
    >
      {/* Header */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          paddingLeft: '20px',
          paddingRight: '20px',
          flexShrink: 0,
          zIndex: 10,
          background: '#244C5A',
          height: '48px',
          boxShadow: '0 1px 0 rgba(0,0,0,0.2)',
        }}
      >
        {/* Left: logo + title + breadcrumb */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          {/* Yellow square logo */}
          <div
            style={{
              width: '32px',
              height: '32px',
              borderRadius: '4px',
              background: '#F2C811',
              flexShrink: 0,
            }}
          />

          {/* Brand name */}
          <span
            style={{
              color: '#ffffff',
              fontWeight: 700,
              fontSize: '14px',
              letterSpacing: '0.01em',
              lineHeight: 1,
            }}
          >
            IA BGP
          </span>

          {/* Separator */}
          <span
            style={{
              color: 'rgba(255,255,255,0.25)',
              fontSize: '14px',
              fontWeight: 300,
              userSelect: 'none',
            }}
          >
            |
          </span>

          {/* Subtitle */}
          <span
            style={{
              color: 'rgba(255,255,255,0.65)',
              fontWeight: 400,
              fontSize: '13px',
              letterSpacing: '0.01em',
              lineHeight: 1,
            }}
          >
            Dashboard &amp; Assistente
          </span>

          {/* Breadcrumb — only when a report is selected */}
          {selectedReport && (
            <>
              <span
                style={{
                  color: 'rgba(255,255,255,0.22)',
                  fontSize: '13px',
                  fontWeight: 400,
                  paddingLeft: '2px',
                  userSelect: 'none',
                }}
              >
                /
              </span>
              <span
                style={{
                  color: 'rgba(255,255,255,0.50)',
                  fontSize: '12px',
                  fontWeight: 400,
                  letterSpacing: '0.01em',
                  maxWidth: '260px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {selectedReport.name}
              </span>
            </>
          )}
        </div>

        {/* Right: chat toggle pill button */}
        <ChatToggleButton
          open={chatOpen}
          disabled={!selectedReport}
          onClick={() => selectedReport && setChatOpen((v) => !v)}
        />
      </header>

      {/* Body */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden', minHeight: 0 }}>
        <ReportList />

        <main style={{ flex: 1, overflow: 'hidden', position: 'relative', minWidth: 0 }}>
          <ReportEmbed />
        </main>

        {/* Chat drawer — slides in/out with smooth width transition */}
        <div
          style={{
            flexShrink: 0,
            overflow: 'hidden',
            width: chatOpen ? '360px' : '0px',
            transition: 'width 280ms cubic-bezier(0.4, 0, 0.2, 1)',
          }}
        >
          <div style={{ width: '360px', height: '100%' }}>
            <ChatSidebar />
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Chat toggle pill button ────────────────────────────────────────────────────

interface ChatToggleButtonProps {
  open: boolean;
  disabled?: boolean;
  onClick: () => void;
}

function ChatToggleButton({ open, disabled, onClick }: ChatToggleButtonProps) {
  const [hovered, setHovered] = useState(false);

  const baseStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    background: disabled
      ? 'rgba(255,255,255,0.05)'
      : hovered
      ? 'rgba(255,255,255,0.20)'
      : open
      ? 'rgba(36,76,90,0.70)'
      : 'rgba(255,255,255,0.11)',
    border: disabled
      ? '1px solid rgba(255,255,255,0.08)'
      : open
      ? '1px solid rgba(36,150,190,0.40)'
      : '1px solid rgba(255,255,255,0.15)',
    boxShadow: open && !disabled ? '0 0 10px rgba(36,150,190,0.20)' : 'none',
    borderRadius: '20px',
    padding: '6px 14px',
    fontSize: '12px',
    fontWeight: 500,
    letterSpacing: '0.02em',
    color: disabled ? 'rgba(255,255,255,0.3)' : '#ffffff',
    cursor: disabled ? 'not-allowed' : 'pointer',
    transition: 'all 150ms ease',
    outline: 'none',
    userSelect: 'none',
    fontFamily: "'Inter', sans-serif",
    lineHeight: 1,
  };

  return (
    <button
      style={baseStyle}
      onClick={onClick}
      disabled={disabled}
      aria-expanded={open}
      title={disabled ? 'Selecione um relatório para abrir o chat' : undefined}
      onMouseEnter={() => !disabled && setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {open ? <X size={13} strokeWidth={2.2} /> : <MessageSquare size={13} strokeWidth={2.2} />}
      {open ? 'Fechar chat' : 'Abrir chat'}
    </button>
  );
}

export default App;
