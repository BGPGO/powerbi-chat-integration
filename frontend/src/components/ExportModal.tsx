import { useEffect, useState } from 'react';
import { X, FileDown, Loader2, CheckSquare, Square, AlertCircle, CheckCircle2, Sparkles } from 'lucide-react';
import { exportApi, type ReportPage } from '../lib/api';
import { useReportStore } from '../stores/reportStore';

interface ExportModalProps {
  onClose: () => void;
}

type ExportStatus = 'idle' | 'loading-pages' | 'ready' | 'exporting' | 'done' | 'error';
type ExportType = 'simple' | 'storytelling';

export default function ExportModal({ onClose }: ExportModalProps) {
  const { selectedReport } = useReportStore();
  const [pages, setPages] = useState<ReportPage[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [status, setStatus] = useState<ExportStatus>('loading-pages');
  const [errorMsg, setErrorMsg] = useState('');
  const [exportType, setExportType] = useState<ExportType>('simple');

  // Carrega páginas ao abrir
  useEffect(() => {
    const load = async () => {
      try {
        const ps = await exportApi.listPages(selectedReport?.powerbi_report_id ?? selectedReport?.id);
        const sorted = [...ps].sort((a, b) => a.order - b.order);
        setPages(sorted);
        setSelected(new Set(sorted.map((p) => p.name)));
        setStatus('ready');
      } catch {
        setErrorMsg('Não foi possível carregar as páginas do relatório.');
        setStatus('error');
      }
    };
    load();
  }, [selectedReport?.id]);

  const togglePage = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === pages.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(pages.map((p) => p.name)));
    }
  };

  const handleExport = async () => {
    if (selected.size === 0) return;
    setStatus('exporting');
    try {
      const selectedPages = pages.filter((p) => selected.has(p.name));
      const allSelected = selectedPages.length === pages.length;
      const reportId = selectedReport?.powerbi_report_id ?? selectedReport?.id;

      let blob: Blob;
      if (exportType === 'storytelling') {
        blob = await exportApi.exportStorytelling(
          reportId,
          allSelected ? undefined : selectedPages,
          selectedReport?.name,
        );
      } else {
        blob = await exportApi.exportPdf(
          reportId,
          allSelected ? undefined : selectedPages.map((p) => p.name),
        );
      }

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      const suffix = exportType === 'storytelling' ? '_storytelling' : '';
      a.download = `${selectedReport?.name ?? 'relatorio'}${suffix}.pdf`;
      a.click();
      URL.revokeObjectURL(url);
      setStatus('done');
      setTimeout(onClose, 1200);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Erro ao exportar PDF.';
      setErrorMsg(msg);
      setStatus('error');
    }
  };

  const allChecked = pages.length > 0 && selected.size === pages.length;
  const someChecked = selected.size > 0 && selected.size < pages.length;
  const canExport = selected.size > 0 && status === 'ready';

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: 'fixed', inset: 0,
          background: 'rgba(0,0,0,0.55)',
          backdropFilter: 'blur(3px)',
          zIndex: 50,
          animation: 'fadeIn 0.18s ease',
        }}
      />

      {/* Modal */}
      <div
        style={{
          position: 'fixed',
          top: '50%', left: '50%',
          transform: 'translate(-50%, -50%)',
          zIndex: 51,
          width: '440px',
          maxHeight: '80vh',
          display: 'flex',
          flexDirection: 'column',
          background: '#162633',
          border: '1px solid rgba(36,120,155,0.4)',
          borderRadius: '16px',
          boxShadow: '0 24px 64px rgba(0,0,0,0.5), 0 0 0 1px rgba(36,76,90,0.2)',
          animation: 'slideUp 0.22s cubic-bezier(0.34,1.56,0.64,1)',
          overflow: 'hidden',
        }}
      >
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '18px 20px 16px',
          borderBottom: '1px solid rgba(36,76,90,0.4)',
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{
              width: '32px', height: '32px', borderRadius: '8px',
              background: 'rgba(242,200,17,0.12)',
              border: '1px solid rgba(242,200,17,0.2)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <FileDown size={15} style={{ color: '#F2C811' }} />
            </div>
            <div>
              <p style={{ margin: 0, fontSize: '14px', fontWeight: 600, color: '#fff' }}>
                Exportar para PDF
              </p>
              <p style={{ margin: 0, fontSize: '11px', color: 'rgba(255,255,255,0.35)' }}>
                Selecione as páginas a incluir
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              width: '28px', height: '28px', borderRadius: '6px',
              border: 'none', background: 'transparent',
              color: 'rgba(255,255,255,0.3)', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              transition: 'background 0.15s, color 0.15s',
            }}
            onMouseEnter={(e) => {
              (e.currentTarget as HTMLButtonElement).style.background = 'rgba(255,255,255,0.07)';
              (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.6)';
            }}
            onMouseLeave={(e) => {
              (e.currentTarget as HTMLButtonElement).style.background = 'transparent';
              (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.3)';
            }}
          >
            <X size={14} />
          </button>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px' }}>

          {/* Loading state */}
          {status === 'loading-pages' && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '24px 0', justifyContent: 'center' }}>
              <Loader2 size={16} className="animate-spin" style={{ color: 'rgba(255,255,255,0.35)' }} />
              <span style={{ fontSize: '13px', color: 'rgba(255,255,255,0.35)' }}>
                Carregando páginas...
              </span>
            </div>
          )}

          {/* Error state */}
          {status === 'error' && (
            <div style={{
              display: 'flex', gap: '10px', alignItems: 'flex-start',
              padding: '12px', borderRadius: '8px',
              background: 'rgba(239,68,68,0.08)',
              border: '1px solid rgba(239,68,68,0.2)',
            }}>
              <AlertCircle size={14} style={{ color: 'rgba(248,113,113,0.9)', marginTop: '1px', flexShrink: 0 }} />
              <p style={{ margin: 0, fontSize: '12px', color: 'rgba(248,113,113,0.9)', lineHeight: 1.5 }}>
                {errorMsg}
              </p>
            </div>
          )}

          {/* Done state */}
          {status === 'done' && (
            <div style={{
              display: 'flex', gap: '10px', alignItems: 'center',
              padding: '12px', borderRadius: '8px',
              background: 'rgba(34,197,94,0.08)',
              border: '1px solid rgba(34,197,94,0.2)',
            }}>
              <CheckCircle2 size={14} style={{ color: 'rgba(74,222,128,0.9)', flexShrink: 0 }} />
              <p style={{ margin: 0, fontSize: '12px', color: 'rgba(74,222,128,0.9)' }}>
                PDF exportado com sucesso!
              </p>
            </div>
          )}

          {/* Export type toggle */}
          {(status === 'ready' || status === 'exporting') && (
            <div style={{
              display: 'flex', gap: '6px', marginBottom: '14px',
            }}>
              {(['simple', 'storytelling'] as ExportType[]).map((type) => {
                const isActive = exportType === type;
                return (
                  <button
                    key={type}
                    onClick={() => setExportType(type)}
                    disabled={status === 'exporting'}
                    style={{
                      flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
                      gap: '6px', padding: '8px 10px', borderRadius: '8px',
                      border: isActive
                        ? '1px solid rgba(242,200,17,0.45)'
                        : '1px solid rgba(36,76,90,0.45)',
                      background: isActive ? 'rgba(242,200,17,0.08)' : 'rgba(36,76,90,0.15)',
                      color: isActive ? 'rgba(242,200,17,0.95)' : 'rgba(255,255,255,0.4)',
                      fontSize: '11.5px', fontWeight: isActive ? 600 : 400,
                      cursor: status === 'exporting' ? 'not-allowed' : 'pointer',
                      transition: 'all 0.15s',
                      fontFamily: "'Inter', sans-serif",
                    }}
                  >
                    {type === 'storytelling'
                      ? <Sparkles size={11} strokeWidth={2} />
                      : <FileDown size={11} strokeWidth={2} />
                    }
                    {type === 'simple' ? 'PDF Simples' : 'PDF com Storytelling'}
                  </button>
                );
              })}
            </div>
          )}

          {/* Storytelling note */}
          {(status === 'ready' || status === 'exporting') && exportType === 'storytelling' && (
            <div style={{
              display: 'flex', gap: '8px', alignItems: 'flex-start',
              padding: '10px 12px', marginBottom: '12px', borderRadius: '8px',
              background: 'rgba(242,200,17,0.05)',
              border: '1px solid rgba(242,200,17,0.18)',
            }}>
              <Sparkles size={12} style={{ color: 'rgba(242,200,17,0.7)', marginTop: '1px', flexShrink: 0 }} />
              <p style={{ margin: 0, fontSize: '11px', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5 }}>
                A IA vai gerar uma narrativa para cada tela selecionada e exportar junto com a imagem do BI. Pode demorar alguns minutos.
              </p>
            </div>
          )}

          {/* Page list */}
          {(status === 'ready' || status === 'exporting') && pages.length > 0 && (
            <>
              {/* Select all */}
              <button
                onClick={toggleAll}
                style={{
                  width: '100%', display: 'flex', alignItems: 'center', gap: '10px',
                  padding: '10px 12px', marginBottom: '8px',
                  borderRadius: '8px', border: '1px solid rgba(36,76,90,0.5)',
                  background: 'rgba(36,76,90,0.2)', cursor: 'pointer',
                  transition: 'background 0.15s',
                }}
                onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(36,76,90,0.35)'; }}
                onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(36,76,90,0.2)'; }}
              >
                {allChecked
                  ? <CheckSquare size={14} style={{ color: '#F2C811', flexShrink: 0 }} />
                  : someChecked
                  ? <CheckSquare size={14} style={{ color: 'rgba(242,200,17,0.5)', flexShrink: 0 }} />
                  : <Square size={14} style={{ color: 'rgba(255,255,255,0.3)', flexShrink: 0 }} />
                }
                <span style={{ fontSize: '12px', fontWeight: 500, color: 'rgba(255,255,255,0.65)' }}>
                  {allChecked ? 'Desmarcar todas' : 'Selecionar todas'}
                </span>
                <span style={{
                  marginLeft: 'auto', fontSize: '11px',
                  color: 'rgba(255,255,255,0.3)',
                }}>
                  {selected.size}/{pages.length}
                </span>
              </button>

              {/* Individual pages */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                {pages.map((page) => {
                  const checked = selected.has(page.name);
                  return (
                    <button
                      key={page.name}
                      onClick={() => togglePage(page.name)}
                      style={{
                        width: '100%', display: 'flex', alignItems: 'center', gap: '10px',
                        padding: '10px 12px', borderRadius: '8px',
                        border: checked
                          ? '1px solid rgba(242,200,17,0.25)'
                          : '1px solid rgba(36,76,90,0.35)',
                        background: checked ? 'rgba(242,200,17,0.06)' : 'transparent',
                        cursor: 'pointer',
                        transition: 'background 0.15s, border-color 0.15s',
                        textAlign: 'left',
                      }}
                      onMouseEnter={(e) => {
                        if (!checked) (e.currentTarget as HTMLButtonElement).style.background = 'rgba(255,255,255,0.04)';
                      }}
                      onMouseLeave={(e) => {
                        if (!checked) (e.currentTarget as HTMLButtonElement).style.background = 'transparent';
                      }}
                    >
                      {checked
                        ? <CheckSquare size={13} style={{ color: '#F2C811', flexShrink: 0 }} />
                        : <Square size={13} style={{ color: 'rgba(255,255,255,0.25)', flexShrink: 0 }} />
                      }
                      <span style={{
                        fontSize: '13px',
                        color: checked ? 'rgba(255,255,255,0.85)' : 'rgba(255,255,255,0.5)',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        transition: 'color 0.15s',
                      }}>
                        {page.display_name}
                      </span>
                    </button>
                  );
                })}
              </div>
            </>
          )}
        </div>

        {/* Footer */}
        {(status === 'ready' || status === 'exporting') && (
          <div style={{
            padding: '14px 20px 18px',
            borderTop: '1px solid rgba(36,76,90,0.4)',
            display: 'flex', gap: '8px',
            flexShrink: 0,
          }}>
            <button
              onClick={onClose}
              style={{
                flex: 1, padding: '9px', borderRadius: '8px',
                border: '1px solid rgba(36,76,90,0.5)',
                background: 'transparent',
                color: 'rgba(255,255,255,0.45)',
                fontSize: '13px', fontWeight: 500, cursor: 'pointer',
                transition: 'background 0.15s, color 0.15s',
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLButtonElement).style.background = 'rgba(255,255,255,0.05)';
                (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.7)';
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLButtonElement).style.background = 'transparent';
                (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.45)';
              }}
            >
              Cancelar
            </button>
            <button
              onClick={handleExport}
              disabled={!canExport || status === 'exporting'}
              style={{
                flex: 2, padding: '9px', borderRadius: '8px',
                border: canExport && status !== 'exporting'
                  ? '1px solid rgba(80,160,195,0.3)'
                  : '1px solid transparent',
                background: canExport && status !== 'exporting'
                  ? '#244C5A'
                  : 'rgba(36,76,90,0.3)',
                color: canExport && status !== 'exporting'
                  ? '#fff'
                  : 'rgba(255,255,255,0.3)',
                fontSize: '13px', fontWeight: 600,
                cursor: canExport && status !== 'exporting' ? 'pointer' : 'not-allowed',
                display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '7px',
                transition: 'background 0.15s, box-shadow 0.15s',
              }}
              onMouseEnter={(e) => {
                if (canExport && status !== 'exporting') {
                  (e.currentTarget as HTMLButtonElement).style.background = '#2d5f72';
                  (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 0 12px rgba(36,150,190,0.25)';
                }
              }}
              onMouseLeave={(e) => {
                if (canExport && status !== 'exporting') {
                  (e.currentTarget as HTMLButtonElement).style.background = '#244C5A';
                  (e.currentTarget as HTMLButtonElement).style.boxShadow = 'none';
                }
              }}
            >
              {status === 'exporting' ? (
                <>
                  <Loader2 size={13} className="animate-spin" />
                  Gerando PDF...
                </>
              ) : (
                <>
                  {exportType === 'storytelling'
                    ? <Sparkles size={13} />
                    : <FileDown size={13} />
                  }
                  {exportType === 'storytelling' ? 'Exportar com Storytelling' : 'Exportar'}{' '}
                  {selected.size > 0 ? `${selected.size} página${selected.size > 1 ? 's' : ''}` : ''}
                </>
              )}
            </button>
          </div>
        )}
      </div>

    </>
  );
}
