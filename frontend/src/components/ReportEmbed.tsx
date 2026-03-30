import { useState, useRef } from 'react';
import { PowerBIEmbed } from 'powerbi-client-react';
import { models } from 'powerbi-client';
import { BarChart2, FileDown, FilterX, BookOpen, ChevronDown } from 'lucide-react';
import { useReportStore } from '../stores/reportStore';
import { PowerBIFilter } from '../types';
import ExportModal from './ExportModal';
import { exportApi } from '../lib/api';
import FilterQuestionnaire from './FilterQuestionnaire';
import StorytellingProgress from './StorytellingProgress';

function buildSdkFilters(filter: PowerBIFilter | null): models.IBasicFilter[] {
  if (!filter || !filter.has_filter) return [];
  const filters: models.IBasicFilter[] = [];

  if (filter.year) {
    filters.push({
      $schema: 'http://powerbi.com/product/schema#basic',
      target: { table: 'data', column: 'Ano ' }, // trailing space in column name
      operator: 'In',
      values: [filter.year],
      filterType: models.FilterType.Basic,
    });
  }

  if (filter.month) {
    filters.push({
      $schema: 'http://powerbi.com/product/schema#basic',
      target: { table: 'data', column: 'Nome mês' },
      operator: 'In',
      values: [filter.month],
      filterType: models.FilterType.Basic,
    });
  } else if (filter.months_in_range && filter.months_in_range.length > 0) {
    filters.push({
      $schema: 'http://powerbi.com/product/schema#basic',
      target: { table: 'data', column: 'Nome mês' },
      operator: 'In',
      values: filter.months_in_range,
      filterType: models.FilterType.Basic,
    });
  }

  return filters;
}

// Fallback: URL-based filtering for public embed links (no embed token)
function buildFilteredUrl(embedUrl: string, filter: PowerBIFilter | null): string {
  if (!filter || !filter.has_filter) return embedUrl;
  const parts: string[] = [];
  if (filter.year) {
    parts.push(`data/Ano  eq '${filter.year}'`);
  }
  if (filter.month) {
    parts.push(`data/Nome mês eq '${filter.month}'`);
  } else if (filter.months_in_range && filter.months_in_range.length > 0) {
    const orParts = filter.months_in_range.map(m => `data/Nome mês eq '${m}'`);
    parts.push(`(${orParts.join(' or ')})`);
  }
  if (parts.length === 0) return embedUrl;
  const filterValue = parts.join(' and ');
  const sep = embedUrl.includes('?') ? '&' : '?';
  return `${embedUrl}${sep}filter=${filterValue}`;
}

// printReport kept as fallback for browser-based printing
// function printReport(embedUrl: string, name: string) { ... }

export default function ReportEmbed() {
  const { selectedReport, activeFilter, clearFilter, setCurrentPage } = useReportStore();
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [showFilterForm, setShowFilterForm] = useState(false);
  const [filterFormMode, setFilterFormMode] = useState<'pdf' | 'storytelling'>('pdf');
  const [storytellingJobId, setStorytellingJobId] = useState<string | null>(null);
  const [pdfLoading, setPdfLoading] = useState(false);

  // Try to capture current iframe URL (may have filters applied by user)
  const getCurrentEmbedUrl = (): string | null => {
    if (!selectedReport) return null;
    // For public embed iframes, we can read the src attribute
    // (same-origin policy won't block since we set the src ourselves)
    if (iframeRef.current) {
      return iframeRef.current.src || null;
    }
    return selectedReport.embed_url;
  };

  const handleExportPdf = async (filters?: Record<string, string>) => {
    const url = getCurrentEmbedUrl() || selectedReport?.embed_url;
    if (!url) return;

    setPdfLoading(true);
    setShowExportMenu(false);
    setShowFilterForm(false);

    try {
      const blob = await exportApi.exportPdf(
        url,
        filters && Object.keys(filters).length > 0 ? filters : undefined,
        selectedReport?.name,
      );
      const blobUrl = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = blobUrl;
      a.download = `${selectedReport?.name ?? 'relatorio'}.pdf`;
      a.click();
      URL.revokeObjectURL(blobUrl);
    } catch (e) {
      console.error('PDF export failed:', e);
    } finally {
      setPdfLoading(false);
    }
  };

  const handleStartStorytelling = async (filters?: Record<string, string>) => {
    const url = getCurrentEmbedUrl() || selectedReport?.embed_url;
    if (!url) return;

    setShowExportMenu(false);
    setShowFilterForm(false);

    try {
      const { job_id } = await exportApi.startStorytelling(
        url,
        filters && Object.keys(filters).length > 0 ? filters : undefined,
        selectedReport?.name,
      );
      setStorytellingJobId(job_id);
    } catch (e) {
      console.error('Storytelling failed:', e);
    }
  };

  const handleExportMenuClick = (mode: 'pdf' | 'storytelling') => {
    setFilterFormMode(mode);
    setShowExportMenu(false);
    // Try to get URL directly first — if we can read iframe src, skip questionnaire
    const url = getCurrentEmbedUrl();
    if (url && url !== selectedReport?.embed_url) {
      // URL has filters baked in, use it directly
      if (mode === 'pdf') {
        handleExportPdf();
      } else {
        handleStartStorytelling();
      }
    } else {
      // No filter changes detected or can't read — show questionnaire
      setShowFilterForm(true);
    }
  };

  const handleFilterSubmit = (filters: Record<string, string>) => {
    if (filterFormMode === 'pdf') {
      handleExportPdf(filters);
    } else {
      handleStartStorytelling(filters);
    }
  };

  if (!selectedReport) {
    return (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '16px',
        }}
      >
        <div
          className="animate-pulse-icon"
          style={{
            width: '64px',
            height: '64px',
            background: 'rgba(36,76,90,0.2)',
            border: '1px solid rgba(36,76,90,0.4)',
            borderRadius: '16px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          <BarChart2 size={28} style={{ color: 'rgba(255,255,255,0.2)' }} />
        </div>

        <div style={{ textAlign: 'center' }}>
          <p style={{ margin: 0, fontSize: '14px', fontWeight: 500, color: 'rgba(255,255,255,0.4)' }}>
            Selecione um relatório
          </p>
          <p style={{ margin: '6px 0 0', fontSize: '12px', color: 'rgba(255,255,255,0.2)' }}>
            Escolha um relatório na lista à esquerda para visualizar
          </p>
        </div>
      </div>
    );
  }

  const isFiltered = activeFilter?.has_filter ?? false;
  const hasEmbedToken = !!selectedReport.embed_token;

  // ── Authenticated embed via SDK ──────────────────────────────────────────
  if (hasEmbedToken) {
    const sdkFilters = buildSdkFilters(activeFilter);

    return (
      <div style={{ width: '100%', height: '100%', overflow: 'hidden', position: 'relative' }}>
        <PowerBIEmbed
          key={selectedReport.id}
          embedConfig={{
            type: 'report',
            id: selectedReport.powerbi_report_id ?? selectedReport.id,
            embedUrl: selectedReport.embed_url,
            accessToken: selectedReport.embed_token!,
            tokenType: models.TokenType.Embed,
            filters: sdkFilters,
            settings: {
              panes: { filters: { visible: false }, pageNavigation: { visible: true } },
              background: models.BackgroundType.Transparent,
            },
          }}
          cssClassName="powerbi-report-container"
          eventHandlers={new Map<string, any>([
            ['pageChanged', (event: any) => {
              const newPage = event?.detail?.newPage;
              if (newPage?.name) {
                setCurrentPage({ name: newPage.name, displayName: newPage.displayName ?? newPage.name });
              }
            }],
          ])}
        />

        {isFiltered && activeFilter && (
          <FilterBadge label={activeFilter.description} onClear={clearFilter} />
        )}
        <ExportDropdown
          showMenu={showExportMenu}
          onToggle={() => setShowExportMenu((v) => !v)}
          onPdf={() => handleExportMenuClick('pdf')}
          onStorytelling={() => handleExportMenuClick('storytelling')}
          loading={pdfLoading}
        />
        {showFilterForm && (
          <FilterQuestionnaire
            onSubmit={handleFilterSubmit}
            onCancel={() => setShowFilterForm(false)}
          />
        )}
        {storytellingJobId && (
          <StorytellingProgress
            jobId={storytellingJobId}
            reportName={selectedReport.name}
            onClose={() => setStorytellingJobId(null)}
          />
        )}
        {exportOpen && <ExportModal onClose={() => setExportOpen(false)} />}
      </div>
    );
  }

  // ── Public embed via iframe ─────────────────────────────────────────────
  const filteredUrl = buildFilteredUrl(selectedReport.embed_url, activeFilter);

  return (
    <div style={{ width: '100%', height: '100%', overflow: 'hidden', position: 'relative' }}>
      <iframe
        ref={iframeRef}
        key={filteredUrl}
        src={filteredUrl}
        style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
        allowFullScreen
        title={selectedReport.name}
      />

      {isFiltered && activeFilter && (
        <FilterBadge label={activeFilter.description} onClear={clearFilter} />
      )}
      <ExportDropdown
        showMenu={showExportMenu}
        onToggle={() => setShowExportMenu((v) => !v)}
        onPdf={() => handleExportMenuClick('pdf')}
        onStorytelling={() => handleExportMenuClick('storytelling')}
        loading={pdfLoading}
      />
      {showFilterForm && (
        <FilterQuestionnaire
          onSubmit={handleFilterSubmit}
          onCancel={() => setShowFilterForm(false)}
        />
      )}
      {storytellingJobId && (
        <StorytellingProgress
          jobId={storytellingJobId}
          reportName={selectedReport.name}
          onClose={() => setStorytellingJobId(null)}
        />
      )}
      {exportOpen && <ExportModal onClose={() => setExportOpen(false)} />}
    </div>
  );
}

// ── Active filter badge ───────────────────────────────────────────────────────

function FilterBadge({ label, onClear }: { label: string; onClear: () => void }) {
  const [hovered, setHovered] = useState(false);
  return (
    <div
      style={{
        position: 'absolute',
        top: '12px',
        left: '12px',
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        padding: '6px 10px',
        borderRadius: '20px',
        border: '1px solid rgba(242,200,17,0.35)',
        background: 'rgba(36,76,90,0.85)',
        backdropFilter: 'blur(10px)',
        color: 'rgba(242,200,17,0.9)',
        fontSize: '11.5px',
        fontWeight: 500,
        zIndex: 10,
        fontFamily: "'Inter', sans-serif",
        boxShadow: '0 2px 10px rgba(0,0,0,0.25)',
      }}
    >
      <span>Filtro: {label}</span>
      <button
        onClick={onClear}
        title="Remover filtro"
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: '16px',
          height: '16px',
          borderRadius: '50%',
          border: 'none',
          background: hovered ? 'rgba(242,200,17,0.25)' : 'transparent',
          color: 'rgba(242,200,17,0.8)',
          cursor: 'pointer',
          padding: 0,
          transition: 'background 0.15s',
        }}
      >
        <FilterX size={10} strokeWidth={2.5} />
      </button>
    </div>
  );
}

// ── Floating export dropdown ─────────────────────────────────────────────────

function ExportDropdown({
  showMenu,
  onToggle,
  onPdf,
  onStorytelling,
  loading,
}: {
  showMenu: boolean;
  onToggle: () => void;
  onPdf: () => void;
  onStorytelling: () => void;
  loading: boolean;
}) {
  const [hovered, setHovered] = useState(false);

  return (
    <div style={{ position: 'absolute', top: '12px', right: '12px', zIndex: 10 }}>
      <button
        onClick={onToggle}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        disabled={loading}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '6px',
          padding: '7px 14px',
          borderRadius: '20px',
          border: hovered
            ? '1px solid rgba(80,160,195,0.45)'
            : '1px solid rgba(36,120,155,0.35)',
          background: hovered ? '#244C5A' : 'rgba(36,76,90,0.75)',
          backdropFilter: 'blur(10px)',
          color: hovered ? '#fff' : 'rgba(255,255,255,0.75)',
          fontSize: '12px',
          fontWeight: 500,
          cursor: loading ? 'wait' : 'pointer',
          transition: 'all 150ms ease',
          boxShadow: hovered
            ? '0 0 14px rgba(36,150,190,0.30), 0 2px 12px rgba(0,0,0,0.35)'
            : '0 2px 10px rgba(0,0,0,0.25)',
          letterSpacing: '0.01em',
          fontFamily: "'Inter', sans-serif",
        }}
      >
        <FileDown size={13} strokeWidth={2.2} />
        {loading ? 'Gerando...' : 'Exportar'}
        <ChevronDown size={11} />
      </button>

      {showMenu && (
        <div style={{
          position: 'absolute',
          top: 'calc(100% + 6px)',
          right: 0,
          width: '220px',
          background: '#162633',
          border: '1px solid rgba(36,120,155,0.4)',
          borderRadius: '10px',
          boxShadow: '0 12px 40px rgba(0,0,0,0.5)',
          overflow: 'hidden',
        }}>
          <button
            onClick={onPdf}
            style={{
              width: '100%',
              display: 'flex', alignItems: 'center', gap: '10px',
              padding: '12px 14px',
              border: 'none',
              borderBottom: '1px solid rgba(36,76,90,0.3)',
              background: 'transparent',
              color: 'rgba(255,255,255,0.75)',
              fontSize: '12px',
              cursor: 'pointer',
              textAlign: 'left',
              transition: 'background 0.15s',
            }}
            onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(36,76,90,0.3)'; }}
            onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'transparent'; }}
          >
            <FileDown size={14} style={{ color: '#F2C811', flexShrink: 0 }} />
            <div>
              <p style={{ margin: 0, fontWeight: 500 }}>PDF Simples</p>
              <p style={{ margin: '2px 0 0', fontSize: '10px', color: 'rgba(255,255,255,0.3)' }}>
                Screenshot do dashboard
              </p>
            </div>
          </button>

          <button
            onClick={onStorytelling}
            style={{
              width: '100%',
              display: 'flex', alignItems: 'center', gap: '10px',
              padding: '12px 14px',
              border: 'none',
              background: 'transparent',
              color: 'rgba(255,255,255,0.75)',
              fontSize: '12px',
              cursor: 'pointer',
              textAlign: 'left',
              transition: 'background 0.15s',
            }}
            onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'rgba(36,76,90,0.3)'; }}
            onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'transparent'; }}
          >
            <BookOpen size={14} style={{ color: 'rgba(167,139,250,0.9)', flexShrink: 0 }} />
            <div>
              <p style={{ margin: 0, fontWeight: 500 }}>PDF com Storytelling</p>
              <p style={{ margin: '2px 0 0', fontSize: '10px', color: 'rgba(255,255,255,0.3)' }}>
                Narrativa executiva por IA
              </p>
            </div>
          </button>
        </div>
      )}
    </div>
  );
}
