import { useState } from 'react';
import { PowerBIEmbed } from 'powerbi-client-react';
import { models } from 'powerbi-client';
import { BarChart2, FileDown, FilterX } from 'lucide-react';
import { useReportStore } from '../stores/reportStore';
import { PowerBIFilter } from '../types';
import ExportModal from './ExportModal';

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

export default function ReportEmbed() {
  const { selectedReport, activeFilter, clearFilter, setCurrentPage } = useReportStore();
  const [exportOpen, setExportOpen] = useState(false);

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
        <ExportButton onClick={() => setExportOpen(true)} />
        {exportOpen && <ExportModal onClose={() => setExportOpen(false)} />}
      </div>
    );
  }

  // ── Fallback: public iframe embed ────────────────────────────────────────
  const filteredUrl = buildFilteredUrl(selectedReport.embed_url, activeFilter);

  return (
    <div style={{ width: '100%', height: '100%', overflow: 'hidden', position: 'relative' }}>
      <iframe
        key={filteredUrl}
        src={filteredUrl}
        style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
        allowFullScreen
        title={selectedReport.name}
      />

      {isFiltered && activeFilter && (
        <FilterBadge label={activeFilter.description} onClear={clearFilter} />
      )}
      <ExportButton onClick={() => setExportOpen(true)} />
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

// ── Floating export button ────────────────────────────────────────────────────

function ExportButton({ onClick }: { onClick: () => void }) {
  const [hovered, setHovered] = useState(false);

  return (
    <button
      onClick={onClick}
      title="Exportar para PDF"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        position: 'absolute',
        top: '12px',
        right: '12px',
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
        cursor: 'pointer',
        transition: 'all 150ms ease',
        boxShadow: hovered
          ? '0 0 14px rgba(36,150,190,0.30), 0 2px 12px rgba(0,0,0,0.35)'
          : '0 2px 10px rgba(0,0,0,0.25)',
        letterSpacing: '0.01em',
        fontFamily: "'Inter', sans-serif",
        zIndex: 10,
      }}
    >
      <FileDown size={13} strokeWidth={2.2} />
      Exportar PDF
    </button>
  );
}
