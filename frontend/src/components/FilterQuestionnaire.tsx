import { useState } from 'react';
import { X, Filter, Plus, Trash2 } from 'lucide-react';

interface FilterQuestionnaireProps {
  onSubmit: (filters: Record<string, string>) => void;
  onCancel: () => void;
}

interface FilterEntry {
  id: number;
  field: string;
  value: string;
}

const COMMON_FILTERS = [
  { label: 'Ano', field: "data/Ano ", example: '2025' },
  { label: 'Mês', field: 'data/Nome mês', example: 'Janeiro' },
  { label: 'Categoria', field: 'data/cNatureza', example: 'R' },
  { label: 'Status', field: 'data/Status', example: 'PAGO' },
];

export default function FilterQuestionnaire({ onSubmit, onCancel }: FilterQuestionnaireProps) {
  const [entries, setEntries] = useState<FilterEntry[]>([
    { id: 1, field: '', value: '' },
  ]);
  let nextId = 2;

  const addEntry = () => {
    setEntries((prev) => [...prev, { id: nextId++, field: '', value: '' }]);
  };

  const removeEntry = (id: number) => {
    setEntries((prev) => prev.filter((e) => e.id !== id));
  };

  const updateEntry = (id: number, key: 'field' | 'value', val: string) => {
    setEntries((prev) =>
      prev.map((e) => (e.id === id ? { ...e, [key]: val } : e))
    );
  };

  const handleSubmit = () => {
    const filters: Record<string, string> = {};
    for (const e of entries) {
      if (e.field.trim() && e.value.trim()) {
        filters[e.field.trim()] = e.value.trim();
      }
    }
    onSubmit(filters);
  };

  const handleSkip = () => {
    onSubmit({});
  };

  const inputStyle: React.CSSProperties = {
    flex: 1,
    padding: '8px 10px',
    borderRadius: '6px',
    border: '1px solid rgba(36,76,90,0.5)',
    background: 'rgba(36,76,90,0.2)',
    color: '#fff',
    fontSize: '12px',
    outline: 'none',
    fontFamily: "'Inter', sans-serif",
  };

  return (
    <>
      <div
        onClick={onCancel}
        style={{
          position: 'fixed', inset: 0,
          background: 'rgba(0,0,0,0.55)',
          backdropFilter: 'blur(3px)',
          zIndex: 50,
        }}
      />

      <div style={{
        position: 'fixed',
        top: '50%', left: '50%',
        transform: 'translate(-50%, -50%)',
        zIndex: 51,
        width: '480px',
        maxHeight: '80vh',
        display: 'flex',
        flexDirection: 'column',
        background: '#162633',
        border: '1px solid rgba(36,120,155,0.4)',
        borderRadius: '16px',
        boxShadow: '0 24px 64px rgba(0,0,0,0.5)',
        overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '18px 20px 16px',
          borderBottom: '1px solid rgba(36,76,90,0.4)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{
              width: '32px', height: '32px', borderRadius: '8px',
              background: 'rgba(242,200,17,0.12)',
              border: '1px solid rgba(242,200,17,0.2)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <Filter size={15} style={{ color: '#F2C811' }} />
            </div>
            <div>
              <p style={{ margin: 0, fontSize: '14px', fontWeight: 600, color: '#fff' }}>
                Filtros do Relatório
              </p>
              <p style={{ margin: 0, fontSize: '11px', color: 'rgba(255,255,255,0.35)' }}>
                Quais filtros você aplicou no dashboard?
              </p>
            </div>
          </div>
          <button
            onClick={onCancel}
            style={{
              width: '28px', height: '28px', borderRadius: '6px',
              border: 'none', background: 'transparent',
              color: 'rgba(255,255,255,0.3)', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}
          >
            <X size={14} />
          </button>
        </div>

        {/* Quick filters */}
        <div style={{ padding: '12px 20px 0', display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
          <span style={{ fontSize: '11px', color: 'rgba(255,255,255,0.3)', width: '100%', marginBottom: '4px' }}>
            Filtros comuns (clique para adicionar):
          </span>
          {COMMON_FILTERS.map((cf) => (
            <button
              key={cf.field}
              onClick={() => {
                setEntries((prev) => [...prev, { id: nextId++, field: cf.field, value: '' }]);
              }}
              style={{
                padding: '4px 10px', borderRadius: '12px',
                border: '1px solid rgba(36,76,90,0.5)',
                background: 'rgba(36,76,90,0.2)',
                color: 'rgba(255,255,255,0.5)',
                fontSize: '11px', cursor: 'pointer',
              }}
            >
              {cf.label}
            </button>
          ))}
        </div>

        {/* Filter entries */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px' }}>
          {entries.map((entry) => (
            <div key={entry.id} style={{ display: 'flex', gap: '8px', marginBottom: '8px', alignItems: 'center' }}>
              <input
                placeholder="Campo (ex: data/Ano )"
                value={entry.field}
                onChange={(e) => updateEntry(entry.id, 'field', e.target.value)}
                style={inputStyle}
              />
              <input
                placeholder="Valor (ex: 2025)"
                value={entry.value}
                onChange={(e) => updateEntry(entry.id, 'value', e.target.value)}
                style={inputStyle}
              />
              <button
                onClick={() => removeEntry(entry.id)}
                style={{
                  width: '28px', height: '28px', borderRadius: '6px',
                  border: 'none', background: 'rgba(239,68,68,0.1)',
                  color: 'rgba(248,113,113,0.7)', cursor: 'pointer',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  flexShrink: 0,
                }}
              >
                <Trash2 size={12} />
              </button>
            </div>
          ))}

          <button
            onClick={addEntry}
            style={{
              display: 'flex', alignItems: 'center', gap: '6px',
              padding: '8px 12px', borderRadius: '8px',
              border: '1px dashed rgba(36,76,90,0.5)',
              background: 'transparent',
              color: 'rgba(255,255,255,0.35)',
              fontSize: '12px', cursor: 'pointer',
              width: '100%', justifyContent: 'center',
            }}
          >
            <Plus size={12} /> Adicionar filtro
          </button>
        </div>

        {/* Footer */}
        <div style={{
          padding: '14px 20px 18px',
          borderTop: '1px solid rgba(36,76,90,0.4)',
          display: 'flex', gap: '8px',
        }}>
          <button
            onClick={handleSkip}
            style={{
              flex: 1, padding: '9px', borderRadius: '8px',
              border: '1px solid rgba(36,76,90,0.5)',
              background: 'transparent',
              color: 'rgba(255,255,255,0.45)',
              fontSize: '13px', fontWeight: 500, cursor: 'pointer',
            }}
          >
            Sem filtros
          </button>
          <button
            onClick={handleSubmit}
            style={{
              flex: 2, padding: '9px', borderRadius: '8px',
              border: '1px solid rgba(80,160,195,0.3)',
              background: '#244C5A',
              color: '#fff',
              fontSize: '13px', fontWeight: 600, cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '7px',
            }}
          >
            <Filter size={13} />
            Aplicar e continuar
          </button>
        </div>
      </div>
    </>
  );
}
