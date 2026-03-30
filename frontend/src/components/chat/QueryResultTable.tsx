import { useMemo, useState } from 'react';
import { ChevronDown, ChevronUp, Download } from 'lucide-react';
import { cn } from '../../lib/utils';

interface QueryResultTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
  maxRows?: number;
}

type SortDirection = 'asc' | 'desc' | null;

export default function QueryResultTable({
  columns,
  rows,
  maxRows = 100,
}: QueryResultTableProps) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [showAll, setShowAll] = useState(false);

  // Sort rows
  const sortedRows = useMemo(() => {
    if (!sortColumn || !sortDirection) return rows;

    return [...rows].sort((a, b) => {
      const aVal = a[sortColumn];
      const bVal = b[sortColumn];

      if (aVal === null || aVal === undefined) return 1;
      if (bVal === null || bVal === undefined) return -1;

      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
      }

      const aStr = String(aVal).toLowerCase();
      const bStr = String(bVal).toLowerCase();
      return sortDirection === 'asc'
        ? aStr.localeCompare(bStr)
        : bStr.localeCompare(aStr);
    });
  }, [rows, sortColumn, sortDirection]);

  // Limit displayed rows
  const displayedRows = showAll ? sortedRows : sortedRows.slice(0, maxRows);
  const hasMore = sortedRows.length > maxRows;

  // Handle sort click
  const handleSort = (column: string) => {
    if (sortColumn === column) {
      if (sortDirection === 'asc') {
        setSortDirection('desc');
      } else if (sortDirection === 'desc') {
        setSortColumn(null);
        setSortDirection(null);
      }
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  // Format cell value
  const formatValue = (value: unknown): string => {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'number') {
      // Format as currency if looks like money
      if (Math.abs(value) >= 1000) {
        return new Intl.NumberFormat('pt-BR', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }).format(value);
      }
      return value.toLocaleString('pt-BR');
    }
    if (typeof value === 'boolean') return value ? 'Sim' : 'Não';
    if (value instanceof Date) {
      return value.toLocaleDateString('pt-BR');
    }
    return String(value);
  };

  // Export to CSV
  const exportToCsv = () => {
    const header = columns.join(',');
    const csvRows = sortedRows.map((row) =>
      columns
        .map((col) => {
          const val = row[col];
          if (val === null || val === undefined) return '';
          if (typeof val === 'string' && val.includes(',')) {
            return `"${val.replace(/"/g, '""')}"`;
          }
          return String(val);
        })
        .join(',')
    );
    const csv = [header, ...csvRows].join('\n');

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `query_result_${Date.now()}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  if (rows.length === 0) {
    return (
      <div className="text-sm text-gray-500 italic">
        Nenhum resultado encontrado.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {/* Header */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-500">
          {sortedRows.length} {sortedRows.length === 1 ? 'linha' : 'linhas'}
        </span>
        <button
          onClick={exportToCsv}
          className="flex items-center gap-1 text-xs text-primary-600 hover:text-primary-700 dark:text-primary-400"
        >
          <Download className="w-3 h-3" />
          Exportar CSV
        </button>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-800">
            <tr>
              {columns.map((column) => (
                <th
                  key={column}
                  onClick={() => handleSort(column)}
                  className={cn(
                    'px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors',
                    sortColumn === column && 'text-primary-600 dark:text-primary-400'
                  )}
                >
                  <div className="flex items-center gap-1">
                    <span className="truncate max-w-[150px]" title={column}>
                      {column}
                    </span>
                    {sortColumn === column && (
                      sortDirection === 'asc' ? (
                        <ChevronUp className="w-3 h-3" />
                      ) : (
                        <ChevronDown className="w-3 h-3" />
                      )
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
            {displayedRows.map((row, rowIndex) => (
              <tr
                key={rowIndex}
                className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
              >
                {columns.map((column) => (
                  <td
                    key={column}
                    className="px-3 py-2 text-sm text-gray-900 dark:text-gray-100 whitespace-nowrap"
                  >
                    <span className="truncate block max-w-[200px]" title={formatValue(row[column])}>
                      {formatValue(row[column])}
                    </span>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Show more */}
      {hasMore && !showAll && (
        <button
          onClick={() => setShowAll(true)}
          className="w-full py-2 text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/20 rounded transition-colors"
        >
          Mostrar todas as {sortedRows.length} linhas
        </button>
      )}
    </div>
  );
}
