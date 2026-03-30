import { useState, useEffect } from 'react';
import {
  X,
  Table,
  Columns,
  Key,
  Link2,
  RefreshCw,
  ChevronRight,
  Search,
  Copy,
  Check,
} from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { datasetsApi } from '../../lib/api';
import type { SchemaResponse, TableInfo, ColumnInfo } from '../../types';
import { cn } from '../../lib/utils';

export default function SchemaPanel() {
  const { selectedDataset, showSchema, toggleSchema } = useChatStore();
  const [schema, setSchema] = useState<SchemaResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedColumn, setCopiedColumn] = useState<string | null>(null);

  // Load schema when dataset changes
  useEffect(() => {
    if (selectedDataset && showSchema) {
      loadSchema();
    }
  }, [selectedDataset, showSchema]);

  const loadSchema = async () => {
    if (!selectedDataset) return;

    try {
      setLoading(true);
      const data = await datasetsApi.getSchema(
        selectedDataset.id,
        selectedDataset.workspace_id
      );
      setSchema(data);
      // Expand first table by default
      if (data.dataset.tables.length > 0) {
        setExpandedTable(data.dataset.tables[0].name);
      }
    } catch (error) {
      console.error('Failed to load schema:', error);
    } finally {
      setLoading(false);
    }
  };

  const copyColumnName = (tableName: string, columnName: string) => {
    const fullName = `'${tableName}'[${columnName}]`;
    navigator.clipboard.writeText(fullName);
    setCopiedColumn(fullName);
    setTimeout(() => setCopiedColumn(null), 2000);
  };

  // Filter tables/columns by search
  const filteredTables = schema?.dataset.tables.filter((table) => {
    if (!searchQuery) return true;
    const query = searchQuery.toLowerCase();
    if (table.name.toLowerCase().includes(query)) return true;
    if (table.business_name?.toLowerCase().includes(query)) return true;
    return table.columns.some(
      (col) =>
        col.name.toLowerCase().includes(query) ||
        col.business_name?.toLowerCase().includes(query)
    );
  });

  if (!showSchema) return null;

  return (
    <div className="w-80 border-l border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <Table className="w-5 h-5 text-primary-600 dark:text-primary-400" />
          <span className="font-semibold text-gray-900 dark:text-gray-100">
            Schema
          </span>
        </div>
        <button
          onClick={toggleSchema}
          className="p-1.5 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Dataset info */}
      {selectedDataset && (
        <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <h3 className="font-medium text-gray-900 dark:text-gray-100 truncate">
            {selectedDataset.name}
          </h3>
          {selectedDataset.description && (
            <p className="text-xs text-gray-500 mt-1 line-clamp-2">
              {selectedDataset.description}
            </p>
          )}
        </div>
      )}

      {/* Search */}
      <div className="px-3 py-2 border-b border-gray-200 dark:border-gray-700">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Buscar tabelas ou colunas..."
            className="w-full pl-9 pr-3 py-2 bg-gray-100 dark:bg-gray-700 border-0 rounded-lg text-sm text-gray-900 dark:text-gray-100 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-primary-500"
          />
        </div>
      </div>

      {/* Tables list */}
      <div className="flex-1 overflow-y-auto">
        {loading ? (
          <div className="flex items-center justify-center py-8">
            <RefreshCw className="w-5 h-5 text-gray-400 animate-spin" />
          </div>
        ) : !schema ? (
          <div className="text-center py-8 text-gray-500 text-sm">
            Selecione um dataset para ver o schema
          </div>
        ) : filteredTables?.length === 0 ? (
          <div className="text-center py-8 text-gray-500 text-sm">
            Nenhuma tabela encontrada
          </div>
        ) : (
          <div className="py-2">
            {filteredTables?.map((table) => (
              <TableItem
                key={table.name}
                table={table}
                glossary={schema.glossary}
                expanded={expandedTable === table.name}
                onToggle={() =>
                  setExpandedTable(
                    expandedTable === table.name ? null : table.name
                  )
                }
                onCopyColumn={copyColumnName}
                copiedColumn={copiedColumn}
                searchQuery={searchQuery}
              />
            ))}
          </div>
        )}
      </div>

      {/* Footer with stats */}
      {schema && (
        <div className="px-4 py-2 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-500">
          {schema.dataset.tables.length} tabelas •{' '}
          {schema.dataset.tables.reduce((acc, t) => acc + t.columns.length, 0)}{' '}
          colunas
        </div>
      )}
    </div>
  );
}

interface TableItemProps {
  table: TableInfo;
  glossary: Record<string, string>;
  expanded: boolean;
  onToggle: () => void;
  onCopyColumn: (tableName: string, columnName: string) => void;
  copiedColumn: string | null;
  searchQuery: string;
}

function TableItem({
  table,
  glossary,
  expanded,
  onToggle,
  onCopyColumn,
  copiedColumn,
  searchQuery,
}: TableItemProps) {
  // Filter columns if searching
  const filteredColumns = searchQuery
    ? table.columns.filter(
        (col) =>
          col.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
          col.business_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
          glossary[col.name]?.toLowerCase().includes(searchQuery.toLowerCase())
      )
    : table.columns;

  return (
    <div className="px-2">
      {/* Table header */}
      <button
        onClick={onToggle}
        className={cn(
          'w-full flex items-center gap-2 px-3 py-2 rounded-lg text-left transition-colors',
          expanded
            ? 'bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300'
            : 'hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300'
        )}
      >
        <ChevronRight
          className={cn(
            'w-4 h-4 transition-transform',
            expanded && 'rotate-90'
          )}
        />
        <Table className="w-4 h-4" />
        <div className="flex-1 min-w-0">
          <span className="text-sm font-medium truncate block">{table.name}</span>
          {(table.business_name || glossary[table.name]) && (
            <span className="text-xs text-gray-500 truncate block">
              {table.business_name || glossary[table.name]}
            </span>
          )}
        </div>
        <span className="text-xs text-gray-400">{table.columns.length}</span>
      </button>

      {/* Columns */}
      {expanded && (
        <div className="ml-6 mt-1 space-y-0.5">
          {filteredColumns.map((column) => (
            <ColumnItem
              key={column.name}
              column={column}
              tableName={table.name}
              businessName={glossary[column.name]}
              onCopy={() => onCopyColumn(table.name, column.name)}
              copied={copiedColumn === `'${table.name}'[${column.name}]`}
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface ColumnItemProps {
  column: ColumnInfo;
  tableName: string;
  businessName?: string;
  onCopy: () => void;
  copied: boolean;
}

function ColumnItem({
  column,
  tableName,
  businessName,
  onCopy,
  copied,
}: ColumnItemProps) {
  const getTypeColor = (type: string) => {
    switch (type.toLowerCase()) {
      case 'int64':
      case 'double':
      case 'decimal':
        return 'text-blue-600 dark:text-blue-400';
      case 'string':
        return 'text-green-600 dark:text-green-400';
      case 'datetime':
      case 'date':
        return 'text-purple-600 dark:text-purple-400';
      case 'boolean':
        return 'text-orange-600 dark:text-orange-400';
      default:
        return 'text-gray-600 dark:text-gray-400';
    }
  };

  return (
    <div className="group flex items-center gap-2 px-3 py-1.5 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
      {column.is_key ? (
        <Key className="w-3 h-3 text-amber-500" />
      ) : (
        <Columns className="w-3 h-3 text-gray-400" />
      )}

      <div className="flex-1 min-w-0">
        <span className="text-sm text-gray-700 dark:text-gray-300 truncate block">
          {column.name}
        </span>
        {(businessName || column.business_name) && (
          <span className="text-xs text-gray-500 truncate block">
            {businessName || column.business_name}
          </span>
        )}
      </div>

      <span className={cn('text-xs', getTypeColor(column.data_type))}>
        {column.data_type}
      </span>

      <button
        onClick={onCopy}
        className="opacity-0 group-hover:opacity-100 p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 transition-opacity"
        title="Copiar nome da coluna"
      >
        {copied ? (
          <Check className="w-3 h-3 text-green-500" />
        ) : (
          <Copy className="w-3 h-3" />
        )}
      </button>
    </div>
  );
}
