import { useState, useEffect } from 'react';
import {
  ChevronLeft,
  ChevronRight,
  Folder,
  Database,
  Table,
  Columns,
  RefreshCw,
  Search,
  Moon,
  Sun,
  Settings,
} from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { workspacesApi } from '../../lib/api';
import type { WorkspaceInfo, DatasetInfo } from '../../types';
import { cn } from '../../lib/utils';

export default function Sidebar() {
  const {
    sidebarOpen,
    toggleSidebar,
    selectedWorkspace,
    selectedDataset,
    setWorkspace,
    setDataset,
    darkMode,
    toggleDarkMode,
    showSchema,
    toggleSchema,
  } = useChatStore();

  const [workspaces, setWorkspaces] = useState<WorkspaceInfo[]>([]);
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedWorkspace, setExpandedWorkspace] = useState<string | null>(null);

  // Load workspaces on mount
  useEffect(() => {
    loadWorkspaces();
  }, []);

  // Load datasets when workspace is selected
  useEffect(() => {
    if (selectedWorkspace) {
      loadDatasets(selectedWorkspace.id);
    }
  }, [selectedWorkspace]);

  const loadWorkspaces = async () => {
    try {
      setLoading(true);
      const data = await workspacesApi.list();
      setWorkspaces(data);
    } catch (error) {
      console.error('Failed to load workspaces:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDatasets = async (workspaceId: string) => {
    try {
      setLoading(true);
      const data = await workspacesApi.listDatasets(workspaceId);
      setDatasets(data);
    } catch (error) {
      console.error('Failed to load datasets:', error);
      setDatasets([]);
    } finally {
      setLoading(false);
    }
  };

  const handleWorkspaceClick = (workspace: WorkspaceInfo) => {
    if (expandedWorkspace === workspace.id) {
      setExpandedWorkspace(null);
    } else {
      setExpandedWorkspace(workspace.id);
      setWorkspace(workspace);
    }
  };

  const handleDatasetClick = (dataset: DatasetInfo) => {
    setDataset(dataset);
  };

  // Filter workspaces by search
  const filteredWorkspaces = workspaces.filter(
    (ws) =>
      ws.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      ws.description?.toLowerCase().includes(searchQuery.toLowerCase())
  );

  if (!sidebarOpen) {
    return (
      <div className="w-12 bg-gray-900 flex flex-col items-center py-4">
        <button
          onClick={toggleSidebar}
          className="p-2 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors"
        >
          <ChevronRight className="w-5 h-5" />
        </button>
      </div>
    );
  }

  return (
    <div className="w-72 bg-gray-900 text-gray-100 flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-powerbi-yellow flex items-center justify-center">
            <Database className="w-5 h-5 text-gray-900" />
          </div>
          <span className="font-semibold">Power BI Chat</span>
        </div>
        <button
          onClick={toggleSidebar}
          className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors"
        >
          <ChevronLeft className="w-5 h-5" />
        </button>
      </div>

      {/* Search */}
      <div className="px-3 py-2">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Buscar workspaces..."
            className="w-full pl-9 pr-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-100 placeholder:text-gray-500 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          />
        </div>
      </div>

      {/* Workspaces & Datasets */}
      <div className="flex-1 overflow-y-auto px-2 py-2">
        {loading && workspaces.length === 0 ? (
          <div className="flex items-center justify-center py-8">
            <RefreshCw className="w-5 h-5 text-gray-500 animate-spin" />
          </div>
        ) : filteredWorkspaces.length === 0 ? (
          <div className="text-center py-8 text-gray-500 text-sm">
            {searchQuery ? 'Nenhum workspace encontrado' : 'Nenhum workspace disponível'}
          </div>
        ) : (
          <div className="space-y-1">
            {filteredWorkspaces.map((workspace) => (
              <div key={workspace.id}>
                {/* Workspace */}
                <button
                  onClick={() => handleWorkspaceClick(workspace)}
                  className={cn(
                    'w-full flex items-center gap-2 px-3 py-2 rounded-lg text-left transition-colors',
                    selectedWorkspace?.id === workspace.id
                      ? 'bg-gray-800 text-white'
                      : 'text-gray-400 hover:bg-gray-800 hover:text-white'
                  )}
                >
                  <Folder className="w-4 h-4 flex-shrink-0" />
                  <span className="text-sm truncate flex-1">{workspace.name}</span>
                  {expandedWorkspace === workspace.id ? (
                    <ChevronLeft className="w-4 h-4 rotate-90" />
                  ) : (
                    <ChevronRight className="w-4 h-4" />
                  )}
                </button>

                {/* Datasets */}
                {expandedWorkspace === workspace.id && (
                  <div className="ml-4 mt-1 space-y-1">
                    {loading ? (
                      <div className="flex items-center gap-2 px-3 py-2 text-gray-500 text-sm">
                        <RefreshCw className="w-3 h-3 animate-spin" />
                        Carregando...
                      </div>
                    ) : datasets.length === 0 ? (
                      <div className="px-3 py-2 text-gray-500 text-sm">
                        Nenhum dataset
                      </div>
                    ) : (
                      datasets.map((dataset) => (
                        <button
                          key={dataset.id}
                          onClick={() => handleDatasetClick(dataset)}
                          className={cn(
                            'w-full flex items-center gap-2 px-3 py-2 rounded-lg text-left transition-colors',
                            selectedDataset?.id === dataset.id
                              ? 'bg-primary-600 text-white'
                              : 'text-gray-400 hover:bg-gray-800 hover:text-white'
                          )}
                        >
                          <Database className="w-4 h-4 flex-shrink-0" />
                          <span className="text-sm truncate">{dataset.name}</span>
                        </button>
                      ))
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Schema toggle */}
      {selectedDataset && (
        <div className="px-3 py-2 border-t border-gray-800">
          <button
            onClick={toggleSchema}
            className={cn(
              'w-full flex items-center gap-2 px-3 py-2 rounded-lg transition-colors',
              showSchema
                ? 'bg-primary-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:text-white'
            )}
          >
            <Table className="w-4 h-4" />
            <span className="text-sm">Ver Schema</span>
          </button>
        </div>
      )}

      {/* Footer */}
      <div className="px-3 py-3 border-t border-gray-800 flex items-center justify-between">
        <button
          onClick={toggleDarkMode}
          className="p-2 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors"
        >
          {darkMode ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
        </button>
        <button
          onClick={loadWorkspaces}
          className="p-2 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors"
          title="Recarregar workspaces"
        >
          <RefreshCw className={cn('w-5 h-5', loading && 'animate-spin')} />
        </button>
        <button className="p-2 text-gray-400 hover:text-white hover:bg-gray-800 rounded-lg transition-colors">
          <Settings className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
}
