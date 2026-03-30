import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { v4 as uuidv4 } from 'uuid';
import type {
  ChatMessage,
  ChatStore,
  DatasetInfo,
  WorkspaceInfo,
} from '../types';
import { chatApi } from '../lib/api';

// Helper to generate unique IDs
const generateId = () => uuidv4().slice(0, 8);

export const useChatStore = create<ChatStore>()(
  persist(
    (set, get) => ({
      // Initial state
      conversationId: null,
      messages: [],
      isLoading: false,
      error: null,
      selectedWorkspace: null,
      selectedDataset: null,
      sidebarOpen: true,
      darkMode: false,
      showSchema: false,

      // Send a message
      sendMessage: async (content: string) => {
        const { conversationId, messages, selectedDataset, selectedWorkspace } = get();

        // Create user message
        const userMessage: ChatMessage = {
          id: generateId(),
          role: 'user',
          content,
          timestamp: new Date(),
        };

        // Create placeholder assistant message
        const assistantMessage: ChatMessage = {
          id: generateId(),
          role: 'assistant',
          content: '',
          timestamp: new Date(),
          isLoading: true,
        };

        // Add messages to state
        set({
          messages: [...messages, userMessage, assistantMessage],
          isLoading: true,
          error: null,
        });

        try {
          // Send to API
          const response = await chatApi.sendMessage({
            message: content,
            conversation_id: conversationId || undefined,
            dataset_id: selectedDataset?.id,
            workspace_id: selectedWorkspace?.id,
          });

          // Update assistant message with response
          set((state) => ({
            conversationId: response.conversation_id,
            messages: state.messages.map((msg) =>
              msg.id === assistantMessage.id
                ? {
                    ...msg,
                    content: response.message,
                    isLoading: false,
                    metadata: {
                      intent: response.intent,
                      agents_used: response.agents_used,
                      query_result: response.query_result,
                      visualizations: response.visualizations,
                      suggestions: response.suggestions,
                      total_time_ms: response.total_time_ms,
                    },
                  }
                : msg
            ),
            isLoading: false,
          }));
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Failed to send message';
          
          // Update assistant message with error
          set((state) => ({
            messages: state.messages.map((msg) =>
              msg.id === assistantMessage.id
                ? {
                    ...msg,
                    content: `Desculpe, ocorreu um erro: ${errorMessage}`,
                    isLoading: false,
                    metadata: { error: true },
                  }
                : msg
            ),
            isLoading: false,
            error: errorMessage,
          }));
        }
      },

      // Clear conversation
      clearConversation: () => {
        set({
          conversationId: null,
          messages: [],
          error: null,
        });
      },

      // Set workspace
      setWorkspace: (workspace: WorkspaceInfo | null) => {
        set({
          selectedWorkspace: workspace,
          selectedDataset: null, // Reset dataset when workspace changes
        });
      },

      // Set dataset
      setDataset: (dataset: DatasetInfo | null) => {
        set({ selectedDataset: dataset });
      },

      // Toggle sidebar
      toggleSidebar: () => {
        set((state) => ({ sidebarOpen: !state.sidebarOpen }));
      },

      // Toggle dark mode
      toggleDarkMode: () => {
        set((state) => {
          const newDarkMode = !state.darkMode;
          // Apply to document
          if (newDarkMode) {
            document.documentElement.classList.add('dark');
          } else {
            document.documentElement.classList.remove('dark');
          }
          return { darkMode: newDarkMode };
        });
      },

      // Toggle schema panel
      toggleSchema: () => {
        set((state) => ({ showSchema: !state.showSchema }));
      },

      // Set error
      setError: (error: string | null) => {
        set({ error });
      },
    }),
    {
      name: 'powerbi-chat-storage',
      partialize: (state) => ({
        darkMode: state.darkMode,
        sidebarOpen: state.sidebarOpen,
      }),
    }
  )
);

// Initialize dark mode from storage
if (typeof window !== 'undefined') {
  const stored = localStorage.getItem('powerbi-chat-storage');
  if (stored) {
    try {
      const { state } = JSON.parse(stored);
      if (state?.darkMode) {
        document.documentElement.classList.add('dark');
      }
    } catch {
      // Ignore parse errors
    }
  }
}
