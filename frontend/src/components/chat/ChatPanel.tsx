import { useEffect, useRef } from 'react';
import { MessageSquare, Database, RefreshCw } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import ChatMessage from './ChatMessage';
import ChatInput from './ChatInput';
import { cn } from '../../lib/utils';

export default function ChatPanel() {
  const {
    messages,
    isLoading,
    selectedDataset,
    sendMessage,
    clearConversation,
  } = useChatStore();

  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Get last suggestions
  const lastMessage = messages[messages.length - 1];
  const suggestions = (lastMessage?.metadata?.suggestions as string[]) || [];

  // Example suggestions when no messages
  const defaultSuggestions = selectedDataset
    ? [
        'Quais são as tabelas disponíveis?',
        'Qual foi o faturamento total?',
        'Mostre os top 10 produtos',
        'Compare vendas por região',
      ]
    : [
        'Quais datasets estão disponíveis?',
        'Listar workspaces',
      ];

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
        <div className="flex items-center gap-3">
          <MessageSquare className="w-5 h-5 text-primary-600 dark:text-primary-400" />
          <div>
            <h2 className="font-semibold text-gray-900 dark:text-gray-100">
              Chat
            </h2>
            {selectedDataset && (
              <div className="flex items-center gap-1 text-xs text-gray-500">
                <Database className="w-3 h-3" />
                <span>{selectedDataset.name}</span>
              </div>
            )}
          </div>
        </div>

        {messages.length > 0 && (
          <button
            onClick={clearConversation}
            className="flex items-center gap-1 px-3 py-1.5 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Nova conversa
          </button>
        )}
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        {messages.length === 0 ? (
          <EmptyState datasetName={selectedDataset?.name} />
        ) : (
          <div className="divide-y divide-gray-100 dark:divide-gray-800">
            {messages.map((message) => (
              <ChatMessage key={message.id} message={message} />
            ))}
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <ChatInput
        onSend={sendMessage}
        isLoading={isLoading}
        suggestions={messages.length === 0 ? defaultSuggestions : suggestions}
        placeholder={
          selectedDataset
            ? `Pergunte sobre ${selectedDataset.name}...`
            : 'Selecione um dataset para começar...'
        }
      />
    </div>
  );
}

function EmptyState({ datasetName }: { datasetName?: string }) {
  return (
    <div className="flex flex-col items-center justify-center h-full p-8 text-center">
      <div className="w-16 h-16 rounded-full bg-primary-100 dark:bg-primary-900/30 flex items-center justify-center mb-4">
        <MessageSquare className="w-8 h-8 text-primary-600 dark:text-primary-400" />
      </div>

      <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
        {datasetName ? `Consulte ${datasetName}` : 'Bem-vindo ao Power BI Chat'}
      </h3>

      <p className="text-gray-500 dark:text-gray-400 max-w-md mb-6">
        {datasetName
          ? 'Faça perguntas em linguagem natural sobre seus dados. Eu vou traduzir para DAX e trazer os resultados.'
          : 'Selecione um workspace e dataset na barra lateral para começar a consultar seus dados com IA.'}
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-w-lg">
        <ExampleCard
          icon="📊"
          title="Análise de Vendas"
          example="Qual foi o faturamento por região no último trimestre?"
        />
        <ExampleCard
          icon="📈"
          title="Tendências"
          example="Mostre a evolução mensal de vendas"
        />
        <ExampleCard
          icon="🏆"
          title="Rankings"
          example="Quais são os top 5 produtos?"
        />
        <ExampleCard
          icon="🔍"
          title="Exploração"
          example="Quais tabelas estão disponíveis?"
        />
      </div>
    </div>
  );
}

function ExampleCard({
  icon,
  title,
  example,
}: {
  icon: string;
  title: string;
  example: string;
}) {
  const { sendMessage, selectedDataset } = useChatStore();

  return (
    <button
      onClick={() => selectedDataset && sendMessage(example)}
      disabled={!selectedDataset}
      className={cn(
        'flex flex-col items-start p-4 rounded-lg border border-gray-200 dark:border-gray-700',
        'bg-white dark:bg-gray-800',
        'hover:border-primary-300 dark:hover:border-primary-700 hover:shadow-sm',
        'transition-all duration-200 text-left',
        'disabled:opacity-50 disabled:cursor-not-allowed'
      )}
    >
      <span className="text-2xl mb-2">{icon}</span>
      <span className="font-medium text-sm text-gray-900 dark:text-gray-100 mb-1">
        {title}
      </span>
      <span className="text-xs text-gray-500 dark:text-gray-400 line-clamp-2">
        "{example}"
      </span>
    </button>
  );
}
