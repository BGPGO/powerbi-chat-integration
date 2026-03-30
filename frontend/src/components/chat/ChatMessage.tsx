import { useMemo } from 'react';
import { format } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { User, Bot, Clock, Zap, AlertCircle, Copy, Check } from 'lucide-react';
import { useState } from 'react';
import type { ChatMessage as ChatMessageType } from '../../types';
import { cn } from '../../lib/utils';
import QueryResultTable from './QueryResultTable';
import VisualizationCard from './VisualizationCard';

interface ChatMessageProps {
  message: ChatMessageType;
}

export default function ChatMessage({ message }: ChatMessageProps) {
  const [copied, setCopied] = useState(false);
  const isUser = message.role === 'user';
  const isError = message.metadata?.error;
  
  const metadata = message.metadata as {
    intent?: string;
    agents_used?: string[];
    query_result?: {
      columns: string[];
      rows: Record<string, unknown>[];
      dax_query?: string;
      execution_time_ms: number;
    };
    visualizations?: Array<{
      type: string;
      title: string;
      confidence: number;
    }>;
    suggestions?: string[];
    total_time_ms?: number;
  } | undefined;

  const formattedTime = useMemo(() => {
    return format(new Date(message.timestamp), 'HH:mm', { locale: ptBR });
  }, [message.timestamp]);

  const copyDaxQuery = () => {
    if (metadata?.query_result?.dax_query) {
      navigator.clipboard.writeText(metadata.query_result.dax_query);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (message.isLoading) {
    return (
      <div className="flex gap-3 p-4">
        <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary-100 dark:bg-primary-900 flex items-center justify-center">
          <Bot className="w-5 h-5 text-primary-600 dark:text-primary-400" />
        </div>
        <div className="flex-1">
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <span>Pensando</span>
            <span className="animate-typing">...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className={cn(
        'flex gap-3 p-4 transition-colors',
        isUser ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-850'
      )}
    >
      {/* Avatar */}
      <div
        className={cn(
          'flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center',
          isUser
            ? 'bg-gray-200 dark:bg-gray-700'
            : isError
            ? 'bg-red-100 dark:bg-red-900'
            : 'bg-primary-100 dark:bg-primary-900'
        )}
      >
        {isUser ? (
          <User className="w-5 h-5 text-gray-600 dark:text-gray-300" />
        ) : isError ? (
          <AlertCircle className="w-5 h-5 text-red-600 dark:text-red-400" />
        ) : (
          <Bot className="w-5 h-5 text-primary-600 dark:text-primary-400" />
        )}
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className="flex items-center gap-2 mb-1">
          <span className="font-medium text-sm text-gray-900 dark:text-gray-100">
            {isUser ? 'Você' : 'Assistente'}
          </span>
          <span className="text-xs text-gray-500">{formattedTime}</span>
          
          {/* Metadata badges */}
          {metadata?.total_time_ms && (
            <span className="flex items-center gap-1 text-xs text-gray-400">
              <Clock className="w-3 h-3" />
              {(metadata.total_time_ms / 1000).toFixed(1)}s
            </span>
          )}
          {metadata?.agents_used && metadata.agents_used.length > 0 && (
            <span className="flex items-center gap-1 text-xs text-primary-500">
              <Zap className="w-3 h-3" />
              {metadata.agents_used.join(', ')}
            </span>
          )}
        </div>

        {/* Message content */}
        <div className="prose prose-sm dark:prose-invert max-w-none">
          <ReactMarkdown
            components={{
              code({ className, children, ...props }) {
                const match = /language-(\w+)/.exec(className || '');
                const isInline = !match;
                
                return isInline ? (
                  <code className="bg-gray-100 dark:bg-gray-700 px-1 py-0.5 rounded text-sm" {...props}>
                    {children}
                  </code>
                ) : (
                  <SyntaxHighlighter
                    style={oneDark}
                    language={match[1]}
                    PreTag="div"
                  >
                    {String(children).replace(/\n$/, '')}
                  </SyntaxHighlighter>
                );
              },
            }}
          >
            {message.content}
          </ReactMarkdown>
        </div>

        {/* Query Result */}
        {metadata?.query_result && (
          <div className="mt-4 space-y-3">
            {/* DAX Query */}
            {metadata.query_result.dax_query && (
              <div className="relative">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-medium text-gray-500">DAX Query</span>
                  <button
                    onClick={copyDaxQuery}
                    className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
                  >
                    {copied ? (
                      <>
                        <Check className="w-3 h-3" />
                        Copiado
                      </>
                    ) : (
                      <>
                        <Copy className="w-3 h-3" />
                        Copiar
                      </>
                    )}
                  </button>
                </div>
                <SyntaxHighlighter
                  language="dax"
                  style={oneDark}
                  customStyle={{ fontSize: '0.75rem' }}
                >
                  {metadata.query_result.dax_query}
                </SyntaxHighlighter>
              </div>
            )}

            {/* Results Table */}
            <QueryResultTable
              columns={metadata.query_result.columns}
              rows={metadata.query_result.rows}
            />
          </div>
        )}

        {/* Visualizations */}
        {metadata?.visualizations && metadata.visualizations.length > 0 && (
          <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
            {metadata.visualizations.map((viz, index) => (
              <VisualizationCard
                key={index}
                type={viz.type}
                title={viz.title}
                data={metadata.query_result?.rows || []}
                columns={metadata.query_result?.columns || []}
              />
            ))}
          </div>
        )}

        {/* Suggestions */}
        {metadata?.suggestions && metadata.suggestions.length > 0 && (
          <div className="mt-4">
            <span className="text-xs font-medium text-gray-500 mb-2 block">
              Sugestões de próximas perguntas:
            </span>
            <div className="flex flex-wrap gap-2">
              {metadata.suggestions.map((suggestion, index) => (
                <button
                  key={index}
                  className="px-3 py-1.5 text-sm bg-primary-50 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300 rounded-full hover:bg-primary-100 dark:hover:bg-primary-900/50 transition-colors"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
