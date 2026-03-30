import { useState, useRef, useEffect, KeyboardEvent } from 'react';
import { Send, Loader2, Sparkles } from 'lucide-react';
import { cn } from '../../lib/utils';

interface ChatInputProps {
  onSend: (message: string) => void;
  isLoading: boolean;
  placeholder?: string;
  suggestions?: string[];
}

export default function ChatInput({
  onSend,
  isLoading,
  placeholder = 'Digite sua pergunta sobre os dados...',
  suggestions = [],
}: ChatInputProps) {
  const [message, setMessage] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 200)}px`;
    }
  }, [message]);

  const handleSend = () => {
    const trimmed = message.trim();
    if (trimmed && !isLoading) {
      onSend(trimmed);
      setMessage('');
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto';
      }
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    onSend(suggestion);
  };

  return (
    <div className="border-t border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
      {/* Suggestions */}
      {suggestions.length > 0 && !message && (
        <div className="px-4 pt-3 pb-1">
          <div className="flex items-center gap-2 mb-2">
            <Sparkles className="w-4 h-4 text-primary-500" />
            <span className="text-xs font-medium text-gray-500">Sugestões</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {suggestions.slice(0, 4).map((suggestion, index) => (
              <button
                key={index}
                onClick={() => handleSuggestionClick(suggestion)}
                disabled={isLoading}
                className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-full hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors disabled:opacity-50"
              >
                {suggestion}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Input */}
      <div className="p-4">
        <div className="flex items-end gap-2">
          <div className="flex-1 relative">
            <textarea
              ref={textareaRef}
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              disabled={isLoading}
              rows={1}
              className={cn(
                'w-full resize-none rounded-xl border border-gray-300 dark:border-gray-600',
                'bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100',
                'px-4 py-3 pr-12',
                'placeholder:text-gray-400 dark:placeholder:text-gray-500',
                'focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent',
                'disabled:opacity-50 disabled:cursor-not-allowed',
                'transition-all duration-200'
              )}
            />
            <div className="absolute right-2 bottom-2 text-xs text-gray-400">
              {message.length > 0 && `${message.length}/4000`}
            </div>
          </div>

          <button
            onClick={handleSend}
            disabled={!message.trim() || isLoading}
            className={cn(
              'flex items-center justify-center',
              'w-12 h-12 rounded-xl',
              'bg-primary-600 hover:bg-primary-700 dark:bg-primary-500 dark:hover:bg-primary-600',
              'text-white',
              'transition-all duration-200',
              'disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-primary-600'
            )}
          >
            {isLoading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <Send className="w-5 h-5" />
            )}
          </button>
        </div>

        {/* Helper text */}
        <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
          Pressione Enter para enviar, Shift+Enter para nova linha
        </p>
      </div>
    </div>
  );
}
