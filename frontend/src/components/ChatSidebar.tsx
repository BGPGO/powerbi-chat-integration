import { useState, useRef, useEffect, KeyboardEvent } from 'react';
import { Send, Bot, User, RotateCcw, Copy, Check } from 'lucide-react';
import { chatApi } from '../lib/api';
import { useReportStore } from '../stores/reportStore';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  loading?: boolean;
}

const SUGGESTIONS = [
  'Quanto faturamos esse ano?',
  'Quem são meus top 10 clientes?',
  'Como foi o resultado mês a mês?',
];

// ── Loading dots ──────────────────────────────────────────────────────────────

function LoadingDots() {
  return (
    <span style={{ display: 'flex', gap: '3px', alignItems: 'center', padding: '2px 0' }}>
      {[0, 1, 2].map((i) => (
        <span
          key={i}
          style={{
            width: '5px',
            height: '5px',
            borderRadius: '50%',
            background: 'rgba(255,255,255,0.35)',
            animation: `bounce 1.2s ease-in-out ${i * 0.2}s infinite`,
            display: 'inline-block',
          }}
        />
      ))}
    </span>
  );
}

// ── Message content — preserves line breaks from AI responses ─────────────────

function MessageContent({ text }: { text: string }) {
  return (
    <span style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
      {text}
    </span>
  );
}

// ── Copy button that appears on hover over assistant messages ─────────────────

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    });
  };

  return (
    <button
      onClick={handleCopy}
      title={copied ? 'Copiado!' : 'Copiar resposta'}
      style={{
        position: 'absolute',
        top: '6px',
        right: '-28px',
        width: '22px',
        height: '22px',
        borderRadius: '5px',
        border: 'none',
        background: copied ? 'rgba(74,222,128,0.15)' : 'rgba(255,255,255,0.07)',
        color: copied ? 'rgba(74,222,128,0.8)' : 'rgba(255,255,255,0.3)',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        transition: 'background 0.15s, color 0.15s, opacity 0.15s',
        flexShrink: 0,
      }}
    >
      {copied ? <Check size={10} strokeWidth={2.5} /> : <Copy size={10} strokeWidth={2} />}
    </button>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function ChatSidebar() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [inputFocused, setInputFocused] = useState(false);
  const [hoveredMsg, setHoveredMsg] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const { selectedReport, setFilter, clearFilter, currentPage } = useReportStore();

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    setMessages([]);
    setConversationId(null);
  }, [selectedReport?.id]);

  const send = async (text: string) => {
    if (!text.trim() || sending || !selectedReport) return;

    const userId = Date.now().toString();
    const assistantId = (Date.now() + 1).toString();

    setMessages((prev) => [
      ...prev,
      { id: userId, role: 'user', content: text.trim() },
      { id: assistantId, role: 'assistant', content: '', loading: true },
    ]);
    setInput('');
    setSending(true);

    try {
      const res = await chatApi.sendMessage({
        message: text.trim(),
        conversation_id: conversationId || undefined,
        dataset_id: selectedReport?.dataset_id,
        workspace_id: selectedReport?.workspace_id,
        report_id: selectedReport?.powerbi_report_id,
        current_page: currentPage?.displayName ?? undefined,
      });
      setConversationId(res.conversation_id);
      // Apply Power BI iframe filter if present
      if (res.powerbi_filters?.has_filter) {
        setFilter(res.powerbi_filters);
      } else if (res.powerbi_filters === null || (res.powerbi_filters && !res.powerbi_filters.has_filter)) {
        // Only clear filter if intent was a data query (to not reset on schema/translation queries)
        if (res.intent === 'data_query') {
          clearFilter();
        }
      }
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId ? { ...m, content: res.message, loading: false } : m
        )
      );
    } catch {
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId
            ? { ...m, content: 'Erro ao processar a solicitação. Tente novamente.', loading: false }
            : m
        )
      );
    } finally {
      setSending(false);
      inputRef.current?.focus();
    }
  };

  const handleKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send(input);
    }
  };

  const clearConversation = () => {
    setMessages([]);
    setConversationId(null);
    clearFilter();
    inputRef.current?.focus();
  };

  const canSend = !!input.trim() && !sending && !!selectedReport;

  return (
    <div
      className="flex flex-col h-full w-full"
      style={{ background: '#162633', borderLeft: '1px solid rgba(36,76,90,0.4)' }}
    >
      {/* ── Header ── */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: '48px',
          padding: '0 14px',
          borderBottom: '1px solid rgba(36,76,90,0.4)',
          flexShrink: 0,
          gap: '8px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
          {/* Bot icon */}
          <div style={{
            width: '26px', height: '26px', borderRadius: '6px',
            background: 'rgba(36,76,90,0.5)',
            border: '1px solid rgba(36,76,90,0.7)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
          }}>
            <Bot size={13} style={{ color: 'rgba(255,255,255,0.55)' }} />
          </div>

          <div style={{ minWidth: 0 }}>
            <p style={{
              fontSize: '13px', fontWeight: 600, color: '#fff',
              lineHeight: 1.2, margin: 0,
            }}>
              Assistente IA
            </p>
            {selectedReport && (
              <p
                className="truncate"
                style={{
                  fontSize: '10px',
                  color: 'rgba(255,255,255,0.35)',
                  marginTop: '1px',
                  lineHeight: 1.2,
                  maxWidth: '220px',
                }}
              >
                {selectedReport.name}
              </p>
            )}
          </div>
        </div>

        {messages.length > 0 && (
          <button
            onClick={clearConversation}
            title="Nova conversa"
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              width: '28px', height: '28px', borderRadius: '6px',
              border: 'none', background: 'transparent',
              color: 'rgba(255,255,255,0.3)', cursor: 'pointer',
              transition: 'background 0.15s, color 0.15s',
              flexShrink: 0,
            }}
            onMouseEnter={(e) => {
              (e.currentTarget as HTMLButtonElement).style.background = 'rgba(255,255,255,0.07)';
              (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.6)';
            }}
            onMouseLeave={(e) => {
              (e.currentTarget as HTMLButtonElement).style.background = 'transparent';
              (e.currentTarget as HTMLButtonElement).style.color = 'rgba(255,255,255,0.3)';
            }}
          >
            <RotateCcw size={13} />
          </button>
        )}
      </div>

      {/* ── Messages area ── */}
      <div
        className="flex-1 overflow-y-auto"
        style={{ padding: '16px 14px', display: 'flex', flexDirection: 'column', gap: '14px' }}
      >
        {/* Empty state */}
        {messages.length === 0 && (
          <div style={{
            display: 'flex', flexDirection: 'column', alignItems: 'center',
            gap: '14px', paddingTop: '20px', textAlign: 'center',
          }}>
            <div style={{
              width: '48px', height: '48px', borderRadius: '50%',
              background: 'rgba(36,76,90,0.3)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              border: '1px solid rgba(36,76,90,0.5)',
            }}>
              <Bot size={22} style={{ color: 'rgba(255,255,255,0.35)' }} />
            </div>

            <div>
              <p style={{
                fontSize: '13px', fontWeight: 500,
                color: 'rgba(255,255,255,0.6)', margin: 0, lineHeight: 1.4,
              }}>
                {selectedReport ? 'Pergunte sobre os dados' : 'Selecione um relatório'}
              </p>
              <p style={{
                fontSize: '11px', color: 'rgba(255,255,255,0.25)',
                marginTop: '4px', lineHeight: 1.4,
              }}>
                {selectedReport ? 'Use linguagem natural, sem código' : 'para iniciar o assistente'}
              </p>
            </div>

            {selectedReport && (
              <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: '5px' }}>
                {SUGGESTIONS.map((s) => (
                  <button
                    key={s}
                    onClick={() => send(s)}
                    style={{
                      width: '100%', textAlign: 'left',
                      fontSize: '11.5px', padding: '9px 12px',
                      borderRadius: '8px',
                      border: '1px solid rgba(36,76,90,0.45)',
                      background: 'rgba(36,76,90,0.18)',
                      color: 'rgba(255,255,255,0.45)',
                      cursor: 'pointer', lineHeight: 1.4,
                      transition: 'background 0.15s, border-color 0.15s, color 0.15s',
                    }}
                    onMouseEnter={(e) => {
                      const el = e.currentTarget as HTMLButtonElement;
                      el.style.background = 'rgba(36,76,90,0.38)';
                      el.style.borderColor = 'rgba(36,150,190,0.35)';
                      el.style.color = 'rgba(255,255,255,0.80)';
                      el.style.boxShadow = '0 0 8px rgba(36,150,190,0.12)';
                    }}
                    onMouseLeave={(e) => {
                      const el = e.currentTarget as HTMLButtonElement;
                      el.style.background = 'rgba(36,76,90,0.18)';
                      el.style.borderColor = 'rgba(36,76,90,0.45)';
                      el.style.color = 'rgba(255,255,255,0.45)';
                      el.style.boxShadow = 'none';
                    }}
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Message bubbles */}
        {messages.map((msg) => {
          const isUser = msg.role === 'user';
          return (
            <div
              key={msg.id}
              className={isUser ? 'message-appear-user' : 'message-appear'}
              onMouseEnter={() => !isUser && !msg.loading && setHoveredMsg(msg.id)}
              onMouseLeave={() => setHoveredMsg(null)}
              style={{
                display: 'flex',
                flexDirection: isUser ? 'row-reverse' : 'row',
                gap: '8px',
                alignItems: 'flex-start',
              }}
            >
              {/* Avatar */}
              <div style={{
                width: '26px', height: '26px', borderRadius: '50%',
                background: isUser ? '#244C5A' : 'rgba(255,255,255,0.07)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, marginTop: '2px',
              }}>
                {isUser
                  ? <User size={11} style={{ color: 'rgba(255,255,255,0.8)' }} />
                  : <Bot size={11} style={{ color: 'rgba(255,255,255,0.5)' }} />
                }
              </div>

              {/* Bubble + copy button container */}
              <div style={{ position: 'relative', maxWidth: '82%' }}>
                <div style={{
                  padding: '9px 12px',
                  borderRadius: isUser ? '14px 4px 14px 14px' : '4px 14px 14px 14px',
                  background: isUser ? '#244C5A' : 'rgba(255,255,255,0.05)',
                  color: isUser ? '#fff' : 'rgba(255,255,255,0.82)',
                  fontSize: '12.5px',
                  lineHeight: 1.65,
                  border: isUser
                    ? 'none'
                    : '1px solid rgba(255,255,255,0.06)',
                }}>
                  {msg.loading ? <LoadingDots /> : <MessageContent text={msg.content} />}
                </div>

                {/* Copy button — only on assistant messages, on hover */}
                {!isUser && !msg.loading && hoveredMsg === msg.id && (
                  <CopyButton text={msg.content} />
                )}
              </div>
            </div>
          );
        })}

        <div ref={messagesEndRef} />
      </div>

      {/* ── Input area ── */}
      <div style={{
        padding: '10px 12px 14px',
        borderTop: '1px solid rgba(36,76,90,0.4)',
        flexShrink: 0,
      }}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: '8px',
          padding: '9px 10px 9px 14px',
          borderRadius: '12px',
          background: 'rgba(255,255,255,0.03)',
          border: inputFocused
            ? '1px solid rgba(36,150,190,0.55)'
            : '1px solid rgba(36,76,90,0.45)',
          boxShadow: inputFocused ? '0 0 0 3px rgba(36,120,160,0.15), 0 0 8px rgba(36,150,190,0.12)' : 'none',
          transition: 'border-color 0.15s, box-shadow 0.15s',
        }}>
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKey}
            onFocus={() => setInputFocused(true)}
            onBlur={() => setInputFocused(false)}
            placeholder={
              !selectedReport
                ? 'Selecione um relatório...'
                : sending
                ? 'Aguardando resposta...'
                : 'Pergunte sobre os dados...'
            }
            disabled={!selectedReport || sending}
            style={{
              flex: 1,
              background: 'transparent',
              border: 'none',
              outline: 'none',
              fontSize: '12.5px',
              color: 'rgba(255,255,255,0.88)',
              caretColor: '#F2C811',
              cursor: !selectedReport ? 'not-allowed' : 'text',
            }}
          />

          <button
            onClick={() => send(input)}
            disabled={!canSend}
            title="Enviar mensagem"
            style={{
              width: '30px', height: '30px',
              borderRadius: '8px',
              border: 'none',
              background: canSend ? '#244C5A' : 'rgba(36,76,90,0.2)',
              border: canSend ? '1px solid rgba(80,160,195,0.25)' : '1px solid transparent',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              cursor: canSend ? 'pointer' : 'not-allowed',
              opacity: canSend ? 1 : 0.4,
              flexShrink: 0,
              transition: 'background 0.15s, opacity 0.15s, box-shadow 0.15s',
            }}
            onMouseEnter={(e) => {
              if (canSend) {
                (e.currentTarget as HTMLButtonElement).style.background = '#2d5f72';
                (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 0 10px rgba(36,150,190,0.30)';
              }
            }}
            onMouseLeave={(e) => {
              (e.currentTarget as HTMLButtonElement).style.background = canSend ? '#244C5A' : 'rgba(36,76,90,0.2)';
              (e.currentTarget as HTMLButtonElement).style.boxShadow = 'none';
            }}
          >
            <Send size={12} style={{ color: canSend ? '#fff' : 'rgba(255,255,255,0.3)' }} />
          </button>
        </div>

        {/* Character hint */}
        {selectedReport && !sending && (
          <p style={{
            margin: '5px 2px 0',
            fontSize: '10px',
            color: 'rgba(255,255,255,0.18)',
            lineHeight: 1,
          }}>
            Enter para enviar
          </p>
        )}
      </div>
    </div>
  );
}
