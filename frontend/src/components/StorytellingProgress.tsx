import { useEffect, useState, useRef } from 'react';
import { X, Download, AlertCircle, BookOpen, Loader2 } from 'lucide-react';
import { exportApi, type StorytellingJobStatus } from '../lib/api';

interface StorytellingProgressProps {
  jobId: string;
  reportName: string;
  onClose: () => void;
}

const POLL_INTERVAL = 3000;

export default function StorytellingProgress({
  jobId,
  reportName,
  onClose,
}: StorytellingProgressProps) {
  const [job, setJob] = useState<StorytellingJobStatus | null>(null);
  const [error, setError] = useState('');
  const intervalRef = useRef<ReturnType<typeof setInterval>>();

  useEffect(() => {
    const poll = async () => {
      try {
        const status = await exportApi.getStorytellingStatus(jobId);
        setJob(status);

        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(intervalRef.current);
          if (status.status === 'failed') {
            setError(status.error ?? 'Erro desconhecido');
          }
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : 'Erro ao verificar status';
        setError(msg);
        clearInterval(intervalRef.current);
      }
    };

    poll();
    intervalRef.current = setInterval(poll, POLL_INTERVAL);

    return () => clearInterval(intervalRef.current);
  }, [jobId]);

  const handleDownload = async () => {
    try {
      const blob = await exportApi.downloadStorytelling(jobId);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${reportName}_storytelling.pdf`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Erro ao baixar PDF';
      setError(msg);
    }
  };

  const isProcessing = job?.status === 'queued' || job?.status === 'processing';
  const isCompleted = job?.status === 'completed';
  const isFailed = job?.status === 'failed' || !!error;

  const progressPct =
    job && job.total_steps > 0
      ? Math.round((job.progress / job.total_steps) * 100)
      : 0;

  return (
    <>
      <div
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
        width: '420px',
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
              background: 'rgba(139,92,246,0.12)',
              border: '1px solid rgba(139,92,246,0.25)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <BookOpen size={15} style={{ color: 'rgba(167,139,250,0.9)' }} />
            </div>
            <div>
              <p style={{ margin: 0, fontSize: '14px', fontWeight: 600, color: '#fff' }}>
                Storytelling com IA
              </p>
              <p style={{ margin: 0, fontSize: '11px', color: 'rgba(255,255,255,0.35)' }}>
                {reportName}
              </p>
            </div>
          </div>
          {!isProcessing && (
            <button
              onClick={onClose}
              style={{
                width: '28px', height: '28px', borderRadius: '6px',
                border: 'none', background: 'transparent',
                color: 'rgba(255,255,255,0.3)', cursor: 'pointer',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}
            >
              <X size={14} />
            </button>
          )}
        </div>

        {/* Body */}
        <div style={{ padding: '24px 20px' }}>
          {/* Processing */}
          {isProcessing && (
            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '48px', height: '48px', margin: '0 auto 16px',
                borderRadius: '12px',
                background: 'rgba(139,92,246,0.1)',
                border: '1px solid rgba(139,92,246,0.2)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Loader2 size={22} className="animate-spin" style={{ color: 'rgba(167,139,250,0.8)' }} />
              </div>

              {/* Progress bar */}
              <div style={{
                width: '100%', height: '6px', borderRadius: '3px',
                background: 'rgba(36,76,90,0.4)',
                overflow: 'hidden',
                marginBottom: '12px',
              }}>
                <div style={{
                  width: `${Math.max(progressPct, 5)}%`,
                  height: '100%',
                  borderRadius: '3px',
                  background: 'linear-gradient(90deg, rgba(139,92,246,0.6), rgba(167,139,250,0.9))',
                  transition: 'width 0.5s ease',
                }} />
              </div>

              <p style={{ margin: 0, fontSize: '13px', color: 'rgba(255,255,255,0.6)', fontWeight: 500 }}>
                {job?.current_step || 'Iniciando...'}
              </p>
              <p style={{ margin: '6px 0 0', fontSize: '11px', color: 'rgba(255,255,255,0.25)' }}>
                {progressPct > 0 ? `${progressPct}% concluído` : 'Aguarde, isso pode levar alguns minutos...'}
              </p>
            </div>
          )}

          {/* Completed */}
          {isCompleted && (
            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '48px', height: '48px', margin: '0 auto 16px',
                borderRadius: '12px',
                background: 'rgba(34,197,94,0.1)',
                border: '1px solid rgba(34,197,94,0.25)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Download size={22} style={{ color: 'rgba(74,222,128,0.9)' }} />
              </div>
              <p style={{ margin: 0, fontSize: '14px', fontWeight: 600, color: 'rgba(74,222,128,0.9)' }}>
                Relatório pronto!
              </p>
              <p style={{ margin: '8px 0 0', fontSize: '12px', color: 'rgba(255,255,255,0.4)' }}>
                Seu PDF com storytelling foi gerado com sucesso.
              </p>
            </div>
          )}

          {/* Error */}
          {isFailed && (
            <div style={{
              display: 'flex', gap: '10px', alignItems: 'flex-start',
              padding: '12px', borderRadius: '8px',
              background: 'rgba(239,68,68,0.08)',
              border: '1px solid rgba(239,68,68,0.2)',
            }}>
              <AlertCircle size={14} style={{ color: 'rgba(248,113,113,0.9)', marginTop: '1px', flexShrink: 0 }} />
              <p style={{ margin: 0, fontSize: '12px', color: 'rgba(248,113,113,0.9)', lineHeight: 1.5 }}>
                {error || job?.error || 'Erro ao gerar storytelling'}
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{
          padding: '14px 20px 18px',
          borderTop: '1px solid rgba(36,76,90,0.4)',
          display: 'flex', gap: '8px',
        }}>
          {isCompleted && (
            <>
              <button
                onClick={onClose}
                style={{
                  flex: 1, padding: '9px', borderRadius: '8px',
                  border: '1px solid rgba(36,76,90,0.5)',
                  background: 'transparent',
                  color: 'rgba(255,255,255,0.45)',
                  fontSize: '13px', fontWeight: 500, cursor: 'pointer',
                }}
              >
                Fechar
              </button>
              <button
                onClick={handleDownload}
                style={{
                  flex: 2, padding: '9px', borderRadius: '8px',
                  border: '1px solid rgba(80,160,195,0.3)',
                  background: '#244C5A',
                  color: '#fff',
                  fontSize: '13px', fontWeight: 600, cursor: 'pointer',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '7px',
                }}
              >
                <Download size={13} />
                Baixar PDF
              </button>
            </>
          )}
          {isFailed && (
            <button
              onClick={onClose}
              style={{
                flex: 1, padding: '9px', borderRadius: '8px',
                border: '1px solid rgba(36,76,90,0.5)',
                background: 'transparent',
                color: 'rgba(255,255,255,0.45)',
                fontSize: '13px', fontWeight: 500, cursor: 'pointer',
              }}
            >
              Fechar
            </button>
          )}
        </div>
      </div>
    </>
  );
}
