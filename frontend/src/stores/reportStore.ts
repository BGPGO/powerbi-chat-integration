import { create } from 'zustand';
import { reportsApi, ReportInfo } from '../lib/api';
import { PowerBIFilter } from '../types';

interface ReportStore {
  reports: ReportInfo[];
  selectedReport: ReportInfo | null;
  loading: boolean;
  error: string | null;
  activeFilter: PowerBIFilter | null;
  currentPage: { name: string; displayName: string } | null;
  fetchReports: () => Promise<void>;
  selectReport: (report: ReportInfo) => void;
  setFilter: (filter: PowerBIFilter | null) => void;
  clearFilter: () => void;
  setCurrentPage: (page: { name: string; displayName: string } | null) => void;
}

export const useReportStore = create<ReportStore>((set) => ({
  reports: [],
  selectedReport: null,
  loading: false,
  error: null,
  activeFilter: null,
  currentPage: null,

  fetchReports: async () => {
    set({ loading: true, error: null });
    try {
      const reports = await reportsApi.list();
      // Auto-selects the first report so the user lands on a populated dashboard
      set({ reports, loading: false, selectedReport: reports[0] ?? null });
    } catch (err) {
      set({
        error: err instanceof Error ? err.message : 'Falha ao carregar relatórios',
        loading: false,
      });
    }
  },

  selectReport: (report: ReportInfo) => {
    set({ selectedReport: report, activeFilter: null, currentPage: null });
  },

  setFilter: (filter: PowerBIFilter | null) => {
    set({ activeFilter: filter });
  },

  clearFilter: () => {
    set({ activeFilter: null });
  },

  setCurrentPage: (page) => {
    set({ currentPage: page });
  },
}));
