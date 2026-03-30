import { useMemo } from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  Legend,
} from 'recharts';
import { TrendingUp, BarChart3, PieChartIcon, Activity } from 'lucide-react';
import { cn } from '../../lib/utils';

interface VisualizationCardProps {
  type: string;
  title: string;
  data: Record<string, unknown>[];
  columns: string[];
  className?: string;
}

const COLORS = [
  '#0ea5e9', // primary
  '#f59e0b', // amber
  '#10b981', // emerald
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#f97316', // orange
  '#6366f1', // indigo
];

export default function VisualizationCard({
  type,
  title,
  data,
  columns,
  className,
}: VisualizationCardProps) {
  // Determine which columns to use for visualization
  const { labelColumn, valueColumns } = useMemo(() => {
    // First string-like column is the label
    const labelCol = columns.find((col) => {
      const firstVal = data[0]?.[col];
      return typeof firstVal === 'string' || firstVal instanceof Date;
    }) || columns[0];

    // Numeric columns are values
    const valueCols = columns.filter((col) => {
      const firstVal = data[0]?.[col];
      return typeof firstVal === 'number' && col !== labelCol;
    });

    return {
      labelColumn: labelCol,
      valueColumns: valueCols.length > 0 ? valueCols : [columns[1] || columns[0]],
    };
  }, [data, columns]);

  // Format data for charts
  const chartData = useMemo(() => {
    return data.slice(0, 50).map((row) => {
      const formatted: Record<string, unknown> = {
        name: String(row[labelColumn] || ''),
      };
      valueColumns.forEach((col) => {
        formatted[col] = Number(row[col]) || 0;
      });
      return formatted;
    });
  }, [data, labelColumn, valueColumns]);

  // Get icon for chart type
  const getIcon = () => {
    switch (type) {
      case 'line_chart':
        return <TrendingUp className="w-4 h-4" />;
      case 'bar_chart':
        return <BarChart3 className="w-4 h-4" />;
      case 'pie_chart':
        return <PieChartIcon className="w-4 h-4" />;
      case 'area_chart':
        return <Activity className="w-4 h-4" />;
      default:
        return <BarChart3 className="w-4 h-4" />;
    }
  };

  // Render chart based on type
  const renderChart = () => {
    const commonProps = {
      data: chartData,
      margin: { top: 5, right: 5, left: 5, bottom: 5 },
    };

    switch (type) {
      case 'line_chart':
        return (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 10 }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis tick={{ fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  borderRadius: '8px',
                  border: '1px solid #e5e7eb',
                  fontSize: '12px',
                }}
              />
              {valueColumns.map((col, index) => (
                <Line
                  key={col}
                  type="monotone"
                  dataKey={col}
                  stroke={COLORS[index % COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  activeDot={{ r: 5 }}
                />
              ))}
              {valueColumns.length > 1 && <Legend />}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'bar_chart':
        return (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 10 }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis tick={{ fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  borderRadius: '8px',
                  border: '1px solid #e5e7eb',
                  fontSize: '12px',
                }}
              />
              {valueColumns.map((col, index) => (
                <Bar
                  key={col}
                  dataKey={col}
                  fill={COLORS[index % COLORS.length]}
                  radius={[4, 4, 0, 0]}
                />
              ))}
              {valueColumns.length > 1 && <Legend />}
            </BarChart>
          </ResponsiveContainer>
        );

      case 'pie_chart':
        const pieData = chartData.map((item, index) => ({
          ...item,
          value: Number(item[valueColumns[0]]) || 0,
          fill: COLORS[index % COLORS.length],
        }));

        return (
          <ResponsiveContainer width="100%" height={200}>
            <PieChart>
              <Pie
                data={pieData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                innerRadius={40}
                outerRadius={70}
                paddingAngle={2}
                label={({ name, percent }) =>
                  `${name}: ${(percent * 100).toFixed(0)}%`
                }
                labelLine={false}
              >
                {pieData.map((entry, index) => (
                  <Cell key={index} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  borderRadius: '8px',
                  border: '1px solid #e5e7eb',
                  fontSize: '12px',
                }}
              />
            </PieChart>
          </ResponsiveContainer>
        );

      case 'area_chart':
        return (
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart {...commonProps}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 10 }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis tick={{ fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  borderRadius: '8px',
                  border: '1px solid #e5e7eb',
                  fontSize: '12px',
                }}
              />
              {valueColumns.map((col, index) => (
                <Area
                  key={col}
                  type="monotone"
                  dataKey={col}
                  stroke={COLORS[index % COLORS.length]}
                  fill={COLORS[index % COLORS.length]}
                  fillOpacity={0.2}
                />
              ))}
              {valueColumns.length > 1 && <Legend />}
            </AreaChart>
          </ResponsiveContainer>
        );

      case 'card':
        // Single value card
        const totalValue = chartData.reduce(
          (sum, item) => sum + (Number(item[valueColumns[0]]) || 0),
          0
        );
        return (
          <div className="flex flex-col items-center justify-center h-[200px]">
            <span className="text-3xl font-bold text-gray-900 dark:text-gray-100">
              {totalValue.toLocaleString('pt-BR', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}
            </span>
            <span className="text-sm text-gray-500 mt-1">{valueColumns[0]}</span>
          </div>
        );

      default:
        return (
          <div className="flex items-center justify-center h-[200px] text-gray-500">
            Visualização não suportada: {type}
          </div>
        );
    }
  };

  return (
    <div
      className={cn(
        'bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden',
        className
      )}
    >
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-200 dark:border-gray-700">
        <span className="text-primary-600 dark:text-primary-400">{getIcon()}</span>
        <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
          {title}
        </span>
      </div>

      {/* Chart */}
      <div className="p-4">{renderChart()}</div>
    </div>
  );
}
