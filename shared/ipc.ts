export type DataKind = "daily" | "tick";

export type DatasetRef = {
  kind: DataKind;
  path: string;
};

export type OrderBy = {
  col: string;
  desc?: boolean;
};

export type FilterOp =
  | "eq"
  | "contains"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "between"
  | "is_null"
  | "not_null";

export type TableFilter = {
  col: string;
  op: FilterOp;
  value?: string | number;
  value2?: string | number;
};

export type TableQuery = {
  dataset: DatasetRef;
  columns?: string[];
  filters?: TableFilter[];
  orderBy?: OrderBy[];
  limit: number;
  offset: number;
};

export type ColumnSchema = {
  name: string;
  type: string;
};

export type TableResult = {
  columns: ColumnSchema[];
  rows: Record<string, unknown>[];
  totalEstimate?: number;
};

export type SeriesQuery = {
  dataset: DatasetRef;
  xCol: string;
  yCol: string;
  filters?: TableFilter[];
  maxPoints: number;
};

export type SeriesPoint = { x: string | number; y: number | null };

export type SeriesResult = {
  points: SeriesPoint[];
};

export type DatasetNode = {
  id: string;
  label: string;
  kind: DataKind;
  symbol?: string;
  name?: string | null;
  partitions: { id: string; label: string; ref: DatasetRef }[];
};

