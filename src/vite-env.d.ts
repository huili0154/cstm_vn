/// <reference types="vite/client" />

import type { DatasetNode, SeriesQuery, SeriesResult, TableQuery, TableResult } from "../shared/ipc";

declare global {
  interface Window {
    datasetApi: {
      listDatasets: () => Promise<DatasetNode[]>;
      preview: (args: { path: string; limit: number }) => Promise<TableResult>;
      queryTable: (q: TableQuery) => Promise<TableResult>;
      querySeries: (q: SeriesQuery) => Promise<SeriesResult>;
    };
  }
}

export {};
