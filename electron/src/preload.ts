import { contextBridge, ipcRenderer } from "electron";

import type { DatasetNode, SeriesQuery, SeriesResult, TableQuery, TableResult } from "../../shared/ipc";

type Api = {
  listDatasets: () => Promise<DatasetNode[]>;
  preview: (args: { path: string; limit: number }) => Promise<TableResult>;
  queryTable: (q: TableQuery) => Promise<TableResult>;
  querySeries: (q: SeriesQuery) => Promise<SeriesResult>;
};

const api: Api = {
  listDatasets: () => ipcRenderer.invoke("datasets:list"),
  preview: (args) => ipcRenderer.invoke("dataset:preview", args),
  queryTable: (q) => ipcRenderer.invoke("table:query", q),
  querySeries: (q) => ipcRenderer.invoke("series:query", q),
};

contextBridge.exposeInMainWorld("datasetApi", api);

