import { create } from "zustand";

import type { DataKind, DatasetNode, DatasetRef } from "../../shared/ipc";

export type SelectedDataset = {
  nodeId: string;
  kind: DataKind;
  symbol: string;
  name?: string | null;
  partitionId: string;
  ref: DatasetRef;
};

type DatasetState = {
  nodes: DatasetNode[];
  status: "idle" | "loading" | "ready" | "error";
  error: string | null;
  selected: SelectedDataset | null;
  refresh: () => Promise<void>;
  select: (sel: SelectedDataset | null) => void;
};

export const useDatasetStore = create<DatasetState>((set) => ({
  nodes: [],
  status: "idle",
  error: null,
  selected: null,
  refresh: async () => {
    set({ status: "loading", error: null });
    try {
      const nodes = await window.datasetApi.listDatasets();
      set({ nodes, status: "ready" });
    } catch (e) {
      set({ status: "error", error: e instanceof Error ? e.message : String(e) });
    }
  },
  select: (sel) => set({ selected: sel }),
}));

