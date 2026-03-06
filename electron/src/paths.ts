import path from "node:path";

export function getProjectRoot(): string {
  return process.cwd();
}

export function getDatasetRoot(): string {
  return path.join(getProjectRoot(), "dataset");
}

export function safeResolveUnder(baseDir: string, relativePath: string): string {
  const resolved = path.resolve(baseDir, relativePath);
  const base = path.resolve(baseDir);
  if (!resolved.startsWith(base + path.sep) && resolved !== base) {
    throw new Error("path_out_of_dataset_root");
  }
  return resolved;
}

