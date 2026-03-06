import duckdb from "duckdb";

export type DuckDbClient = {
  all: (sql: string, params: unknown[]) => Promise<Record<string, unknown>[]>;
};

export function createDuckDbClient(): DuckDbClient {
  const db = new duckdb.Database(":memory:");
  const conn = db.connect();

  return {
    all: (sql: string, params: unknown[]) =>
      new Promise((resolve, reject) => {
        conn.all(sql, params, (err: Error | null, rows: Record<string, unknown>[]) => {
          if (err) {
            reject(err);
            return;
          }
          resolve(rows ?? []);
        });
      }),
  };
}

