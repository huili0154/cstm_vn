import type { OrderBy, TableFilter, TableQuery } from "../../shared/ipc";

type SqlWithParams = { sql: string; params: unknown[] };

function quoteIdent(ident: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(ident)) {
    throw new Error("invalid_identifier");
  }
  return `"${ident}"`;
}

function buildWhere(filters: TableFilter[] | undefined): SqlWithParams {
  if (!filters?.length) {
    return { sql: "", params: [] };
  }

  const clauses: string[] = [];
  const params: unknown[] = [];

  for (const f of filters) {
    const col = quoteIdent(f.col);
    if (f.op === "is_null") {
      clauses.push(`${col} IS NULL`);
      continue;
    }
    if (f.op === "not_null") {
      clauses.push(`${col} IS NOT NULL`);
      continue;
    }

    if (f.op === "contains") {
      clauses.push(`${col} ILIKE ?`);
      params.push(`%${String(f.value ?? "")}%`);
      continue;
    }

    if (f.op === "between") {
      clauses.push(`${col} BETWEEN ? AND ?`);
      params.push(f.value ?? null, f.value2 ?? null);
      continue;
    }

    const opMap: Record<string, string> = {
      eq: "=",
      gt: ">",
      gte: ">=",
      lt: "<",
      lte: "<=",
    };
    const op = opMap[f.op];
    if (!op) {
      throw new Error("invalid_filter_op");
    }
    clauses.push(`${col} ${op} ?`);
    params.push(f.value ?? null);
  }

  return { sql: `WHERE ${clauses.join(" AND ")}`, params };
}

function buildOrderBy(orderBy: OrderBy[] | undefined): string {
  if (!orderBy?.length) {
    return "";
  }
  const parts = orderBy.map((o) => `${quoteIdent(o.col)} ${o.desc ? "DESC" : "ASC"}`);
  return `ORDER BY ${parts.join(", ")}`;
}

export function buildTableQuerySql(filePath: string, q: TableQuery): SqlWithParams {
  const cols = q.columns?.length ? q.columns.map(quoteIdent).join(", ") : "*";
  const where = buildWhere(q.filters);
  const order = buildOrderBy(q.orderBy);
  const sql = `SELECT ${cols} FROM read_parquet(?) ${where.sql} ${order} LIMIT ? OFFSET ?`;
  return { sql, params: [filePath, ...where.params, q.limit, q.offset] };
}

export function buildPreviewSql(filePath: string, limit: number): SqlWithParams {
  return {
    sql: `SELECT * FROM read_parquet(?) LIMIT ?`,
    params: [filePath, limit],
  };
}

export function buildCountSql(filePath: string, filters: TableFilter[] | undefined): SqlWithParams {
  const where = buildWhere(filters);
  return {
    sql: `SELECT COUNT(*)::BIGINT as cnt FROM read_parquet(?) ${where.sql}`,
    params: [filePath, ...where.params],
  };
}

export function buildSeriesSql(
  filePath: string,
  xCol: string,
  yCol: string,
  filters: TableFilter[] | undefined,
  maxPoints: number,
  step: number,
): SqlWithParams {
  const x = quoteIdent(xCol);
  const y = quoteIdent(yCol);
  const where = buildWhere(filters);
  const sql = `WITH t AS (
    SELECT ${x} as x, ${y} as y, row_number() OVER (ORDER BY ${x} ASC) as rn
    FROM read_parquet(?) ${where.sql}
  )
  SELECT x, y FROM t WHERE rn % ? = 1 ORDER BY x ASC LIMIT ?`;
  return { sql, params: [filePath, ...where.params, Math.max(1, step), Math.max(1, maxPoints)] };
}

