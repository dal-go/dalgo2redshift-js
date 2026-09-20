import { DOCUMENT_ID, UnsupportedError, type QueryFilter, type QueryOrder, type StructuredQuery } from "@dal-go/dalgo";
import type { RedshiftTable } from "./types.js";

const identifier = /^[A-Za-z_][A-Za-z0-9_$]*$/u;

export interface RedshiftParameter { readonly name: string; readonly value: string; }
export interface CompiledRedshiftQuery { readonly sql: string; readonly parameters: readonly RedshiftParameter[]; }

export function quoteIdentifier(value: string, label = "identifier"): string {
  if (!identifier.test(value)) throw new TypeError(`${label} must be a simple Redshift identifier`);
  return `"${value}"`;
}

export function quoteTable(table: RedshiftTable): string {
  return `${quoteIdentifier(table.schema, "schema")}.${quoteIdentifier(table.table, "table")}`;
}

function fieldColumn(table: RedshiftTable, field: string): string {
  if (field === DOCUMENT_ID) return table.keyColumn;
  const column = table.columns[field];
  if (column === undefined) throw new UnsupportedError(`Redshift field is not declared in table mapping: ${field}`);
  return column;
}

function parameter(name: string, value: unknown): RedshiftParameter {
  if (value === null || value === undefined) throw new UnsupportedError("Redshift Data API null/undefined SQL parameters");
  if (typeof value === "string") {
    if (value.length === 0) throw new UnsupportedError("Redshift Data API empty SQL parameters");
    return { name, value };
  }
  if (typeof value === "number" && Number.isFinite(value)) return { name, value: String(value) };
  if (typeof value === "boolean") return { name, value: value ? "true" : "false" };
  throw new UnsupportedError("Redshift Data API scalar parameter type");
}

function keyParameter(name: string, value: unknown, table: RedshiftTable): RedshiftParameter {
  if (table.keyType === "string") {
    if (typeof value !== "string") throw new TypeError("Redshift string key requires a string DALgo key");
  } else if (typeof value !== "number" || !Number.isSafeInteger(value) || Object.is(value, -0)) {
    throw new TypeError("Redshift integer key requires a safe integer DALgo key");
  }
  return parameter(name, value);
}

function filterSql<T>(filter: QueryFilter<T>, table: RedshiftTable, index: number): { readonly sql: string; readonly parameter?: RedshiftParameter } {
  const column = `t.${quoteIdentifier(fieldColumn(table, String(filter.field)), "column")}`;
  const name = `p${String(index)}`;
  switch (filter.operator) {
    case "==": return filter.value === null ? { sql: `${column} IS NULL` } : { sql: `${column} = :${name}`, parameter: String(filter.field) === DOCUMENT_ID ? keyParameter(name, filter.value, table) : parameter(name, filter.value) };
    case "!=": return filter.value === null ? { sql: `${column} IS NOT NULL` } : { sql: `${column} IS NOT NULL AND ${column} != :${name}`, parameter: String(filter.field) === DOCUMENT_ID ? keyParameter(name, filter.value, table) : parameter(name, filter.value) };
    case "<": case "<=": case ">": case ">=": return { sql: `${column} ${filter.operator} :${name}`, parameter: String(filter.field) === DOCUMENT_ID ? keyParameter(name, filter.value, table) : parameter(name, filter.value) };
    default: throw new UnsupportedError(`Redshift query operator: ${String(filter.operator)}`);
  }
}

function orderSql<T>(orders: readonly QueryOrder<T>[], table: RedshiftTable): string {
  if (orders.length === 0) return "";
  return ` ORDER BY ${orders.map((order) => {
    if (order.direction !== "asc" && order.direction !== "desc") throw new TypeError("Redshift order direction must be asc or desc");
    return `t.${quoteIdentifier(fieldColumn(table, String(order.field)), "column")} ${order.direction.toUpperCase()}`;
  }).join(", ")}`;
}

export function compileRedshiftQuery<T>(table: RedshiftTable, query: StructuredQuery<T>, limit: number, offset: number | undefined): CompiledRedshiftQuery {
  if (query.source.kind !== "collection" || query.source.parent !== undefined) throw new UnsupportedError("Redshift collection-group or nested collection queries");
  if (query.startAt !== undefined || query.startAfter !== undefined || query.endAt !== undefined || query.endBefore !== undefined) throw new UnsupportedError("Redshift cursors");
  if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError("limit must be a positive safe integer");
  if (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0)) throw new TypeError("offset must be a non-negative safe integer");
  const parameters: RedshiftParameter[] = [];
  const filters = query.filters ?? [];
  const orders = query.orders ?? [];
  const clauses = filters.map((filter, index) => {
    const compiled = filterSql(filter, table, index);
    if (compiled.parameter !== undefined) parameters.push(compiled.parameter);
    return compiled.sql;
  });
  const selected = [
    `t.${quoteIdentifier(table.keyColumn, "keyColumn")} AS "__dalgo_key"`,
    ...Object.entries(table.columns).map(([field, column]) => `t.${quoteIdentifier(column, "column")} AS ${quoteIdentifier(field, "field")}`),
  ];
  return { sql: `SELECT ${selected.join(", ")} FROM ${quoteTable(table)} AS t${clauses.length === 0 ? "" : ` WHERE ${clauses.join(" AND ")}`}${orderSql(orders, table)} LIMIT ${String(limit)}${offset === undefined ? "" : ` OFFSET ${String(offset)}`}`, parameters };
}
