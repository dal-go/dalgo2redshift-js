import { DescribeStatementCommand, ExecuteStatementCommand, GetStatementResultCommand } from "@aws-sdk/client-redshift-data";
import { Key, UnsupportedError, identityCodec, type Codec, type Database, type ExistingRecord, type QueryPage, type ReadwriteTransaction, type RecordSnapshot, type StructuredQuery } from "@dal-go/dalgo";
import { compileRedshiftQuery, quoteIdentifier, quoteTable, type RedshiftParameter } from "./sql.js";
import type { RedshiftDatabaseOptions, RedshiftTable } from "./types.js";

const terminalFailures = new Set(["ABORTED", "FAILED"]);
const isBrowser = typeof window !== "undefined" && typeof window.document !== "undefined";

export class RedshiftDataError extends Error { public constructor(message: string) { super(message); this.name = "RedshiftDataError"; } }

function positive(value: number | undefined, fallback: number, label: string): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 1) throw new TypeError(`${label} must be a positive safe integer`);
  return result;
}

function tableMappings(tables: Readonly<Record<string, RedshiftTable>>): Readonly<Record<string, RedshiftTable>> {
  const result: Record<string, RedshiftTable> = {};
  for (const [collection, table] of Object.entries(tables)) {
    if (collection.length === 0) throw new TypeError("collection mapping must not be empty");
    if (table.keyType !== "string" && table.keyType !== "integer") throw new TypeError("keyType must be string or integer");
    quoteTable(table); quoteIdentifier(table.keyColumn, "keyColumn");
    if (Object.hasOwn(table.columns, "__dalgo_key")) throw new TypeError("__dalgo_key is reserved for the generated key projection");
    const physicalColumns = [table.keyColumn, ...Object.values(table.columns)];
    if (new Set(physicalColumns).size !== physicalColumns.length) throw new TypeError("keyColumn and mapped physical columns must be unique");
    for (const [field, column] of Object.entries(table.columns)) { quoteIdentifier(field, "field"); quoteIdentifier(column, "column"); }
    result[collection] = Object.freeze({ ...table, columns: Object.freeze({ ...table.columns }) });
  }
  return Object.freeze(result);
}

function fieldValue(value: unknown): unknown {
  if (value === undefined || value === null || typeof value !== "object") throw new RedshiftDataError("Redshift returned an invalid field union");
  const field = value as Record<string, unknown>;
  const variants = ["stringValue", "longValue", "doubleValue", "booleanValue", "isNull", "blobValue"].filter((name) => field[name] !== undefined);
  if (variants.length !== 1) throw new RedshiftDataError("Redshift returned a malformed field union");
  switch (variants[0]) {
    case "stringValue": if (typeof field.stringValue === "string") return field.stringValue; break;
    case "longValue": if (typeof field.longValue === "number" && Number.isSafeInteger(field.longValue) && !Object.is(field.longValue, -0)) return field.longValue; break;
    case "doubleValue": if (typeof field.doubleValue === "number" && Number.isFinite(field.doubleValue)) return field.doubleValue; break;
    case "booleanValue": if (typeof field.booleanValue === "boolean") return field.booleanValue; break;
    case "isNull": if (field.isNull === true) return null; break;
    case "blobValue": throw new UnsupportedError("Redshift blob result fields");
    default: break;
  }
  throw new RedshiftDataError("Redshift returned an invalid field union variant");
}

export class RedshiftDatabase implements Database {
  readonly #options: RedshiftDatabaseOptions;
  readonly #tables: Readonly<Record<string, RedshiftTable>>;
  readonly #pollIntervalMs: number;
  readonly #timeoutMs: number;
  readonly #maxPolls: number;
  readonly #maxResultPages: number;
  readonly #maxRows: number;
  readonly #maxGetManyKeys: number;

  public constructor(options: RedshiftDatabaseOptions) {
    if (isBrowser && options.allowBrowser !== true) throw new UnsupportedError("Redshift Data API browser use; opt in only with short-lived credentials");
    if (typeof options.database !== "string" || options.database.length === 0) throw new TypeError("database is required");
    if ((options.clusterIdentifier === undefined) === (options.workgroupName === undefined)) throw new TypeError("configure exactly one of clusterIdentifier or workgroupName");
    this.#options = options;
    this.#tables = tableMappings(options.tables);
    this.#pollIntervalMs = positive(options.pollIntervalMs, 200, "pollIntervalMs");
    this.#timeoutMs = positive(options.timeoutMs, 30_000, "timeoutMs");
    this.#maxPolls = positive(options.maxPolls, 120, "maxPolls");
    this.#maxResultPages = positive(options.maxResultPages, 100, "maxResultPages");
    this.#maxRows = positive(options.maxRows, 1_000, "maxRows");
    this.#maxGetManyKeys = positive(options.maxGetManyKeys, 100, "maxGetManyKeys");
  }

  public async get<T>(key: Key, codec?: Codec<T>): Promise<RecordSnapshot<T>> {
    const table = this.#tableForKey(key);
    this.#assertKeyType(key.id, table);
    const result = await this.#execute(`SELECT ${this.#columns(table)} FROM ${quoteTable(table)} AS t WHERE t.${quoteIdentifier(table.keyColumn, "keyColumn")} = :key LIMIT 2`, [{ name: "key", value: String(key.id) }], 2, table);
    if (result.length === 0) return { key, exists: false };
    if (result.length > 1) throw new RedshiftDataError(`DALgo key ${key.path} matched multiple Redshift rows`);
    const data = this.#record(result[0]!, table);
    if (data.__dalgo_key !== key.id) throw new RedshiftDataError("Redshift point read returned a different key");
    delete data.__dalgo_key;
    return { key, exists: true, data: (codec ?? identityCodec).decode(data) as T };
  }

  public async getMany<T>(keys: readonly Key[], codec?: Codec<T>): Promise<readonly RecordSnapshot<T>[]> {
    if (keys.length > this.#maxGetManyKeys) throw new UnsupportedError(`Redshift getMany exceeded maxGetManyKeys (${String(this.#maxGetManyKeys)})`);
    return Promise.all(keys.map((key) => this.get(key, codec)));
  }

  public async query<T>(query: StructuredQuery<T>): Promise<QueryPage<T>> {
    const table = this.#tableForCollection(query.source.name);
    const requestedLimit = query.limit;
    if (requestedLimit !== undefined && (!Number.isSafeInteger(requestedLimit) || requestedLimit < 1)) throw new TypeError("query limit must be a positive safe integer");
    if (requestedLimit !== undefined && requestedLimit > this.#maxRows) throw new UnsupportedError(`Redshift query limit exceeded maxRows (${String(this.#maxRows)})`);
    const resultLimit = requestedLimit ?? this.#maxRows + 1;
    const compiled = compileRedshiftQuery(table, query, resultLimit, query.offset);
    const rows = await this.#execute(compiled.sql, compiled.parameters, resultLimit, table);
    if (requestedLimit === undefined && rows.length > this.#maxRows) throw new UnsupportedError(`Redshift query exceeded maxRows (${String(this.#maxRows)})`);
    return { records: rows.map((row): ExistingRecord<T> => {
      const data = this.#record(row, table);
      const id = data.__dalgo_key;
      this.#assertKeyType(id, table);
      delete data.__dalgo_key;
      return { key: new Key(query.source.name, id), exists: true, data: (query.source.codec ?? identityCodec).decode(data) as T };
    }) };
  }

  public insert<T>(key: Key, data: T, codec?: Codec<T>): Promise<void> { void [key, data, codec]; return this.#unsupported("insert"); }
  public set<T>(key: Key, data: T, codec?: Codec<T>): Promise<void> { void [key, data, codec]; return this.#unsupported("set"); }
  public update(key: Key, data: Readonly<Record<string, unknown>>): Promise<void> { void [key, data]; return this.#unsupported("update"); }
  public delete(key: Key): Promise<void> { void key; return this.#unsupported("delete"); }
  public runReadwriteTransaction<Result>(callback: (transaction: ReadwriteTransaction) => Promise<Result>): Promise<Result> { void callback; return Promise.reject(new UnsupportedError("Redshift Data API callback transactions")); }

  async #execute(sql: string, parameters: readonly RedshiftParameter[], rowLimit: number, table: RedshiftTable): Promise<readonly (readonly unknown[])[]> {
    const started = Date.now();
    const output = await this.#withTimeout(started, (abortSignal) => this.#options.client.send(new ExecuteStatementCommand({
      Sql: sql, Database: this.#options.database, ResultFormat: "JSON", Parameters: [...parameters],
      ...(this.#options.clusterIdentifier === undefined ? { WorkgroupName: this.#options.workgroupName } : { ClusterIdentifier: this.#options.clusterIdentifier }),
      ...(this.#options.secretArn === undefined ? {} : { SecretArn: this.#options.secretArn }),
      ...(this.#options.dbUser === undefined ? {} : { DbUser: this.#options.dbUser }),
    }), { abortSignal }));
    if (typeof output.Id !== "string" || output.Id.length === 0) throw new RedshiftDataError("ExecuteStatement omitted statement ID");
    const id = output.Id;
    for (let polls = 0; polls < this.#maxPolls; polls += 1) {
      this.#assertDeadline(started);
      const status = await this.#withTimeout(started, (abortSignal) => this.#options.client.send(new DescribeStatementCommand({ Id: id }), { abortSignal }));
      if (status.Status === "FINISHED") return this.#results(id, started, rowLimit, table);
      if (status.Status !== undefined && terminalFailures.has(status.Status)) throw new RedshiftDataError(`Redshift statement ${status.Status}`);
      if (status.Status !== "SUBMITTED" && status.Status !== "PICKED" && status.Status !== "STARTED") throw new RedshiftDataError("Redshift statement returned an unknown status");
      if (polls + 1 === this.#maxPolls) break;
      await new Promise<void>((resolve) => setTimeout(resolve, Math.min(this.#pollIntervalMs, this.#remaining(started))));
    }
    throw new RedshiftDataError(`Redshift statement exceeded ${String(this.#maxPolls)} status polls`);
  }

  async #results(id: string, started: number, rowLimit: number, table: RedshiftTable): Promise<readonly (readonly unknown[])[]> {
    const rows: (readonly unknown[])[] = []; let token: string | undefined;
    for (let page = 0; page < this.#maxResultPages; page += 1) {
      this.#assertDeadline(started);
      const result = await this.#withTimeout(started, (abortSignal) => this.#options.client.send(new GetStatementResultCommand({ Id: id, ...(token === undefined ? {} : { NextToken: token }) }), { abortSignal }));
      if (page === 0) this.#assertMetadata(result.ColumnMetadata, table);
      for (const row of result.Records ?? []) { rows.push(row.map(fieldValue)); if (rows.length >= rowLimit) return rows; }
      token = result.NextToken;
      if (token === undefined || token.length === 0) return rows;
    }
    throw new UnsupportedError(`Redshift result exceeded maxResultPages (${String(this.#maxResultPages)})`);
  }

  #columns(table: RedshiftTable): string { return [`t.${quoteIdentifier(table.keyColumn, "keyColumn")} AS "__dalgo_key"`, ...Object.entries(table.columns).map(([field, column]) => `t.${quoteIdentifier(column, "column")} AS ${quoteIdentifier(field, "field")}`)].join(", "); }
  #record(row: readonly unknown[], table: RedshiftTable): Record<string, unknown> { const names = ["__dalgo_key", ...Object.keys(table.columns)]; if (row.length !== names.length) throw new RedshiftDataError("Redshift result column count did not match configured projection"); return Object.fromEntries(names.map((name, index) => [name, row[index]])); }
  #assertMetadata(metadata: unknown, table: RedshiftTable): void {
    const expected = ["__dalgo_key", ...Object.keys(table.columns)];
    if (!Array.isArray(metadata) || metadata.length !== expected.length) throw new RedshiftDataError("Redshift first result page omitted or changed column metadata");
    metadata.forEach((column, index) => {
      if (column === null || typeof column !== "object") throw new RedshiftDataError("Redshift returned invalid column metadata");
      const item = column as Record<string, unknown>; const name = expected[index];
      if (item.name !== name || item.label !== name) throw new RedshiftDataError("Redshift result metadata did not match configured projection");
    });
  }
  #assertKeyType(value: unknown, table: RedshiftTable): asserts value is string | number {
    if (table.keyType === "string" && typeof value === "string") return;
    if (table.keyType === "integer" && typeof value === "number" && Number.isSafeInteger(value) && !Object.is(value, -0)) return;
    throw new RedshiftDataError(`Redshift result key did not match configured ${table.keyType} key type`);
  }
  #tableForKey(key: Key): RedshiftTable { if (key.parent !== undefined) throw new UnsupportedError("Redshift nested keys"); return this.#tableForCollection(key.collection); }
  #tableForCollection(collection: string): RedshiftTable { const table = this.#tables[collection]; if (table === undefined) throw new UnsupportedError(`Redshift collection has no table mapping: ${collection}`); return table; }
  #remaining(started: number): number { const remaining = this.#timeoutMs - (Date.now() - started); if (remaining < 1) throw new RedshiftDataError("Redshift operation exceeded timeout"); return remaining; }
  async #withTimeout<T>(started: number, operation: (abortSignal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => { controller.abort(); reject(new RedshiftDataError("Redshift operation exceeded timeout")); }, this.#remaining(started));
      void operation(controller.signal).then(
        (value) => { clearTimeout(timer); resolve(value); },
        (error: unknown) => {
          clearTimeout(timer);
          if (controller.signal.aborted) { reject(new RedshiftDataError("Redshift operation exceeded timeout")); return; }
          reject(error instanceof RedshiftDataError || error instanceof UnsupportedError ? error : new RedshiftDataError("Redshift Data API request failed"));
        },
      );
    });
  }
  #assertDeadline(started: number): void { this.#remaining(started); }
  #unsupported(operation: string): Promise<never> { return Promise.reject(new UnsupportedError(`Redshift Data API ${operation}: read-only adapter`)); }
}
