import { ExecuteStatementCommand, DescribeStatementCommand, GetStatementResultCommand } from "@aws-sdk/client-redshift-data";
import { UnsupportedError, collection, key } from "@dal-go/dalgo";
import { describe, expect, it } from "vitest";
import { RedshiftDatabase } from "../src/index.js";

const table = { schema: "public", table: "todos", keyColumn: "id", keyType: "integer", columns: { title: "title", done: "done" } } as const;
const metadata = [{ name: "__dalgo_key", label: "__dalgo_key" }, { name: "title", label: "title" }, { name: "done", label: "done" }];

function database(responses: unknown[], overrides: Partial<ConstructorParameters<typeof RedshiftDatabase>[0]> = {}): { readonly database: RedshiftDatabase; readonly commands: unknown[] } {
  const commands: unknown[] = [];
  const client = { send: async (command: unknown): Promise<unknown> => { commands.push(command); const result = responses.shift(); if (result instanceof Error) throw result; if (command instanceof GetStatementResultCommand && result !== null && typeof result === "object" && !("ColumnMetadata" in result)) return { ...result, ColumnMetadata: metadata }; return result; } };
  return { database: new RedshiftDatabase({ client: client as never, database: "dev", workgroupName: "workgroup", tables: { todos: table }, pollIntervalMs: 1, ...overrides }), commands };
}

describe("RedshiftDatabase", () => {
  it("polls then pages a mapped query", async () => {
    const fixture = database([{ Id: "statement" }, { Status: "STARTED" }, { Status: "FINISHED" }, { Records: [[{ longValue: 1 }, { stringValue: "first" }, { booleanValue: false }]], NextToken: "next" }, { Records: [[{ longValue: 2 }, { stringValue: "second" }, { booleanValue: true }]] }]);
    const page = await fixture.database.query(collection("todos").query().build());
    expect(page.records).toEqual([
      { key: key("todos", 1), exists: true, data: { title: "first", done: false } },
      { key: key("todos", 2), exists: true, data: { title: "second", done: true } },
    ]);
    expect(fixture.commands.map((command) => (command as { constructor: unknown }).constructor)).toEqual([ExecuteStatementCommand, DescribeStatementCommand, DescribeStatementCommand, GetStatementResultCommand, GetStatementResultCommand]);
    expect((fixture.commands[0] as ExecuteStatementCommand).input.Sql).toContain('FROM "public"."todos"');
    expect((fixture.commands[4] as GetStatementResultCommand).input.NextToken).toBe("next");
  });

  it("binds a key rather than interpolating it", async () => {
    const stringTable = { ...table, keyType: "string" as const };
    const fixture = database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [] }], { tables: { todos: stringTable } });
    await fixture.database.get(key("todos", "a'quoted"));
    const input = (fixture.commands[0] as ExecuteStatementCommand).input;
    expect(input.Sql).toContain("= :key");
    expect(input.Sql).not.toContain("quoted");
    expect(input.Parameters).toEqual([{ name: "key", value: "a'quoted" }]);
  });

  it("does not expose the configured key column as record data", async () => {
    const fixture = database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: 1 }, { stringValue: "one" }, { booleanValue: false }]] }]);
    await expect(fixture.database.get(key("todos", 1))).resolves.toEqual({ key: key("todos", 1), exists: true, data: { title: "one", done: false } });
  });

  it("fails closed on a bounded result", async () => {
    const fixture = database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: 1 }, { stringValue: "a" }, { booleanValue: true }], [{ longValue: 2 }, { stringValue: "b" }, { booleanValue: false }]] }], { maxRows: 1 });
    await expect(fixture.database.query(collection("todos").query().build())).rejects.toThrow(UnsupportedError);
  });

  it("honors requested query limit and offset", async () => {
    const fixture = database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: 3 }, { stringValue: "c" }, { booleanValue: false }]] }]);
    const page = await fixture.database.query(collection("todos").query().limit(1).offset(2).build());
    expect(page.records).toHaveLength(1);
    expect((fixture.commands[0] as ExecuteStatementCommand).input.Sql).toContain("LIMIT 1 OFFSET 2");
  });

  it("rejects a request limit above maxRows", async () => {
    const fixture = database([], { maxRows: 1 });
    await expect(fixture.database.query(collection("todos").query().limit(2).build())).rejects.toThrow(UnsupportedError);
    expect(fixture.commands).toHaveLength(0);
  });

  it("rejects key type drift, mismatched point reads, invalid fields, and metadata", async () => {
    await expect(database([], {}).database.get(key("todos", "1"))).rejects.toThrow("key type");
    await expect(database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: 2 }, { stringValue: "x" }, { booleanValue: false }]] }]).database.get(key("todos", 1))).rejects.toThrow("different key");
    await expect(database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: Number.MAX_SAFE_INTEGER + 1 }, { stringValue: "x" }, { booleanValue: false }]] }]).database.query(collection("todos").query().build())).rejects.toThrow("invalid field union variant");
    const fixture = database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [], ColumnMetadata: [] }]);
    await expect(fixture.database.query(collection("todos").query().build())).rejects.toThrow("column metadata");
    await expect(database([], {}).database.get(key("todos", -0))).rejects.toThrow("key type");
    await expect(database([{ Id: "statement" }, { Status: "FINISHED" }, { Records: [[{ longValue: -0 }, { stringValue: "x" }, { booleanValue: false }]] }]).database.query(collection("todos").query().build())).rejects.toThrow("invalid field union variant");
  });

  it("rejects unsafe mapping aliases and duplicate physical columns", () => {
    expect(() => new RedshiftDatabase({ client: {} as never, database: "dev", workgroupName: "workgroup", tables: { todos: { ...table, columns: { __dalgo_key: "name" } } } })).toThrow("reserved");
    expect(() => new RedshiftDatabase({ client: {} as never, database: "dev", workgroupName: "workgroup", tables: { todos: { ...table, columns: { title: "id" } } } })).toThrow("unique");
  });

  it("fails closed on unknown statuses without exposing service error text", async () => {
    await expect(database([{ Id: "statement" }, { Status: "FAILED", Error: "secret SQL text" }]).database.query(collection("todos").query().build())).rejects.toThrow("Redshift statement FAILED");
    try { await database([{ Id: "statement" }, { Status: "FAILED", Error: "secret SQL text" }]).database.query(collection("todos").query().build()); } catch (error) { expect(String(error)).not.toContain("secret SQL"); }
    await expect(database([{ Id: "statement" }, { Status: "FUTURE_STATUS" }]).database.query(collection("todos").query().build())).rejects.toThrow("unknown status");
    await expect(database([new Error("service body with a secret")]).database.query(collection("todos").query().build())).rejects.toThrow("request failed");
  });

  it("marks all write and callback transaction paths unsupported", async () => {
    const fixture = database([]).database;
    await expect(fixture.insert(key("todos", 1), { title: "x" })).rejects.toThrow(UnsupportedError);
    await expect(fixture.runReadwriteTransaction(async () => "no")).rejects.toThrow(UnsupportedError);
  });

  it("requires exactly one Redshift target", () => {
    expect(() => new RedshiftDatabase({ client: {} as never, database: "dev", tables: {}, clusterIdentifier: "cluster", workgroupName: "workgroup" })).toThrow("exactly one");
  });

  it("bounds getMany before it creates statements", async () => {
    const fixture = database([], { maxGetManyKeys: 1 });
    await expect(fixture.database.getMany([key("todos", 1), key("todos", 2)])).rejects.toThrow(UnsupportedError);
    expect(fixture.commands).toHaveLength(0);
  });

  it("aborts an AWS SDK request at the configured deadline", async () => {
    const client = { send: async (): Promise<never> => new Promise<never>(() => {}) };
    const subject = new RedshiftDatabase({ client: client as never, database: "dev", workgroupName: "workgroup", tables: { todos: table }, timeoutMs: 1 });
    await expect(subject.get(key("todos", 1))).rejects.toThrow("exceeded timeout");
  });
});
