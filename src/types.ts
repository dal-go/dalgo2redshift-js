import type { RedshiftDataClient } from "@aws-sdk/client-redshift-data";

export interface RedshiftTable {
  /** Explicit physical schema; it is never inferred from the collection name. */
  readonly schema: string;
  readonly table: string;
  readonly keyColumn: string;
  /** Prevents a Redshift integer/string wire value from silently changing a DALgo key identity. */
  readonly keyType: "string" | "integer";
  /** DALgo field name to physical column name. Key is deliberately separate. */
  readonly columns: Readonly<Record<string, string>>;
}

export interface RedshiftDatabaseOptions {
  /** A caller-configured AWS SDK client. Its credentials and region stay outside this adapter. */
  readonly client: RedshiftDataClient;
  readonly database: string;
  readonly clusterIdentifier?: string;
  readonly workgroupName?: string;
  readonly secretArn?: string;
  readonly dbUser?: string;
  readonly tables: Readonly<Record<string, RedshiftTable>>;
  readonly pollIntervalMs?: number;
  readonly timeoutMs?: number;
  readonly maxPolls?: number;
  readonly maxResultPages?: number;
  readonly maxRows?: number;
  readonly maxGetManyKeys?: number;
  /** Explicit opt-in only. Browser bundles must use short-lived credentials and a narrow IAM role. */
  readonly allowBrowser?: boolean;
}
