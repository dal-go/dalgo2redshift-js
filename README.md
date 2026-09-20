# `@dal-go/dalgo2redshift`

Read-only [DALgo](https://dalgo.io/) adapter for the [Amazon Redshift Data API](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html). It uses the AWS SDK v3 client supplied by the application, runs each statement asynchronously, polls `DescribeStatement`, and follows JSON `GetStatementResult` pages with hard limits.

## Install and configure

```sh
pnpm add @dal-go/dalgo2redshift @aws-sdk/client-redshift-data @dal-go/dalgo
```

```ts
import { RedshiftDataClient } from "@aws-sdk/client-redshift-data";
import { RedshiftDatabase } from "@dal-go/dalgo2redshift";

const database = new RedshiftDatabase({
  client: new RedshiftDataClient({ region: "eu-west-1" }),
  database: "dev",
  workgroupName: "analytics", // or clusterIdentifier, exactly one
  tables: {
    todos: {
      schema: "public", table: "todos", keyColumn: "id", keyType: "integer",
      columns: { title: "title", done: "done" },
    },
  },
  maxRows: 1_000,
  maxResultPages: 100,
});
```

Collection/table, key, and projection mappings are explicit. `keyType` is required and prevents integer/string key identity drift. The adapter accepts only simple identifiers and emits quoted identifiers; values are passed as Redshift Data API named parameters, never interpolated into SQL. `__dalgo_key` is reserved, and physical key/projection columns may not overlap.

## Semantics and limits

- `get`, `getMany`, and top-level collection `query` are supported. `getMany` is bounded by `maxGetManyKeys` (100 by default). Query `limit` is honored (and rejected above `maxRows`); `offset` is compiled to SQL `OFFSET`. With no limit, the adapter requests `maxRows + 1` and fails closed on overflow. Queries accept equality/inequality and scalar comparison filters and ordering. Nested keys, collection groups, cursors, membership/array filters, and unmapped fields are rejected.
- Every statement is `ResultFormat: "JSON"`; the adapter polls only to `FINISHED`, treats `ABORTED`/`FAILED` as errors, and bounds polls, elapsed time, result pages, and rows. A query exceeding `maxRows` fails rather than returning a silently truncated page.
- `insert`, `set`, `update`, `delete`, and `runReadwriteTransaction` always throw `UnsupportedError`. A Data API SQL session is not a DALgo callback transaction: a callback can issue multiple asynchronous statements, cannot safely provide DALgo's atomic read/write contract, and an uncertain network outcome must not be retried as a write.
- Results are temporary service-side statement results; the Data API JSON result operation and its `NextToken` pagination are documented by AWS in [GetStatementResult](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_GetStatementResult.html). The first page must include `ColumnMetadata` with both `name` and `label` exactly matching the generated aliases (`__dalgo_key`, then configured fields); the documented response syntax includes those values, so an omitted or inconsistent metadata response fails closed.

## Authentication and security

Pass an AWS SDK `RedshiftDataClient`; do not pass access keys, database passwords, or secrets to this adapter. The SDK's standard credential provider chain should use workload/role, IAM Identity Center, or other short-lived credentials. Redshift requires `redshift-data` permissions plus target-specific credentials permissions; use a least-privilege policy scoped to the cluster/workgroup and statement owner, following AWS's [Data API IAM guidance](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-iam.html).

Node/server use is the default. Browser construction is rejected unless `allowBrowser: true` is set explicitly. That opt-in is only appropriate for a trusted application that receives short-lived, narrowly scoped credentials through a secure identity flow. Never ship IAM user access keys, a Secrets Manager ARN usable by the browser identity, or an unrestricted Data API policy in browser code. AWS signs requests with SigV4 and documents credential/IAM responsibilities in its [identity and access-control guide](https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-iam-authentication-access-control.html).

The adapter does not log SQL, parameter values, credentials, tokens, statement IDs, or result data.
