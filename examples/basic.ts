import { RedshiftDataClient } from "@aws-sdk/client-redshift-data";
import { collection } from "@dal-go/dalgo";
import { RedshiftDatabase } from "@dal-go/dalgo2redshift";

const database = new RedshiftDatabase({
  client: new RedshiftDataClient({ region: "eu-west-1" }),
  database: "dev",
  workgroupName: "my-serverless-workgroup",
  tables: { todos: { schema: "public", table: "todos", keyColumn: "id", keyType: "integer", columns: { title: "title", done: "done" } } },
});

console.log(await database.query(collection("todos").query().limit(10).offset(0).build()));
