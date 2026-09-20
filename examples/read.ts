import { RedshiftDataClient } from "@aws-sdk/client-redshift-data";
import { collection, key } from "@dal-go/dalgo";
import { RedshiftDatabase } from "@dal-go/dalgo2redshift";

const client = new RedshiftDataClient({ region: "eu-west-1" });
const database = new RedshiftDatabase({
  client,
  database: "dev",
  workgroupName: "my-serverless-workgroup",
  tables: { todos: { schema: "public", table: "todos", keyColumn: "id", keyType: "integer", columns: { title: "title", done: "done" } } },
});

console.log(await database.get(key("todos", 1)));
console.log(await database.query(collection("todos").query().build()));
