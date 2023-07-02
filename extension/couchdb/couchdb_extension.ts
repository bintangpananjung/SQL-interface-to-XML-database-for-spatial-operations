import { bindAll, result, sumBy } from "lodash";
import nano, { DocumentScope, ServerScope } from "nano";
import { JsonExtension } from "../json_extension";
import { Select, From, Column } from "flora-sql-parser";
import { GeoJSON } from "../extension";
import { AutoEncryptionExtraOptions } from "mongodb";

class CouchDbExtension extends JsonExtension<any> {
  supportPreExecutionQuery: boolean = false;
  canJoin: boolean = false;
  supportedProjectionFunctions: {
    regex: RegExp;
    name: string;
    args: number;
    postGISName: string;
    isAggregation: boolean;
  }[] = [];
  constructFunctionQuery(clause: any): string {
    const funcStr = this.astToFuncStr(clause);
    for (const pattern of this.supportedSelectionFunctions) {
      pattern.regex.lastIndex = 0;
      let regResult = pattern.regex.exec(funcStr);
      if (regResult == null) {
        continue;
      }
      const { groups } = regResult!;
      const { fname } = groups as any;
      switch (fname) {
        case "mod":
          return this.constructModFunction(regResult.groups!);
        default:
          break;
      }
      break;
    }
    return "";
  }

  supportedSelectionFunctions = [
    {
      regex:
        /(?<fname>mod)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+), (?<constant1>[0-9]+)\) (?<operator>[=]) (?<constant2>[0-9]*)/g,
      matches: ["mod"],
    },
  ];

  standardizeData(data: any): GeoJSON[] {
    return data;
  }

  constructor() {
    super("http://hanif:1256@localhost:5984", null, "test");
  }

  async connect() {
    this.client = nano(this.url);
  }

  async getAllFields(col_name: string): Promise<string[]> {
    return [];
  }

  // // Melakukan construct query mongo dari nama table, selection, alias, dan table_id
  async getResult(
    collection: string,
    where: string,
    projection: string
  ): Promise<any> {
    let col;

    if (!this.client) {
      this.connect();
    }
    const db = this.client.db.use(collection);
    const selector = JSON.parse(where);
    let result: any = [];
    const recursive = async (
      selector: any,
      batchCount: number,
      db: any,
      fields: string[]
    ) => {
      let query = {
        selector,
        skip: batchCount * 25,
      } as any;
      if (fields.length != 0) {
        query.fields = fields;
      }
      const batch = await db.find(query);

      const { docs } = batch;
      result.push(...docs);
      if (docs.length > 0) {
        await recursive(selector, batchCount + 1, db, fields);
      }
    };
    await recursive(selector, 0, db, JSON.parse(projection));
    return result;
  }

  async getCollections(): Promise<string[]> {
    let db = this.client!.db(this.db_name);
    let collections = db!.listCollections().toArray();
    let listCollections = [];
    for (let idx = 0; idx < (await collections!).length; idx++) {
      listCollections.push((await collections!)[idx].name);
    }
    return listCollections;
  }

  getDbName() {
    return this.db_name;
  }

  async getCollectionsName(): Promise<string[]> {
    if (!this.client) {
      await this.connect();
    }
    return await this.client!.db.list();
  }

  constructModFunction(groups: { [key: string]: string }): string {
    const { fname, tname, colname, constant1, operator, constant2 } =
      groups as any;
    return `{ "properties.${colname}": { "$mod": [ ${constant1}, ${constant2} ] } }`;
  }

  constructProjectionQuery(columns: Set<string>, collection: string): string {
    // return JSON.stringify([]);
    const result = [];
    for (const column of columns) {
      if (column === "geometry") {
        result.push(column);
        continue;
      }
      result.push(`properties.${column}`);
    }
    return JSON.stringify(result);
  }
}
export { CouchDbExtension };
