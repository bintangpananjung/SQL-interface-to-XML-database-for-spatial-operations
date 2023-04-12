import { MongoClient } from "mongodb";
import { JsonExtension } from "../json_extension";
import { Column } from "flora-sql-parser";
import { GeoJSON } from "../extension";

class MongoExtension extends JsonExtension<MongoClient> {
  constructFunctionQuery(clause: any): string {
    const funcStr = this.astToFuncStr(clause);
    for (const pattern of this.supportedFunctions) {
      pattern.lastIndex = 0;
      let regResult = pattern.exec(funcStr);
      if (regResult == null) {
        continue;
      }
      const { groups } = regResult!;
      const { fname } = groups as any;
      switch (fname) {
        case "mod":
          return this.constructModFunction(regResult.groups!);
        case "ST_Distance":
          return this.constructSTDistanceFunction(regResult.groups!);
        default:
          break;
      }
      break;
    }
    return "";
  }

  supportedFunctions = [
    /(?<fname>date)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>[=<>]) '(?<constant>.*)'/g,
    /(?<fname>mod)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+), (?<constant1>[0-9]+)\) (?<operator>[=]) (?<constant2>[0-9]*)/g,
    /(?<fname>ST_Distance)\(ST_AsText\(ST_GeomFromGeoJSON\('(?<constant1>.*type.*coordinates.*)'\)\), (?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>=|<=|>=) (?<constant2>[0-9\.]*)/g,
  ];

  constructor() {
    super("mongodb://localhost:27017", null, "test");
  }

  async connect() {
    this.client = await new MongoClient(this.url, { useUnifiedTopology: true });
    try {
      await this.client.connect();
    } catch (e) {
      console.log(
        `Connection to : ${this.url} is failed, error : ${e.nessage}`
      );
    }
  }

  async getAllFields(col_name: string): Promise<string[]> {
    const db = this.client!.db();
    const col = db.collection(col_name);
    const result = await col.findOne(
      {},
      { projection: { _id: 0, properties: 1 } }
    );
    return Object.keys(result.properties);
  }

  async getResult(
    collection: string,
    where: string,
    projection: string
  ): Promise<any> {
    let col;

    if (!this.client) {
      await this.connect();
    }
    let db = this.client!.db("test");
    col = db.collection(collection);
    JSON.parse(where);
    const result = col
      .find(JSON.parse(where), { projection: JSON.parse(projection) })
      .toArray();
    return result;
  }

  getDbName() {
    return this.db_name;
  }

  standardizeData(data: any): GeoJSON[] {
    return data as GeoJSON[];
  }

  async getCollectionsName(): Promise<string[]> {
    await this.connect();
    let db = this.client!.db(this.db_name);
    let collections = db.listCollections().toArray();
    let listCollections = [];
    for (let idx = 0; idx < (await collections).length; idx++) {
      listCollections.push((await collections)[idx].name);
    }

    return listCollections;
  }

  constructModFunction(groups: { [key: string]: string }): string {
    const { fname, tname, colname, constant1, operator, constant2 } =
      groups as any;
    return `{ "properties.${colname}": { "$mod": [ ${constant1}, ${constant2} ] } }`;
  }

  constructSTDistanceFunction(groups: { [key: string]: string }): string {
    const { fname, tname, colname, constant1, operator, constant2 } =
      groups as any;
    let maxDistance = null;
    let minDistance = null;
    if (operator === "<=") {
      maxDistance = constant2;
    } else if (operator === ">=") {
      minDistance = constant2;
    } else if (operator === "=") {
      maxDistance = constant2;
      minDistance = constant2;
    }
    let result = `{"geometry" : {"$near": {"$geometry": ${constant1} `;

    if (maxDistance != null) {
      result += `, "$maxDistance": ${maxDistance * 111.32 * 1000}`;
    }
    if (minDistance != null) {
      result += `, "$minDistance": ${minDistance * 111.32 * 1000}`;
    }

    return result + "}}}";
  }

  constructProjectionQuery(columns: Set<string>): string {
    if (columns.size == 0) {
      return "{}";
    }
    let result = `{"_id": 0`;
    for (const column of columns) {
      if (column === "geometry") {
        result += `,"geometry": 1`;
        continue;
      }
      result += `,"properties.${column}": { "$ifNull": [ "$properties.${column}", null ] }`;
    }
    return result + "}";
  }
}
export { MongoExtension };
