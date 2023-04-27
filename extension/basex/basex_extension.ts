import { MongoClient } from "mongodb";
import { Column } from "flora-sql-parser";
import { GeoJSON } from "../extension";
import { XMLExtension } from "../xml_extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";

var basex = require("basex");

class BaseXExtension extends XMLExtension<typeof basex> {
  supportedXMLExtensionType = ["kml", "gml"];
  spatialModuleNamespaces = [
    { prefix: "geo", namespace: "http://expath.org/ns/geo" },
  ];
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
    super("localhost", null, "test", "admin", "admin");
  }

  async supportedExtensionCheck(collection: string): Promise<any> {
    let extensionsArray = "(";
    this.supportedXMLExtensionType.forEach((element, idx) => {
      extensionsArray += `'${element}'`;
      if (idx != this.supportedXMLExtensionType.length - 1) {
        extensionsArray += ",";
      }
    });
    extensionsArray += ")";

    let queryCheck = `for $i in ${extensionsArray} 
    let $namespace := fn:namespace-uri-for-prefix($i, db:open("${this.db_name}","${collection}")/*)
    return 
      if(fn:exists($namespace)) then (
      element {$i} {$namespace})
      else ()
      `;
    const query = new Promise((resolve, reject) => {
      this.client.query(queryCheck).results((err: any, res: any) => {
        if (res.result.length > 0) {
          const result: any = [];
          (res.result as any).forEach((element: any) => {
            const doc = new dom().parseFromString(element);
            const nodes: any = xpath.select("/*", doc);
            result.push({
              prefix: nodes[0].localName,
              namespace: nodes[0].firstChild.data,
            });
          });

          resolve(result);
        } else {
          reject(
            new Error(
              "no spatial namespace found in the collection or extension type is not valid"
            )
          );
        }
      });
    });
    const result = await query;
    return result;
  }
  async connect() {
    try {
      this.client = new basex.Session(
        this.url,
        1984,
        this.username,
        this.password
      );
      this.client.execute(`open ${this.db_name}`);
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
    if (!this.client) {
      await this.connect();
    }

    const constructXQuery = (spatialNamespace: any) => {
      const namespaces = this.constructSpatialNamespace(
        spatialNamespace,
        false
      );
      const moduleNamespaces = this.constructSpatialNamespace(
        this.spatialModuleNamespaces,
        true
      );

      return (
        namespaces +
        moduleNamespaces +
        `for $i in db:open("${this.db_name}","${collection}")//gml:featureMember/*
      return element {'result'}
      { $i/@*,
        for $j in $i/*
        return
        if(boolean($j/*/@srsName)) then (
        element {$j/*/local-name()} {geo:as-text($j/*)}
        )
        else (
            element {$j/local-name()}{$j/text()}
        )
      }`
      );
    };
    const checkResult = await this.supportedExtensionCheck(collection);

    const query = new Promise((resolve, reject) => {
      this.client
        .query(constructXQuery(checkResult))
        .results((err: any, res: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(res.result);
          }
        });
    });
    const result = await query;
    // console.log(result);

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
    const promise: Promise<string[]> = new Promise((resolve, reject) => {
      this.client
        .query(`db:list-details("${this.db_name}")/text()`)
        .results((err: any, res: any) => {
          if (err) {
            resolve(err);
          } else {
            resolve(res.result);
          }
        });
    });

    let listCollections: string[] = await promise;

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
export { BaseXExtension };
