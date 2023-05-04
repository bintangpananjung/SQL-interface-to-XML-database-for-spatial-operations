import { MongoClient } from "mongodb";
import { Column } from "flora-sql-parser";
import { GeoJSON } from "../extension";
import { XMLExtension } from "../xml_extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";

var basex = require("basex");

class BaseXExtension extends XMLExtension<typeof basex> {
  supportedXMLExtensionType = ["kml", "gml"];
  supportedFunctionPrefix: {
    name: string;
    args: number;
    postGISName: string;
  }[] = [
    { name: "distance", postGISName: "ST_Distance", args: 2 },
    { name: "within", postGISName: "ST_Within", args: 2 },
    { name: "dimension", postGISName: "ST_Dimension", args: 1 },
  ];
  spatialModuleNamespaces = [
    { prefix: "geo", namespace: "http://expath.org/ns/geo" },
  ];
  supportedSpatialType = [
    "MultiPoint",
    "Point",
    "LineString",
    "LinearRing",
    "Polygon",
    "MultiLineString",
    "MultiPolygon",
    "MultiGeometry",
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
      const funcPrefix = this.supportedFunctionPrefix.find(
        val => (val.postGISName = fname)
      );
      if (fname == "mod") {
        return this.constructModFunction(regResult.groups!);
      }
      if (funcPrefix && funcPrefix.args == 2) {
        return this.constructSpatialFunctionOneArgs(regResult.groups!);
      }
      // switch (fname) {
      //   case "mod":
      //     return this.constructModFunction(regResult.groups!);
      //   case this.supportedFunctionPrefix.find(val=>val.postGISName=fname)?.args:
      //     return this.constructSpatialFunctionOneArgs(regResult.groups!);
      //   default:
      //     break;
      // }
      break;
    }
    return "";
  }

  supportedFunctions = [
    /(?<fname>date)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>[=<>]) '(?<constant>.*)'/g,
    /(?<fname>mod)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+), (?<constant1>[0-9]+)\) (?<operator>[=]) (?<constant2>[0-9]*)/g,
    /(?<fname>.*)\(ST_AsText\(ST_GeomFromGML\('(?<constant1>.*)'\)\), (?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>=|<=|>=|>|<) (?<constant2>[0-9\.]*)/g,
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

    const constructXQuery = (
      spatialNamespace: any,
      where: any,
      projection: any
    ) => {
      const namespaces = this.constructSpatialNamespace(
        spatialNamespace,
        false
      );
      const moduleNamespaces = this.constructSpatialNamespace(
        this.spatialModuleNamespaces,
        true
      );
      let whereQuery = "";
      if (where.length > 0) {
        whereQuery = `[${where}]`;
      }
      return (
        namespaces +
        moduleNamespaces +
        `for $i in db:open("${this.db_name}","${collection}")//gml:featureMember/*${whereQuery}
      return json:serialize(element {'json'}
      { attribute {'objects'}{'json'},
        for $j in $i/${projection}
        return
        if(boolean($j/*/@srsName)) then (
        element {'geometry'} {geo:as-text($j/*)}
        )
        else if(boolean($j/@srsName)) then(
          element {'geometry'} {geo:as-text($j)}          
        )
        else (
            element {$j/local-name()}{$j/text()}
        )
      })`
      );
    };
    const checkResult = await this.supportedExtensionCheck(collection);

    const query = new Promise((resolve, reject) => {
      this.client
        .query(constructXQuery(checkResult, where, projection))
        .results((err: any, res: any) => {
          if (err) {
            reject(err);
          } else {
            const jsonResult: any = [];
            res.result.forEach((element: any) => {
              jsonResult.push(JSON.parse(element));
            });
            resolve(jsonResult);
          }
        });
    });
    let result: any = [];
    try {
      result = await query;
      // console.log(result);
    } catch (error) {
      console.log(error);
    }
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
    return `*:${colname} mod ${constant1} ${operator} ${constant2}`;
  }

  constructSpatialFunctionOneArgs(groups: { [key: string]: string }): string {
    const { fname, tname, colname, constant1, operator, constant2 } =
      groups as any;
    let result = `geo:distance(${constant1}, *[*/@srsName]/*) ${operator} ${constant2}`;

    return result;
  }

  constructProjectionQuery(columns: Set<string>): string {
    if (columns.size == 0) {
      return "*";
    }
    let result = `(`;
    const ignoreQName = "*:";
    let arrColumns = [...columns];
    arrColumns.forEach((column, index) => {
      if (column == "geometry") {
        result += `*[*/@srsName]/*`;
      } else {
        result += `${ignoreQName}${column}`;
      }
      if (index < arrColumns.length - 1) {
        result += ` | `;
      }
    });
    // for (const column of columns) {
    //   result += `${ignoreQName}${column}`;
    // }
    return result + ")";
  }
}
export { BaseXExtension };
