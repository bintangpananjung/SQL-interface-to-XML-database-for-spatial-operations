import { MongoClient } from "mongodb";
import { Column } from "flora-sql-parser";
import { GML, XMLConfig } from "../extension";
import { XMLExtension } from "../xml_extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";

var basex = require("basex");

class BaseXExtension extends XMLExtension<typeof basex> {
  version: XMLConfig;
  supportPreExecutionQuery: boolean = true;
  spatialNamespace: { prefix: string; namespace: string };
  moduleConfig: XMLConfig[] = [
    {
      version: ["7.6"],
      getDocFunc(collection: any, db_name: any) {
        return `db:open("${db_name}","${collection}")`;
      },
      getCollectionNamesFunc(db_name: string) {
        return `db:list-details("${db_name}")`;
      },
      modules: [
        {
          supportedSpatialFunctionPrefix: [
            { name: "distance", postGISName: "ST_Distance", args: 2 },
            { name: "within", postGISName: "ST_Within", args: 2 },
            { name: "dimension", postGISName: "ST_Dimension", args: 1 },
          ],
          getSTAsTextfunc(node: any) {
            return `geo:as-text(${node})`;
          },
          extension: "gml",
        },
      ],
    },
  ];
  supportedXMLExtensionType = ["kml", "gml"];
  spatialModuleNamespaces = [
    {
      modules: [{ prefix: "geo", namespace: "http://expath.org/ns/geo" }],
      extension: "gml",
    },
  ];
  supportedSpatialType = [
    {
      types: [
        "MultiPoint",
        "Point",
        "LineString",
        "LinearRing",
        "Polygon",
        "MultiLineString",
        "MultiPolygon",
        "MultiGeometry",
      ],
      extType: "gml",
    },
    {
      types: ["Point", "LineString", "Polygon", "MultiGeometry"],
      extType: "kml",
    },
  ];

  supportedFunctions = [
    /(?<fname>date)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>[=<>]) '(?<constant>.*)'/g,
    /(?<fname>mod)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+), (?<constant1>[0-9]+)\) (?<operator>[=]) (?<constant2>[0-9]*)/g,
    /(?<fname>.*)\((?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>=|<=|>=|>|<) (?<constant>[0-9\.]*)/g,
    /(?<fname>.*)\(ST_AsText\(ST_GeomFromGML\('(?<constant1>.*)'\)\), (?<tname>[a-zA-Z0-9_]+)\.(?<colname>[a-zA-Z0-9_]+)\) (?<operator>=|<=|>=|>|<) (?<constant2>[0-9\.]*)/g,
  ];

  constructor() {
    super("localhost", null, "test", "admin", "admin");
    this.version = {} as any;
    this.spatialNamespace = {} as any;
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

  async initVersion() {
    if (!this.client) {
      await this.connect();
    }
    const promise: Promise<string> = new Promise((resolve, reject) => {
      this.client
        .query(`data(db:system()//version)`)
        .results((err: any, res: any) => {
          if (err) {
            resolve(err);
          } else {
            resolve(res.result[0]);
          }
        });
    });

    const version = await promise;
    const moduleInVersion = this.moduleConfig.find(val =>
      val.version.some(el => el == version)
    );
    if (moduleInVersion) {
      this.version = moduleInVersion;
    } else {
      throw new Error(
        "This BaseX version is still not implemented in this program"
      );
    }
  }

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
      const moduleVersion = this.version.modules.find(
        val => val.extension === this.spatialNamespace.prefix
      );
      const funcPrefix = moduleVersion?.supportedSpatialFunctionPrefix.find(
        (val: any) => val.postGISName == fname
      );
      if (fname == "mod") {
        return this.constructModFunction(regResult.groups!);
      }
      if (funcPrefix && funcPrefix.args == 2) {
        return this.constructSpatialFunctionTwoArgs(
          regResult.groups!,
          funcPrefix.name
        );
      }
      if (funcPrefix && funcPrefix.args == 1) {
        return this.constructSpatialFunctionOneArgs(
          regResult.groups!,
          funcPrefix.name
        );
      }
      // switch (fname) {
      //   case "mod":
      //     return this.constructModFunction(regResult.groups!);
      //   case this.supportedFunctionPrefix.find(val=>val.postGISName=fname)?.args:
      //     return this.constructSpatialFunctionOneArgs(regResult.groups!);
      //   default:
      //     break;
      // }
    }
    return "";
  }

  //return all fields/column in query

  async getAllFields(col_name: string): Promise<string[]> {
    const db = this.client!.db();
    const col = db.collection(col_name);
    const result = await col.findOne(
      {},
      { projection: { _id: 0, properties: 1 } }
    );
    return Object.keys(result.properties);
  }
  async executePreExecutionQuery(collection: string): Promise<void> {
    const queryCheck = this.supportedExtensionCheck(collection);
    const checkPromise = new Promise((resolve, reject) => {
      this.client.query(queryCheck).results((err: any, res: any) => {
        if (res.result.length > 0) {
          // console.log(res);

          let result: any;
          const doc = new dom().parseFromString(res.result[0]);
          const nodes: any = xpath.select("/*", doc);
          result = {
            prefix: nodes[0].localName,
            namespace: nodes[0].firstChild.data,
          };

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
    const checkResult: any = await checkPromise;
    this.spatialNamespace = checkResult;
  }

  async getResult(
    collection: any[],
    where: any[],
    projection: any[],
    columnAs?: any
  ): Promise<any> {
    if (!this.client) {
      await this.connect();
    }
    // console.log(this.constructXQuery(collection, where, projection, columnAs));

    const query = new Promise((resolve, reject) => {
      this.client
        .query(this.constructXQuery(collection, where, projection, columnAs))
        .results((err: any, res: any) => {
          if (err) {
            reject(err);
          } else {
            // const jsonResult: any = [];
            // res.result.forEach((element: any) => {
            //   jsonResult.push(JSON.parse(element));
            // });
            // resolve(jsonResult);
            resolve(res.result);
          }
        });
    });
    let result: any = [];
    try {
      let getResultTime = new Date().getTime();
      result = await query;
      console.log(
        `waktu eksekusi pada DBMS BaseX adalah ${
          new Date().getTime() - getResultTime
        }ms`
      );
      // console.log(result);
    } catch (error) {
      console.log(error);
    }
    if (result.length == 0) {
      throw Error("no data found");
    }
    return result;
  }

  getDbName() {
    return this.db_name;
  }

  standardizeData(data: any): XMLDocument[] {
    return data as XMLDocument[];
  }

  async getCollectionsName(): Promise<string[]> {
    if (!this.client) {
      await this.connect();
    }
    const promise: Promise<string[]> = new Promise((resolve, reject) => {
      this.client
        .query(`${this.version.getCollectionNamesFunc(this.db_name)}/text()`)
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

  constructSpatialFunctionTwoArgs(
    groups: { [key: string]: string },
    funcName: string
  ): string {
    const { fname, tname, colname, constant1, operator, constant2 } =
      groups as any;
    let spatialTypes = this.supportedSpatialType.find(element => {
      return element.extType == this.spatialNamespace.prefix;
    });
    let tempSpatialTypes: any[] = [];
    spatialTypes?.types.forEach(element => {
      tempSpatialTypes.push(`*/local-name()='${element}'`);
    });
    let result = `geo:${funcName}(${constant1}, *[${tempSpatialTypes.join(
      " or "
    )}]/*) ${operator} ${constant2}`;

    return result;
  }
  constructSpatialFunctionOneArgs(
    groups: { [key: string]: string },
    funcName: string
  ): string {
    const { fname, tname, colname, constant, operator } = groups as any;
    let spatialTypes = this.supportedSpatialType.find(element => {
      return element.extType == this.spatialNamespace.prefix;
    });
    let tempSpatialTypes: any[] = [];
    spatialTypes?.types.forEach(element => {
      tempSpatialTypes.push(`*/local-name()='${element}'`);
    });
    let result = `geo:${funcName}(*[${tempSpatialTypes.join(
      " or "
    )}]/*) ${operator} ${constant}`;

    return result;
  }
}
export { BaseXExtension };
