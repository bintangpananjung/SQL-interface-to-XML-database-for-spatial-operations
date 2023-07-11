import { MongoClient } from "mongodb";
import { Column } from "flora-sql-parser";
import { GML, XMLConfig } from "../extension";
import { XMLExtension } from "../xml_extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";
const existdb = require("@existdb/node-exist");

class ExistDBExtension extends XMLExtension<typeof existdb> {
  supportPreExecutionQuery: boolean = true;
  version: XMLConfig;
  supportedXMLExtensionType = ["gml"];
  spatialModuleNamespaces = [];
  spatialNamespace: { prefix: string; namespace: string };
  canJoin: boolean = true;
  supportedProjectionFunctions: {
    regex: RegExp;
    name: string;
    args: number;
    postGISName: string;
    isAggregation: boolean;
  }[] = [
    {
      regex: /'/g,
      name: "count",
      args: 1,
      isAggregation: true,
      postGISName: "count",
    },
  ];
  moduleConfig: XMLConfig[] = [
    {
      version: ["6.0.1"],
      getDocFunc(collection: any, db_name: any, client: any) {
        return `collection("/db/${db_name}/${collection}")`;
      },
      mapOperator: ":",
      getCollectionNamesFunc(db_name: string, client: any) {
        return client.collection.read(`db/${db_name}`);
      },
      modules: [],
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
      types: [],
      extType: "kml",
    },
  ];

  supportedSelectionFunctions: any[] = [];

  constructor() {
    super("localhost", null, "test", "admin", "admin");
    this.version = {} as any;
    this.spatialNamespace = {} as any;
  }
  async connect() {
    try {
      this.client = existdb.connect({
        basic_auth: { user: "admin", pass: "" },
        secure: true,
        host: "localhost",
        port: "8443",
        path: "/exist/xmlrpc",
      });
      //   console.log("connected");
    } catch (e) {
      console.log(
        `Connection to : ${this.url} is failed, error : ${e.message}`
      );
    }
  }

  async initVersion() {
    if (!this.client) {
      await this.connect();
    }
    const version = await this.client.server.version();
    const moduleInVersion = this.moduleConfig.find(
      val => val.version == version
    );
    if (moduleInVersion) {
      this.version = moduleInVersion;
      return version;
    } else {
      throw new Error(
        "This ExistDB version is still not implemented in this program"
      );
      return "";
    }
  }

  constructFunctionQuery(clause: any): string {
    const funcStr = this.astToFuncStr(clause);
    for (const pattern of this.supportedSelectionFunctions) {
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
      break;
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

    const queryChecked = await this.client.queries.read(queryCheck, {
      "omit-xml-declaration": "no",
      "insert-final-newline": "yes",
      limit: 999999999,
    });
    const doc = new dom().parseFromString(queryChecked);
    const nodes: any = xpath.select("/*/*", doc);
    if (nodes.length > 0) {
      let checkResult = {
        prefix: nodes[0].localName,
        namespace: nodes[0].firstChild.data,
      };
      this.spatialNamespace = checkResult;
    } else {
      throw new Error(
        "no spatial namespace found in the collection or extension type is not valid"
      );
    }
  }

  async getResult(
    collection: string | any[],
    where: string | any[],
    projection: string | any[],
    groupby: string | any[],
    columnAs?: any | undefined
  ): Promise<any> {
    if (!this.client) {
      await this.connect();
    }
    let result: any[] = [];
    // console.log("yes");

    // console.log(this.constructXQuery(collection, where, projection, columnAs));

    try {
      const query = await this.client.queries.read(
        this.constructXQuery(collection, where, projection, groupby, columnAs),
        {
          "omit-xml-declaration": "no",
          "insert-final-newline": "yes",
          limit: 999999999,
        }
      );
      console.log(Boolean(this.client));
      // console.log(query);

      const docResult = new dom().parseFromString(query);
      const nodesResult: any = xpath.select("/*/*", docResult);
      // console.log(nodesResult[0].toString());

      nodesResult.forEach((node: any) => {
        result.push(node.toString());
      });

      // console.log(result[0]);
    } catch (error) {
      throw Error(`getResult error : ${error}`);
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
    await this.connect();
    const data = await this.client.collections.read("/db/test");
    // console.log(data.collections);

    let listCollections: string[] = await data.collections;

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
    let result = `geo:${funcName}(${constant1}, *[*/@srsName]/*) ${operator} ${constant2}`;

    return result;
  }
  constructSpatialFunctionOneArgs(
    groups: { [key: string]: string },
    funcName: string
  ): string {
    const { fname, tname, colname, constant, operator } = groups as any;
    let result = `geo:${funcName}(*[*/@srsName]/*) ${operator} ${constant}`;

    return result;
  }
}
export { ExistDBExtension };
