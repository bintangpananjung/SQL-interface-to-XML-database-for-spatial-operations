import { Column, From } from "flora-sql-parser";
import { dropRight, values } from "lodash";
import { types } from "pg";
import { XMLNamespace, GeoJSON, Supported } from "./extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";
import { doubleTheQuote } from "../src/sqlrebuilder";

abstract class XMLExtension<T> implements XMLNamespace {
  abstract connect(): void;
  abstract getAllFields(col_name: string): Promise<string[]>;
  abstract getResult(
    collection: string,
    where: string,
    projection: string
  ): Promise<any>;
  abstract getDbName(): string;
  abstract standardizeData(data: any): XMLDocument[];
  abstract getCollectionsName(): Promise<string[]>;
  abstract constructFunctionQuery(clause: any): string;
  abstract constructProjectionQuery(columns: Set<string>): string;
  abstract supportedFunctions: RegExp[];
  abstract spatialModuleNamespaces: { prefix: string; namespace: string }[];
  abstract supportedExtensionCheck(collection: string): Promise<any>;
  abstract supportedXMLExtensionType: string[];
  abstract supportedSpatialType: string[];
  abstract supportedFunctionPrefix: {
    name: string;
    args: number;
    postGISName: string;
  }[];
  getRowValuesRebuild(dataList: any[], columns: any[], mapType: any): any[] {
    let rows: any[] = [];

    for (const data of dataList) {
      let row: any = {
        type: "row_value",
        keyword: true,
        value: [],
      };
      const doc = new dom().parseFromString(
        data.replace(/^\s+|\s+$|\s+(?=<)/g, "")
      );

      for (const column of columns) {
        const nodes: any = xpath.select(`/result/${column}`, doc);

        if (nodes.length > 0) {
          const node: any = nodes[0];

          if (node.localName === "geometry") {
            // console.log(node.toString());

            row.value.push({
              type: "string",
              value: node.firstChild.data.toString(),
              // value: "a"
            });
          } else {
            if (node.firstChild) {
              let value = node.firstChild.data;
              // console.log(value, mapType[column]);

              if (mapType[column] === "string") {
                if (value === null) {
                  value = "";
                } else if (typeof value !== "string") {
                  value = value.toString();
                } else if (typeof value === "string") {
                  value = `${value.toString()}`;
                }
                if (value.includes("'")) {
                  value = doubleTheQuote(value);
                }
              }
              if (value == null) {
                value = 0;
              }
              row.value.push({
                type: mapType[column],
                value: value,
              });
            } else {
              row.value.push({
                type: mapType[column],
                value: "",
              });
            }
          }
        } else {
          row.value.push({
            type: mapType[column],
            value: null,
          });
        }
      }

      // else {
      //   for (const column of columns) {
      //     row.value.push({
      //       type: mapType[column],
      //       value: null,
      //     });
      //   }
      // }
      rows.push(row);
    }
    return rows;
  }
  addColumnAndMapKeyRebuild(sample: any): { columns: any[]; mapType: any } {
    let columns: any[] = [];
    let mapType = {} as any;
    const doc = new dom().parseFromString(sample);
    const nodes: any = xpath.select("/result/*", doc);

    nodes.forEach((value: any) => {
      columns.push(value.localName);
      if (!isNaN(parseFloat(value.firstChild.data))) {
        mapType[value.localName] = "number";
      } else if (typeof value.firstChild.data === "string") {
        mapType[value.localName] = "string";
      } else {
        mapType[value.localName] = "null";
      }
    });
    return { columns, mapType };
  }
  addSelectTreeColumnsRebuild(sample: any, listColumns: any[]) {
    const doc = new dom().parseFromString(sample);
    const nodes: any = xpath.select("/result/*", doc);
    nodes.forEach(
      (value: any) => {
        // if (value.localName == "geometry") {
        //   listColumns.push({
        //     expr: {
        //       type: "function",
        //       name: "ST_AsText",
        //       args: {
        //         type: "expr_list",
        //         value: [
        //           {
        //             type: "function",
        //             name: "ST_GeomFromGML",
        //             args: {
        //               type: "expr_list",
        //               value: [
        //                 {
        //                   type: "column_ref",
        //                   table: null,
        //                   column: "geometry",
        //                 },
        //               ],
        //             },
        //           },
        //         ],
        //       },
        //     },
        //     as: "geometry",
        //   });
        // } else {
        listColumns.push({
          expr: {
            type: "column_ref",
            table: null,
            column: value.localName,
          },
          as: null,
        });
      }
      // }
    );
    return listColumns;
  }
  getFieldsData(totalGetField: Map<string, Set<string>>, finalResult: any[]) {
    for (const countResult of finalResult) {
      const { result, as } = countResult as any;
      const sample = result[0];
      const fields = new Set<string>();
      const doc = new dom().parseFromString(sample);
      const nodes: any = xpath.select("/result/*", doc);
      nodes.forEach((node: any) => {
        fields.add(node.localName);
      });
      totalGetField.set(as, fields);
    }
    return totalGetField;
  }

  extensionType = "xml";
  supportedTypes = ["number", "string", "bool", "expr_list"];
  spatialNamespace = [
    { prefix: "gml", namespace: "http://www.opengis.net/gml" },
    { prefix: "kml", namespace: "" },
  ];
  supportedOperators = [
    { origin: "AND", translation: "and" },
    { origin: "OR", translation: "or" },
    { origin: "=", translation: "=" },
    { origin: "<", translation: "<" },
    { origin: ">", translation: ">" },
    { origin: "<=", translation: "<=" },
    { origin: ">=", translation: ">=" },
    { origin: "IS NOT", translation: "not()" },
    { origin: "IN", translation: "=" },
    { origin: "NOT IN", translation: "!=" },
    { origin: "!=", translation: "!=" },
  ];
  constructor(
    protected url: string,
    protected client: T | null,
    protected db_name: string,
    protected username: string | null,
    protected password: string | null
  ) {}

  constructSpatialNamespace(
    namespaces: { prefix: string; namespace: string }[],
    module: boolean
  ) {
    let namespaceQuery = "";
    namespaces.forEach(element => {
      if (module) {
        namespaceQuery += `import module `;
      } else {
        namespaceQuery += `declare `;
      }
      namespaceQuery += `namespace ${element.prefix} = "${element.namespace}"; `;
    });
    return namespaceQuery;
  }
  astToFuncStr(ast: any) {
    if (ast.type !== "binary_expr") {
      // Explore non binary func
      return "";
    }
    // Move function to left
    if (ast.left.type !== "function") {
      const temp = ast.left;
      ast.left = ast.right;
      ast.right = temp;
    }
    const recursive = (ast: any) => {
      if (ast.type === "column_ref") {
        return ast.table + "." + ast.column;
      }
      if (ast.type === "number") {
        return ast.value;
      }
      if (ast.type === "string") {
        return "'" + ast.value + "'";
      }
      let result = ast.name + "(";
      for (let idx = 0; idx < ast.args.value.length; idx++) {
        if (idx !== 0) {
          result += ", ";
        }
        result += recursive(ast.args.value[idx]);
      }
      result += ")";
      return result;
    };
    let result = recursive(ast.left);
    result += " " + ast.operator + " ";
    if (ast.right.type === "string") {
      result += "'" + ast.right.value + "'";
    } else {
      result += ast.right.value;
    }
    return result;
  }

  // // Melakukan construct MongoDB Query.
  constructSelectionQuery(where: any): string {
    if (!where) {
      return "";
    }

    const conditionalOperators = ["AND", "OR"];

    const recursion = (where: any, numOfOr: number, depth: number): string => {
      let selection = "";
      const { operator } = where;

      if (operator == null) {
        if (where.type == "bool") {
          return "";
        }
        return selection;
      }

      if (where.left.type === "function" || where.right.type === "function") {
        return this.constructFunctionQuery(where);
      }

      const {
        left: { column },
        right: { type, value },
      } = where;

      const newNumOfOr = where.operator == "OR" ? numOfOr + 1 : numOfOr;
      let resultLeft = recursion(where.left, newNumOfOr, depth + 1);
      let resultRight = recursion(where.right, newNumOfOr, depth + 1);

      // if case != and or
      if (conditionalOperators.includes(where.operator)) {
        // const { left, right } = where;
        // if (left.operator == "AND" || left.operator == "OR") {
        //   const { translation } = this.supportedOperators.find(
        //     ({ origin }) => origin === left.operator
        //   ) as Supported;
        //   resultLeft = ` ${translation} ${resultLeft}`;
        // }
        // if (right.operator == "AND" || right.operator == "OR") {
        //   const { translation } = this.supportedOperators.find(
        //     ({ origin }) => origin === right.operator
        //   ) as Supported;
        //   resultRight = `${resultRight}${translation}`;
        // }
        // selection += resultLeft + resultRight;
        const { translation } = this.supportedOperators.find(
          ({ origin }) => origin === where.operator
        ) as Supported;
        selection += `${resultLeft}${translation} ${resultRight}`;
        return selection;
      }

      const as = this.supportedOperators.find(
        ({ origin }) => origin === where.operator
      ) as Supported;
      const { translation } = this.supportedOperators.find(
        ({ origin }) => origin === where.operator
      ) as Supported;
      const access_col = "*:";
      if (type === "number") {
        selection += `${access_col}${column} ${translation} ${value} `;
      } else if (type === "string") {
        selection += `${access_col}${column} ${translation} '${value}' `;
      } else if (type === "null") {
        if (operator === "IS") {
          selection += `fn:exists(${access_col}${column}/text()) `;
        } else if (operator === "IS NOT") {
          selection += `not(fn:exists(${access_col}${column}/text()))`;
        }
      } else if (type === "expr_list") {
        selection += `{"properties.${column}": {"${translation}": [`;
        const values = value as any[];
        const lastVal = values.pop();
        for (const val of values) {
          if (val.type === "number") {
            selection += `${val.value}, `;
          } else {
            selection += `"${val.value}", `;
          }
        }
        if (lastVal.type === "number") {
          selection += `${lastVal.value}]}}`;
        } else {
          selection += `"${lastVal.value}"]}}`;
        }
      }

      return selection;
    };

    const selection = recursion(where, 0, 0);

    return selection;
  }
}

export { XMLExtension };
