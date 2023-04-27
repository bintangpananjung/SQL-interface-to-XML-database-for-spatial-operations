import { Column, From } from "flora-sql-parser";
import { dropRight } from "lodash";
import { types } from "pg";
import { XMLNamespace, GeoJSON, Supported } from "./extension";

abstract class XMLExtension<T> implements XMLNamespace {
  abstract connect(): void;
  abstract getAllFields(col_name: string): Promise<string[]>;
  abstract getResult(
    collection: string,
    where: string,
    projection: string
  ): Promise<any>;
  abstract getDbName(): string;
  abstract standardizeData(data: any): GeoJSON[];
  abstract getCollectionsName(): Promise<string[]>;
  abstract constructFunctionQuery(clause: any): string;
  abstract constructProjectionQuery(columns: Set<string>): string;
  abstract supportedFunctions: RegExp[];
  abstract spatialModuleNamespaces: { prefix: string; namespace: string }[];
  abstract supportedExtensionCheck(collection: string): Promise<any>;
  abstract supportedXMLExtensionType: string[];

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
        const { left, right } = where;
        if (
          left.operator != operator &&
          (left.operator == "AND" || left.operator == "OR")
        ) {
          const { translation } = this.supportedOperators.find(
            ({ origin }) => origin === left.operator
          ) as Supported;
          resultLeft = `${resultLeft} ${translation} `;
        }
        if (
          right.operator != operator &&
          (right.operator == "AND" || right.operator == "OR")
        ) {
          const { translation } = this.supportedOperators.find(
            ({ origin }) => origin === right.operator
          ) as Supported;
          resultRight = `${resultRight} ${translation} `;
        }
        selection += resultLeft + resultRight;
        if (depth == 0) {
          const { translation } = this.supportedOperators.find(
            ({ origin }) => origin === where.operator
          ) as Supported;
          selection = `${resultLeft} ${translation} `;
        }

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
        selection += `${access_col}${column} ${translation} ${value} `;
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
    console.log(selection);

    return selection;
  }
}

export { XMLExtension };
