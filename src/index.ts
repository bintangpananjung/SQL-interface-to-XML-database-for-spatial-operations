import { util, Parser, Select, From } from "flora-sql-parser";
import _, { cond, find, map } from "lodash";
import { Extension } from "../extension/extension";
import Pool from "../pg-db";
import { buildAst, filterWhereStatement, fixAst } from "./preprocessing";
import { getData } from "./getdata";
import { rebuildTree } from "./sqlrebuilder";
import { XMLExtension } from "../extension/xml_extension";

class PostgisExtension {
  private parser: Parser;
  private _driver: Extension;
  private totalData: number;
  private totalGetField: Map<string, Set<string>>;

  get driver() {
    return this._driver;
  }

  constructor(driver: Extension) {
    this.parser = new Parser();
    this._driver = driver;
    this.totalData = 0;
    this.totalGetField = new Map();
  }

  convertToSQL(tree: Select): string {
    // console.log(JSON.stringify(tree, null, 2));

    let query = util.astToSQL(JSON.parse(JSON.stringify(tree)));
    query = query.replace(/\\r\\n/g, "");
    query = query.replace(/\\/g, "");
    query = query.replace(/`/g, "");
    query = query.replace(/^\s+|\s+$|\s+(?=\s)/g, "");
    return query;

    // console.log(query);

    // query = query.replace(/^\s+|\s+$|\s+(?=<)/g, "");
  }

  async finalresult(tree: Select) {
    let getResultTime = new Date().getTime();
    let query = this.convertToSQL(tree);
    console.log(
      `waktu eksekusi pada PostgreSQL adalah ${
        new Date().getTime() - getResultTime
      }ms`
    );

    // console.log(JSON.stringify(tree, null, 2));
    // console.log(query);

    query = query.replace(/ROW/g, "");
    let pgclient = await Pool.connect();
    let result = await pgclient.query(query);
    pgclient.release();
    return result;
  }

  async processSubQueryFrom(tree: Select): Promise<Select> {
    let froms = tree.from!;
    for (let i = 0; i < froms.length; i++) {
      if (froms[i].expr) {
        froms[i] = {
          expr: await this.processSelect(froms[i].expr),
          as: froms[i].as,
        };
      }
    }
    return tree;
  }

  async processSubQueryWhere(tree: Select): Promise<Select> {
    const subQueryInWhere = async (tree: Select) => {
      let sublocation;

      if (!tree.where) {
        return tree;
      }

      if (tree.where.type === "unary_expr") {
        sublocation = tree.where.expr.ast;
      } else if (tree.where.type === "binary_expr") {
        sublocation = tree.where.right.value;
      }

      if (!(sublocation instanceof Array)) {
        return tree;
      }

      let listItem = sublocation[0];

      if (_.has(listItem, "from")) {
        const treeResult = await this.processSelect(listItem);
        const whereResult = await this.finalresult(treeResult);
        const fields = whereResult.fields.map(v => v.name);
        const rows = whereResult.rows;
        let values = [];
        for (const row of rows) {
          let innerValue = [];
          for (const field of fields) {
            const value = row[field];
            let type: string;
            if (_.isString(value)) {
              type = "string";
            } else if (_.isBoolean(value)) {
              type = "bool";
            } else {
              type = "number";
            }
            innerValue.push({
              type,
              value,
            });
          }
          if (innerValue.length == 1) {
            values.push(innerValue[0]);
          } else {
            values.push({
              type: "expr_list",
              value: innerValue,
              paratheses: true,
            });
          }
        }
        if (tree.where.type === "unary_expr") {
          tree.where.expr.ast = values;
        } else if (tree.where.type === "binary_expr") {
          tree.where.right.value = values;
        }

        return tree;
      } else {
        return tree;
      }
    };
    await subQueryInWhere(tree);
    return tree;
  }

  getColumns = (
    tree: Select,
    clauses: any[]
  ): { mapColumns: Map<string, Set<string>>; funcColumns: any[] } => {
    const mapColumnsPerTable = new Map<string, Set<string>>();
    const tempFuncColumns: any[] = [];
    const recursive = (ast: any) => {
      if (
        ast == null ||
        ["boolean", "string", "number", "undefined", "null"].includes(
          typeof ast
        )
      ) {
        return;
      }
      if (ast.expr) {
        if (ast.expr.type !== "column_ref") {
          const functionSupported =
            this.driver.supportedProjectionFunctions.find(
              val =>
                val.postGISName == ast.expr.name.toLowerCase() && val.args == 1
            );
          let colref: any;
          if (ast.expr.type == "function" && ast.expr.args.value.length == 1) {
            colref = ast.expr.args.value[0];
          }
          if (ast.expr.type == "aggr_func") {
            colref = ast.expr.args.expr;
          }
          if (!mapColumnsPerTable.has(colref.table)) {
            mapColumnsPerTable.set(colref.table, new Set<string>());
          }
          if (functionSupported) {
            mapColumnsPerTable
              .get(colref.table)!
              .add(
                `${ast.expr.name.toLowerCase()}(${colref.table}.${
                  colref.column
                })`
              );
            tempFuncColumns.push({
              func_name: ast.expr.name.toLowerCase(),
              table: colref.table,
              column: colref.column,
            });
          } else {
            mapColumnsPerTable.get(colref.table)!.add(colref.column);
          }
          return;
        } else {
          const { table, column } = ast.expr;
          if (!mapColumnsPerTable.has(table)) {
            mapColumnsPerTable.set(table, new Set<string>());
          }
          mapColumnsPerTable.get(table)!.add(column);
        }

        for (const key in ast.expr) {
          recursive(ast[key]);
        }
      } else {
        if (ast.type == "column_ref") {
          const { table, column } = ast;
          if (!mapColumnsPerTable.has(table)) {
            mapColumnsPerTable.set(table, new Set<string>());
          }
          mapColumnsPerTable.get(table)!.add(column);
        }

        for (const key in ast) {
          recursive(ast[key]);
        }
      }
    };

    if (tree.columns == "*") {
      return { mapColumns: new Map<string, Set<string>>(), funcColumns: [] };
    }

    recursive(tree.columns);
    recursive(tree.from);
    for (const clause of clauses) {
      recursive(clause);
    }

    return { mapColumns: mapColumnsPerTable, funcColumns: tempFuncColumns };
  };

  async processSelect(tree: Select) {
    tree = fixAst(tree);
    console.log(JSON.stringify(tree, null, 2));

    tree = await this.processSubQueryFrom(tree);
    tree = await this.processSubQueryWhere(tree);
    const collections = tree
      .from!.filter(val => !val.expr)
      .map(val => {
        const from = val as From;
        return {
          name: from.table,
          as: from.as as string,
          join: (from as any).join,
          on: (from as any).on,
        };
      }) as { name: string; as: string }[];

    const { supportedClauses, unsupportedClauses } = filterWhereStatement(
      tree,
      this.driver,
      this.driver.canJoin && collections.length > 1
    );

    console.log(unsupportedClauses, "unsup", supportedClauses, "sup");

    const columns = this.getColumns(tree, unsupportedClauses);
    // console.log(columns, "columnsget");
    // console.log(supportedClauses, unsupportedClauses, "unsu");

    const { finalResult, totalData } = await getData(
      tree,
      supportedClauses,
      this.driver,
      columns.mapColumns,
      collections
    );
    this.totalData += totalData;
    // console.log(totalData);
    // console.log(finalResult.length);
    this.totalGetField = this._driver.getFieldsData(
      this.totalGetField,
      finalResult
    );
    // console.log(this.totalGetField);

    // console.log(JSON.stringify(finalResult, null, 2));

    if (finalResult.length == 0) {
      return tree;
    }
    return rebuildTree(
      tree,
      finalResult,
      unsupportedClauses,
      columns.mapColumns,
      columns.funcColumns,
      this._driver
    );
  }

  convertRestoGeoJSON(result: any) {
    let features = [] as any[];
    for (let idx = 0; idx < result.length; idx++) {
      let feature = {} as any;
      feature["type"] = "Feature";

      for (const key in result[idx]) {
        if (key !== "st_asgeojson") {
          feature["properties"] = {};
          feature["properties"][key] = result[idx][key];
        } else {
          feature["geometry"] = JSON.parse(result[idx][key]);
        }
      }

      features.push(feature);
    }

    let result_geojson = {
      type: "FeatureCollection",
      features: features,
    };

    return result_geojson;
  }

  async processQuery(sql: string) {
    this.totalData = 0;
    this.totalGetField = new Map<string, Set<string>>();
    if (sql != "") {
      let tree = buildAst(sql, this.parser);
      const newTree = await this.processSelect(tree);

      const finalResult = await this.finalresult(newTree);

      let geoJsonResult;
      if (_.has(finalResult.rows[0], "st_asgeojson")) {
        geoJsonResult = this.convertRestoGeoJSON(finalResult.rows);
      }
      return {
        finalResult,
        geoJsonResult,
        totalData: this.totalData,
        totalGetField: this.totalGetField,
      };
    }
  }
}

export { PostgisExtension };
