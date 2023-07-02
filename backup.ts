import {
  Parser,
  AST,
  Select,
  From,
  TableColumnAst,
  Column,
} from "node-sql-parser";
import _ from "lodash";
import { MongoExtension } from "./extension/mongodb/mongo_extension";
import pgconnect from "./pg-db";
import { Pool, QueryResult } from "pg";
import { Extension } from "./extension/extension";
import { PgResult } from "./pgresult";

const parser = new Parser();

type Tree = AST | AST[];

interface FromExtend extends From {
  expr: TableColumnAstExtends;
}

interface TableColumnAstExtends extends TableColumnAst {
  parentheses: boolean;
}

class PostgisExtension {
  driver: Extension;
  pgconnect: Pool;

  constructor(driver: Extension) {
    this.driver = driver;
    this.pgconnect = pgconnect;
  }

  nullRemoval(tree: Select): Select {
    let newobj: any = {};
    const map: { [key: string]: any } = tree;
    for (const key in tree) {
      if (map[key] !== null) {
        newobj[key] = map[key];
      }
    }
    return newobj as Select;
  }

  getSubQuery(tree: Select) {
    let subqfrom: Select[] = [];
    let subqwhere: Select[] = [];

    const subQueryInFrom = (
      tree: Select,
      subquery: Select[],
      idx: number = 0
    ) => {
      if (tree == null) {
        return;
      }
      const from = tree.from![idx] as FromExtend;
      if (_.has(tree.from![idx], "expr")) {
        let sublocation = from.expr.ast as Select;

        if (sublocation instanceof Array) {
          sublocation = sublocation[0];
        }

        if (_.has(sublocation, "from")) {
          subQueryInFrom(sublocation, subquery);
          subquery.push(this.nullRemoval(sublocation));
          // from.expr.ast = subquery.length-1
        } else {
          return;
        }
      }
    };

    const subQueryInWhere = (tree: Select, subquery: Select[]) => {
      let sublocation;

      if (tree.where?.type === "unary_expr") {
        sublocation = tree.where.expr.ast;
      } else if (tree.where?.type === "binary_expr") {
        sublocation = tree.where.right.value;
      }

      if (sublocation instanceof Array) {
        sublocation = sublocation[0];
      }

      if (_.has(sublocation, "from")) {
        subQueryInWhere(sublocation, subquery);
        subquery.push(this.nullRemoval(sublocation));

        if (tree.where.type === "unary_expr") {
          tree.where.expr.ast = subquery.length - 1;
        } else if (tree.where.type === "binary_expr") {
          tree.where.right.value = subquery.length - 1;
        }
      } else {
        return;
      }
    };

    if (_.has(tree, "from")) {
      for (let idx = 0; idx < tree.from!.length; idx++)
        subQueryInFrom(tree, subqfrom, idx);
    }

    if (_.has(tree, "where")) {
      subQueryInWhere(tree, subqwhere);
    }

    return {
      subqfrom: subqfrom,
      subqwhere: subqwhere,
    };
  }

  isDontHaveSubQuery(subqfrom: Select[], subqwhere: Select[]) {
    return subqfrom.length === 0 && subqwhere.length === 0;
  }

  getTables(tree: Select): {
    tables: string[];
    alias: { [key: string]: string };
  } {
    // console.log(`Get tables for tree : ${tree}`);
    const froms = tree.from! as From[];
    if (froms[0].table != null) {
      let table: string[] = [];
      let alias: { [key: string]: string } = {};

      froms.forEach((element, index) => {
        table.push(element.table);
        if (element.as) {
          alias[element.as] = element.table;
        } else {
          alias[element.table] = element.table;
        }
        element.table = "table-idx " + index;
      }); //everytable in from

      return { tables: table, alias: alias };
    } else {
      return { tables: [], alias: {} };
    }
  }

  getWhereStatement(clause: any, table: string) {
    // console.log(`Get where statement, for clause : ${JSON.stringify(clause)} and table: ${table}`);
    let listwhere: any[] = [];
    let table_ids: any[] = [];
    let final = [];
    let arr_exist = false;
    let func_exist = false;
    let table_id: string | null;

    if (clause) {
      // console.log(`Process clause where : ${JSON.stringify(clause)}`);
      const recursion = (clause: any) => {
        // console.log(`Recursion clause WHERE: ${JSON.stringify(clause)}`);
        if (clause.left) {
          recursion(clause.left);
        }

        if (clause.right) {
          recursion(clause.right);
        }

        if (clause.expr) {
          recursion(clause.expr);
        }
        // TODO : Ini harus digeneralisir.
        switch (clause.type) {
          case "binary_expr":
            listwhere.push(clause.operator);
            break;
          case "unary_expr":
            listwhere.push(clause.operator);
            break;
          case "column_ref":
            let colTable: string;
            if (clause.table == null) {
              // console.log(`Clause.table is null, set to default value : ${table}`);
              colTable = table;
            } else {
              // console.log(`Clause.table is not null, clause.table : ${clause.table}`);
              colTable = clause.table;
            }
            table_ids.push(colTable);
            listwhere.push(clause.column);
            break;
          case "number":
            listwhere.push(clause.value);
            break;
          case "string":
            listwhere.push(clause.value);
            break;
          case "bool":
            listwhere.push(clause.value);
            break;
          case "function":
            func_exist = true;
            break;
          case "expr_list":
            arr_exist = true;
            break;
        }
      };

      recursion(clause);
      // console.log(`Table ids before filter: ${JSON.stringify(table_ids)}`)

      const onlyUnique = (value: number, index: number, self: any) => {
        return self.indexOf(value) === index;
      };

      table_ids = table_ids.filter(onlyUnique);

      // console.log(`Table ids : ${JSON.stringify(table_ids)}, list_where : ${listwhere}`);

      if (table_ids.length <= 1 && !func_exist && !arr_exist) {
        const iter =
          (listwhere.length -
            listwhere.filter(x => x === "AND" || x === "OR").length) /
          3;

        // console.log(`Number of iterator : ${iter}`);
        // console.log(JSON.stringify(listwhere));

        for (let i = 0; i < iter; i++) {
          final.push(listwhere.splice(0, 3));
          // console.log(`iter ke-${i}, indeks-0 = ${listwhere[0]}`)
          const len = listwhere.length;
          if (i == 0) {
            continue;
          }
          if (listwhere[len - 1] === "AND" || listwhere[len - 1] === "OR") {
            // console.log(listwhere[len - 1])
            final[i - 1].push(listwhere.splice(len - 1, 1)[0]);
          }
        }
        // console.log(`table_ids : ${table_ids}`);
        // console.log(`Final list where : ${final}`);
        table_id = table_ids[0];
      } else {
        // TODO : Return Yes if WHERE is valid
        final = [];
        table_id = null;
      }
    } else {
      final = [];
      table_id = null;
    }

    return { listwhere: final, table_id };
  }

  async reconstructTree(
    tree: Select,
    result: any[],
    tables: string[],
    table_id: string
  ) {
    // console.log(`Reconstruct tree. tree: ${JSON.stringify(tree)}, result : ${JSON.stringify(result)}, tables : ${tables}, table_id : ${table_id}`);
    // let newcolumn = {};
    let newcolumn: Promise<any[]> | any[] = [];
    let funccol: Select;

    function doubleTheQuote(str: string) {
      function getIndicesOf(searchStr: string, str: string) {
        var searchStrLen = searchStr.length;
        if (searchStrLen == 0) {
          return [];
        }
        var startIndex = 0,
          index,
          indices = [];

        while ((index = str.indexOf(searchStr, startIndex)) > -1) {
          indices.push(index);
          startIndex = index + searchStrLen;
        }
        return indices;
      }

      function insert(str: string, index: number, value: string) {
        return str.substr(0, index) + value + str.substr(index);
      }

      let quoteindex = getIndicesOf("'", str);

      for (let idx = quoteindex.length; idx > 0; idx--) {
        str = insert(str, quoteindex[idx - 1], "'");
      }

      return str;
    }

    function perResult(res_array: any[], newcol: any[]): any[] {
      // console.log(`Process per result, res_array : ${JSON.stringify(res_array)}, newcol: ${JSON.stringify(newcol)}`);
      for (let i = 0; i < res_array.length; i++) {
        let type: string;
        let columns: any[];
        let funccol: any;

        for (let idx = 0; idx < res_array[i].length; idx++) {
          // console.log(`Res result for idx ${idx} is = ${JSON.stringify(res_array[i])}`);
          type = "select";
          columns = [];

          if (res_array[i][idx].hasOwnProperty("properties")) {
            // console.log(`Res result indeks ${i} has own properties`);
            for (let [key, value] of Object.entries(
              res_array[i][idx].properties
            )) {
              if (typeof value === "string" && value.includes("'")) {
                value = doubleTheQuote(value);
              }

              // TODO : sebelumnya typeof nya = typeof value;
              columns.push({
                expr: {
                  type: typeof value,
                  value: value,
                },
                as: key.toLowerCase(),
              });
            }
          }

          // console.log(`After handle properties, column value : ${JSON.stringify(columns)}`);

          if (res_array[i][idx].hasOwnProperty("geometry")) {
            // console.log(`Res result indeks ${i} has own geometry`);
            let geocol = {
              type: "function",
              name: "ST_AsText",
              args: {
                type: "expr_list",
                value: [
                  {
                    type: "function",
                    name: "ST_GeomFromGeoJSON",
                    args: {
                      type: "expr_list",
                      value: [
                        {
                          type: "string",
                          value: JSON.stringify(res_array[i][idx].geometry),
                        },
                      ],
                    },
                  },
                ],
              },
            };

            columns.push({
              expr: geocol,
              as: "geometry",
            });
          }

          // console.log(`After handle geometries, column value : ${JSON.stringify(columns)}`);

          funccol = { type, columns };
          if (!Object.is(idx, res_array[i].length - 1)) {
            funccol = { _next: funccol };
          }
        }

        newcol.push(funccol);
      }

      return newcol;
    }

    newcolumn = perResult(result, newcolumn);
    // console.log(`New column : ${JSON.stringify(newcolumn)}`);
    let from = tree.from![0] as FromExtend;
    // console.log(`test from.table : ${from.table}`)

    for (let idx = 0; idx < tree.from!.length; idx++) {
      let from = tree.from![idx] as FromExtend;
      const idxtable = parseInt(from.table.split(" ")[1]);
      const ast = newcolumn[idxtable] as Select;
      const expr = { ast } as TableColumnAstExtends;
      from.expr = expr;

      // delete from.table;
      from.table = "";

      from.expr.parentheses = true;
      if (from.as === null) {
        from.as = tables[idx];
      }
    }

    if (table_id) {
      delete tree.where;
    }

    return tree;
  }

  async preProcessQuery(tree: Select) {
    // let select = getSelect(tree);
    // console.log(`Preprocess query with tree: ${JSON.stringify(tree)}`);

    let { tables, alias } = this.getTables(tree);

    // console.log(`Get tables and alias, table : ${JSON.stringify(tables)}, alias : ${JSON.stringify(alias)}`);

    let where = tree.where;
    let selection = "";
    // console.log(`Tree.from before getwherestatement : ${JSON.stringify(tree.from![0])}`);
    const from = tree.from![0] as From;
    let { listwhere, table_id } = this.getWhereStatement(
      where,
      from.as as string
    );
    // console.log(`Get where statement result : ${listwhere} and table_id ${table_id}`)

    if (listwhere) {
      let prop_upper: boolean = false;

      if (table_id != null) {
        const fields = await this.driver.getAllFields(alias[table_id]);
        // console.log(`Get all fields in collection, fields = ${fields}`)

        const isUpper = (str: string): boolean => {
          return str === str.toUpperCase();
        };

        prop_upper = isUpper(fields[0]);
      }

      selection = this.driver.constructSelectionQuery(
        listwhere
        // prop_upper
      );
      // console.log(`Construct selection result : ${JSON.stringify(selection)}`);
    }

    const result = await this.driver.getResult(
      tables as any,
      selection,
      alias as any,
      ""
      // table_id as string
    );
    // console.log(`Result : ${JSON.stringify(result)}`);

    return this.reconstructTree(tree, result, tables, table_id as string);
  }

  async convertToSQL(tree: Select) {
    let query = parser.sqlify(tree);

    query = query.replace(/\\/g, "");
    query = query.replace(/`/g, "");

    return query;
  }

  async finalresult(query: string): Promise<QueryResult<any>> {
    let pgclient = await this.pgconnect.connect();
    let result = await pgclient.query(query);
    pgclient.release();
    return result;
  }

  insertSubQueriesFrom(tree: Select, subqfrom: Select[]): Select {
    if (subqfrom.length > 0) {
      for (let idx = 0; idx < subqfrom.length; idx++) {
        const from = tree.from![idx] as FromExtend;
        from.expr.ast = subqfrom[idx];
      }
    }

    return tree;
  }

  insertSubQueriesWhere(tree: Select, subqwhere: Select[]): Select {
    function subQueriesWhere(tree: Select, subqwhere: Select[]) {
      let sublocation;

      if (_.has(tree, "where")) {
        if (tree.where.type === "unary_expr") {
          sublocation = tree.where.expr.ast;
        } else if (tree.where.type === "binary_expr") {
          sublocation = tree.where.right.value;
        }
      }

      if (typeof sublocation === "number") {
        if (tree.where.type === "unary_expr") {
          tree.where.expr.ast = subqwhere[sublocation];
          sublocation = tree.where.expr.ast;
          subQueriesWhere(sublocation[0], subqwhere);
        } else if (tree.where.type === "binary_expr") {
          tree.where.right.value = [subqwhere[sublocation]];
          sublocation = tree.where.right.value;
          subQueriesWhere(sublocation[0], subqwhere);
        }
      }
    }

    if (subqwhere.length > 0) {
      subQueriesWhere(tree, subqwhere);
    }

    return tree;
  }

  convertRestoGeoJSON(result: any[]): any {
    let features = [];
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

  fillAsFrom(tree: Select): Select {
    let froms = tree.from! as From[];
    for (let from of froms) {
      if (from.as == null) {
        from.as = from.table;
      }
    }
    return tree;
  }

  // New Filter where statement
  filterWhereStatement(clause: any, table: string) {
    // console.log(`Get where statement, for clause : ${JSON.stringify(clause)} and table: ${table}`);
    let listwhere: any[] = [];
    let table_ids: any[] = [];
    let final = [];
    let arr_exist = false;
    let func_exist = false;
    let table_id: string | null;

    if (clause) {
      // console.log(`Process clause where : ${JSON.stringify(clause)}`);
      const recursion = (clause: any) => {
        // console.log(`Recursion clause WHERE: ${JSON.stringify(clause)}`);
        if (clause.left) {
          recursion(clause.left);
        }

        if (clause.right) {
          recursion(clause.right);
        }

        if (clause.expr) {
          recursion(clause.expr);
        }
        // TODO : Ini harus digeneralisir.

        switch (clause.type) {
          case "binary_expr":
            listwhere.push(clause.operator);
            break;
          case "unary_expr":
            listwhere.push(clause.operator);
            break;
          case "column_ref":
            let colTable: string;
            if (clause.table == null) {
              // console.log(`Clause.table is null, set to default value : ${table}`);
              colTable = table;
            } else {
              // console.log(`Clause.table is not null, clause.table : ${clause.table}`);
              colTable = clause.table;
            }
            table_ids.push(colTable);
            listwhere.push(clause.column);
            break;
          case "number":
            listwhere.push(clause.value);
            break;
          case "string":
            listwhere.push(clause.value);
            break;
          case "bool":
            listwhere.push(clause.value);
            break;
          case "function":
            func_exist = true;
            break;
          case "expr_list":
            arr_exist = true;
            break;
        }
      };

      recursion(clause);
      // console.log(`Table ids before filter: ${JSON.stringify(table_ids)}`)

      const onlyUnique = (value: number, index: number, self: any) => {
        return self.indexOf(value) === index;
      };

      table_ids = table_ids.filter(onlyUnique);

      // console.log(`Table ids : ${JSON.stringify(table_ids)}, list_where : ${listwhere}`);

      if (table_ids.length <= 1 && !func_exist && !arr_exist) {
        const iter =
          (listwhere.length -
            listwhere.filter(x => x === "AND" || x === "OR").length) /
          3;

        // console.log(`Number of iterator : ${iter}`);
        // console.log(JSON.stringify(listwhere));

        for (let i = 0; i < iter; i++) {
          final.push(listwhere.splice(0, 3));
          // console.log(`iter ke-${i}, indeks-0 = ${listwhere[0]}`)
          const len = listwhere.length;
          if (i == 0) {
            continue;
          }
          if (listwhere[len - 1] === "AND" || listwhere[len - 1] === "OR") {
            // console.log(listwhere[len - 1])
            final[i - 1].push(listwhere.splice(len - 1, 1)[0]);
          }
        }
        // console.log(`table_ids : ${table_ids}`);
        // console.log(`Final list where : ${final}`);
        table_id = table_ids[0];
      } else {
        // TODO : Return Yes if WHERE is valid
        final = [];
        table_id = null;
      }
    } else {
      final = [];
      table_id = null;
    }

    return { listwhere: final, table_id };
  }

  async processQuery(sql: string) {
    await this.driver.connect();
    // console.log(`Process Query, sql : ${sql}`);
    let error = null;
    let query: string = "";
    let final: PgResult = {
      rows: [],
    };

    if (sql != "") {
      let tree = parser.astify(sql) as Select;
      // console.log(`Tree created : ${tree}`);
      tree = this.nullRemoval(tree);
      tree = this.fillAsFrom(tree);
      // console.log(`Tree query sql : ${JSON.stringify(tree)}`);
      let { subqfrom, subqwhere } = this.getSubQuery(tree);

      if (this.isDontHaveSubQuery(subqfrom, subqwhere)) {
        try {
          let finaltree = await this.preProcessQuery(tree);
          // console.log(`Final tree : ${JSON.stringify(finaltree)}`);
          query = await this.convertToSQL(finaltree);
          // console.log(query);

          final = await this.finalresult(query); // JANGAN LUPA AWAIT
          // console.log('SQL', (await final).rows)
        } catch (e) {
          error = e;
          console.error(e.stack);
        }
      }
    }
  }

  //   if (sql !== "") {
  //     // parser astify
  //     let tree = parser.astify(sql) as Select;
  //     // console.log(JSON.stringify(tree));
  //     // null removal
  //     tree = this.nullRemoval(tree) as Select;
  //     tree = this.fillAsFrom(tree);
  //     // console.log(`Tree query sql : ${JSON.stringify(tree)}`);
  //     // // Get SubQuery
  //     let { subqfrom, subqwhere } = this.getSubQuery(tree);

  //     // // Ga ada subquery
  //     if (this.isDontHaveSubQuery(subqfrom, subqwhere)) {
  //       // console.log(`Query sql dont have sub query, query sql : ${sql}`);
  //       try {
  //         let finaltree = await this.preProcessQuery(tree);
  //         // console.log(`Final tree : ${JSON.stringify(finaltree)}`);
  //         query = await this.convertToSQL(finaltree);
  //         // console.log(query);

  //         final = await this.finalresult(query); // JANGAN LUPA AWAIT
  //         // console.log('SQL', (await final).rows)
  //       } catch (e) {
  //         error = e;
  //         console.error(e.stack);
  //       }
  //     } else {

  //       try {
  //           let finaltree: Select = {} as Select;

  //           // Handle subquery from...
  //           if(subqfrom.length !== 0){
  //               for(let idx = 0; idx < subqfrom.length; idx++){
  //                   // melakukan preprocessing subqfrom....
  //                   subqfrom[idx] = await this.preProcessQuery(subqfrom[idx])
  //               }
  //               finaltree = await this.insertSubQueriesFrom(tree, subqfrom)
  //           }

  //           // Handle subquery where...
  //           if(subqwhere.length !== 0){
  //               let operator = ['IN', 'EXISTS', 'NOT EXISTS'] // Handle di file config
  //               for(let idx = 0; idx < subqwhere.length; idx++){
  //                   // Handle selain ÍN, EXISTS, dan NOT EXISTS
  //                   if(!operator.includes(subqwhere[idx].where.operator)){
  //                       subqwhere[idx] = await this.preProcessQuery(subqwhere[idx])
  //                   }
  //               }

  //               finaltree = await this.insertSubQueriesWhere(tree, subqwhere)
  //               finaltree = await this.preProcessQuery(finaltree)

  //           }

  //           query = await this.convertToSQL(finaltree)
  //           final = await this.finalresult(query);
  //           // console.log('SQL', (final).rows)
  //       } catch (e) {
  //           error = e
  //           console.error(e.stack)
  //       }
  //       }
  //       let listCollections = await this.driver.getCollections()

  //   if(typeof (query) === 'string'){
  //       if((query).length > 2147483648){
  //           // do something if query length exceed the maximum query length PostgreSQL can handle
  //       }
  //   }

  //   let result_geojson = null

  //   if(_.has(final.rows[0], 'st_asgeojson')){
  //       result_geojson = this.convertRestoGeoJSON(final.rows)
  //   }

  //   return {error: error, query: query, results: (final).rows, result_geojson: result_geojson, db: this.driver.getDbName(), listCollections: listCollections};
  //   }
  // }
}

const db = new MongoExtension();
const test = new PostgisExtension(db);
test.processQuery("select name from test where id is null");
