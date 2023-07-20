import { Column, ColumnRef, Select, util } from "flora-sql-parser";
import _, { join } from "lodash";
import { Extension } from "../extension/extension";

function rebuildWhere(clauses: any[]) {
  // console.log(clauses);

  if (clauses.length == 0) {
    return {
      type: "bool",
      value: true,
    };
  }

  const recursive = (clauses: any[], idx: number): any => {
    if (idx == clauses.length - 1) {
      return clauses[idx];
    }

    const right = recursive(clauses, idx + 1);

    return {
      type: "binary_expr",
      operator: "AND",
      left: clauses[idx],
      right,
    };
  };

  return recursive(clauses, 0);
}
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

function rebuildFromTree(
  tree: Select,
  data: any,
  columnNames: Set<string>,
  driver: Extension
) {
  const tableName = data.table;
  const tableas = data.as;
  const dataList = data.result as any[];

  if (dataList.length == 0) {
    const columns = [] as any[];
    if (columnNames.size == 0) {
      columns.push({
        expr: {
          type: "null",
          value: null,
        },
        as: "*",
      });
    } else {
      for (const column of columnNames) {
        columns.push({
          expr: {
            type: "null",
            value: null,
          },
          as: column,
        });
      }
    }
    return {
      expr: {
        type: "select",
        columns,
        where: {
          type: "bool",
          value: false,
        },
        parentheses: true,
      },
      as: tableas,
      lateral: false,
    };
  }

  let selectTree: any = {
    type: "select",
  };

  const defaultTree = (tree: Select) => {
    let value: any;
    let columnNameList: string[] = [];
    if (tree.columns == "*") {
    } else {
      const columns = tree.columns as Column[];
      let fromValues: any[] = [];
      for (const column of columns) {
        const columnName = column.expr as ColumnRef;
        fromValues.push({
          type: "string",
          value: columnName.column,
        });
        columnNameList.push(columnName.column);
      }
      value = [
        {
          type: "row_value",
          keyword: true,
          value: fromValues,
        },
      ];
    }
    let from = [
      {
        expr: {
          type: "values",
          value,
        },
        columns: columnNameList,
        as: tableas,
      },
    ];
    const where = {
      type: "bool",
      value: false,
    };

    let selectTree: any = {
      type: "select",
      columns: tree.columns,
      as: tableas,
      from,
      parentheses: true,
      where,
    };

    const finalResult = {
      expr: selectTree,
      as: tableas,
      lateral: false,
      columns: null,
    };
    return finalResult;
  };

  let listColumns: any[] = [];
  if (dataList.length == 0) {
    return defaultTree(tree);
  }
  const sample = dataList[0];

  // console.log(selectTree.columns);
  selectTree.columns = driver.addSelectTreeColumnsRebuild(sample, listColumns);

  let { columns, mapType } = driver.addColumnAndMapKeyRebuild(sample);
  // console.log(selectTree.columns, columns, mapType);
  let undefinedCol;
  if (tree.columns != "*") {
    undefinedCol = tree.columns.filter(
      val =>
        val.expr.type == "column_ref" && val.expr.column.includes("_undef__")
    );
    if (undefinedCol.length > 0) {
      const replaceCol = undefinedCol.map(el => ({
        prevCol: columns.find(val =>
          val.includes(el.expr.column.split("__")[1])
        ),
        replacedCol: el.expr.column,
      }));
      // console.log(replaceCol);

      // columns = columns.map(val => {
      //   const replacedCol = replaceCol.find(
      //     el => el.prevCol == val
      //   )?.replacedCol;
      //   if (replacedCol) {
      //     return replacedCol;
      //   }
      //   return val;
      // });
      columns = [...columns, ...replaceCol.map(el => el.replacedCol)];
      // selectTree.columns = selectTree.columns.map((val: any) => {
      //   const replacedCol = replaceCol.find(
      //     el => val.expr.type == "column_ref" && el.prevCol == val.expr.column
      //   )?.replacedCol;
      //   if (replacedCol) {
      //     const temp = { ...val };
      //     temp.expr.column = replacedCol;
      //     return temp;
      //   }
      //   return val;
      // });
      selectTree.columns = [
        ...selectTree.columns,
        ...replaceCol.map(el => ({
          expr: { type: "column_ref", table: null, column: el.replacedCol },
          as: null,
        })),
      ];
      replaceCol.forEach(element => {
        mapType[element.replacedCol] = mapType[element.prevCol];
        // delete mapType[element.prevCol];
      });
    }
  }
  // console.log(selectTree.columns, columns, mapType);

  let rows = driver.addRowValuesRebuild(dataList, columns, mapType);
  // console.log("asd");

  if (typeof sample === "object") {
    if (
      sample.hasOwnProperty("geometry") &&
      !columns.some(val => val == "geometry")
    ) {
      columns.push("geometry");
    }
  }
  // console.log(columns);

  selectTree.from = [
    {
      expr: {
        type: "values",
        value: rows,
      },
      as: tableName,
      columns,
    },
  ];
  selectTree.where = null;
  selectTree.parentheses = true;

  const oldFrom = tree.from?.find(val => val.as === tableas) as any;
  const finalResult = {
    expr: selectTree,
    as: tableas,
    lateral: false,
    columns: null,
    join: oldFrom?.join,
    on: oldFrom?.on,
  };
  // console.log(JSON.stringify(finalResult, null, 2));

  return finalResult;
}

function rebuildJoinedColumn(
  newTree: Select,
  joined: boolean,
  driver: Extension
) {
  const recursion = (column: any, as: any) => {
    if (column.type == "column_ref") {
      let tempColRef = column;
      tempColRef.table = newTree.from![0].as;
      tempColRef.column = as ? as : column.column;
      return tempColRef;
    }
    if (column.type == "function") {
      let tempCol = column;
      for (let i = 0; i < tempCol.args.value.length; i++) {
        tempCol.args.value[i] = recursion(tempCol.args.value[i], as);
      }
      return tempCol;
    }
    return column;
  };
  if (driver.canJoin && newTree.columns != "*" && joined) {
    // console.log(newTree.columns);

    return newTree.columns.map(val => ({
      expr: recursion(val.expr, val.as),
      as: val.as ? val.as : val.expr.column ? val.expr.column : null,
    }));
  }
  return newTree.columns;
}
function rebuildJoinedWhere(where: any, joinAs: string, driver: Extension) {
  const recursive = (where: any): any => {
    // console.log(where);
    if (!where.left && !where.right) {
      if (where.type === "column_ref") {
        let tempWhere = where;
        where.table = joinAs;
        return tempWhere;
      }
      if (where.type == "function") {
        where.args.value.forEach((element: any, idx: number) => {
          return recursive(element);
        });
      }
      return where;
    }

    where.right = recursive(where.right);
    where.left = recursive(where.left);

    return where;
  };
  if (driver.canJoin) {
    return recursive(where);
  }
  return where;
}

function rebuildFunctionColumns(
  columns: any[],
  funcColumns: any[],
  joined: boolean
) {
  if (funcColumns.length == 0) {
    return columns;
  }
  return columns.map((val: any) => {
    if (val.expr.type == "aggr_func") {
      const isAggrFunctionSupported = funcColumns.find(
        (el: any) =>
          el.func_name == val.expr.name.toLowerCase() &&
          el.table == val.expr.args.expr.table &&
          el.column == val.expr.args.expr.column
      );
      if (isAggrFunctionSupported) {
        return {
          expr: {
            type: "column_ref",
            table: joined ? null : val.expr.args.expr.table,
            column: `_func__${val.expr.name.toLowerCase()}__${
              val.expr.args.expr.column
            }`,
          },
          as: val.as,
        };
      }
    }
    if (val.expr.type == "function") {
      const isProjectionFunctionSupported = funcColumns.find(
        (el: any) =>
          val.expr.args.value.length == 1 &&
          el.func_name == val.expr.name.toLowerCase() &&
          el.table == val.expr.args.value[0].table &&
          el.column == val.expr.args.value[0].column
      );
      if (isProjectionFunctionSupported) {
        return {
          expr: {
            type: "column_ref",
            table: joined ? null : val.expr.args.value[0].table,
            column: `_func__${val.expr.name.toLowerCase()}__${
              val.expr.args.value[0].column
            }`,
          },
          as: val.as,
        };
      }
    }
    return val;
  });
}

function rebuildTree(
  tree: Select,
  dataList: any[],
  unsupportedClauses: any[],
  mapColumnsPerTable: Map<string, Set<string>>,
  funcColumns: any[],
  driver: Extension,
  isGroupBySupported: boolean
): Select {
  let exectime = new Date().getTime();
  const oldTree = _.cloneDeep(tree);
  const newTree = tree;
  newTree.from = newTree.from as any[];
  newTree.from = newTree.from.filter(val => {
    if (val.expr) {
      return true;
    }
    return false;
  });
  for (const data of dataList) {
    const columns = mapColumnsPerTable.has(data.as)
      ? mapColumnsPerTable.get(data.as)!
      : new Set<string>();
    // console.log(columns);

    const treePerFrom = rebuildFromTree(oldTree, data, columns, driver);
    newTree.from.push(treePerFrom);
  }

  const joined = isJoined(oldTree);

  newTree.where = rebuildJoinedWhere(
    rebuildWhere(unsupportedClauses),
    dataList[0].as,
    driver
  );
  // console.log(JSON.stringify(newTree.where, null, 2));

  newTree.columns = rebuildJoinedColumn(newTree, joined, driver);
  if (newTree.columns != "*") {
    newTree.columns = rebuildFunctionColumns(
      newTree.columns,
      funcColumns,
      joined
    );
  }
  if (driver.constructGroupByQuery && isGroupBySupported) {
    newTree.groupby = null;
  } else {
    if (
      newTree.groupby &&
      !isGroupBySupported &&
      newTree.columns != "*" &&
      joined &&
      oldTree.from?.length == 2
    ) {
      newTree.groupby = newTree.groupby.map(val => {
        const temp = { ...val };
        temp.table = null;
        const col = (oldTree.columns as Column[]).find(
          el => el.expr.type == "column_ref" && el.expr.column == val.column
        );
        temp.column = col?.as ? col.as : val.column;
        return temp as ColumnRef;
      });
    }
  }
  console.log(new Date().getTime() - exectime, "pre4");
  // console.log(newTree.columns, funcColumns);

  // console.log(JSON.stringify(newTree.columns, null, 2));

  // console.log(JSON.stringify(newTree, null, 2));

  return newTree;
}

function isJoined(oldTree: Select) {
  let joined = false;
  // console.log(oldTree.from);

  if (oldTree.from && Array.isArray(oldTree.from)) {
    oldTree.from.forEach(element => {
      // console.log(element);

      if (element.join && element.on && element.join != "FULL JOIN") {
        joined = true;
        return;
      }
    });
  }

  return joined;
}

export { rebuildWhere, rebuildTree, rebuildFromTree, doubleTheQuote };
