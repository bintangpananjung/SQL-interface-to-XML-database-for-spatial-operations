import { Column, ColumnRef, Select, util } from "flora-sql-parser";
import _ from "lodash";
import { Extension } from "../extension/extension";

function rebuildWhere(clauses: any[]) {
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

  // function doubleTheQuote(str: string) {
  //   function getIndicesOf(searchStr: string, str: string) {
  //     var searchStrLen = searchStr.length;
  //     if (searchStrLen == 0) {
  //       return [];
  //     }
  //     var startIndex = 0,
  //       index,
  //       indices = [];

  //     while ((index = str.indexOf(searchStr, startIndex)) > -1) {
  //       indices.push(index);
  //       startIndex = index + searchStrLen;
  //     }
  //     return indices;
  //   }

  //   function insert(str: string, index: number, value: string) {
  //     return str.substr(0, index) + value + str.substr(index);
  //   }

  //   let quoteindex = getIndicesOf("'", str);

  //   for (let idx = quoteindex.length; idx > 0; idx--) {
  //     str = insert(str, quoteindex[idx - 1], "'");
  //   }

  //   return str;
  // }

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
  // if (sample.hasOwnProperty("properties")) {
  //   for (let [key, value] of Object.entries(sample.properties)) {
  //     listColumns.push({
  //       expr: {
  //         type: "column_ref",
  //         table: null,
  //         column: key,
  //       },
  //       as: null,
  //     });
  //   }
  // }

  // if (sample.hasOwnProperty("geometry")) {
  //   listColumns.push({
  //     expr: {
  //       type: "function",
  //       name: "ST_AsText",
  //       args: {
  //         type: "expr_list",
  //         value: [
  //           {
  //             type: "function",
  //             name: "ST_GeomFromGeoJSON",
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
  // }

  selectTree.columns = driver.addSelectTreeColumnsRebuild(sample, listColumns);

  // let columns: any[] = [];
  // let mapType = {} as any;
  // if (sample.hasOwnProperty("properties")) {
  //   for (let [key, value] of Object.entries(sample.properties)) {
  //     columns.push(key);
  //     if (typeof value === "string") {
  //       mapType[key] = "string";
  //     } else if (typeof value === "number") {
  //       mapType[key] = "number";
  //     } else {
  //       mapType[key] = "null";
  //     }
  //   }
  // }

  let { columns, mapType } = driver.addColumnAndMapKeyRebuild(sample);

  // let rows: any[] = [];
  // for (const data of dataList) {
  //   let row: any = {
  //     type: "row_value",
  //     keyword: true,
  //     value: [],
  //   };
  //   if (data.hasOwnProperty("properties")) {
  //     for (const column of columns) {
  //       const { properties } = data;
  //       if (properties.hasOwnProperty(column)) {
  //         let value = properties[column];
  //         if (mapType[column] === "string") {
  //           if (value === null) {
  //             value = "";
  //           } else if (typeof value !== "string") {
  //             value = value.toString();
  //           }
  //           if (value.includes("'")) {
  //             value = doubleTheQuote(value);
  //           }
  //         }
  //         if (value == null) {
  //           value = 0;
  //         }

  //         row.value.push({
  //           type: mapType[column],
  //           value: value,
  //         });
  //       } else {
  //         row.value.push({
  //           type: mapType[column],
  //           value: null,
  //         });
  //       }
  //     }
  //   } else {
  //     for (const column of columns) {
  //       row.value.push({
  //         type: mapType[column],
  //         value: null,
  //       });
  //     }
  //   }
  //   if (data.hasOwnProperty("geometry")) {
  //     row.value.push({
  //       type: "string",
  //       value: JSON.stringify(data.geometry),
  //       // value: "a"
  //     });
  //   }
  //   rows.push(row);
  // }
  let rows = driver.getRowValuesRebuild(dataList, columns, mapType);

  if (typeof sample === "object") {
    if (
      sample.hasOwnProperty("geometry") &&
      !columns.some(val => val == "geometry")
    ) {
      columns.push("geometry");
    }
  }
  console.log(columns);

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
    join: oldFrom.join,
    on: oldFrom.on,
  };

  return finalResult;
}

function rebuildTree(
  tree: Select,
  dataList: any[],
  unsupportedClauses: any[],
  mapColumnsPerTable: Map<string, Set<string>>,
  driver: Extension
): Select {
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
  // console.log(newTree.columns);

  newTree.where = rebuildWhere(unsupportedClauses);

  return newTree;
}

export { rebuildWhere, rebuildTree, rebuildFromTree, doubleTheQuote };
