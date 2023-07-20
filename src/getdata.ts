import { Column, ColumnRef, From, Select } from "flora-sql-parser";
import { Extension } from "../extension/extension";
import { XMLExtension } from "../extension/xml_extension";
import { proj_func_args_1 } from "./constant";

async function getData(
  tree: Select,
  groupWhere: any,
  driver: Extension,
  mapColumnsPerTable: Map<string, Set<string>>,
  collections: Array<any>,
  isGroupBySupported: boolean
): Promise<any> {
  const exectime = new Date().getTime();
  let columnAs: any = tree.columns;
  if (tree.columns != "*") {
    columnAs = new Map<string, Array<any>>();
    mapColumnsPerTable.forEach((val, table) => {
      const columnPerTable = [...val];
      columnPerTable
        .filter(el => !proj_func_args_1.exec(el))
        .forEach(el => {
          const as = (tree.columns as any[]).find(
            col =>
              col.expr.type == "column_ref" &&
              col.expr.table == table &&
              col.expr.column == el
          )?.as;
          if (!columnAs.has(table)) {
            columnAs.set(table, []);
          }
          if (el.includes("_undef__")) {
            let undefCol = el.split("__")[1];
            columnAs.get(table).push({
              as: `${undefCol}`,
              column: undefCol,
            });
          } else {
            columnAs.get(table).push({
              as: as ? as : el,
              column: el,
            });
          }
        });
    });
  }

  // console.log(columnAs);

  if (collections.length == 0) {
    const result = {
      finalResult: [],
      totalData: 0,
    };
    return new Promise(resolve => resolve(result));
  }
  let joinIsFullJoin = false;
  if (collections.length == 2) {
    joinIsFullJoin =
      collections.find(val => val.join && val.on).join == "FULL JOIN";
  }
  let resultPromise: any[] = [];
  // if (driver.supportPreExecutionQuery) {
  //   await driver.executePreExecutionQuery!(collections[0].name);
  // }
  // console.log(groupWhere);
  let selectionQueryList: any[] = [];
  let projectionQueryList: any[] = [];
  // let groupbyQueryList: any[] = [];

  collections.forEach((col, idx) => {
    const { as, name } = col;

    const selectionQuery = driver.constructSelectionQuery(groupWhere[as]);
    // console.log(selectionQuery, "where");

    const columns = mapColumnsPerTable.has(as)
      ? mapColumnsPerTable.get(as)!
      : new Set<string>();
    let groupbyQuery = "";
    if (driver.constructGroupByQuery && isGroupBySupported) {
      const groupbyCol = tree.groupby
        ?.filter(val => val.type == "column_ref" && val.table == as)
        .map(val => {
          const temp = { ...val };
          temp.table = name;
          return temp;
        });
      // console.log(groupbyCol, "groupbyCol");

      groupbyQuery = driver.constructGroupByQuery(groupbyCol, col);
    }

    const projectionQuery = driver.constructProjectionQuery(columns, col);
    if (!driver.canJoin || collections.length != 2 || joinIsFullJoin) {
      const result = driver.getResult(
        col.name,
        selectionQuery,
        projectionQuery,
        groupbyQuery
      );
      resultPromise.push(result);
    } else {
      selectionQueryList.push(selectionQuery);
      projectionQueryList.push(projectionQuery);
      // groupbyQueryList.push(groupbyQuery);
    }
  });
  let getResultTime = new Date().getTime();
  if (driver.canJoin && collections.length == 2 && !joinIsFullJoin) {
    let groupbyQuery = ``;
    if (driver.constructGroupByQuery && isGroupBySupported) {
      groupbyQuery = driver.constructGroupByQuery(
        tree.groupby
          ?.filter(val => val.type == "column_ref")
          .map(val => {
            const temp = { ...val };
            temp.table = collections.find(el => el.as == val.table).name;
            let tempCol = val.column;
            if (columnAs != "*") {
              const columnAsWithTable = (columnAs as Map<string, any[]>).get(
                val.table!
              );
              if (columnAsWithTable) {
                tempCol = columnAsWithTable.find(
                  el => el.column == val.column
                ).as;
              }
            }
            temp.column = tempCol;
            return temp;
          }),
        collections
      );
    }
    const result = driver.getResult(
      collections.map(val => {
        if (val.join && val.on && val.on.type == "binary_expr") {
          const temp = { ...val };
          temp.on.left.table = collections.find(
            el => el.as == val.on.left.table
          ).name;
          temp.on.right.table = collections.find(
            el => el.as == val.on.right.table
          ).name;
          return temp;
        }
        return val;
      }),
      selectionQueryList,
      projectionQueryList,
      groupbyQuery,
      columnAs
    );
    resultPromise.push(result);
  }
  // console.log(resultPromise);
  let resultList: any[] = [];
  console.log(new Date().getTime() - exectime, "pre2");
  if ((!driver.canJoin || joinIsFullJoin) && driver.extensionType == "xml") {
    for (const promise of resultPromise) {
      try {
        const res = await promise;
        if (!res) {
          return;
        }
        resultList.push(res);
      } catch (error) {
        console.log(error);
      }
    }
  } else {
    resultList = await Promise.all(resultPromise);
  }
  // console.log(resultList);

  console.log(
    `waktu pembangunan query dan eksekusi pada DBMS adalah ${
      new Date().getTime() - getResultTime
    }ms`
  );
  let exectime2 = new Date().getTime();
  let finalResult: any[] = [];
  let totalData = 0;

  for (let i = 0; i < resultList.length; i++) {
    let result = resultList[i];
    driver.totalRow.push(result.length);
    if (!driver.canJoin || joinIsFullJoin) {
      result = driver.standardizeData(resultList[i]);
      finalResult.push({
        table: collections[i].name,
        as: collections[i].as,
        result,
      });
    } else {
      let table = "";
      let as = "";
      collections.forEach((element, idx) => {
        table += element.name;
        as += element.as;
        if (idx < collections.length - 1) {
          table += "__";
          as += "__";
        }
      });
      finalResult.push({
        table: table,
        as: as,
        result,
      });
    }
    totalData += resultList.length;
  }
  console.log(new Date().getTime() - exectime2, "pre3");
  // console.log(finalResult[0].table, finalResult[0].as);

  return { finalResult, totalData };
}

export { getData };
