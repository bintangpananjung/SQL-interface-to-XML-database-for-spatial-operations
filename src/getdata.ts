import { Column, ColumnRef, From, Select } from "flora-sql-parser";
import { Extension } from "../extension/extension";
import { XMLExtension } from "../extension/xml_extension";

async function getData(
  tree: Select,
  groupWhere: any,
  driver: Extension,
  mapColumnsPerTable: Map<string, Set<string>>,
  collections: Array<any>
): Promise<any> {
  let columnAs: any = tree.columns;
  if (tree.columns != "*") {
    columnAs = new Map<string, Array<any>>();
    tree.columns.forEach((val: Column) => {
      if (val.expr.type == "column_ref") {
        const table = tree.from?.find(
          el => el.as == (val.expr as ColumnRef).table
        ).table;
        if (!columnAs.has(table)) {
          columnAs.set(table, []);
        }
        columnAs.get(table)!.push({
          as: val.as ? val.as : val.expr.column,
          column: val.expr.column,
        });
      }
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

  let resultPromise: any[] = [];
  if (driver.supportPreExecutionQuery) {
    await driver.executePreExecutionQuery!(collections[0].name);
  }
  // console.log(groupWhere);
  let selectionQueryList: any[] = [];
  let projectionQueryList: any[] = [];
  // let groupbyQueryList: any[] = [];

  collections.forEach((col, idx) => {
    const { as } = col;

    const selectionQuery = driver.constructSelectionQuery(groupWhere[as]);
    // console.log(selectionQuery, "where");

    const columns = mapColumnsPerTable.has(as)
      ? mapColumnsPerTable.get(as)!
      : new Set<string>();
    let groupbyQuery = "";
    if (driver.constructGroupByQuery) {
      const groupbyCol = tree.groupby?.filter(
        val => val.type == "column_ref" && val.table == as
      );
      // console.log(groupbyCol, "groupbyCol");

      groupbyQuery = driver.constructGroupByQuery(groupbyCol, col);
    }

    const projectionQuery = driver.constructProjectionQuery(columns, col);
    if (!driver.canJoin || collections.length != 2) {
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
  if (driver.canJoin && collections.length == 2) {
    let groupbyQuery = ``;
    if (driver.constructGroupByQuery) {
      groupbyQuery = driver.constructGroupByQuery(
        tree.groupby?.filter(val => val.type == "column_ref"),
        collections
      );
    }
    const result = driver.getResult(
      collections,
      selectionQueryList,
      projectionQueryList,
      groupbyQuery,
      columnAs
    );
    resultPromise.push(result);
  }
  // console.log(resultPromise);
  let resultList: any[] = [];

  if (!driver.canJoin && driver.extensionType == "xml") {
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
  console.log(resultList);

  console.log(
    `waktu pembangunan query dan eksekusi pada DBMS adalah ${
      new Date().getTime() - getResultTime
    }ms`
  );

  let finalResult: any[] = [];
  let totalData = 0;

  for (let i = 0; i < resultList.length; i++) {
    let result = resultList[i];
    if (!driver.canJoin) {
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
  // console.log(finalResult[0].table, finalResult[0].as);

  return { finalResult, totalData };
}

export { getData };
