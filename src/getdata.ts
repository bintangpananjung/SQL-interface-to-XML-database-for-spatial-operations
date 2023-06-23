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

  collections.forEach((col, idx) => {
    const { as } = col;

    const selectionQuery = driver.constructSelectionQuery(groupWhere[as]);
    // console.log(selectionQuery, "where");

    const columns = mapColumnsPerTable.has(as)
      ? mapColumnsPerTable.get(as)!
      : new Set<string>();

    const projectionQuery = driver.constructProjectionQuery(columns, col);
    if (driver.extensionType != "xml") {
      const result = driver.getResult(
        col.name,
        selectionQuery,
        projectionQuery
      );
      resultPromise.push(result);
    } else {
      selectionQueryList.push(selectionQuery);
      projectionQueryList.push(projectionQuery);
    }
  });
  if (driver.extensionType == "xml") {
    const result = driver.getResult(
      collections,
      selectionQueryList,
      projectionQueryList,
      columnAs
    );
    resultPromise.push(result);
  }

  const resultList = await Promise.all(resultPromise);
  let finalResult: any[] = [];
  let totalData = 0;

  for (let i = 0; i < resultList.length; i++) {
    let result = resultList[i];
    if (driver.extensionType != "xml") {
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
    totalData += result.length;
  }
  // console.log(finalResult[0].table, finalResult[0].as);

  return { finalResult, totalData };
}

export { getData };
