import { From, Select } from "flora-sql-parser";
import { Extension } from "../extension/extension";
import { XMLExtension } from "../extension/xml_extension";

async function getData(
  tree: Select,
  groupWhere: any,
  driver: Extension,
  mapColumnsPerTable: Map<string, Set<string>>
): Promise<any> {
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

  if (collections.length == 0) {
    const result = {
      finalResult: [],
      totalData: 0,
    };
    return new Promise(resolve => resolve(result));
  }
  let resultPromise: any[] = [];
  if (driver.extensionType == "xml") {
    await (driver as XMLExtension<any>).executeExtensionCheckQuery(
      collections[0].name
    );
  }
  // console.log(collections);
  let selectionQueryList: any[] = [];
  let projectionQueryList: any[] = [];
  // for (const [key, value] of mapColumnsPerTable) {
  //   console.log(key, value);
  // }

  collections.forEach((col, idx) => {
    // console.log(col);

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
      projectionQueryList
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
  // console.log(finalResult);

  return { finalResult, totalData };
}

export { getData };
