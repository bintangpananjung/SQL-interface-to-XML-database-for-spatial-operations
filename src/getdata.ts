import { From, Select } from "flora-sql-parser";
import { Extension } from "../extension/extension";

async function getData(
  tree: Select,
  groupWhere: any,
  driver: Extension,
  mapColumnsPerTable: Map<string, Set<string>>
): Promise<any> {
  const collections = tree
    .from!.filter((val) => !val.expr)
    .map((val) => {
      const from = val as From;
      return {
        name: from.table,
        as: from.as as string,
      };
    }) as { name: string; as: string }[];

  if (collections.length == 0) {
    const result = {
      finalResult: [],
      totalData: 0
    }
    return new Promise((resolve) => resolve(result));
  }

  let resultPromise: any[] = [];
  collections.forEach((col) => {
    const { as } = col;
    const selectionQuery = driver.constructSelectionQuery(groupWhere[as]);
    const columns = mapColumnsPerTable.has(as) ? mapColumnsPerTable.get(as)! : new Set<string>();
    const projectionQuery = driver.constructProjectionQuery(columns);
    const result = driver.getResult(col.name, selectionQuery, projectionQuery);
    resultPromise.push(result);
  });

  const resultList = await Promise.all(resultPromise);
  let finalResult: any[] = [];
  let totalData = 0;
  for (let i = 0; i < resultList.length; i++) {
    const result = driver.standardizeData(resultList[i]);
    finalResult.push({
      table: collections[i].name,
      as: collections[i].as,
      result,
    });
    totalData += result.length
  }
  return { finalResult, totalData };
}

export { getData };
