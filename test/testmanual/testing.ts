import { BaseXExtension } from "../../extension/basex/basex_extension";
import { ExistDBExtension } from "../../extension/existdb_extension/existdb_extension";
import { CouchDbExtension } from "./../../extension/couchdb/couchdb_extension";
import { MongoExtension } from "./../../extension/mongodb/mongo_extension";
import { PostgisExtension } from "./../../src";
let testcases = require("./testcasexml");

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

var fs = require("fs");
const asyncTest = async () => {
  const resultList = [];
  const existdb = new ExistDBExtension();

  const basex = new BaseXExtension();
  const dbmsGIS = new PostgisExtension(existdb);
  if (dbmsGIS.driver.extensionType == "xml") {
    await (dbmsGIS.driver as any).initVersion();
  }
  for (const testcase of testcases.testcaseKinerja) {
    console.log(testcase.id);

    const start = new Date();
    let res: any;
    try {
      res = await dbmsGIS.processQuery(testcase.query);
    } catch (e) {
      console.log("ass, error" + e.message);
    }
    const totalfinalresultdata = res ? res?.finalResult.rows.length : 0;
    const fields = [];
    for (const [key, value] of res!.totalGetField.entries()) {
      fields.push({
        col: key,
        sum: value.size,
      });
    }
    const executionTime = new Date().getTime() - start.getTime();
    const result = {
      id: testcase.id,
      executionTime,
      totaldata: res!.totalData,
      totalField: fields,
      dbms_executionTime: dbmsGIS.driver.executionTime[0],
      pg_executionTime: dbmsGIS.executionTime,
      totalRows: totalfinalresultdata,
      dbmsRows: dbmsGIS.driver.totalRow,
    };
    dbmsGIS.driver.executionTime = [];
    dbmsGIS.driver.totalRow = [];
    console.log(executionTime);
    resultList.push(result);
    await sleep(2000);
  }
  console.log(JSON.stringify(resultList, null, 2));
  fs.writeFileSync(
    `./test/testmanual/result/${new Date().getTime()}.json`,
    JSON.stringify(resultList, null, 2),
    { flag: "wx" }
  );
};

asyncTest();
