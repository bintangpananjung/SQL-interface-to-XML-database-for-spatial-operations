import { CouchDbExtension } from "./../../extension/couchdb/couchdb_extension";
import { MongoExtension } from "./../../extension/mongodb/mongo_extension";
import { PostgisExtension } from "./../../src";
let testcases = require("./testcase");

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  var fs = require('fs');
const asyncTest = async () => {
    const resultList = [];
    const mongo = new MongoExtension();
    
const couch = new CouchDbExtension();
    const mongoGis = new PostgisExtension(couch);
    for (const testcase of testcases) {
        console.log(testcase.id);
    
        const start = new Date();
        let res: any;
        try {
            res = await mongoGis.processQuery(testcase.query);
        } catch(e) {
            console.log("ass, error" + e.message);
        }
        const fields = [];
        for (const [key, value] of res!.totalGetField.entries()) {
            fields.push({
                col: key,
                sum: value.size
            })
        }
        const executionTime = new Date().getTime() - start.getTime();
        const result = {
            id: testcase.id,
            executionTime,
            totaldata : res!.totalData,
            totalField : fields,
        };
        console.log(executionTime)
        resultList.push(result);
        await sleep(15000);
    }
    console.log(JSON.stringify(resultList, null, 2));
    fs.writeFileSync(`./test/testmanual/result/${new Date().getTime()}.json`, JSON.stringify(resultList, null, 2), { flag: 'wx' });
}

asyncTest();