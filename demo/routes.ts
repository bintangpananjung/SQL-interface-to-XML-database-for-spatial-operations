import express from "express";
import path from "path";
import { CouchDbExtension } from "../extension/couchdb/couchdb_extension";
import { MongoExtension } from "../extension/mongodb/mongo_extension";
import { PostgisExtension } from "../src";
import { BaseXExtension } from "../extension/basex/basex_extension";
const view = path.join(__dirname, "./views/");
const publicDir = path.join(__dirname, "./public/");

const couch = new CouchDbExtension();
const couchGis = new PostgisExtension(couch);

const mongo = new MongoExtension();
const mongoGis = new PostgisExtension(mongo);

const basex = new BaseXExtension();
const basexGis = new PostgisExtension(basex);

const router = express.Router();
const title = "Sistem Perbaikan";
router.get("/", async (req, res) => {
  let dbms = "mongodb";
  if ("dbms" in req.query) {
    dbms = req.query.dbms as string;
  }
  const db = mongoGis.driver.getDbName(); // default
  const listCollections = await mongoGis.driver.getCollectionsName();
  res.render("index", {
    title,
    input: undefined,
    query: undefined,
    results: undefined,
    result_geojson: null,
    db: db,
    listCollections: listCollections,
    error: null,
    statistic: undefined,
    dbms,
  });
});

router.post("/", async (req, res) => {
  let { sql, dbms } = req.body;
  console.log(dbms);
  let gis;
  switch (dbms) {
    case "mongodb":
      gis = mongoGis;
      break;
    case "couchdb":
      gis = couchGis;
      break;
    case "basex":
      gis = basexGis;
      break;
    default:
      gis = mongoGis;
      break;
  }
  // const gis = dbms === "mongodb" ? mongoGis : couchGis;
  const db = gis.driver.getDbName();
  const listCollections = await gis.driver.getCollectionsName();
  let start = new Date().getTime();
  try {
    let results = await gis.processQuery(sql);
    const executionTime = new Date().getTime() - start;
    const totalfetchdata = 0;
    const totalfinalresultdata = results ? results?.finalResult.rows.length : 0;
    res.render("index", {
      title,
      input: sql,
      query: undefined,
      fields: results?.finalResult.fields,
      results: results?.finalResult.rows,
      result_geojson: results?.geoJsonResult,
      db,
      listCollections: listCollections,
      error: null,
      statistic: {
        executionTime,
        dbms,
        totalfetchdata: results?.totalData,
        totalfinalresultdata,
      },
      dbms,
      totalGetField: results?.totalGetField,
    });
  } catch (e) {
    res.render("index", {
      title,
      input: sql,
      query: undefined,
      db,
      listCollections: listCollections,
      error: e.message,
      results: undefined,
      result_geojson: null,
      dbms,
      statistic: undefined,
    });
  }
});

module.exports = router;
