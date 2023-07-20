import { Column, From } from "flora-sql-parser";
import _, { dropRight, isArray, subtract, template, values } from "lodash";
import { types } from "pg";
import { XMLInterface, GeoJSON, Supported, XMLConfig } from "./extension";
import { DOMParserImpl as dom } from "xmldom-ts";
import * as xpath from "xpath-ts";
import { doubleTheQuote } from "../src/sqlrebuilder";
import { proj_func_args_1 } from "../src/constant";
import e from "express";

abstract class XMLExtension<T> implements XMLInterface {
  abstract supportedSelectionFunctions: {
    regex: RegExp;
    matches: string[];
    version?: string[];
  }[];
  abstract totalRow: number[];
  abstract executionTime: number[];
  abstract spatialModuleNamespaces: any[];
  abstract supportedXMLExtensionType: string[];
  abstract supportedSpatialType: { extType: string; types: string[] }[];
  abstract version: XMLConfig;
  abstract moduleConfig: XMLConfig[];
  abstract spatialNamespace: { prefix: string; namespace: string };
  abstract supportPreExecutionQuery: boolean;
  abstract canJoin: boolean;
  abstract supportedProjectionFunctions: {
    regex: RegExp;
    name: string;
    args: number;
    postGISName: string;
    isAggregation: boolean;
  }[];

  abstract connect(): void;
  abstract getAllFields(col_name: string): Promise<string[]>;
  abstract executePreExecutionQuery(collection: string): Promise<void>;
  abstract getResult(
    collection: string | any[],
    where: string | any[],
    projection: string | any[],
    groupby: string | any[],
    columnAs?: any | undefined
  ): Promise<any>;
  abstract getDbName(): string;
  abstract standardizeData(data: any): XMLDocument[];
  abstract getCollectionsName(): Promise<string[]>;
  abstract constructFunctionQuery(clause: any): string;
  abstract initVersion(): any;
  // abstract executeExtensionCheckQuery(collection: string): Promise<void>;
  extensionType = "xml";
  supportedTypes = ["number", "string", "bool", "expr_list"];
  // spatialNamespace = [
  //   { prefix: "gml", namespace: "http://www.opengis.net/gml" },
  //   { prefix: "kml", namespace: "" },
  // ];
  supportedOperators = [
    { origin: "AND", translation: "and" },
    { origin: "OR", translation: "or" },
    { origin: "=", translation: "=" },
    { origin: "<", translation: "<" },
    { origin: ">", translation: ">" },
    { origin: "<=", translation: "<=" },
    { origin: ">=", translation: ">=" },
    { origin: "IS NOT", translation: "not()" },
    { origin: "IN", translation: "=" },
    { origin: "NOT IN", translation: "!=" },
    { origin: "!=", translation: "!=" },
  ];
  constructor(
    protected url: string,
    protected client: T | null,
    protected db_name: string,
    protected username: string | null,
    protected password: string | null
  ) {}

  supportedExtensionCheck(collection: string): string {
    let extensionsArray = "(";
    this.supportedXMLExtensionType.forEach((element, idx) => {
      extensionsArray += `'${element}'`;
      if (idx != this.supportedXMLExtensionType.length - 1) {
        extensionsArray += ",";
      }
    });
    extensionsArray += ")";

    let queryCheck = `for $i in ${extensionsArray} 
    let $doc := ${this.version.getDocFunc(collection, this.db_name)}/*
    let $namespace := fn:namespace-uri-for-prefix($i, $doc)
    return 
      if(fn:exists($namespace)) then (element {$i} {$namespace})
      else (
        for $prefix in fn:in-scope-prefixes($doc)[. eq '']
        return if(name($doc)=$i) then(element {name($doc)} {fn:namespace-uri-for-prefix($prefix, $doc)}) else()
      )
      `;

    // if($prefix='' and $name($doc)=$i) then(element {name($doc)} {fn:namespace-uri-for-prefix($prefix, $doc) }) else()
    return queryCheck;
  }
  addRowValuesRebuild(dataList: any[], columns: any[], mapType: any): any[] {
    let rows: any[] = [];
    for (const data of dataList) {
      let row: any = {
        type: "row_value",
        keyword: true,
        value: [],
      };
      if (typeof data !== "object") {
        const doc = new dom().parseFromString(
          data.replace(/^\s+|\s+$|\s+(?=<)/g, "")
        );

        for (const column of columns) {
          let path = column;
          let nodes: any = xpath.select(`/result/${path}`, doc);
          if (nodes.length == 0) {
            if (column.includes("_undef__")) {
              path = `*[@group='${column.split("__")[1]}'][1]`;
              nodes = xpath.select(`/result/${path}`, doc);
            }
          }

          if (nodes.length > 0) {
            const node: any = nodes[0];
            const geotype = this.supportedSpatialType
              .find(val => val.extType == this.spatialNamespace.prefix)
              ?.types.find(el => el == node.firstChild?.localName);

            if (node.localName === "geometry" || geotype) {
              if (node.firstChild) {
                row.value.push({
                  type: "string",
                  value: node.firstChild.data
                    ? node.firstChild.data.toString()
                    : node.firstChild.toString(),
                  // value: "a"
                });
              } else {
                row.value.push({
                  type: "string",
                  value: "",
                });
              }
            } else {
              if (node.firstChild) {
                let value = node.firstChild.data
                  ? node.firstChild.data
                  : node.childNodes.toString();
                // console.log(value, mapType[column]);

                if (mapType[column] === "string") {
                  if (value === null) {
                    value = "";
                  } else if (typeof value !== "string") {
                    value = value.toString();
                  } else if (typeof value === "string") {
                    value = `${value.toString()}`;
                  }
                  if (value.includes("'")) {
                    value = doubleTheQuote(value);
                  }
                }
                if (value == null) {
                  value = 0;
                }
                row.value.push({
                  type: mapType[column],
                  value: value,
                });
              } else {
                row.value.push({
                  type: "null",
                  value: null,
                });
              }
            }
          } else {
            row.value.push({
              type: "null",
              value: null,
            });
          }
        }
      } else {
        for (const column of columns) {
          if (data.hasOwnProperty(column)) {
            if (column == "geometry") {
              row.value.push({
                type: "string",
                value: JSON.stringify(data.geometry),
                // value: "a"
              });
            } else {
              let value = data[column];
              if (mapType[column] === "string") {
                if (value === null) {
                  value = "";
                } else if (typeof value !== "string") {
                  value = value.toString();
                }
                if (value.includes("'")) {
                  value = doubleTheQuote(value);
                }
              }
              if (value == null) {
                value = 0;
              }

              row.value.push({
                type: mapType[column],
                value: value,
              });
            }
          } else {
            row.value.push({
              type: null,
              value: null,
            });
          }
        }
        // if (data.hasOwnProperty("geometry")) {
        //   row.value.push({
        //     type: "string",
        //     value: JSON.stringify(data.geometry),
        //     // value: "a"
        //   });
        // }
      }

      rows.push(row);
    }
    return rows;
  }
  addColumnAndMapKeyRebuild(sample: any): { columns: any[]; mapType: any } {
    let columns: any[] = [];
    let mapType = {} as any;

    if (typeof sample === "object") {
      for (let [key, value] of Object.entries(sample)) {
        columns.push(key);
        if (typeof value === "string") {
          mapType[key] = "string";
        } else if (typeof value === "number") {
          mapType[key] = "number";
        } else {
          mapType[key] = "null";
        }
      }
    } else {
      const doc = new dom().parseFromString(sample);
      const nodes: any = xpath.select("/result/*", doc);

      nodes.forEach((value: any) => {
        columns.push(value.localName);
        // console.log(value.firstChild.data);
        if (value.firstChild) {
          if (!isNaN(parseFloat(value.firstChild?.data))) {
            mapType[value.localName] = "number";
          } else if (typeof value.firstChild?.data === "string") {
            mapType[value.localName] = "string";
          } else {
            mapType[value.localName] = "null";
          }
        } else {
          mapType[value.localName] = "null";
        }
      });
    }
    // columns = [...new Set(columns)];
    return { columns, mapType };
  }
  addSelectTreeColumnsRebuild(sample: any, listColumns: any[]) {
    const moduleVersion = this.version.modules.find(
      val => val.extension === this.spatialNamespace.prefix
    );
    if (typeof sample === "object") {
      for (let [key, value] of Object.entries(sample)) {
        if (key === "geometry") {
          if (!moduleVersion || !moduleVersion.getSTAsTextfunc) {
            listColumns.push({
              expr: {
                type: "function",
                name: "ST_AsText",
                args: {
                  type: "expr_list",
                  value: [
                    {
                      type: "function",
                      name:
                        this.spatialNamespace.prefix == "gml"
                          ? "ST_GeomFromGML"
                          : "ST_GeomFromKML",
                      args: {
                        type: "expr_list",
                        value: [
                          {
                            type: "column_ref",
                            table: null,
                            column: "geometry",
                          },
                        ],
                      },
                    },
                  ],
                },
              },
              as: "geometry",
            });
          } else {
            listColumns.push({
              expr: {
                type: "column_ref",
                table: null,
                column: key,
              },
              as: null,
            });
          }
        } else {
          listColumns.push({
            expr: {
              type: "column_ref",
              table: null,
              column: key,
            },
            as: null,
          });
        }
      }
    } else {
      const doc = new dom().parseFromString(sample);
      const nodes: any = xpath.select("/result/*", doc);
      nodes.forEach((value: any) => {
        const geotype = this.supportedSpatialType
          .find(val => val.extType == this.spatialNamespace.prefix)
          ?.types.find(el => el == value.firstChild?.localName);
        if (value.localName == "geometry" || geotype) {
          if (!moduleVersion || !moduleVersion.getSTAsTextfunc) {
            listColumns.push({
              expr: {
                type: "function",
                name: "ST_AsText",
                args: {
                  type: "expr_list",
                  value: [
                    {
                      type: "function",
                      name:
                        this.spatialNamespace.prefix == "gml"
                          ? "ST_GeomFromGML"
                          : "ST_GeomFromKML",
                      args: {
                        type: "expr_list",
                        value: [
                          {
                            type: "column_ref",
                            table: null,
                            column: value.localName,
                          },
                        ],
                      },
                    },
                  ],
                },
              },
              as: value.localName,
            });
          } else {
            listColumns.push({
              expr: {
                type: "column_ref",
                table: null,
                column: value.localName,
              },
              as: null,
            });
          }
        } else {
          listColumns.push({
            expr: {
              type: "column_ref",
              table: null,
              column: value.localName,
            },
            as: null,
          });
        }
      });
    }

    return listColumns;
  }
  getFieldsData(totalGetField: Map<string, Set<string>>, finalResult: any[]) {
    for (const countResult of finalResult) {
      const { result, as } = countResult as any;
      const sample = result[0];
      const fields = new Set<string>();
      if (typeof sample === "object") {
        for (const prop in sample) {
          fields.add(prop);
        }
      } else {
        const doc = new dom().parseFromString(sample);
        const nodes: any = xpath.select("/result/*", doc);
        nodes.forEach((node: any) => {
          fields.add(node.localName);
        });
      }
      totalGetField.set(as, fields);
    }
    return totalGetField;
  }

  constructSpatialNamespace(
    namespaces: { prefix: string; namespace: string }[],
    module: boolean
  ) {
    let namespaceQuery = "";
    namespaces.forEach(element => {
      if (module) {
        namespaceQuery += `import module `;
      } else {
        namespaceQuery += `declare `;
      }
      namespaceQuery += `namespace ${element.prefix} = "${element.namespace}"; `;
    });
    return namespaceQuery;
  }
  astToFuncStr(ast: any) {
    if (ast.type !== "binary_expr") {
      // Explore non binary func
      return "";
    }
    // Move function to left
    if (ast.left.type !== "function") {
      const temp = ast.left;
      ast.left = ast.right;
      ast.right = temp;
    }
    const recursive = (ast: any) => {
      if (ast.type === "column_ref") {
        return ast.table + "." + ast.column;
      }
      if (ast.type === "number") {
        return ast.value;
      }
      if (ast.type === "string") {
        return "'" + ast.value + "'";
      }
      let result = ast.name + "(";
      for (let idx = 0; idx < ast.args.value.length; idx++) {
        if (idx !== 0) {
          result += ", ";
        }
        result += recursive(ast.args.value[idx]);
      }
      result += ")";
      return result;
    };
    let result = recursive(ast.left);
    result += " " + ast.operator + " ";
    if (ast.right.type === "string") {
      result += "'" + ast.right.value + "'";
    } else {
      result += ast.right.value;
    }
    return result;
  }
  constructExtensionQuery(
    extension: any,
    collection: string,
    moduleVersion: any,
    projection?: any
  ): {
    path: string;
    spatialTypeSelection: string;
    retrieveNestedDataCondition: string;
    retrieveCustomDataConditionWithAttr: string;
  } {
    const result = {
      path: "",
      spatialTypeSelection: "",
      retrieveNestedDataCondition: "",
      retrieveCustomDataConditionWithAttr: "",
    };
    let spatialTypes = this.supportedSpatialType.find(element => {
      return element.extType == extension;
    });
    let tempSpatialTypes: any[] = [];
    spatialTypes?.types.forEach(element => {
      tempSpatialTypes.push(`local-name()='${element}'`);
    });
    switch (extension) {
      case "gml":
        result.path = "gml:featureMember/*";
        result.spatialTypeSelection = `if(fn:exists($${collection}j/*[${tempSpatialTypes.join(
          " or "
        )}])) then (element {'geometry'} {
          attribute{'order'}{1},
          attribute{'group'}{'geometry'},
          ${
            moduleVersion?.getSTAsTextfunc
              ? moduleVersion.getSTAsTextfunc(`$${collection}j/*`)
              : `$${collection}j/*`
          }}
        )`;
        result.retrieveNestedDataCondition = `
          return element{concat($nestedcollection/local-name(),'__',$col/local-name())}{
            if(count($col/*)>0) then($col/*) else($col/text())
          }
          `;
        result.retrieveCustomDataConditionWithAttr = `
        else (
          if(not($${collection}j instance of attribute()))
          then(
            for $${collection}k in ${projection.childColumns}
            return if(not($${collection}k instance of attribute()))
            then(
                element{$${collection}k/local-name()}{
                attribute{'order'}{1},
                attribute{'group'}{$${collection}k/local-name()},
                if(count($${collection}k/*)>0)
                then($${collection}k/*)
                else($${collection}k/text())
              }
            )
            else(
              element{concat('_attribute__',$${collection}j/local-name(),'__',$${collection}k/local-name())}{
                attribute{'order'}{3},
                attribute{'group'}{$${collection}k/local-name()},
                $${collection}k/data()
              }
            )
          )
          else(
            element{concat('_attribute__',$${collection}j/local-name())}{
              attribute{'order'}{2},
              attribute{'group'}{$${collection}j/local-name()},
              $${collection}j/data()
            }
          )
        )
        `;
        break;
      case "kml":
        result.path = "kml:Placemark";
        result.spatialTypeSelection = `if(fn:exists($${collection}j[${tempSpatialTypes.join(
          " or "
        )}]))
        then (element {'geometry'} {
          attribute{'order'}{1},
          attribute{'group'}{'geometry'},
          ${
            moduleVersion?.getSTAsTextfunc
              ? moduleVersion.getSTAsTextfunc(`$${collection}j`)
              : `$${collection}j`
          }}
        )`;
        result.retrieveNestedDataCondition = `
        else if($${collection}j/local-name()='ExtendedData') then(
          for $extendeddata in $${collection}j/*/${projection.extendedColumns}
          return 
            for $${collection}k in ${projection.childColumns}
            return if(not($${collection}k instance of attribute()))
            then(
                element{$${collection}k/@name}
                {
                  element{$${collection}k/local-name()}{
                  if(count($${collection}k/*)>0)
                  then($${collection}k/*)
                  else($${collection}k/text())
                }
            )
            else(
              element{concat('_attribute__',$extendeddata/@name,'__',$${collection}k/local-name())}{$${collection}k/data()}
            )
      )
      else(
      )`;
        result.retrieveCustomDataConditionWithAttr = `
        else if($${collection}j/local-name()='ExtendedData') then(
          for $extendeddata in $${collection}j/*/${projection.extendedColumns}
          return 
            for $${collection}k in ${projection.childColumns}
            return if(not($${collection}k instance of attribute()))
            then(
                element{$${collection}k/@name}
                {
                  attribute{'order'}{1},
                  attribute{'group'}{$${collection}k/@name},
                  if(count($${collection}k/*)>0)
                  then($${collection}k/*)
                  else($${collection}k/text())
                }
            )
            else(
              element{concat('_attribute__',$extendeddata/@name,'__',$${collection}k/local-name())}{
                attribute{'order'}{3},
                attribute{'group'}{$${collection}k/local-name()},
                $${collection}k/data()
              }
            )
      )
      else(
      )`;
        break;
      default:
        break;
    }
    return result;
  }
  constructJoinQuery(
    result: string,
    collection: any[],
    where: any[],
    projection: any[],
    groupby: any,
    columnAs: any,
    constructTableQuery: any,
    constructNestedTableQuery: any,
    buildNestedCollectionQuery: any,
    moduleVersion: any
  ): string {
    const constructNoMatchedProjection = (elements: string[]) => {
      return elements.map(val => `exists(${val})`).join(" or ");
    };
    let whereQuery: any[] = [];
    where.forEach((element, idx) => {
      whereQuery.push({
        root: element.root.length > 0 ? `[${where[idx].root}]` : "",
        nested: element.nested.length > 0 ? `[${where[idx].nested}]` : "",
      });
    });
    const joinType = collection[1].join;
    let attributeHandleQuery = ``;
    const columnAsArray = Array.from(columnAs, ([table, col]) => ({
      table,
      col,
    }));
    const mapColumnArray: string[] = [];
    const mapColumnResult: string[] = [];
    const aggregationProjection: Set<string> = new Set();
    let getUniqueColumnOnly: string = `return element {'result'}{for $node in distinct-values($joinedCol/*/local-name()) return $joinedCol/*[local-name() eq $node][1]}`;
    if (columnAs) {
      if (columnAs == "*") {
        attributeHandleQuery = `return $aggregated`;
      } else {
        attributeHandleQuery = `for $i in $aggregated
          let $projection :=for $j in $i/*
            order by $j/@order ascending
            group by $group := $j/@group
          return $j
        return element{'result'}{$projection}`;
        //   attributeHandleQuery = `for $i in $aggregated
        //   let $projection :=for $j in $i/*
        //     group by $group := $j/@group
        //     let $first := min($j/@order)
        //   return $j[@order=$first]
        // return element{'result'}{$projection}`;
      }
    }
    collection.forEach((element, idx) => {
      if (
        projection[idx].columns.length > 0 ||
        projection[idx].childColumns.length > 0 ||
        projection[idx].extendedColumns.length > 0
      ) {
        aggregationProjection.add(`$aggregaterow[1]/*`);
      } else {
        if (projection[idx].nestedColumns.length > 0) {
          aggregationProjection.add(`$aggregaterow[1]/*`);
        }
      }
      if (projection[idx].funcColumns.length > 0) {
        if (columnAs && columnAs.size != 0 && columnAs != "*") {
          const pattern = /(?<=:)[A-Za-z0-9_]+(?=\))/;
          const column = (projection[idx].funcColumns as string).match(
            pattern
          )![0];
          // console.log(element, columnAs);

          const columnMapped = columnAs
            .get(element.name)
            .find((val: any) => val.column == column).as;
          let func = (projection[idx].funcColumns as string).replace(
            pattern,
            columnMapped
          );
          aggregationProjection.add(func);
        } else {
          aggregationProjection.add(projection[idx].funcColumns);
        }
      }
    });
    let aggregationQuery = ``;
    if (
      groupby.length == 0 &&
      projection.some(val => val.funcColumns.length > 0)
    ) {
      aggregationQuery = `let $aggregaterow := $doc
        let $aggregated:=element{'result'}{(${[...aggregationProjection].join(
          ","
        )})}`;
    } else {
      aggregationQuery = `let $aggregated :=for $aggregaterow in $doc
        ${groupby.length > 0 ? `group by $group := ${groupby}` : ""}
      return element{'result'}{(${[...aggregationProjection].join(",")})}`;
    }

    if (joinType == "INNER JOIN" || !joinType) {
      const recursion = (join: any, depth: number): string => {
        let joinOnQuery = "";
        if (join.left == null || join.right == null) {
          return "";
        }
        const { left, right } = join;

        let resultLeft = recursion(join.left, depth + 1);
        let resultRight = recursion(join.right, depth + 1);

        // if case != and or
        if (join.operator == "AND") {
          const { translation } = this.supportedOperators.find(
            ({ origin }) => origin === join.operator
          ) as Supported;
          joinOnQuery += `${resultLeft}${translation} ${resultRight}`;
          return joinOnQuery;
        }

        const as = this.supportedOperators.find(
          ({ origin }) => origin === join.operator
        ) as Supported;
        const { translation } = this.supportedOperators.find(
          ({ origin }) => origin === join.operator
        ) as Supported;
        const access_col = "*:";

        const constructColumnOn = (column: string) => {
          let columnOn = "";
          if (column.includes("_attribute__")) {
            let columnAttr = column.split("__");
            if (columnAttr.length == 2) {
              columnOn += `@${columnAttr[1]}/data()`;
            }
            if (columnAttr.length == 3) {
              if (this.spatialNamespace.prefix == "kml") {
                columnOn += `*:ExtendedData/*/*[@name='${columnAttr[1]}']/@${columnAttr[2]}/data()`;
              } else {
                columnOn += `${access_col}${columnAttr[1]}/@${columnAttr[2]}/data()`;
              }
            }
          } else {
            if (this.spatialNamespace.prefix == "kml") {
              columnOn += `*:ExtendedData/*/*[@name='${column}']/text()`;
            } else {
              columnOn += `${access_col}${column}/text()`;
            }
          }
          return columnOn;
        };

        if (right.type === "column_ref") {
          joinOnQuery += `$${right.table}i/${constructColumnOn(
            right.column
          )} ${translation} $${left.table}i/${constructColumnOn(left.column)} `;
        }

        return joinOnQuery;
      };

      const joinOn = recursion(collection[1].on, 0);

      let iterateTwoCollections: string[] = [];
      let iterateTwoNestedCollections: string[] = [];
      let getTwoNestedCollections: string[] = [];
      let checkTwoNestedCollections: string[] = [];
      let getTwoNestedTableQuery: string[] = [];
      let getTwoTableQuery: string[] = [];
      let nestedResult: string = "";
      let nestedResultLeft: string = "";
      let nestedResultRight: string = "";
      let noNestedResult: string = "";
      const resultOfCartesianProduct: string[] = [];
      const resultOfCartesianProductNested: string[] = [];
      // collection.map(
      //   element => `$nestedColumn${element.name}`
      // );

      collection.forEach((element, idx) => {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          element.name,
          moduleVersion,
          projection[idx]
        );
        iterateTwoCollections.push(
          `$${element.name}i in ${this.version.getDocFunc(
            element.name,
            this.db_name
          )}//${extensionQuery.path}${whereQuery[idx].root}`
        );
        if (
          projection[idx].columns.length > 0 ||
          projection[idx].childColumns.length > 0 ||
          projection[idx].extendedColumns.length > 0
        ) {
          getTwoTableQuery.push(
            constructTableQuery({ name: element.name }, projection[idx])
          );
          resultOfCartesianProductNested.push(`$element${element.name}`);
          resultOfCartesianProduct.push(`$element${element.name}`);
          // aggregationProjection.add(`$aggregaterow[1]/*`);
        } else {
          resultOfCartesianProduct.push(`$${element.name}i/*`);
          if (projection[idx].nestedColumns.length > 0) {
            // aggregationProjection.add(`$aggregaterow[1]/*`);
          }
        }

        getTwoNestedCollections.push(
          `let $nestedcollection${element.name} := $${element.name}i/${
            this.spatialNamespace.prefix == "kml" ? "*:ExtendedData/*/" : ""
          }*[@_is_collection="true"]`
        );
        iterateTwoNestedCollections.push(
          `$${element.name}nestedcol in $nestedcollection${element.name}/*${whereQuery[idx].nested}`
        );
        checkTwoNestedCollections.push(
          `exists($nestedcollection${element.name})`
        );
        resultOfCartesianProductNested.push(`$nestedColumn${element.name}`);
        getTwoNestedTableQuery.push(
          constructNestedTableQuery(element.name, projection[idx])
        );
      });
      const constructResultColumnAll = (resultArr: Array<string>): string => {
        let temp = `
        if(${constructNoMatchedProjection(resultArr)}) then(
          let $joinedCol :=  element{'result'}{(${resultArr.join(",")})}
          ${getUniqueColumnOnly}
        )
        else()
        `;
        return temp;
      };
      const constructResultColumnProj = (
        resultArr: Array<string>,
        ret?: string
      ): string => {
        let temp = `
        ${mapColumnArray.join(" ")}
        ${ret ? ret : ""} if(${constructNoMatchedProjection([
          ...resultArr,
          ...mapColumnResult,
        ])}) then(
        let $joinedCol:= element{'result'}{(
          ${resultArr.join(",")}${
          columnAsArray.length > 0 && resultArr.length > 0 ? "," : ""
        }
          ${mapColumnResult.join(",")}
        )}
        ${getUniqueColumnOnly}
        )
        else()`;
        return temp;
      };
      if (columnAs) {
        if (columnAs == "*") {
          nestedResult = `return ${constructResultColumnAll(
            resultOfCartesianProductNested
          )}`;
          nestedResultLeft = `return ${constructResultColumnAll(
            resultOfCartesianProductNested.filter(
              val => `$nestedColumn${collection[1].name}` != val
            )
          )}`;
          nestedResultRight = `return ${constructResultColumnAll(
            resultOfCartesianProductNested.filter(
              val => `$nestedColumn${collection[0].name}` != val
            )
          )}`;
          noNestedResult = constructResultColumnAll(resultOfCartesianProduct);
        } else {
          columnAsArray.forEach((element, index) => {
            mapColumnArray.push(
              `let $mapColumn${index} := map {${element.col
                .map(
                  (val: any, idxcol: any) =>
                    `'${val.column}' ${this.version.mapOperator} '${val.as}'`
                )
                .join(",")}}`
            );
            // $element${element.table}[local-name()=$col${index}]/${
            //   moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
            mapColumnResult.push(`for $col${index} in map:keys($mapColumn${index}) 
            return if(count($element${element.table}[local-name()=$col${index}]/*)>0 or $element${element.table}[local-name()=$col${index}]/text())
            then(element{$mapColumn${index}($col${index})}{
              attribute{'order'}{$element${element.table}[local-name()=$col${index}]/@order},
              attribute{'group'}{$mapColumn${index}($col${index})},
              if(count($element${element.table}[local-name()=$col${index}]/*)>0)
              then($element${element.table}[local-name()=$col${index}]/*)
              else($element${element.table}[local-name()=$col${index}]/text())
            })else()`);
          });
          nestedResult = `${constructResultColumnProj(
            resultOfCartesianProductNested,
            "return"
          )}`;
          nestedResultLeft = `${constructResultColumnProj(
            resultOfCartesianProductNested.filter(
              val => `$nestedColumn${collection[1].name}` != val
            ),
            "return"
          )}`;
          nestedResultRight = `${constructResultColumnProj(
            resultOfCartesianProductNested.filter(
              val => `$nestedColumn${collection[0].name}` != val
            ),
            "return"
          )}`;
          noNestedResult = constructResultColumnProj(
            resultOfCartesianProduct,
            "return"
          );
        }
      }

      result += `
      let $doc:= for ${iterateTwoCollections.join(",")}
      ${joinOn && joinOn.length > 0 ? `where ${joinOn}` : ""}
      ${getTwoTableQuery.join(" ")}
      ${getTwoNestedCollections.join(" ")}
      return if(${checkTwoNestedCollections.join(" and ")})then(
        for ${iterateTwoNestedCollections.join(",")}
        ${getTwoNestedTableQuery.join(" ")}
        ${nestedResult}
      )else if(${checkTwoNestedCollections[0]})then(
        for ${iterateTwoNestedCollections[0]}
        ${getTwoNestedTableQuery[0]}
        ${nestedResultLeft}
      )else if(${checkTwoNestedCollections[1]})then(
        for ${iterateTwoNestedCollections[1]}
        ${getTwoNestedTableQuery[1]}
        ${nestedResultRight}
      )
      else(
        ${noNestedResult}
      )
      ${aggregationQuery}
      ${attributeHandleQuery}
      `;
    }
    // if (joinType == "NATURAL JOIN") {
    //   let iterateColumnSameName = "";
    //   let columnSameName = "";
    //   let columnSameNameQuery = "";
    //   collection.forEach((element, idx) => {
    //     const extensionQuery = this.constructExtensionQuery(
    //       this.spatialNamespace.prefix,
    //       element.name,
    //       moduleVersion,
    //       projection
    //     );
    //     iterateColumnSameName += `$${
    //       element.name
    //     }on in ${this.version.getDocFunc(element.name, this.db_name)}//${
    //       extensionQuery.path
    //     }[1]/*`;
    //     columnSameName += ` $${element.name}on/local-name()`;
    //     columnSameNameQuery += ` '$${element.name}/',$${element.name}/local-name(),'/text()'`;
    //     if (idx < collection.length - 1) {
    //       iterateColumnSameName += ", ";
    //       columnSameName += " =";
    //       columnSameNameQuery += " =";
    //     }
    //   });
    //   result += ` let $joinOn = for ${iterateColumnSameName} where ${columnSameName} return concat(${columnSameNameQuery})`;

    //   result += `for`;
    //   collection.forEach((element, idx) => {
    //     const extensionQuery = this.constructExtensionQuery(
    //       this.spatialNamespace.prefix,
    //       element.name,
    //       moduleVersion,
    //       projection
    //     );
    //     result += ` $${element.name}i in ${this.version.getDocFunc(
    //       element.name,
    //       this.db_name
    //     )}//${extensionQuery.path}${
    //       where[idx].length > 0 ? `[${where[idx]}]` : ""
    //     }`;
    //     if (idx < collection.length - 1) {
    //       result += `,`;
    //     }
    //   });
    //   collection.forEach((element, idx) => {
    //     result = constructTableQuery(result, element, projection[idx]);
    //   });

    //   // collection.forEach((element ,idx)=> {
    //   //   result+=`'$${element.name}i/',$${}`
    //   // });
    //   if (columnAs) {
    //     if (columnAs == "*") {
    //       result += ` let $element := element {'result'}{(`;
    //       collection.forEach((val, idx) => {
    //         result += `$element${val.name}`;
    //         if (idx < collection.length - 1) {
    //           result += `,`;
    //         }
    //       });
    //       result += `)}`;
    //       result += ` where xquery:eval(string-join($joinOn,' and'))`;
    //       result += ` return element {'result'}{for $node in distinct-values($element/*/local-name()) return $element/*[local-name() eq $node][1]}`;
    //     } else {
    //       const columnAsArray = Array.from(columnAs, ([table, col]) => ({
    //         table,
    //         col,
    //       }));

    //       columnAsArray.forEach((element, index) => {
    //         result += ` let $mapColumn${index} := map {`;
    //         // const columns = columnAs.get(key);

    //         element.col.forEach((val: any, idxcol: any) => {
    //           result += `'${val.column}' ${this.version.mapOperator} '${val.as}'`;
    //           if (idxcol < element.col.length - 1) {
    //             result += ",";
    //           }
    //         });
    //         result += `} `;
    //       });
    //       result += ` where ${joinOn}`;
    //       result += ` return element{'result'}{(`;
    //       columnAsArray.forEach((element, idx) => {
    //         result += `for $col${idx} in map:keys($mapColumn${idx}) return element{$mapColumn${idx}($col${idx})}{$element${
    //           element.table
    //         }[local-name()=$col${idx}]/${
    //           moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
    //         }}`;
    //         if (idx < columnAsArray.length - 1) {
    //           result += ",";
    //         }
    //       });
    //       result += `)}`;
    //     }
    //   }
    // }
    if (joinType == "LEFT JOIN" || joinType == "RIGHT JOIN") {
      const recursion = (join: any, depth: number): string => {
        let joinOnQuery = "";
        if (join.left == null || join.right == null) {
          return "";
        }
        const { left, right } = join;

        let resultLeft = recursion(join.left, depth + 1);
        let resultRight = recursion(join.right, depth + 1);

        // if case != and or
        if (join.operator == "AND") {
          const { translation } = this.supportedOperators.find(
            ({ origin }) => origin === join.operator
          ) as Supported;
          joinOnQuery += `${resultLeft}${translation} ${resultRight}`;
          return joinOnQuery;
        }

        const as = this.supportedOperators.find(
          ({ origin }) => origin === join.operator
        ) as Supported;
        const { translation } = this.supportedOperators.find(
          ({ origin }) => origin === join.operator
        ) as Supported;
        const access_col = "*:";

        const constructColumnOn = (column: string) => {
          let columnOn = "";
          columnOn += `${access_col}${column}/text()`;
          return columnOn;
        };

        if (right.type === "column_ref") {
          // const leftTable = collection.findIndex((el)=>el.name == left.table)!
          const rightTable = collection.findIndex(
            el => el.name == right.table
          )!;
          joinOnQuery += `${
            rightTable == 1 ? `$right` : "$left"
          }/${constructColumnOn(right.column)} ${translation} ${
            rightTable == 1 ? `$left` : "$right"
          }/${constructColumnOn(left.column)} `;
        }

        return joinOnQuery;
      };

      const constructColumnNull = (
        collection: string,
        projection: any,
        doc: string,
        tableQuery?: string
      ): string => {
        let result = `
          let $nullCol${collection} := for $${collection}i in ${doc}[1] 
          ${
            tableQuery
              ? tableQuery
              : `let $element${collection}:=for $${collection}j in $${collection}i/*
          return element{$${collection}j/local-name()}{attribute{'order'}{$${collection}j/@order},attribute{'group'}{$${collection}j/@group}}`
          }
          return $element${collection}
        `;
        return result;
      };
      const joinOn = recursion(collection[1].on, 0);
      const variables: { left: any; right: any } = { left: {}, right: {} };
      const variableNames = ["left", "right"];
      variableNames.forEach((element, idx) => {
        const variableName = element as "left" | "right";
        variables[variableName].collection = collection[idx].name;
        variables[variableName].extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          collection[idx].name,
          moduleVersion,
          projection[idx]
        );
        variables[variableName].whereQuery = whereQuery[idx];
        variables[variableName].projection = projection[idx];
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          collection[idx].name,
          moduleVersion,
          projection[idx]
        );

        variables[variableName].iterateCollection = `$${
          collection[idx].name
        }i in ${this.version.getDocFunc(collection[idx].name, this.db_name)}//${
          extensionQuery.path
        }${whereQuery[idx].root}`;
      });

      const left = variables.left;
      const right = variables.right;
      let noMatchedResult = ``;
      let matchedResult = ``;
      if (columnAs && columnAs != "*") {
        columnAsArray.forEach((element, index) => {
          mapColumnArray.push(
            `let $mapColumn${index} := map {${element.col
              .map(
                (val: any, idxcol: any) =>
                  `'${val.column}' ${this.version.mapOperator} '${val.as}'`
              )
              .join(",")}}`
          );
          // $element${element.table}[local-name()=$col${index}]/${
          //   moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
          mapColumnResult.push(`for $col${index} in map:keys($mapColumn${index}) 
          return for $childCol in $element${element.table}[local-name()=$col${index} or @group=$col${index}]
          return
          element{if(exists($element${element.table}[local-name()=$col${index}]))then($mapColumn${index}($col${index}))else($childCol/local-name())}{
            attribute{'order'}{$childCol/@order},
            attribute{'group'}
            {
              if(exists($element${element.table}[local-name()=$col${index}]))
              then($mapColumn${index}($col${index}))
              else($childCol/local-name())
            },
            if(count($childCol/*)>0)
            then($childCol/*)
            else($childCol/text())
          }`);
        });
      }
      if (joinType == "LEFT JOIN") {
        if (columnAs) {
          if (columnAs == "*") {
            noMatchedResult = `let $joinedCol :=  element{'result'}{($left/*,$nullCol${right.collection})}
          ${getUniqueColumnOnly}`;
            matchedResult = `let $joinedCol :=  element{'result'}{($left/*,$right/*)}
          ${getUniqueColumnOnly}`;
          } else {
            noMatchedResult = `
          ${mapColumnArray.join(" ")}
          let $joinedCol:= element{'result'}{(
          ${
            mapColumnResult.length > 0
              ? mapColumnResult.join(",")
              : `$left/*,$nullCol${right.collection}`
          })}
          ${getUniqueColumnOnly}
          `;
            matchedResult = `
          ${mapColumnArray.join(" ")}
          let $joinedCol:= element{'result'}{(
          ${
            mapColumnResult.length > 0
              ? mapColumnResult.join(",")
              : `$left/*,$right/*`
          })}
          ${getUniqueColumnOnly}
          `;
          }
        }
        result += `
        ${buildNestedCollectionQuery(
          left.projection,
          left.collection,
          left.whereQuery,
          left.extensionQuery
        )}
        ${buildNestedCollectionQuery(
          right.projection,
          right.collection,
          right.whereQuery,
          right.extensionQuery
        )}
        ${constructColumnNull(
          right.collection,
          right.projection,
          `$doc${right.collection}`
        )}
        let $doc:= for $left in $doc${left.collection}
        let $element${left.collection} :=$left/*
        let $matchedRow := for $right in $doc${right.collection}
        ${joinOn && joinOn.length > 0 ? `where ${joinOn}` : ""}
        let $element${right.collection}:=$right/*
        ${matchedResult}
        return if(empty($matchedRow)) then(
          let $element${right.collection}:=$nullCol${right.collection}
          ${noMatchedResult}
        )else(
          $matchedRow
        )
        ${aggregationQuery}
        ${attributeHandleQuery}

        `;
      }
      if (joinType == "RIGHT JOIN") {
        if (columnAs) {
          if (columnAs == "*") {
            noMatchedResult = `let $joinedCol :=  element{'result'}{($left/*,$nullCol${left.collection})}
          ${getUniqueColumnOnly}`;
            matchedResult = `let $joinedCol :=  element{'result'}{($left/*,$right/*)}
          ${getUniqueColumnOnly}`;
          } else {
            noMatchedResult = `
          ${mapColumnArray.join(" ")}
          let $joinedCol:= element{'result'}{(
          ${
            mapColumnResult.length > 0
              ? mapColumnResult.join(",")
              : `$left/*,$nullCol${left.collection}`
          })}
          ${getUniqueColumnOnly}
          `;
            matchedResult = `
          ${mapColumnArray.join(" ")}
          let $joinedCol:= element{'result'}{(
          ${
            mapColumnResult.length > 0
              ? mapColumnResult.join(",")
              : `$left/*,$right/*`
          })}
          ${getUniqueColumnOnly}
          `;
          }
        }
        result += `
        ${buildNestedCollectionQuery(
          right.projection,
          right.collection,
          right.whereQuery,
          right.extensionQuery
        )}
        ${buildNestedCollectionQuery(
          left.projection,
          left.collection,
          left.whereQuery,
          left.extensionQuery
        )}
        ${constructColumnNull(
          left.collection,
          left.projection,
          `$doc${left.collection}`
        )}
        let $doc:= for $left in $doc${right.collection}
        let $element${right.collection} :=$left/*
        let $matchedRow := for $right in $doc${left.collection}
        ${joinOn && joinOn.length > 0 ? `where ${joinOn}` : ""}
        let $element${left.collection}:=$right/*
        ${matchedResult}
        return if(empty($matchedRow)) then(
          let $element${left.collection}:=$nullCol${left.collection}
          ${noMatchedResult}
        )else(
          $matchedRow
        )
        ${aggregationQuery}
        ${attributeHandleQuery}

        `;
      }
    }
    return result;
  }
  constructXQuery = (
    collection: any[] | any,
    where: any[] | any,
    projection: any[] | any,
    groupby: any[] | any,
    columnAs: any
  ) => {
    console.log(collection, "collection");

    const moduleVersion = this.version.modules.find(
      val => val.extension === this.spatialNamespace.prefix
    );
    const constructTableQuery = (collection: any, projection: any): string => {
      const extensionQuery = this.constructExtensionQuery(
        this.spatialNamespace.prefix,
        collection.name,
        moduleVersion,
        projection
      );
      result = `
      let $element${collection.name} := for $${collection.name}j in $${collection.name}i[1]/${projection.columns}
        return
        ${extensionQuery.spatialTypeSelection}
        ${extensionQuery.retrieveCustomDataConditionWithAttr}
      `;
      return result;
    };
    const constructNestedTableQuery = (
      collection: string,
      projection: any
    ): string => {
      const localName =
        this.spatialNamespace.prefix == "gml"
          ? `$nestedcollection${collection}/local-name()`
          : `$nestedcollection${collection}/@name`;
      let result = `
      let $nestedColumn${collection}:= 
      for $col in $${collection}nestedcol/${projection.nestedColumns}
        for $childCol in $${collection}nestedcol/${projection.nestedChildColumns}
          return if(not($childCol instance of attribute()))
          then(
              element{concat(${localName},'__',$childCol/local-name())}{
              attribute{'order'}{4},
              attribute{'group'}{$childCol/local-name()},
              if(count($childCol/*)>0)
              then($childCol/*)
              else($childCol/text())
            }
          )
          else(
            element{concat('_attribute__',${localName},'__',$col/local-name(),'__',$childCol/local-name())}{
              attribute{'order'}{5},
              attribute{'group'}{$childCol/local-name()},
              $childCol/data()
            }
          )
        `;
      return result;
    };

    const buildNestedCollectionQuery = (
      projection: any,
      collection: string,
      whereQuery: any,
      extensionQuery: any,
      joinOn?: any
    ): string => {
      let result = "";
      result += `let $doc${collection} := `;
      result += `for $${collection}i in ${this.version.getDocFunc(
        collection,
        this.db_name
      )}//${extensionQuery.path}${whereQuery.root}
      `;
      if (joinOn) {
        result += `where ${joinOn}`;
      }
      let resultArr: string[] = [`$nestedColumn${collection}`];
      if (
        projection.columns.length > 0 ||
        projection.childColumns.length > 0 ||
        projection.extendedColumns.length > 0
      ) {
        result += constructTableQuery({ name: collection }, projection);
        resultArr.push(`$element${collection}`);
      }
      if (this.spatialNamespace.prefix == "gml") {
        result += `let $nestedcollection${collection} := $${collection}i/*[@_is_collection="true"]`;
      } else {
        result += `let $nestedcollection${collection} := $${collection}i/*:ExtendedData/*/*[@_is_collection="true"]`;
      }
      result += `
      
      return if(exists($nestedcollection${collection}))then(
        for $${collection}nestedcol in $nestedcollection${collection}/*${
        whereQuery.nested
      }
        ${constructNestedTableQuery(collection, projection)}
        return element{'result'}{(${resultArr.join(",")})}
      )
      else(`;
      if (
        projection.columns.length > 0 ||
        projection.childColumns.length > 0 ||
        projection.extendedColumns.length > 0
      ) {
        result += `
        if(exists($element${collection})) then(element{'result'}{$element${collection}})
        else()`;
      } else {
        result += `element{'result'}{$${collection}i/*}`;
      }
      result += `)`;

      return result;
    };
    const extensionQuery = this.constructExtensionQuery(
      this.spatialNamespace.prefix,
      collection,
      moduleVersion,
      projection
    );

    const namespaces = this.constructSpatialNamespace(
      [this.spatialNamespace],
      false
    );

    const modules = this.version.modules.find(
      val => val.extension == this.spatialNamespace.prefix
    );
    const moduleNamespaces = this.constructSpatialNamespace(
      modules ? [modules.namespaceModule] : [],
      true
    );
    let result = namespaces + moduleNamespaces;
    // console.log(collection);
    console.log(projection, "projjj");

    if (
      !Array.isArray(collection) &&
      !Array.isArray(where) &&
      !Array.isArray(projection)
    ) {
      let whereQuery = { root: "", nested: "" };
      if (where.root.length > 0) {
        whereQuery.root = `[${where.root}]`;
      }
      if (where.nested.length > 0) {
        whereQuery.nested = `[${where.nested}]`;
      }
      let projArr: string[] = [];
      if (
        projection.columns.length > 0 ||
        projection.childColumns.length > 0 ||
        projection.extendedColumns.length > 0
      ) {
        projArr.push(`$aggregaterow[1]/*`);
      } else {
        if (projection.nestedColumns.length > 0) {
          projArr.push(`$aggregaterow[1]/*`);
        }
      }
      if (projection.funcColumns.length > 0) {
        projArr.push(projection.funcColumns);
      }
      result += `
      ${buildNestedCollectionQuery(
        projection,
        collection,
        whereQuery,
        extensionQuery
      )}`;
      if (groupby.length == 0 && projection.funcColumns.length > 0) {
        result += `
        let $aggregaterow := $doc${collection}
        let $aggregated:=element{'result'}{(${projArr.join(",")})}`;
      } else {
        result += `
        let $aggregated :=for $aggregaterow in $doc${collection} 
          ${groupby.length > 0 ? `group by $group := ${groupby}` : ""}
          return element{'result'}{(${projArr.join(",")})}`;
      }
      if (projection.rawColumns.length > 0) {
        // order by $j/@order ascending
        // result += `
        // for $i in $aggregated
        //   let $projection :=for $j in $i/*
        //     group by $group := $j/@group
        //     let $first := min($j/@order)
        //   return $j[@order=$first]
        // return element{'result'}{$projection}
        // `;
        result += `for $i in $aggregated
        let $projection :=for $j in $i/*
          order by $j/@order ascending
          group by $group := $j/@group
        return $j
      return element{'result'}{$projection}`;
      } else {
        result += ` return $aggregated`;
      }
    } else {
      if (collection.length > 1 && where.length > 1 && projection.length > 1) {
        result = this.constructJoinQuery(
          result,
          collection,
          where,
          projection,
          groupby,
          columnAs,
          constructTableQuery,
          constructNestedTableQuery,
          buildNestedCollectionQuery,
          moduleVersion
        );
      } else {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          collection[0].name,
          moduleVersion,
          projection
        );
        let whereQuery = { root: "", nested: "" };
        if (where[0].root.length > 0) {
          whereQuery.root = `[${where[0].root}]`;
        }
        if (where[0].nested.length > 0) {
          whereQuery.nested = `[${where[0].nested}]`;
        }

        result += `for $${collection[0].name}i in ${this.version.getDocFunc(
          collection[0].name,
          this.db_name
        )}//${extensionQuery.path}${whereQuery.root}`;
        if (groupby[0].length > 0) {
          result += ` group by $${collection[0].name}group := ${groupby[0]} `;
        }
        let projectionResult = "";

        if (
          projection[0].columns.length > 0 ||
          projection[0].childColumns.length > 0
        ) {
          result += constructTableQuery(collection[0], projection[0]);
          projectionResult = `$element${collection[0].name}`;
        }

        let funcProjQuery = "";
        if (projection[0].funcColumns.length > 0) {
          funcProjQuery += `${projection[0].funcColumns}`;
        }

        result += ` return element {'result'}
        { 
          (${funcProjQuery}${
          projectionResult.length > 0 && funcProjQuery.length > 0 ? `,` : ``
        }${projectionResult})
        }`;
      }
    }

    // console.log(result);
    return result;
    // `for $i in ${this.version.getDocFunc(
    //   collection,
    //   this.db_name
    // )}//gml:featureMember/*${whereQuery}
    // return json:serialize(element {'json'}
    // { attribute {'objects'}{'json'},
    //   for $j in $i/${projection}
    //   return
    //   if(boolean($j/*/@srsName)) then (
    //   element {'geometry'} {${
    //     this.version.getSTAsTextfunc
    //       ? this.version.getSTAsTextfunc("$j/*")
    //       : "$j/*"
    //   }}
    //   )
    //   else if(boolean($j/@srsName)) then(
    //     element {'geometry'} {${
    //       this.version.getSTAsTextfunc
    //         ? this.version.getSTAsTextfunc("$j")
    //         : "$j"
    //     }}
    //   )
    //   else (
    //       element {$j/local-name()}{if(fn:exists($j/text())) then($j/text()) else($j/data())}
    //   )
    // })`
  };

  constructSelectionQuery(where: any): any {
    // console.log(where);

    if (!where) {
      return { root: "", nested: "" };
    }

    const conditionalOperators = ["AND", "OR"];

    const recursion = (
      where: any,
      numOfOr: number,
      depth: number
    ): { root: string; nested: string } => {
      let selection = { root: "", nested: "" };
      const { operator } = where;

      if (operator == null) {
        if (where.type == "bool") {
          return { root: "", nested: "" };
        }
        return selection;
      }

      if (where.left.type === "function" || where.right.type === "function") {
        console.log("kenapa ini", where);

        return { root: this.constructFunctionQuery(where), nested: "" };
      }

      const {
        left: { column },
        right: { type, value },
      } = where;

      const newNumOfOr = where.operator == "OR" ? numOfOr + 1 : numOfOr;
      let resultLeft = recursion(where.left, newNumOfOr, depth + 1);
      let resultRight = recursion(where.right, newNumOfOr, depth + 1);

      // if case != and or
      if (conditionalOperators.includes(where.operator)) {
        const { translation } = this.supportedOperators.find(
          ({ origin }) => origin === where.operator
        ) as Supported;
        selection.root += `${resultLeft.root}${translation} ${resultRight.root}`;
        const tempNestedArr: string[] = [];
        if (resultLeft.nested.length > 0) {
          tempNestedArr.push(resultLeft.nested);
        }
        if (resultRight.nested.length > 0) {
          tempNestedArr.push(resultRight.nested);
        }
        selection.nested = tempNestedArr.join(" and ");
        return selection;
      }

      const as = this.supportedOperators.find(
        ({ origin }) => origin === where.operator
      ) as Supported;
      const { translation } = this.supportedOperators.find(
        ({ origin }) => origin === where.operator
      ) as Supported;
      const access_col = "*:";
      let selectionPath = ``;
      if (this.spatialNamespace.prefix == "gml") {
        selectionPath = `${access_col}${column}`;
      }
      if (this.spatialNamespace.prefix == "kml") {
        // *:ExtendedData/*/*[@name='nama']='Masjid Algufron Malendeng'
        selectionPath = `${access_col}ExtendedData/*/*[@name='${column}']`;
      }
      if (type === "number" || type === "string") {
        if (column.includes("_attribute__")) {
          let columnAttr = column.split("__");
          if (columnAttr.length == 2) {
            selection.root += `@${columnAttr[1]} ${translation} ${
              type === "number" ? value : `'${value}'`
            } `;
          }
          if (columnAttr.length == 3) {
            if (this.spatialNamespace.prefix == "gml") {
              selection.root += `${access_col}${columnAttr[1]}`;
            }
            if (this.spatialNamespace.prefix == "kml") {
              selection.root += `${access_col}ExtendedData/*/*[@name='${columnAttr[1]}']`;
            }
            selection.root += `/@${columnAttr[2]} ${translation} ${
              type === "number" ? value : `'${value}'`
            } `;
          }
          if (columnAttr.length == 4) {
            if (this.spatialNamespace.prefix == "gml") {
              selection.root += `${access_col}${columnAttr[1]}/*`;
            }
            if (this.spatialNamespace.prefix == "kml") {
              selection.root += `${access_col}ExtendedData/*/*[@name='${columnAttr[1]}']/*`;
            }
            selection.root += `/${access_col}${columnAttr[2]}/@${
              columnAttr[3]
            } ${translation} ${type === "number" ? value : `'${value}'`} `;
            selection.nested = `${access_col}${columnAttr[2]}/@${
              columnAttr[3]
            } ${translation}${type === "number" ? value : `'${value}'`} `;
            // selection.root.add(`$col/@${columnAttr[3]}`);
            // tempNestedArr.add(`*:${columnAttr[2]}`);
          }
        } else if (column.includes("_undef__")) {
          let undefCol = column.split("__")[1];
          let selectionPath = ``;
          let subselectionPath = ``;
          if (this.spatialNamespace.prefix == "gml") {
            subselectionPath = `${access_col}${undefCol}`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            // *:ExtendedData/*/*[@name='nama']='Masjid Algufron Malendeng'
            selectionPath = `${access_col}ExtendedData/*/`;
            subselectionPath = `*[@name='${undefCol}']`;
          }
          selection.root += `(${selectionPath}${subselectionPath} ${translation} ${
            type === "number" ? value : `'${value}'`
          } or `;
          selection.root += `@${undefCol} ${translation} ${
            type === "number" ? value : `'${value}'`
          } or `;
          selection.root += `${selectionPath}*/@${undefCol} ${translation} ${
            type === "number" ? value : `'${value}'`
          } or `;
          selection.root += `${selectionPath}*[@_is_collection='true']/*/*:${undefCol} ${translation} ${
            type === "number" ? value : `'${value}'`
          } or `;
          selection.root += `${selectionPath}*[@_is_collection='true']/*/@${undefCol} ${translation} ${
            type === "number" ? value : `'${value}'`
          })`;
        } else if (column.includes("__")) {
          let nestedColumn = column.split("__");
          if (this.spatialNamespace.prefix == "gml") {
            selection.root += `${access_col}${nestedColumn[0]}/*`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            selection.root += `${access_col}ExtendedData/*/*[@name='${nestedColumn[0]}']/*`;
          }
          selection.root += `/${access_col}${nestedColumn[1]} ${translation} ${
            type === "number" ? value : `'${value}'`
          } `;
          selection.nested = `${access_col}${nestedColumn[1]} ${translation}${
            type === "number" ? value : `'${value}'`
          } `;
        } else {
          selection.root += `${selectionPath} ${translation} ${
            type === "number" ? value : `'${value}'`
          } `;
          // console.log(selection, "selection");

          // selection.root += `@${column} ${translation} ${
          //   type === "number" ? value : `'${value}'`
          // } or `;
          // selection.root += `${selectionPath}/@${column} ${translation} ${
          //   type === "number" ? value : `'${value}'`
          // } `;
        }
      } else if (type === "null") {
        if (operator === "IS") {
          selection.root += `fn:exists(${selectionPath}/text()) or `;
          selection.root += `fn:exists(@${column}/data()) or `;
          selection.root += `fn:exists(${selectionPath}/@${column}/data())`;
        } else if (operator === "IS NOT") {
          selection.root += `not(fn:exists(${selectionPath}/text())) or `;
          selection.root += `not(fn:exists(@${column}/data())) or `;
          selection.root += `not(fn:exists(${selectionPath}/@${column}/data()))`;
        }
      } else if (type === "expr_list") {
        const values = value as any[];
        const lastVal = values.pop();
        let midselection = "";
        let lastselection = "";
        for (const val of values) {
          if (val.type === "number") {
            midselection += `${val.value}, `;
          } else {
            midselection += `"${val.value}", `;
          }
        }
        if (lastVal.type === "number") {
          lastselection += `${lastVal.value})`;
        } else {
          lastselection += `"${lastVal.value}")`;
        }
        if (column.includes("_attribute__")) {
          let columnAttr = column.split("__");
          if (columnAttr.length == 2) {
            selection.root += `@${columnAttr[1]} ${translation} (${midselection}${lastselection}`;
          }
          if (columnAttr.length == 3) {
            if (this.spatialNamespace.prefix == "gml") {
              selection.root += `${access_col}${columnAttr[1]}`;
            }
            if (this.spatialNamespace.prefix == "kml") {
              selection.root += `${access_col}ExtendedData/*/*[@name='${columnAttr[1]}']`;
            }
            selection.root += `/@${columnAttr[2]} ${translation} (${midselection}${lastselection}`;
          }
        } else if (column.includes("__")) {
          let nestedColumn = column.split("__");
          if (this.spatialNamespace.prefix == "gml") {
            selection.root += `${access_col}${nestedColumn[0]}/*`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            selection.root += `${access_col}ExtendedData/*/*[@name='${nestedColumn[0]}']/*`;
          }
          selection.root += `/${access_col}${nestedColumn[1]} ${translation} (${midselection}${lastselection}`;
          selection.nested += `${access_col}${nestedColumn[1]} ${translation} (${midselection}${lastselection}`;
        } else {
          selection.root += `${selectionPath} ${translation} (${midselection}${lastselection}`;
          // selection += `@${column} ${translation} (${midselection}${lastselection} or `;
          // selection += `${selectionPath}/@${column} ${translation} (${midselection}${lastselection}`;
        }
      }

      return selection;
    };

    const selection = recursion(where, 0, 0);
    console.log(selection, "selection");

    return selection;
  }
  constructProjectionQuery(columns: Set<string>, collection: any): any {
    // console.log(columns);
    let childProjection = ``;
    if (this.spatialNamespace.prefix == "gml") {
      childProjection += `${collection.name}j`;
    }
    if (this.spatialNamespace.prefix == "kml") {
      childProjection += `extendeddata`;
    }
    if (columns.size == 0) {
      return {
        columns:
          this.spatialNamespace.prefix == "gml"
            ? "(*[not(@_is_collection='true')]|@*)"
            : "(*|@*)",
        childColumns: `($${childProjection}|$${childProjection}/@*)`,
        funcColumns: "",
        extendedColumns: "*[not(@_is_collection='true')]",
        nestedColumns: "(*|@*)",
        rawColumns: [...columns],
        nestedChildColumns: "($col|$col/@*)",
      };
    }
    let tempresultArr: Set<string> = new Set();
    let tempchildResultArr: Set<string> = new Set();
    let tempExtendedArr: Set<string> = new Set();
    let tempNestedArr: Set<string> = new Set();
    let tempNestedChildArr: Set<string> = new Set();
    const ignoreQName = "*:";
    let funcArr: string[] = [];
    let arrColumns = [...columns];
    arrColumns
      .filter(val => proj_func_args_1.exec(val))
      .forEach(val => {
        const colname = proj_func_args_1.exec(val)!.groups!.colname;
        columns.add(colname);
      });

    arrColumns = [...columns];

    arrColumns.forEach((column, index) => {
      // let tempresult = ``;
      // let tempchildResult = ``;
      // let tempExtended = ``;
      // let tempNestedResult = ``;
      // let tempNestedChildResult = ``;
      if (column == "geometry") {
        // console.log(this.spatialNamespace.prefix);

        let spatialTypes = this.supportedSpatialType.find(element => {
          return element.extType == this.spatialNamespace.prefix;
        });
        let tempSpatialTypes: any[] = [];
        spatialTypes?.types.forEach(element => {
          if (this.spatialNamespace.prefix == "kml") {
            tempSpatialTypes.push(`local-name()='${element}'`);
          } else {
            tempSpatialTypes.push(`*/local-name()='${element}'`);
          }
        });

        if (this.spatialNamespace.prefix == "gml") {
          tempresultArr.add(`*[${tempSpatialTypes.join(" or ")}]`);
        }
        if (this.spatialNamespace.prefix == "kml") {
          tempresultArr.add(`${tempSpatialTypes.join(" or ")}`);
        }
      } else {
        const pattern_func = proj_func_args_1.exec(column);
        // console.log(column, pattern_func);
        if (pattern_func) {
          const func_detail = pattern_func.groups!;
          const func_projection = this.supportedProjectionFunctions.find(
            val => val.postGISName == func_detail.fname
          )!;
          let pathProjection = "";
          pathProjection += `/*:${func_detail.colname}`;
          // if (this.spatialNamespace.prefix == "gml") {
          // }
          // if (this.spatialNamespace.prefix == "kml") {
          //   pathProjection += `/*:ExtendedData/*/*[@name='${func_detail.colname}']`;
          // }
          funcArr.push(
            `element{'_func__${func_projection.name}__${
              func_detail.colname
            }'}{attribute{'order'}{'1'},attribute{'group'}{'_func__${
              func_projection.name
            }__${func_detail.colname}'},${func_projection.name}($aggregaterow${
              func_detail.colname == "*" ? "" : pathProjection
            })}`
          );
        } else if (column.includes("_attribute__")) {
          let columnAttr = column.split("__");
          if (columnAttr.length == 2) {
            if (this.spatialNamespace.prefix == "kml") {
              tempExtendedArr.add(`@${columnAttr[1]}`);
            } else {
              tempresultArr.add(`@${columnAttr[1]}`);
            }
          }
          if (columnAttr.length == 3) {
            if (this.spatialNamespace.prefix == "kml") {
              tempchildResultArr.add(
                `$${childProjection}[@name='${columnAttr[1]}']/@${columnAttr[2]}`
              );
              tempExtendedArr.add(`@name='${columnAttr[1]}'`);
            }
            if (this.spatialNamespace.prefix == "gml") {
              tempchildResultArr.add(
                `$${childProjection}[local-name()='${columnAttr[1]}']/@${columnAttr[2]}`
              );
              tempresultArr.add(`*:${columnAttr[1]}`);
            }
          }
          if (columnAttr.length == 4) {
            tempNestedChildArr.add(`$col/@${columnAttr[3]}`);
            tempNestedArr.add(`local-name()='${columnAttr[2]}'`);
            // tempresultArr.add("*[@_is_collection='true']");
          }
        } else if (column.includes("_undef__")) {
          let undefCol = column.split("__")[1];
          if (this.spatialNamespace.prefix == "gml") {
            tempresultArr.add(`${ignoreQName}${undefCol}`);
            tempresultArr.add(`@${undefCol}`);
            tempchildResultArr.add(`$${childProjection}/@${undefCol}`);
          }
          if (this.spatialNamespace.prefix == "kml") {
            tempExtendedArr.add(`@name='${undefCol}'`);
            tempExtendedArr.add(`@${undefCol}`);
            tempchildResultArr.add(`$${childProjection}/@${undefCol}`);
          }
          tempNestedArr.add(`local-name()='${undefCol}'`);
          tempNestedChildArr.add(`$col/@${undefCol}`);
        } else if (column.includes("__")) {
          let nestedColumn = column.split("__");
          // if (this.spatialNamespace.prefix == "kml") {
          //   tempExtendedArr.add(`@name='${nestedColumn[0]}'`);
          // }
          // if (this.spatialNamespace.prefix == "gml") {
          //   tempresultArr.add( `${ignoreQName}${nestedColumn[0]}`);
          // }
          tempNestedArr.add(`local-name()='${nestedColumn[1]}'`);
          tempNestedChildArr.add(`$col`);
        } else {
          if (this.spatialNamespace.prefix == "gml") {
            tempresultArr.add(`${ignoreQName}${column}`);
            // tempresult += `| @${column}`;
            // tempchildResultArr.add(`$${childProjection}/@${column}`);
            // tempNestedResult += `local-name()='${column}'`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            tempExtendedArr.add(`@name='${column}'`);
            tempchildResultArr.add(`$${childProjection}`);
            // tempExtended += `or @${column}`;
            // tempchildResult += `$${childProjection}/@${column}`;
            // tempNestedResult += `local-name()='${column}'`;
          }
        }
      }
    });
    let resultArr = [...tempresultArr];
    let childResultArr = [...tempchildResultArr];
    let nestedArr = [...tempNestedArr];
    let extendedArr = [...tempExtendedArr];

    let result = ``;
    let childResult = ``;
    let extendedResult = ``;
    let nestedResult = `*`;

    if (resultArr.length > 0 || extendedArr.length > 0) {
      childResultArr.push(`$${childProjection}`);
      tempNestedChildArr.add(`$col`);
      if (this.spatialNamespace.prefix == "kml") {
        result += `*:ExtendedData`;
      }
    }
    let nestedChildArr = [...tempNestedChildArr];
    let nestedChildResult = ``;
    if (resultArr.length > 0) {
      if (this.spatialNamespace.prefix == "kml") {
        result = `(*[${resultArr.join(" or ")}] | *:ExtendedData)`;
      }
      if (this.spatialNamespace.prefix == "gml") {
        result = `(${resultArr.join(" | ")})`;
      }
    }
    if (extendedArr.length > 0) {
      extendedResult = `*[${extendedArr.join(" or ")}]`;
    }
    if (childResultArr.length > 0) {
      childResult = `(${childResultArr.join(" | ")})`;
    }
    if (nestedArr.length > 0) {
      nestedResult = `*[${nestedArr.join(" or ")}]`;
    }
    if (nestedChildArr.length > 0) {
      nestedChildResult = `(${nestedChildArr.join(" | ")})`;
    }

    // console.log(tempresultArr, "tempres", tempchildResultArr, "tempchild");
    let funcResult = funcArr.join(",");
    // console.log(result, childResult);

    return {
      columns: result,
      childColumns: childResult,
      funcColumns: funcResult,
      extendedColumns: extendedResult,
      rawColumns: arrColumns,
      nestedColumns: nestedResult,
      nestedChildColumns: nestedChildResult,
    };
  }
  constructGroupByQuery(groupby: any, collection: any): string {
    if (!groupby || groupby.length == 0) {
      return "";
    }
    let groupbyQuery = ``;
    groupby.forEach((el: any, idx: number) => {
      groupbyQuery += `$aggregaterow/`;
      groupbyQuery += `*:${el.column}`;
      // if (this.spatialNamespace.prefix == "gml") {
      // }
      // if (this.spatialNamespace.prefix == "kml") {
      //   groupbyQuery += `*:ExtendedData/*/*[@name='${el.column}']`;
      // }
      if (idx < groupby.length - 1) {
        groupbyQuery += `,`;
      }
    });
    console.log(groupbyQuery, groupby, "groupbyQuery");

    return groupbyQuery;
  }
}

export { XMLExtension };
