import { Column, From } from "flora-sql-parser";
import { dropRight, isArray, subtract, template, values } from "lodash";
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
          const nodes: any = xpath.select(`/result/${column}`, doc);

          if (nodes.length > 0) {
            const node: any = nodes[0];
            const geotype = this.supportedSpatialType
              .find(val => val.extType == this.spatialNamespace.prefix)
              ?.types.find(el => el == node.firstChild?.localName);

            if (node.localName === "geometry" || geotype) {
              row.value.push({
                type: "string",
                value: node.firstChild.data
                  ? node.firstChild.data.toString()
                  : node.firstChild.toString(),
                // value: "a"
              });
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
                  type: "string",
                  value: "",
                });
              }
            }
          } else {
            row.value.push({
              type: "string",
              value: "",
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
    retrieveCustomDataCondition: string;
  } {
    const result = {
      path: "",
      spatialTypeSelection: "",
      retrieveCustomDataCondition: "",
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
        )}])) then (element {'geometry'} {${
          moduleVersion?.getSTAsTextfunc
            ? moduleVersion.getSTAsTextfunc(`$${collection}j/*`)
            : `$${collection}j/*`
        }}
        )`;
        result.retrieveCustomDataCondition = `
        else (
          if(not($${collection}j instance of attribute()))
          then(
            for $${collection}k in (${projection.childColumns})
            return if(not($${collection}k instance of attribute()))
            then(
                element{$${collection}k/local-name()}{
                if(count($${collection}k/*)>0)
                then($${collection}k/*)
                else($${collection}k/text())
              }
            )
            else(
              element{concat('_attribute__',$${collection}j/local-name(),'__',$${collection}k/local-name())}{$${collection}k/data()}
            )
          )
          else(
            element{concat('_attribute__',$${collection}j/local-name())}{$${collection}j/data()}
          )
        )`;
        // else (
        //   if($${collection}j/data()='' or fn:exists($${collection}j/text()))
        //   then(
        //   for $${collection}k in ${projection.childColumns}
        //   let $retrieve_condition :=$${collection}k/data()='' or fn:exists($${collection}k/text())
        //   let $has_child :=fn:exists($${collection}k[count(*)>0])
        //   return element {if($retrieve_condition) then($${collection}k/local-name()) else(concat('_attribute__',$${collection}j/local-name(),'__',$${collection}k/local-name()))}{if($retrieve_condition) then($${collection}k/text()) else(if($has_child)then($${collection}k/*)else($${collection}k/data()))}
        //   )
        //   else(
        //     element {if(fn:exists($${collection}j[count(*)>0]))then($${collection}j/local-name())else(concat('_attribute__',$${collection}j/local-name()))}{if(fn:exists($${collection}j[count(*)>0]))then($${collection}j/*)else($${collection}j/data())}
        //   )
        // )
        break;
      case "kml":
        result.path = "kml:Placemark";
        result.spatialTypeSelection = `if(fn:exists($${collection}j[${tempSpatialTypes.join(
          " or "
        )}]))
        then (element {'geometry'} {${
          moduleVersion?.getSTAsTextfunc
            ? moduleVersion.getSTAsTextfunc(`$${collection}j`)
            : `$${collection}j`
        }}
        )`;
        result.retrieveCustomDataCondition = `
        else if($${collection}j/local-name()='ExtendedData') then(
          for $extendeddata in $${collection}j/*/${projection.extendedColumns}
          return 
            for $${collection}k in ${projection.childColumns}
            return if(not($${collection}k instance of attribute()))
            then(
                element{$${collection}k/@name}
                {
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
        // else if($${collection}j/local-name()='ExtendedData') then(
        //   for $extendeddata in $${collection}j/*/${projection.columns}
        //   return
        //   if($extendeddata/data()='' or fn:exists($extendeddata/text()))
        //   then(
        //   for $${collection}k in ${projection.childColumns}
        //   let $retrieve_condition :=$${collection}k/data()='' or fn:exists($${collection}k/text())
        //   let $has_child :=fn:exists($${collection}k[count(*)>0])
        //   return element {if($retrieve_condition) then($${collection}k/@name) else(concat('_attribute__',$extendeddata/@name,'__',$${collection}k/local-name()))}{if($retrieve_condition) then($${collection}k/text()) else(if($has_child)then($${collection}k/*)else($${collection}k/data()))}
        //   )
        //   else(
        //     element {if(fn:exists($extendeddata[count(*)>0]))then($extendeddata/@name)else(concat('_attribute__',$extendeddata/local-name()))}{if(fn:exists($extendeddata[count(*)>0]))then($extendeddata/*)else($extendeddata/data())}
        //   )
        // )
        // else()
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
    moduleVersion: any
  ): string {
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
    const joinType = collection[1].join;
    const joinOn =
      joinType != "NATURAL JOIN" ? recursion(collection[1].on, 0) : null;

    if (joinType == "INNER JOIN") {
      result += `for`;
      collection.forEach((element, idx) => {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          element.name,
          moduleVersion,
          projection
        );
        result += ` $${element.name}i in ${this.version.getDocFunc(
          element.name,
          this.db_name
        )}//${extensionQuery.path}${
          where[idx].length > 0 ? `[${where[idx]}]` : ""
        }`;
        if (idx < collection.length - 1) {
          result += `,`;
        }
      });
      result += ` where ${joinOn}`;
      if (groupby.length > 0) {
        result += ` group by $group := ${groupby} `;
      }
      if (columnAs) {
        let projArr: string[] = [];
        collection.forEach((element, idx) => {
          if (
            projection[idx].columns.length > 0 ||
            projection[idx].childColumns.length > 0 ||
            projection[idx].extendedColumns.length > 0
          ) {
            result = constructTableQuery(result, element, projection[idx]);
            if (columnAs == "*") {
              projArr.push(`$element${element.name}`);
            }
          }
          if (projection[idx].funcColumns.length > 0) {
            projArr.push(`${projection[idx].funcColumns}`);
          }
        });

        if (columnAs == "*") {
          result += ` let $element := element {'result'}{(`;
          result += projArr.join(",");
          result += `)}`;
          result += ` return element {'result'}{for $node in distinct-values($element/*/local-name()) return $element/*[local-name() eq $node][1]}`;
        } else {
          const columnAsArray = Array.from(columnAs, ([table, col]) => ({
            table,
            col,
          }));

          columnAsArray.forEach((element, index) => {
            result += ` let $mapColumn${index} := map {`;
            // const columns = columnAs.get(key);

            element.col.forEach((val: any, idxcol: any) => {
              result += `'${val.column}' ${this.version.mapOperator} '${val.as}'`;
              if (idxcol < element.col.length - 1) {
                result += ",";
              }
            });
            result += `} `;
          });
          result += ` return element{'result'}{(`;
          result += projArr.join(",");
          if (columnAsArray.length > 0 && projArr.length > 0) {
            result += `,`;
          }
          columnAsArray.forEach((element, idx) => {
            result += `for $col${idx} in map:keys($mapColumn${idx}) return element{$mapColumn${idx}($col${idx})}{$element${
              element.table
            }[local-name()=$col${idx}]/${
              moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
            }}`;
            if (idx < columnAsArray.length - 1) {
              result += ",";
            }
          });
          result += `)}`;
        }
      }
    }
    if (joinType == "NATURAL JOIN") {
      let iterateColumnSameName = "";
      let columnSameName = "";
      let columnSameNameQuery = "";
      collection.forEach((element, idx) => {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          element.name,
          moduleVersion,
          projection
        );
        iterateColumnSameName += `$${
          element.name
        }on in ${this.version.getDocFunc(element.name, this.db_name)}//${
          extensionQuery.path
        }[1]/*`;
        columnSameName += ` $${element.name}on/local-name()`;
        columnSameNameQuery += ` '$${element.name}/',$${element.name}/local-name(),'/text()'`;
        if (idx < collection.length - 1) {
          iterateColumnSameName += ", ";
          columnSameName += " =";
          columnSameNameQuery += " =";
        }
      });
      result += ` let $joinOn = for ${iterateColumnSameName} where ${columnSameName} return concat(${columnSameNameQuery})`;

      result += `for`;
      collection.forEach((element, idx) => {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          element.name,
          moduleVersion,
          projection
        );
        result += ` $${element.name}i in ${this.version.getDocFunc(
          element.name,
          this.db_name
        )}//${extensionQuery.path}${
          where[idx].length > 0 ? `[${where[idx]}]` : ""
        }`;
        if (idx < collection.length - 1) {
          result += `,`;
        }
      });
      collection.forEach((element, idx) => {
        result = constructTableQuery(result, element, projection[idx]);
      });

      // collection.forEach((element ,idx)=> {
      //   result+=`'$${element.name}i/',$${}`
      // });
      if (columnAs) {
        if (columnAs == "*") {
          result += ` let $element := element {'result'}{(`;
          collection.forEach((val, idx) => {
            result += `$element${val.name}`;
            if (idx < collection.length - 1) {
              result += `,`;
            }
          });
          result += `)}`;
          result += ` where xquery:eval(string-join($joinOn,' and'))`;
          result += ` return element {'result'}{for $node in distinct-values($element/*/local-name()) return $element/*[local-name() eq $node][1]}`;
        } else {
          const columnAsArray = Array.from(columnAs, ([table, col]) => ({
            table,
            col,
          }));

          columnAsArray.forEach((element, index) => {
            result += ` let $mapColumn${index} := map {`;
            // const columns = columnAs.get(key);

            element.col.forEach((val: any, idxcol: any) => {
              result += `'${val.column}' ${this.version.mapOperator} '${val.as}'`;
              if (idxcol < element.col.length - 1) {
                result += ",";
              }
            });
            result += `} `;
          });
          result += ` where ${joinOn}`;
          result += ` return element{'result'}{(`;
          columnAsArray.forEach((element, idx) => {
            result += `for $col${idx} in map:keys($mapColumn${idx}) return element{$mapColumn${idx}($col${idx})}{$element${
              element.table
            }[local-name()=$col${idx}]/${
              moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
            }}`;
            if (idx < columnAsArray.length - 1) {
              result += ",";
            }
          });
          result += `)}`;
        }
      }
    }

    if (joinType == "LEFT JOIN") {
      // let spatialTypes = this.supportedSpatialType.find(element => {
      //   return element.extType == this.spatialNamespace.prefix;
      // });
      // let tempSpatialTypes: any[] = [];
      // spatialTypes?.types.forEach(element => {
      //   tempSpatialTypes.push(`local-name()='${element}'`);
      // });
      // const extensionQuery = this.constructExtensionQuery(
      //   this.spatialNamespace.prefix,
      //   collection[0].name,
      //   moduleVersion,
      //   projection
      // );
      // const colQueryGeo = this.constructExtensionQuery(
      //   this.spatialNamespace.prefix,
      //   collection[1].name,
      //   moduleVersion,
      //   projection
      // );
      // result += ` for $${collection[0].name}i in ${this.version.getDocFunc(
      //   collection[0].name,
      //   this.db_name
      // )}//${extensionQuery.path}${where[0].length > 0 ? `[${where[0]}]` : ""}`;
      // //get columns for projecting the null values
      // result += ` let $column${collection[1].name} := for $${
      //   collection[1].name
      // }col in ${this.version.getDocFunc(collection[1].name, this.db_name)}//${
      //   extensionQuery.path
      // }[1]/* return `;
      // if (this.spatialNamespace.prefix == "gml") {
      //   result += ` element{if(${colQueryGeo.spatialSelectionNoCondition})then('geometry')else($${collection[1].name}col/local-name())}{} `;
      // }
      // if (this.spatialNamespace.prefix == "kml") {
      //   result += ` if(${colQueryGeo.spatialSelectionNoCondition})
      //   then(element{'geometry'}{})
      // else if($${collection[1].name}col/local-name()='ExtendedData')
      //     then(for $extendeddata in $${collection[1].name}col/*/*
      //     return element{$extendeddata/@name}{}
      //     )
      // else()`;
      // }
      // //loop in second table joined with table 1
      // result += `let $element${collection[1].name} := for $${
      //   collection[1].name
      // }i in ${this.version.getDocFunc(collection[1].name, this.db_name)}//${
      //   extensionQuery.path
      // }${where[1].length > 0 ? `[${where[1]}]` : ""}`;
      // result = constructTableQuery(result, collection[1], projection[1]);
      // result += ` where ${joinOn} return $element${collection[1].name}`;
      // result = constructTableQuery(result, collection[0], projection[0]);
      // if (columnAs) {
      //   if (columnAs == "*") {
      //     result += ` let $element := element {'result'}{(`;
      //     collection.forEach((val, idx) => {
      //       if (idx > 0) {
      //         result += `if(fn:exists($element${val.name})) then($element${val.name}) else($column${val.name})`;
      //       } else {
      //         result += `$element${val.name}`;
      //       }
      //       if (idx < collection.length - 1) {
      //         result += `,`;
      //       }
      //     });
      //     result += `)}`;
      //     result += ` return element {'result'}{for $node in distinct-values($element/*/local-name()) return $element/*[local-name() eq $node][1]}`;
      //   } else {
      //     const columnAsArray = Array.from(columnAs, ([table, col]) => ({
      //       table,
      //       col,
      //     }));
      //     columnAsArray.forEach((element, index) => {
      //       result += ` let $mapColumn${index} := map {`;
      //       // const columns = columnAs.get(key);
      //       element.col.forEach((val: any, idxcol: any) => {
      //         result += `'${val.column}' := '${val.as}'`;
      //         if (idxcol < element.col.length - 1) {
      //           result += ",";
      //         }
      //       });
      //       result += `} `;
      //     });
      //     result += ` return element{'result'}{(`;
      //     columnAsArray.forEach((element, idx) => {
      //       result += `for $col${idx} in map:keys($mapColumn${idx}) return element{$mapColumn${idx}($col${idx})}{if(fn:exists($element${
      //         element.table
      //       }))then($element${element.table}[local-name()=$col${idx}]/${
      //         moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
      //       })else()}`;
      //       if (idx < columnAsArray.length - 1) {
      //         result += ",";
      //       }
      //     });
      //     result += `)}`;
      //   }
      // }
    }
    if (joinType == "RIGHT JOIN") {
      // let collectionReversed = collection.reverse();
      // let whereReversed = where.reverse();
      // let projectionReversed = projection.reverse();
      // let spatialTypes = this.supportedSpatialType.find(element => {
      //   return element.extType == this.spatialNamespace.prefix;
      // });
      // let tempSpatialTypes: any[] = [];
      // spatialTypes?.types.forEach(element => {
      //   tempSpatialTypes.push(`local-name()='${element}'`);
      // });
      // const extensionQuery = this.constructExtensionQuery(
      //   this.spatialNamespace.prefix,
      //   collectionReversed[0].name,
      //   moduleVersion,
      //   projection
      // );
      // const colQueryGeo = this.constructExtensionQuery(
      //   this.spatialNamespace.prefix,
      //   collectionReversed[1].name,
      //   moduleVersion,
      //   projectionReversed
      // );
      // result += ` for $${
      //   collectionReversed[0].name
      // }i in ${this.version.getDocFunc(
      //   collectionReversed[0].name,
      //   this.db_name
      // )}//${extensionQuery.path}${
      //   whereReversed[0].length > 0 ? `[${whereReversed[0]}]` : ""
      // }`;
      // //get columns for projecting the null values
      // result += ` let $column${collectionReversed[1].name} := for $${
      //   collectionReversed[1].name
      // }col in ${this.version.getDocFunc(
      //   collectionReversed[1].name,
      //   this.db_name
      // )}//${extensionQuery.path}[1]/* return `;
      // if (this.spatialNamespace.prefix == "gml") {
      //   result += ` element{if(${colQueryGeo.spatialSelectionNoCondition})then('geometry')else($${collectionReversed[1].name}col/local-name())}{} `;
      // }
      // if (this.spatialNamespace.prefix == "kml") {
      //   result += ` if(${colQueryGeo.spatialSelectionNoCondition})
      //   then(element{'geometry'}{})
      // else if($${collectionReversed[1].name}col/local-name()='ExtendedData')
      //     then(for $extendeddata in $${collectionReversed[1].name}col/*/*
      //     return element{$extendeddata/@name}{}
      //     )
      // else()`;
      // }
      // //loop in second table joined with table 1
      // result += `let $element${collectionReversed[1].name} := for $${
      //   collectionReversed[1].name
      // }i in ${this.version.getDocFunc(
      //   collectionReversed[1].name,
      //   this.db_name
      // )}//${extensionQuery.path}${
      //   whereReversed[1].length > 0 ? `[${whereReversed[1]}]` : ""
      // }`;
      // result = constructTableQuery(
      //   result,
      //   collectionReversed[1],
      //   projectionReversed[1]
      // );
      // result += ` where ${joinOn} return $element${collectionReversed[1].name}`;
      // result = constructTableQuery(
      //   result,
      //   collectionReversed[0],
      //   projectionReversed[0]
      // );
      // if (columnAs) {
      //   if (columnAs == "*") {
      //     result += ` let $element := element {'result'}{(`;
      //     collectionReversed.forEach((val, idx) => {
      //       if (idx > 0) {
      //         result += `if(fn:exists($element${val.name})) then($element${val.name}) else($column${val.name})`;
      //       } else {
      //         result += `$element${val.name}`;
      //       }
      //       if (idx < collectionReversed.length - 1) {
      //         result += `,`;
      //       }
      //     });
      //     result += `)}`;
      //     result += ` return element {'result'}{for $node in distinct-values($element/*/local-name()) return $element/*[local-name() eq $node][1]}`;
      //   } else {
      //     const columnAsArray = Array.from(columnAs, ([table, col]) => ({
      //       table,
      //       col,
      //     })).reverse();
      //     columnAsArray.forEach((element, index) => {
      //       result += ` let $mapColumn${index} := map {`;
      //       // const columns = columnAs.get(key);
      //       element.col.forEach((val: any, idxcol: any) => {
      //         result += `'${val.column}' := '${val.as}'`;
      //         if (idxcol < element.col.length - 1) {
      //           result += ",";
      //         }
      //       });
      //       result += `} `;
      //     });
      //     result += ` return element{'result'}{(`;
      //     columnAsArray.forEach((element, idx) => {
      //       result += `for $col${idx} in map:keys($mapColumn${idx}) return element{$mapColumn${idx}($col${idx})}{if(fn:exists($element${
      //         element.table
      //       }))then($element${element.table}[local-name()=$col${idx}]/${
      //         moduleVersion?.getSTAsTextfunc() ? "text()" : "*"
      //       })else()}`;
      //       if (idx < columnAsArray.length - 1) {
      //         result += ",";
      //       }
      //     });
      //     result += `)}`;
      //   }
      // }
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
    const moduleVersion = this.version.modules.find(
      val => val.extension === this.spatialNamespace.prefix
    );
    const constructTableQuery = (
      result: string,
      collection: any,
      projection: any
    ) => {
      const extensionQuery = this.constructExtensionQuery(
        this.spatialNamespace.prefix,
        collection.name,
        moduleVersion,
        projection
      );
      result += `
      let $element${collection.name} := for $${collection.name}j in $${collection.name}i[1]/${projection.columns}
        return
        ${extensionQuery.spatialTypeSelection}
        ${extensionQuery.retrieveCustomDataCondition}
      `;
      return result;
    };

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
      const extensionQuery = this.constructExtensionQuery(
        this.spatialNamespace.prefix,
        collection,
        moduleVersion,
        projection
      );
      let whereQuery = "";
      if (where.length > 0) {
        whereQuery = `[${where}]`;
      }

      result += `for $${collection}i in ${this.version.getDocFunc(
        collection,
        this.db_name
      )}//${extensionQuery.path}${whereQuery}`;
      if (groupby.length > 0) {
        result += ` group by $${collection}group := ${groupby} `;
      }
      let projectionResult = "";
      if (
        projection.columns.length > 0 ||
        projection.childColumns.length > 0 ||
        projection.extendedColumns.length > 0
      ) {
        result = constructTableQuery(result, { name: collection }, projection);
        projectionResult = `$element${collection}`;
      }

      let funcProjQuery = "";
      if (projection.funcColumns.length > 0) {
        funcProjQuery += `${projection.funcColumns}`;
      }

      result += ` return element {'result'}
      {(${funcProjQuery}${
        projectionResult.length > 0 && funcProjQuery.length > 0 ? `,` : ``
      }${projectionResult})}`;
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
          moduleVersion
        );
      } else {
        const extensionQuery = this.constructExtensionQuery(
          this.spatialNamespace.prefix,
          collection[0].name,
          moduleVersion,
          projection
        );
        let whereQuery = "";
        if (where[0].length > 0) {
          whereQuery = `[${where[0]}]`;
        }

        result += `for $${collection[0].name}i in ${this.version.getDocFunc(
          collection[0].name,
          this.db_name
        )}//${extensionQuery.path}${whereQuery}`;
        if (groupby[0].length > 0) {
          result += ` group by $${collection[0].name}group := ${groupby[0]} `;
        }
        let projectionResult = "";

        if (
          projection[0].columns.length > 0 ||
          projection[0].childColumns.length > 0
        ) {
          result = constructTableQuery(result, collection[0], projection[0]);
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

    console.log(result);
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

  constructSelectionQuery(where: any): string {
    // console.log(where);

    if (!where) {
      return "";
    }

    const conditionalOperators = ["AND", "OR"];

    const recursion = (where: any, numOfOr: number, depth: number): string => {
      let selection = "";
      const { operator } = where;

      if (operator == null) {
        if (where.type == "bool") {
          return "";
        }
        return selection;
      }

      if (where.left.type === "function" || where.right.type === "function") {
        return this.constructFunctionQuery(where);
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
        selection += `${resultLeft}${translation} ${resultRight}`;
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
        selectionPath += `${access_col}${column}`;
      }
      if (this.spatialNamespace.prefix == "kml") {
        // *:ExtendedData/*/*[@name='nama']='Masjid Algufron Malendeng'
        selectionPath += `${access_col}ExtendedData/*/*[@name='${column}']`;
      }
      if (type === "number" || type === "string") {
        if (column.includes("_attribute__")) {
          let columnAttr = column.split("__");
          if (columnAttr.length == 2) {
            selection += `@${columnAttr[1]} ${translation} ${
              type === "number" ? value : `'${value}'`
            } `;
          }
          if (columnAttr.length == 3) {
            if (this.spatialNamespace.prefix == "gml") {
              selection += `${access_col}${columnAttr[1]}`;
            }
            if (this.spatialNamespace.prefix == "kml") {
              selection += `${access_col}ExtendedData/*/*[@name='${columnAttr[1]}']`;
            }
            selection += `/@${columnAttr[2]} ${translation} ${
              type === "number" ? value : `'${value}'`
            } `;
          }
        } else {
          selection += `${selectionPath} ${translation} ${
            type === "number" ? value : `'${value}'`
          } `;
        }
      } else if (type === "null") {
        if (operator === "IS") {
          selection += `fn:exists(${selectionPath}/text()) `;
        } else if (operator === "IS NOT") {
          selection += `not(fn:exists(${selectionPath}/text()))`;
        }
      } else if (type === "expr_list") {
        selection += `${selectionPath} ${translation} (`;
        const values = value as any[];
        const lastVal = values.pop();
        for (const val of values) {
          if (val.type === "number") {
            selection += `${val.value}, `;
          } else {
            selection += `"${val.value}", `;
          }
        }
        if (lastVal.type === "number") {
          selection += `${lastVal.value})`;
        } else {
          selection += `"${lastVal.value}")`;
        }
      }

      return selection;
    };

    const selection = recursion(where, 0, 0);
    // console.log(selection, "selection");

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
        columns: "(*|@*)",
        childColumns: `($${childProjection}|$${childProjection}/@*)`,
        funcColumns: "",
        extendedColumns: "*",
      };
    }
    let tempresultArr: string[] = [];
    let tempchildResultArr: string[] = [];
    let tempExtendedArr: string[] = [];
    const ignoreQName = "*:";
    let funcArr: string[] = [];
    let arrColumns = [...columns];
    arrColumns.forEach((column, index) => {
      let tempresult = ``;
      let tempchildResult = ``;
      let tempExtended = ``;
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
          tempresult += `*[${tempSpatialTypes.join(" or ")}]`;
        }
        if (this.spatialNamespace.prefix == "kml") {
          tempresult += `${tempSpatialTypes.join(" or ")}`;
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
          if (this.spatialNamespace.prefix == "gml") {
            pathProjection += `/*:${func_detail.colname}`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            pathProjection += `/*:ExtendedData/*/*[@name='${func_detail.colname}']`;
          }
          funcArr.push(`element{'_func__${func_projection.name}__${
            func_detail.colname
          }'}{${func_projection.name}($${collection.name}i${
            func_detail.colname == "*" ? "" : pathProjection
          }
            )}`);
        } else if (column.includes("_attribute__")) {
          let columnAttr = column.split("__");
          if (columnAttr.length == 2) {
            if (this.spatialNamespace.prefix == "kml") {
              tempExtended += `@${columnAttr[1]}`;
            } else {
              tempresult += `@${columnAttr[1]}`;
            }
          }
          if (columnAttr.length == 3) {
            if (this.spatialNamespace.prefix == "kml") {
              tempchildResult += `$${childProjection}[@name='${columnAttr[1]}']/@${columnAttr[2]}`;
              tempExtended += `@name='${columnAttr[1]}'`;
            }
            if (this.spatialNamespace.prefix == "gml") {
              tempchildResult += `$${childProjection}[local-name()='${columnAttr[1]}']/@${columnAttr[2]}`;
            }
          }
        } else {
          if (this.spatialNamespace.prefix == "gml") {
            tempresult += `${ignoreQName}${column}`;
          }
          if (this.spatialNamespace.prefix == "kml") {
            tempExtended += `@name='${column}'`;
          }
        }
      }
      if (tempresult.length > 0) {
        tempresultArr.push(tempresult);
      }
      if (tempchildResult.length > 0) {
        tempchildResultArr.push(tempchildResult);
      }
      if (tempExtended.length > 0) {
        tempExtendedArr.push(tempExtended);
      }
    });
    let result = ``;
    let childResult = ``;
    let extendedResult = ``;
    if (tempresultArr.length > 0 || tempExtendedArr.length > 0) {
      tempchildResultArr.push(`$${childProjection}`);
      if (this.spatialNamespace.prefix == "kml") {
        result += `*:ExtendedData`;
      }
    }
    if (tempresultArr.length > 0) {
      if (this.spatialNamespace.prefix == "kml") {
        result = `(*[${tempresultArr.join(" or ")}] | *:ExtendedData)`;
      }
      if (this.spatialNamespace.prefix == "gml") {
        result = `(${tempresultArr.join(" | ")})`;
      }
    }
    if (tempExtendedArr.length > 0) {
      extendedResult = `*[${tempExtendedArr.join(" or ")}]`;
    }
    if (tempchildResultArr.length > 0) {
      childResult = `(${tempchildResultArr.join(" | ")})`;
    }
    // console.log(tempresultArr, "tempres", tempchildResultArr, "tempchild");
    let funcResult = funcArr.join(",");
    // console.log(result, childResult);

    return {
      columns: result,
      childColumns: childResult,
      funcColumns: funcResult,
      extendedColumns: extendedResult,
    };
  }
  constructGroupByQuery(groupby: any, collection: any): string {
    if (!groupby || groupby.length == 0) {
      return "";
    }
    let groupbyQuery = ``;
    groupby.forEach((el: any, idx: number) => {
      groupbyQuery += `$${el.table}i/`;
      if (this.spatialNamespace.prefix == "gml") {
        groupbyQuery += `*:${el.column}`;
      }
      if (this.spatialNamespace.prefix == "kml") {
        groupbyQuery += `*:ExtendedData/*/*[@name='${el.column}']`;
      }
      if (idx < groupby.length - 1) {
        groupbyQuery += `,`;
      }
    });
    console.log(groupbyQuery, groupby, "groupbyQuery");

    return groupbyQuery;
  }
}

export { XMLExtension };
