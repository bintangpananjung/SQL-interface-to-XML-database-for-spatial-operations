import { From, Parser, Select } from "flora-sql-parser";
import { cond, result } from "lodash";
import { Extension } from "../extension/extension";
import { DEFAULT_TABLE } from "./constant";
import { rebuildWhere } from "./sqlrebuilder";

function filterWhereStatement(
  tree: Select,
  driver: Extension
): { supportedClauses: any; unsupportedClauses: any[] } {
  let supportedClauses: any = {};
  let unsupportedClauses: any[] = [];
  tree
    .from!.filter(val => !val.expr)
    .forEach(val => {
      const from = val as From;
      supportedClauses[from.as as string] = [];
    });

  if (!tree.where) {
    for (const key in supportedClauses) {
      if (key != DEFAULT_TABLE) {
        supportedClauses[key] = rebuildWhere([]);
      }
    }
    return {
      supportedClauses,
      unsupportedClauses,
    };
  }

  const { where } = tree;
  const conditionList: any[] = [];
  supportedClauses[DEFAULT_TABLE] = [];
  const findAllCondition = (where: any) => {
    const { operator } = where;
    if (!(operator === "AND")) {
      conditionList.push(where);
      return;
    }
    findAllCondition(where.left);
    findAllCondition(where.right);
  };
  findAllCondition(where);

  const isSupportedClause = (
    clause: any
  ): { isSupported: boolean; table: string } => {
    const isSupportedCondition = (
      condition: any
    ): { isSupported: boolean; table: string } => {
      let supportedFunction = true;
      if (condition.type === "function") {
        supportedFunction = false;
      }

      let supportedOperator: boolean = true;
      if (condition.type === "binary_expr") {
        supportedOperator = driver.supportedOperators.some(
          val => val.origin === condition.operator
        );
      }

      const allSupportedTypes = [
        "binary_expr",
        "unary_expr",
        "column_ref",
        "function",
      ];
      const supportedType =
        allSupportedTypes.includes(condition.type) ||
        driver.supportedTypes.includes(condition.type);

      let isNotQuery = true;
      if (Array.isArray(condition.value)) {
        if (condition.value[0].type === "select") {
          isNotQuery = false;
        }
      }

      let table: string = DEFAULT_TABLE;
      if (condition.type === "column_ref") {
        table = condition.table;
      } else if (condition.type === "function") {
        table = condition.args.value[0].table;
      }
      return {
        isSupported:
          supportedFunction && supportedType && supportedOperator && isNotQuery,
        table,
      };
    };

    const isSupportedFunction = (
      ast: any
    ): { isSupported: boolean; table: string } => {
      let regexPatterns = driver.supportedFunctions;
      let isSupported = false;
      let table = "";
      for (const pattern of regexPatterns) {
        const funcStr = astToFuncStr(ast);
        pattern.lastIndex = 0;
        const result = pattern.exec(funcStr);
        if (result == null) {
          continue;
        }
        isSupported = true;
        table = result.groups!.tname;
        break;
      }
      let funcOps = ast.left;
      if (funcOps.type !== "function") {
        funcOps = ast.right;
      }
      if (table === "") {
        table = funcOps.args.value[0].table;
      }
      return {
        isSupported,
        table,
      };
    };

    const isSupportedClauseAndChildCondition = (
      clause: any
    ): { isSupported: boolean; table: string } => {
      const { left, right, operator } = clause;
      let isLeftSupported: any;
      let isRightSupported: any;
      let isClauseSupported = { isSupported: true, table: DEFAULT_TABLE };

      if (!(operator === "AND" || operator === "OR")) {
        if (
          clause.left.type === "function" ||
          clause.right.type === "function"
        ) {
          const result = isSupportedFunction(clause);
          return result;
        }

        isLeftSupported = isSupportedCondition(left);
        isRightSupported = isSupportedCondition(right);
        isClauseSupported = isSupportedCondition(clause);
      } else {
        isLeftSupported = isSupportedClauseAndChildCondition(left);
        isRightSupported = isSupportedClauseAndChildCondition(right);
      }

      const tableLeft = isLeftSupported.table;
      const tableRight = isRightSupported.table;

      let isRefOneTable: boolean;
      if (
        tableLeft === DEFAULT_TABLE ||
        tableRight === DEFAULT_TABLE ||
        tableLeft === tableRight
      ) {
        isRefOneTable = true;
      } else {
        isRefOneTable = false;
      }

      table = tableLeft;
      if (table === DEFAULT_TABLE) {
        table = tableRight;
      }

      return {
        isSupported:
          isRefOneTable &&
          isClauseSupported.isSupported &&
          isLeftSupported.isSupported &&
          isRightSupported.isSupported,
        table: table,
      };
    };

    let table = DEFAULT_TABLE;
    if (!(clause.operator === "OR" || clause.operator === "AND")) {
      if (clause.type === "binary_expr") {
        return isSupportedClauseAndChildCondition(clause);
      } else {
        return isSupportedCondition(clause);
      }
    }

    return isSupportedClauseAndChildCondition(clause);
  };

  for (const condition of conditionList) {
    const { isSupported, table } = isSupportedClause(condition);
    if (isSupported) {
      if (table in supportedClauses) {
        supportedClauses[table].push(condition);
      } else {
        supportedClauses[table] = [condition];
      }
    } else {
      unsupportedClauses.push(condition);
    }
  }

  for (const key in supportedClauses) {
    if (key != DEFAULT_TABLE) {
      supportedClauses[key].push(...supportedClauses[DEFAULT_TABLE]);

      supportedClauses[key] = rebuildWhere(supportedClauses[key]);
    }
  }
  delete supportedClauses[DEFAULT_TABLE];

  return {
    supportedClauses,
    unsupportedClauses,
  };
}

function fillAsFrom(tree: Select): Select {
  let froms = tree.from! as From[];
  for (let from of froms) {
    if (from.as == null) {
      from.as = from.table;
    }
  }
  return tree;
}

function fillTableWhere(tree: Select): Select {
  if (!tree.where) {
    return tree;
  }
  const mapAs: any = {};
  tree
    .from!.filter(val => !val.expr)
    .forEach(val => {
      const from = val as From;
      const as = from.as as string;
      mapAs[as] = from.table;
    });
  const defaultTable = Object.keys(mapAs)[0];

  const recursive = (clause: any): any => {
    const { operator, type } = clause;
    if (!(operator === "AND" || operator === "OR")) {
      if (type == "binary_expr") {
        if (clause.left.type === "column_ref") {
          if (!clause.left.table) {
            clause.left.table = defaultTable;
          }
        }
        if (clause.right.type === "column_ref") {
          if (!clause.right.table) {
            clause.right.table = defaultTable;
          }
        }
      }
      return clause;
    }
    clause.left = recursive(clause.left);
    clause.right = recursive(clause.right);
    return clause;
  };

  tree.where = recursive(tree.where);

  return tree;
}

function nullRemoval(tree: Select): Select {
  let newobj: any = {};
  const map: { [key: string]: any } = tree;
  for (const key in tree) {
    if (map[key] !== null) {
      newobj[key] = map[key];
    }
  }
  return newobj as Select;
}

function fillAsSelect(tree: Select) {
  const mapAs: any = {};
  tree
    .from!.filter(val => !val.expr)
    .forEach(val => {
      const from = val as From;
      const as = from.as as string;
      mapAs[as] = from.table;
    });
  const defaultTable = Object.keys(mapAs)[0];

  const recursive = (ast: any) => {
    if (
      ast == null ||
      ["boolean", "string", "number", "undefined", "null"].includes(typeof ast)
    ) {
      return;
    }

    if (ast.type == "column_ref") {
      const { table, column } = ast;
      if (table == null) {
        ast.table = defaultTable;
      }
    }

    for (const key in ast) {
      recursive(ast[key]);
    }
  };

  recursive(tree.columns);
  return tree;
}

function fixAst(tree: Select) {
  let result = nullRemoval(tree);
  result = fillAsFrom(result);
  result = fillAsSelect(result);
  return fillTableWhere(result);
}

function buildAst(query: string, parser: Parser): Select {
  let result = parser.parse(query) as Select;

  return fixAst(result);
}

function astToFuncStr(ast: any) {
  if (ast.type !== "binary_expr") {
    return "";
  }
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
    if (ast.type === "bool") {
      return ast.value;
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

export {
  filterWhereStatement,
  buildAst,
  fixAst,
  nullRemoval,
  fillAsFrom,
  fillTableWhere,
};
