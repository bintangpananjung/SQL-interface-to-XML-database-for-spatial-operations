type GeoJSON = {
  type: string;
  geometry: {
    type: string;
    coordinates: any;
  };
  properties: any;
};

type GML = any;

type XMLConfig = {
  //include all function that needed in building query
  version: string[];
  getDocFunc(collection: string, db_name: string, client?: any): string;
  getCollectionNamesFunc(db_name: string, client?: any): string;
  mapOperator: string;
  modules: {
    supportedSpatialFunctionPrefix: {
      name: string;
      args: number;
      postGISName: string;
    }[];
    getSTAsTextfunc?(node: any): string;
    extension: string;
    namespaceModule: { prefix: string; namespace: string };
  }[];
};

interface Supported {
  origin: string;
  translation: string;
}

interface Extension {
  supportedTypes: string[];
  supportedSelectionFunctions: {
    regex: RegExp;
    matches: string[];
    version?: string[];
  }[];
  supportedProjectionFunctions: {
    regex: RegExp;
    name: string;
    args: number;
    postGISName: string;
    isAggregation: boolean;
  }[];
  supportedOperators: Supported[];
  extensionType: string;
  supportPreExecutionQuery: boolean;
  canJoin: boolean;
  // canAggregationAndGroupBy: boolean;

  connect(): void;
  constructSelectionQuery(where: any): any;
  constructProjectionQuery(columns: Set<string>, collection: any): string;
  constructGroupByQuery?(groupby: any, collection: any): string;
  executePreExecutionQuery?(collection: string): Promise<void>;
  getAllFields(col_name: string): Promise<string[]>;
  getResult(
    collection: string | any[],
    where: string | any[],
    projection: string | any[],
    groupby: string | any[],
    columnAs?: any | undefined
  ): Promise<any>;
  getDbName(): string;
  standardizeData(data: any): any[];
  getCollectionsName(): Promise<string[]>;
  getFieldsData(
    totalGetField: Map<string, Set<string>>,
    finalResult: any[]
  ): Map<string, Set<string>>;
  addSelectTreeColumnsRebuild(sample: any, listColumns: any[]): any;
  addColumnAndMapKeyRebuild(sample: any): {
    columns: any[];
    mapType: any;
  };
  addRowValuesRebuild(dataList: any[], columns: any[], mapType: any): any[];
}

interface XMLInterface extends Extension {
  version: XMLConfig;
  spatialNamespace: { prefix: string; namespace: string };
  spatialModuleNamespaces: Array<any>;
  supportedXMLExtensionType: string[];
  supportedSpatialType: { extType: string; types: string[] }[];
  supportedExtensionCheck(collection: string): string;
  // executeExtensionCheckQuery(collection: string): Promise<void>;
  constructSpatialNamespace: (
    namespace: { prefix: string; namespace: string }[],
    module: boolean
  ) => string; //module=true-> construct module namespace
  constructExtensionQuery(
    extension: any,
    varName: string,
    moduleVersion: any,
    projection?: any
  ): {
    path: string;
    spatialTypeSelection: string;
    retrieveCustomDataCondition: string;
    retrieveCustomDataConditionWithAttr: string;
  };
  // constructJoinQuery(
  //   type: "inner" | "left" | "right" | "full" | "natural",
  //   collection: Array<any>,
  //   where: Array<any>,
  //   projection: Array<any>
  // ): string;
  constructXQuery(
    collection: any,
    spatialNamespace: any,
    where: any,
    groupby: any,
    projection: any
  ): any;
  moduleConfig: XMLConfig[];
  initVersion(): any;
}
export { Extension, Supported, GeoJSON, XMLInterface, GML, XMLConfig };
