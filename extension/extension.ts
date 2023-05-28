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
  modules: {
    supportedSpatialFunctionPrefix: {
      name: string;
      args: number;
      postGISName: string;
    }[];
    getSTAsTextfunc?(node: any): string;
    extension: string;
  }[];
};

interface Supported {
  origin: string;
  translation: string;
}

interface Extension {
  supportedTypes: string[];
  supportedFunctions: RegExp[];
  supportedOperators: Supported[];
  extensionType: string;

  connect(): void;
  constructSelectionQuery(where: any): any;
  constructProjectionQuery(columns: Set<string>): string;
  getAllFields(col_name: string): Promise<string[]>;
  getResult(
    collection: string,
    where: string,
    projection: string
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
  getRowValuesRebuild(dataList: any[], columns: any[], mapType: any): any[];
}

interface XMLNamespace extends Extension {
  version: XMLConfig;
  spatialNamespace: { prefix: string; namespace: string };
  spatialModuleNamespaces: Array<any>;
  supportedXMLExtensionType: string[];
  supportedExtensionCheck(collection: string): string;
  executeExtensionCheckQuery(collection: string): Promise<void>;
  supportedSpatialType: string[];
  constructSpatialNamespace: (
    namespace: { prefix: string; namespace: string }[],
    module: boolean
  ) => string; //module=true-> construct module namespace
  constructExtensionQuery(extension: any): {
    path: string;
    spatialSelectionNoCondition: string;
    spatialSelectionWithCondition: string;
  };
  constructXQuery(
    collection: any,
    spatialNamespace: any,
    where: any,
    projection: any
  ): any;
  moduleConfig: XMLConfig[];
  initVersion(): any;
}
export { Extension, Supported, GeoJSON, XMLNamespace, GML, XMLConfig };
