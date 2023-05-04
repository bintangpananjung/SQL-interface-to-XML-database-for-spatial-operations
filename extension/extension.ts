type GeoJSON = {
  type: string;
  geometry: {
    type: string;
    coordinates: any;
  };
  properties: any;
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
  constructSelectionQuery(where: any): string;
  constructProjectionQuery(columns: Set<string>): string;
  getAllFields(col_name: string): Promise<string[]>;
  getResult(
    collection: string,
    where: string,
    projection: string
  ): Promise<any>;
  getDbName(): string;
  standardizeData(data: any): GeoJSON[];
  getCollectionsName(): Promise<string[]>;
}

interface XMLNamespace extends Extension {
  spatialNamespace: { prefix: string; namespace: string }[];
  spatialModuleNamespaces: { prefix: string; namespace: string }[];
  constructSpatialNamespace: (
    namespace: { prefix: string; namespace: string }[],
    module: boolean
  ) => string; //module=true-> construct module namespace
  supportedXMLExtensionType: string[];
  supportedExtensionCheck(collection: string): Promise<any>;
  supportedSpatialType: string[];
  supportedFunctionPrefix: {
    name: string;
    args: number;
    postGISName: string;
  }[];
}
export { Extension, Supported, GeoJSON, XMLNamespace };
