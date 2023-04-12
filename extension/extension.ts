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

interface XMLExtension extends Extension {
  namespaces: string[];
  moduleNamespaces: string[];
}
export { Extension, Supported, GeoJSON, XMLExtension };
