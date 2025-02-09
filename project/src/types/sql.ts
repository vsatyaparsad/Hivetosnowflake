export interface SQLMapping {
  hiveType: string;
  snowflakeType: string;
}

export interface ConversionResult {
  success: boolean;
  sql?: string;
  warnings?: string[];
  errors?: string[];
  metadata?: {
    tablesReferenced: string[];
    functionsUsed: string[];
    dataTypesUsed: string[];
    unsupportedFeatures: string[];
  };
}

export interface TableDefinition {
  name: string;
  columns: ColumnDefinition[];
  external?: boolean;
  format?: string;
  clusterBy?: string[];
  comment?: string;
}

export interface ColumnDefinition {
  name: string;
  type: string;
  nullable?: boolean;
  defaultValue?: string;
  comment?: string;
}

export interface ConversionOptions {
  preserveComments?: boolean;
  handleUnsupported?: 'warn' | 'error' | 'ignore';
  validateSyntax?: boolean;
  targetSnowflakeVersion?: string;
}