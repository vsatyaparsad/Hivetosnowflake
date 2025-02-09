export const HIVE_PATTERNS = {
  // DDL Patterns
  CREATE_TABLE: /CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)/i,
  DROP_TABLE: /DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)/i,
  ALTER_TABLE: /ALTER\s+TABLE\s+([^\s]+)\s+(ADD|DROP|CHANGE|RENAME|SET|UNSET|PARTITION)/i,
  TRUNCATE_TABLE: /TRUNCATE\s+(?:TABLE\s+)?([^\s;]+)/i,
  
  // DML Patterns
  INSERT_OVERWRITE: /INSERT\s+OVERWRITE\s+(?:TABLE\s+)?([^\s(]+)/i,
  INSERT_INTO: /INSERT\s+INTO\s+(?:TABLE\s+)?([^\s(]+)/i,
  MULTI_INSERT: /FROM\s+([^\s]+)\s+INSERT\s+(?:OVERWRITE|INTO)/i,
  DYNAMIC_PARTITION: /PARTITION\s*\(([^)]+)\)/i,
  
  // Complex Types
  ARRAY_TYPE: /ARRAY\s*<\s*([^>]+)\s*>/i,
  MAP_TYPE: /MAP\s*<\s*([^,]+)\s*,\s*([^>]+)\s*>/i,
  STRUCT_TYPE: /STRUCT\s*<\s*([^>]+)\s*>/i,
  
  // Hive-Specific Features
  LATERAL_VIEW: /LATERAL\s+VIEW\s+(?:OUTER\s+)?(\w+)\s*\(([^)]+)\)\s+(\w+)\s+AS\s+([^,\s]+(?:\s*,\s*[^,\s]+)*)/i,
  DISTRIBUTE_BY: /DISTRIBUTE\s+BY\s+([^;\s]+)/i,
  CLUSTER_BY: /CLUSTER\s+BY\s+([^;\s]+)/i,
  SORT_BY: /SORT\s+BY\s+([^;\s]+)/i,
  
  // Properties and Storage
  STORED_AS: /STORED\s+AS\s+(\w+)/i,
  ROW_FORMAT: /ROW\s+FORMAT\s+(?:DELIMITED|SERDE)\s+([^;\n]+)/i,
  LOCATION: /LOCATION\s+('[^']+'|"[^"]+")/i,
  TBLPROPERTIES: /TBLPROPERTIES\s*\(([^)]+)\)/i,
  
  // Hints and Settings
  MAPJOIN_HINT: /\/\*\+\s*MAPJOIN\s*\(([^)]+)\)\s*\*\//i,
  STREAMTABLE_HINT: /\/\*\+\s*STREAMTABLE\s*\(([^)]+)\)\s*\*\//i,
  SET_STATEMENT: /SET\s+([^=\s]+)\s*=\s*([^;\n]+)/i,
  
  // Functions and Expressions
  UDF_PATTERN: /(?:(?:org\.apache\.hadoop\.hive\.ql\.udf\.generic|org\.apache\.hadoop\.hive\.ql\.udf)\.)(\w+)/i,
  WINDOW_FUNCTION: /OVER\s*\((?:PARTITION\s+BY\s+[^)]+)?(?:\s*ORDER\s+BY\s+[^)]+)?\)/i,
  
  // Comments
  SINGLE_LINE_COMMENT: /--[^\n]*/g,
  MULTI_LINE_COMMENT: /\/\*[^*]*\*+(?:[^/*][^*]*\*+)*\//g
};

export const HIVE_KEYWORDS = [
  'CLUSTERED',
  'SORTED',
  'BUCKETS',
  'SKEWED',
  'STORED',
  'DELIMITED',
  'FIELDS TERMINATED BY',
  'COLLECTION ITEMS TERMINATED BY',
  'MAP KEYS TERMINATED BY',
  'LINES TERMINATED BY',
  'INPUTFORMAT',
  'OUTPUTFORMAT',
  'LOCATION',
  'TABLESAMPLE',
  'SERDEPROPERTIES',
  'SEQUENCEFILE',
  'TEXTFILE',
  'RCFILE',
  'ORC',
  'PARQUET',
  'AVRO',
  'JSONFILE',
  'DISTRIBUTE',
  'CLUSTER',
  'SORT'
];

export const COMPLEX_TYPE_HANDLERS = {
  array: (innerType: string) => `ARRAY`,
  map: (keyType: string, valueType: string) => `OBJECT`,
  struct: (fields: string) => {
    const parsedFields = fields.split(',').map(field => {
      const [name, type] = field.trim().split(':');
      return `${name.trim()} ${type.trim()}`;
    });
    return `OBJECT`;
  }
};

export const HIVE_TO_SNOWFLAKE_STORAGE = {
  'TEXTFILE': 'CSV',
  'SEQUENCEFILE': 'PARQUET',
  'RCFILE': 'PARQUET',
  'ORC': 'PARQUET',
  'PARQUET': 'PARQUET',
  'AVRO': 'PARQUET',
  'JSONFILE': 'JSON'
};