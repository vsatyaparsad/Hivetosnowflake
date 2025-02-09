# Hive to Snowflake SQL Converter Documentation

## Overview
The Hive to Snowflake SQL Converter is a comprehensive tool that automatically converts Hive SQL syntax to Snowflake SQL syntax. It handles a wide range of SQL constructs, data types, functions, and provides detailed warnings about compatibility issues and transformations.

## Features

### 1. Data Types

#### Basic Types
| Hive Type | Snowflake Type | Notes |
|-----------|----------------|-------|
| STRING | VARCHAR | Text data |
| INT/INTEGER | NUMBER(38,0) | Full precision integers |
| BIGINT | NUMBER(38,0) | Large integers |
| SMALLINT | NUMBER(5,0) | Small integers |
| TINYINT | NUMBER(3,0) | Very small integers |
| DECIMAL | NUMBER | Configurable precision/scale |
| DOUBLE | DOUBLE | Double precision floating point |
| FLOAT | FLOAT | Single precision floating point |
| BOOLEAN | BOOLEAN | True/false values |
| BINARY | BINARY | Binary data |
| TIMESTAMP | TIMESTAMP_NTZ | Timestamps without timezone |
| DATE | DATE | Date values |

#### Complex Types
| Hive Type | Snowflake Type | Size Limits |
|-----------|----------------|-------------|
| ARRAY<T> | ARRAY | 16MB per array |
| MAP<K,V> | OBJECT | 16MB per object |
| STRUCT<...> | OBJECT | 16MB per object |

### 2. Functions

#### Date/Time Functions
| Hive Function | Snowflake Function | Example |
|---------------|-------------------|---------|
| UNIX_TIMESTAMP() | TO_NUMBER(CURRENT_TIMESTAMP(3)::timestamp_tz AT TIME ZONE 'UTC') | Current Unix timestamp |
| UNIX_TIMESTAMP(col) | TO_NUMBER(col::timestamp_tz AT TIME ZONE 'UTC') | Convert timestamp to Unix epoch |
| FROM_UNIXTIME(col) | TO_TIMESTAMP(col) | Convert Unix epoch to timestamp |
| DATE_ADD(date, days) | DATEADD(DAY, days, date) | Add days to date |
| DATE_SUB(date, days) | DATEADD(DAY, -days, date) | Subtract days from date |
| ADD_MONTHS(date, n) | DATEADD(MONTH, n, date) | Add months to date |
| LAST_DAY(date) | LAST_DAY(date) | Last day of month |

#### String Functions
| Hive Function | Snowflake Function | Example |
|---------------|-------------------|---------|
| CONCAT_WS(sep, str1, str2) | ARRAY_TO_STRING([str1, str2], sep) | Concatenate with separator |
| INSTR(str, substr) | POSITION(substr IN str) | Find substring position |
| SUBSTRING_INDEX(str, delim, count) | SPLIT_PART(str, delim, count) | Split and get part |
| REGEXP_REPLACE(str, pattern, replace) | REGEXP_REPLACE(str, pattern, replace) | Replace using regex |

#### Collection Functions
| Hive Function | Snowflake Function | Example |
|---------------|-------------------|---------|
| COLLECT_SET(col) | ARRAY_AGG(DISTINCT col) | Aggregate unique values |
| COLLECT_LIST(col) | ARRAY_AGG(col) | Aggregate values into array |
| ARRAY_CONTAINS(arr, val) | ARRAY_CONTAINS(arr, val) | Check array contains value |
| SIZE(arr) | ARRAY_SIZE(arr) | Get array length |

#### JSON Functions
| Hive Function | Snowflake Function | Example |
|---------------|-------------------|---------|
| GET_JSON_OBJECT(json, path) | GET_PATH(PARSE_JSON(json), path) | Extract JSON value |
| JSON_TUPLE(json, k1, k2) | Multiple GET_PATH calls | Extract multiple values |

### 3. SQL Constructs

#### CREATE TABLE
```sql
-- Hive
CREATE EXTERNAL TABLE IF NOT EXISTS table_name (
  col1 STRING,
  col2 INT
)
PARTITIONED BY (dt STRING)
CLUSTERED BY (id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/path/to/data'
TBLPROPERTIES ('key'='value');

-- Converted to Snowflake
CREATE OR REPLACE TABLE table_name (
  col1 VARCHAR,
  col2 NUMBER(38,0)
)
CLUSTER BY (id)
FILE_FORMAT = PARQUET;
```

#### File Formats
| Hive Format | Snowflake Format | Notes |
|-------------|------------------|-------|
| TEXTFILE | CSV | Text files |
| SEQUENCEFILE | PARQUET | Converted to Parquet |
| RCFILE | PARQUET | Converted to Parquet |
| ORC | PARQUET | Converted to Parquet |
| PARQUET | PARQUET | Native support |
| AVRO | PARQUET | Converted to Parquet |
| JSONFILE | JSON | JSON files |

#### INSERT Statements
```sql
-- Hive
INSERT OVERWRITE TABLE target
PARTITION (dt = '2024-02-09')
SELECT * FROM source;

-- Converted to Snowflake
INSERT INTO target
SELECT * FROM source;
```

#### LATERAL VIEW
```sql
-- Hive
SELECT col1, item
FROM table
LATERAL VIEW EXPLODE(items) exploded AS item;

-- Converted to Snowflake
SELECT col1, exploded.value as item
FROM table,
LATERAL FLATTEN(input => items) exploded;
```

### 4. Optimization Features

#### Bucketing and Clustering
```sql
-- Hive
CLUSTERED BY (id) INTO 8 BUCKETS;

-- Converted to Snowflake
CLUSTER BY (id);
```

#### Distribution and Sorting
| Hive Clause | Snowflake Equivalent | Notes |
|-------------|---------------------|-------|
| DISTRIBUTE BY | ORDER BY | For result ordering |
| SORT BY | ORDER BY | For result ordering |
| CLUSTER BY | CLUSTER BY | For data organization |

### 5. SET Commands
The converter removes all Hive SET commands, including:
- Configuration settings (SET hive.*)
- MapReduce settings (SET mapred.*)
- Tez settings (SET tez.*)
- Spark settings (SET spark.*)
- Dynamic partition settings
- Parallel execution settings
- Compression settings
- Memory settings
- Join settings
- Bucketing settings
- Statistics settings
- Optimization settings

### 6. Unsupported Features

1. Hive-Specific Hints
   - MAPJOIN
   - STREAMTABLE
   - BROADCAST
   - SKEWJOIN

2. Storage Properties
   - STORED AS
   - ROW FORMAT
   - SERDE
   - INPUTFORMAT/OUTPUTFORMAT

3. Table Properties
   - LOCATION
   - TBLPROPERTIES

4. Partitioning
   - PARTITIONED BY
   - Dynamic partitioning

## Best Practices

### 1. Data Type Handling
- Review NUMBER precision/scale for numeric types
- Consider column size limits for VARCHAR
- Monitor complex type sizes (16MB limit)

### 2. Performance Optimization
- Use Snowflake clustering for frequently filtered columns
- Consider materialized views for complex queries
- Use appropriate file formats for data loading

### 3. Testing and Validation
- Test converted queries with sample data
- Verify data type conversions
- Check complex type handling
- Validate function results

### 4. Error Handling
- Review all warnings from the converter
- Test error conditions
- Implement appropriate error handling

### 5. Migration Planning
- Plan for large data migrations
- Consider incremental migration strategies
- Test performance at scale

## Troubleshooting

### Common Issues

1. Data Type Mismatches
   - Check precision/scale for numeric types
   - Verify timestamp/date conversions
   - Review complex type conversions

2. Function Compatibility
   - Test function equivalents
   - Verify argument types
   - Check return values

3. Performance Issues
   - Review clustering keys
   - Check join conditions
   - Analyze query plans

### Error Messages

Common error messages and solutions:
1. "Invalid type conversion" - Review data type mappings
2. "Function not found" - Check function compatibility
3. "Complex type size exceeded" - Review data structures

## Limitations

1. Size Limits
   - 16MB limit for complex types
   - Maximum VARCHAR length
   - Number precision limits

2. Functionality
   - No direct equivalent for some Hive features
   - Different partitioning approach
   - Limited hint support

3. Performance
   - Different optimization techniques
   - Varied execution plans
   - Resource management differences