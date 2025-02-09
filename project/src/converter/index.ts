import { Parser } from 'node-sql-parser';
import { format } from 'sql-formatter';
import { functionMappings } from '../mappings/functions';
import { ConversionResult } from '../types/sql';

export class HiveToSnowflakeConverter {
  private input: string;
  private warnings: string[] = [];

  constructor(hiveSql: string) {
    this.input = hiveSql;
  }

  public convert(): ConversionResult {
    try {
      // Preprocess and clean SQL
      let sql = this.preprocessSQL(this.input);

      // Split into statements
      const statements = this.splitStatements(sql);
      
      // Convert each statement
      const convertedStatements = statements
        .filter(stmt => stmt.trim())
        .map(stmt => this.convertStatement(stmt.trim()));

      // Format the final SQL
      const finalSQL = this.formatSQL(convertedStatements.join('\n\n'));

      return {
        success: true,
        sql: finalSQL,
        warnings: this.warnings
      };
    } catch (error) {
      return {
        success: false,
        errors: [error instanceof Error ? error.message : String(error)],
        warnings: this.warnings
      };
    }
  }

  private preprocessSQL(sql: string): string {
    // Remove SET statements
    sql = sql.replace(/^SET\s+[^;]+;/gm, '');

    // Remove Hive hints
    sql = sql.replace(/\/\*\+\s*(?:MAPJOIN|STREAMTABLE|BROADCAST)\([^)]*\)\s*\*\//g, '');

    // Preserve comments but remove Hive-specific markers
    sql = sql.replace(/--\s*#####/g, '--');

    return sql;
  }

  private splitStatements(sql: string): string[] {
    const statements: string[] = [];
    let currentStatement = '';
    let inString = false;
    let stringChar = '';
    let inComment = false;
    let inMultilineComment = false;
    let depth = 0;

    for (let i = 0; i < sql.length; i++) {
      const char = sql[i];
      const nextChar = sql[i + 1] || '';

      // Handle string literals
      if ((char === "'" || char === '"') && !inComment && !inMultilineComment) {
        if (!inString) {
          inString = true;
          stringChar = char;
        } else if (char === stringChar) {
          inString = false;
        }
      }

      // Handle comments
      if (!inString) {
        if (char === '-' && nextChar === '-') {
          inComment = true;
        } else if (char === '/' && nextChar === '*') {
          inMultilineComment = true;
          i++;
        } else if (char === '*' && nextChar === '/' && inMultilineComment) {
          inMultilineComment = false;
          i++;
          continue;
        } else if (char === '\n' && inComment) {
          inComment = false;
        }
      }

      // Handle parentheses
      if (!inString && !inComment && !inMultilineComment) {
        if (char === '(') depth++;
        if (char === ')') depth--;
      }

      // Handle statement separation
      if (char === ';' && !inString && !inComment && !inMultilineComment && depth === 0) {
        if (currentStatement.trim()) {
          statements.push(currentStatement.trim());
        }
        currentStatement = '';
        continue;
      }

      currentStatement += char;
    }

    if (currentStatement.trim()) {
      statements.push(currentStatement.trim());
    }

    return statements.filter(stmt => stmt.length > 0);
  }

  private convertStatement(sql: string): string {
    // Convert CREATE TABLE statements
    if (/CREATE\s+(?:EXTERNAL\s+)?TABLE/i.test(sql)) {
      return this.convertCreateTable(sql);
    }

    // Convert INSERT statements
    if (/INSERT\s+(?:INTO|OVERWRITE)/i.test(sql)) {
      return this.convertInsert(sql);
    }

    // Convert WITH clauses
    if (/WITH\s+\w+\s+AS/i.test(sql)) {
      return this.convertWithClause(sql);
    }

    // Convert general SELECT statements
    return this.convertSelect(sql);
  }

  private convertCreateTable(sql: string): string {
    // Convert CREATE TABLE syntax
    sql = sql.replace(
      /CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i,
      'CREATE OR REPLACE TABLE $1'
    );

    // Convert PARTITIONED BY to CLUSTER BY
    const partitionMatch = sql.match(/PARTITIONED\s+BY\s*\(([^)]+)\)/i);
    if (partitionMatch) {
      const columns = partitionMatch[1]
        .split(',')
        .map(col => col.trim().split(/\s+/)[0]);
      sql = sql.replace(
        /PARTITIONED\s+BY\s*\([^)]+\)/i,
        `CLUSTER BY (${columns.join(', ')})`
      );
    }

    // Convert CLUSTERED BY to CLUSTER BY
    const clusterMatch = sql.match(/CLUSTERED\s+BY\s*\(([^)]+)\)/i);
    if (clusterMatch) {
      sql = sql.replace(
        /CLUSTERED\s+BY\s*\([^)]+\)\s+INTO\s+\d+\s+BUCKETS/i,
        `CLUSTER BY (${clusterMatch[1]})`
      );
    }

    // Remove Hive-specific storage clauses
    sql = sql
      .replace(/STORED\s+AS\s+\w+/gi, '')
      .replace(/ROW\s+FORMAT\s+[^;]+/gi, '')
      .replace(/LOCATION\s+('[^']+'|"[^"]+")/gi, '')
      .replace(/TBLPROPERTIES\s*\([^)]*\)/gi, '');

    // Convert data types
    sql = this.convertDataTypes(sql);

    return sql;
  }

  private convertInsert(sql: string): string {
    // Convert INSERT OVERWRITE
    sql = sql.replace(
      /INSERT\s+OVERWRITE\s+(?:TABLE\s+)?(\w+)/i,
      'INSERT OVERWRITE INTO $1'
    );

    // Convert INSERT INTO
    sql = sql.replace(
      /INSERT\s+INTO\s+(?:TABLE\s+)?(\w+)/i,
      'INSERT INTO $1'
    );

    // Remove PARTITION clause
    sql = sql.replace(/PARTITION\s*\([^)]+\)/i, '');

    return this.convertSelect(sql);
  }

  private convertWithClause(sql: string): string {
    // Convert function calls in WITH clause
    sql = this.convertFunctions(sql);

    // Convert window functions
    sql = this.convertWindowFunctions(sql);

    return sql;
  }

  private convertSelect(sql: string): string {
    // Convert LATERAL VIEW
    sql = sql.replace(
      /LATERAL\s+VIEW\s+(?:OUTER\s+)?EXPLODE\s*\(([^)]+)\)\s+(\w+)\s+AS\s+(\w+)/gi,
      'CROSS JOIN TABLE(FLATTEN(input => $1)) AS $2($3)'
    );

    // Convert DISTRIBUTE BY to ORDER BY
    sql = sql.replace(/DISTRIBUTE\s+BY/gi, 'ORDER BY');

    // Convert SORT BY to ORDER BY
    sql = sql.replace(/SORT\s+BY/gi, 'ORDER BY');

    // Convert functions
    sql = this.convertFunctions(sql);

    // Convert window functions
    sql = this.convertWindowFunctions(sql);

    return sql;
  }

  private convertFunctions(sql: string): string {
    // Convert unix_timestamp
    sql = sql.replace(
      /unix_timestamp\(([^)]*)\)/gi,
      (match, args) => args ? `date_part('epoch', ${args})` : `date_part('epoch', current_timestamp())`
    );

    // Convert date_add/date_sub
    sql = sql.replace(
      /date_add\(([^,]+),\s*(\d+)\)/gi,
      'dateadd(day, $2, $1)'
    );
    sql = sql.replace(
      /date_sub\(([^,]+),\s*(\d+)\)/gi,
      'dateadd(day, -$2, $1)'
    );

    // Convert collect_set/collect_list
    sql = sql.replace(/collect_set\(([^)]+)\)/gi, 'array_agg(DISTINCT $1)');
    sql = sql.replace(/collect_list\(([^)]+)\)/gi, 'array_agg($1)');

    // Convert JSON functions
    sql = sql.replace(
      /get_json_object\(([^,]+),\s*([^)]+)\)/gi,
      'get_path(parse_json($1), $2)'
    );

    return sql;
  }

  private convertWindowFunctions(sql: string): string {
    // Convert OVER clause ranges
    return sql.replace(
      /OVER\s*\(([^)]+)\)/gi,
      (match, contents) => {
        let result = contents
          // Convert RANGE BETWEEN
          .replace(
            /RANGE\s+BETWEEN\s+(\d+)\s+PRECEDING\s+AND\s+CURRENT\s+ROW/gi,
            'ROWS BETWEEN $1 PRECEDING AND CURRENT ROW'
          )
          // Convert ROWS BETWEEN
          .replace(
            /ROWS\s+BETWEEN\s+(\d+)\s+PRECEDING\s+AND\s+CURRENT\s+ROW/gi,
            'ROWS BETWEEN $1 PRECEDING AND CURRENT ROW'
          );
        return `OVER (${result})`;
      }
    );
  }

  private convertDataTypes(sql: string): string {
    return sql
      .replace(/STRING/gi, 'VARCHAR')
      .replace(/INT|INTEGER/gi, 'NUMBER(38,0)')
      .replace(/BIGINT/gi, 'NUMBER(38,0)')
      .replace(/SMALLINT/gi, 'NUMBER(5,0)')
      .replace(/TINYINT/gi, 'NUMBER(3,0)')
      .replace(/DOUBLE/gi, 'DOUBLE')
      .replace(/FLOAT/gi, 'FLOAT')
      .replace(/BOOLEAN/gi, 'BOOLEAN')
      .replace(/BINARY/gi, 'BINARY')
      .replace(/TIMESTAMP/gi, 'TIMESTAMP_NTZ')
      .replace(/DATE/gi, 'DATE')
      .replace(/DECIMAL\(([^)]+)\)/gi, 'NUMBER($1)')
      .replace(/ARRAY\s*<[^>]+>/gi, 'ARRAY')
      .replace(/MAP\s*<[^>]+>/gi, 'OBJECT')
      .replace(/STRUCT\s*<[^>]+>/gi, 'OBJECT');
  }

  private formatSQL(sql: string): string {
    return format(sql, {
      language: 'snowflake',
      keywordCase: 'upper',
      indentStyle: 'standard',
      linesBetweenQueries: 2
    });
  }
}