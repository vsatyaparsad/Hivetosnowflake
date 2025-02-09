import { TableDefinition, ColumnDefinition } from '../../types/sql';
import { HIVE_PATTERNS, HIVE_TO_SNOWFLAKE_STORAGE } from '../hivePatterns';
import { dataTypeMappings } from '../../mappings/dataTypes';

export class DDLTransformer {
  transformCreateTable(sql: string): string {
    const tableMatch = sql.match(HIVE_PATTERNS.CREATE_TABLE);
    if (!tableMatch) return sql;

    const tableDef: TableDefinition = {
      name: tableMatch[1],
      columns: [],
      external: /CREATE\s+EXTERNAL\s+TABLE/i.test(sql)
    };

    // Extract column definitions
    const columnDefsMatch = sql.match(/\(([\s\S]+?)\)/);
    if (columnDefsMatch) {
      const columnDefs = columnDefsMatch[1].split(',').map(col => col.trim());
      tableDef.columns = this.parseColumnDefinitions(columnDefs);
    }

    // Handle partitioning
    const partitionMatch = sql.match(HIVE_PATTERNS.DYNAMIC_PARTITION);
    if (partitionMatch) {
      tableDef.partitionColumns = this.parseColumnDefinitions(partitionMatch[1].split(','));
      tableDef.clusterBy = tableDef.partitionColumns.map(col => col.name);
    }

    // Handle storage format
    const storageMatch = sql.match(HIVE_PATTERNS.STORED_AS);
    if (storageMatch) {
      const hiveFormat = storageMatch[1].toUpperCase();
      tableDef.format = HIVE_TO_SNOWFLAKE_STORAGE[hiveFormat] || 'CSV';
    }

    return this.generateSnowflakeCreateTable(tableDef);
  }

  private parseColumnDefinitions(defs: string[]): ColumnDefinition[] {
    return defs.map(def => {
      const parts = def.trim().split(/\s+/);
      const name = parts[0];
      let type = parts[1].toUpperCase();

      // Handle complex types
      if (type.includes('ARRAY<')) {
        type = 'ARRAY';
      } else if (type.includes('MAP<')) {
        type = 'OBJECT';
      } else if (type.includes('STRUCT<')) {
        type = 'OBJECT';
      }

      // Map Hive type to Snowflake type
      const mapping = dataTypeMappings.find(m => m.hiveType === type);
      const snowflakeType = mapping ? mapping.snowflakeType : type;

      const column: ColumnDefinition = {
        name,
        type: snowflakeType
      };

      // Handle column comment if present
      const commentMatch = def.match(/COMMENT\s+['"]([^'"]+)['"]/i);
      if (commentMatch) {
        column.comment = commentMatch[1];
      }

      return column;
    });
  }

  private generateSnowflakeCreateTable(tableDef: TableDefinition): string {
    let sql = `CREATE OR REPLACE TABLE ${tableDef.name} (\n`;

    // Add columns
    sql += tableDef.columns
      .map(col => {
        let colDef = `  ${col.name} ${col.type}`;
        if (col.comment) {
          colDef += ` COMMENT '${col.comment}'`;
        }
        return colDef;
      })
      .join(',\n');

    sql += '\n)';

    // Add clustering if specified
    if (tableDef.clusterBy && tableDef.clusterBy.length > 0) {
      sql += `\nCLUSTER BY (${tableDef.clusterBy.join(', ')})`;
    }

    // Add format if specified
    if (tableDef.format) {
      sql += `\nFILE_FORMAT = ${tableDef.format}`;
    }

    return sql;
  }

  transformAlterTable(sql: string): string {
    // Handle various ALTER TABLE operations
    if (sql.includes('ADD COLUMNS')) {
      return sql.replace('ADD COLUMNS', 'ADD');
    }
    if (sql.includes('CHANGE COLUMN')) {
      return sql.replace(/CHANGE COLUMN\s+(\w+)\s+(\w+)/, 'RENAME COLUMN $1 TO $2');
    }
    if (sql.includes('SET TBLPROPERTIES')) {
      // Snowflake doesn't have direct equivalent, convert to comments
      return sql.replace(/SET TBLPROPERTIES\s*\((.*?)\)/s, 'COMMENT = $1');
    }
    return sql;
  }
}