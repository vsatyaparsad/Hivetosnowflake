import { HIVE_PATTERNS } from '../hivePatterns';

export class DMLTransformer {
  transformInsert(sql: string): string {
    // Handle INSERT OVERWRITE
    if (sql.match(HIVE_PATTERNS.INSERT_OVERWRITE)) {
      sql = sql.replace(/INSERT\s+OVERWRITE\s+(?:TABLE\s+)?/i, 'INSERT OVERWRITE INTO ');
    }

    // Handle dynamic partitioning
    const partitionMatch = sql.match(HIVE_PATTERNS.DYNAMIC_PARTITION);
    if (partitionMatch) {
      // Remove PARTITION clause as Snowflake handles this automatically
      sql = sql.replace(HIVE_PATTERNS.DYNAMIC_PARTITION, '');
    }

    // Handle multi-insert statements
    if (sql.match(HIVE_PATTERNS.MULTI_INSERT)) {
      return this.transformMultiInsert(sql);
    }

    return sql;
  }

  private transformMultiInsert(sql: string): string {
    // Split the multi-insert into separate INSERT statements
    const fromMatch = sql.match(/FROM\s+([^\s]+)\s+/i);
    if (!fromMatch) return sql;

    const sourceTable = fromMatch[1];
    const insertClauses = sql.split(/INSERT\s+(?:INTO|OVERWRITE)/i).slice(1);

    return insertClauses
      .map(clause => {
        const isOverwrite = clause.trim().startsWith('OVERWRITE');
        return `INSERT ${isOverwrite ? 'OVERWRITE INTO' : 'INTO'} ${clause.trim()}\nFROM ${sourceTable}`;
      })
      .join(';\n\n');
  }

  transformMerge(sql: string): string {
    // Convert Hive MERGE syntax to Snowflake
    return sql.replace(
      /MERGE\s+INTO\s+([^\s]+)\s+(?:AS\s+)?(\w+)\s+USING\s+([^\s]+)\s+(?:AS\s+)?(\w+)/i,
      'MERGE INTO $1 $2 USING $3 $4'
    );
  }

  transformUpdate(sql: string): string {
    // Handle UPDATE with JOIN
    if (sql.includes('JOIN')) {
      return this.transformUpdateWithJoin(sql);
    }
    return sql;
  }

  private transformUpdateWithJoin(sql: string): string {
    // Convert UPDATE with JOIN to MERGE
    const matches = sql.match(/UPDATE\s+([^\s]+)\s+(\w+)\s+JOIN\s+([^\s]+)\s+(\w+)\s+ON\s+(.+?)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i);
    if (!matches) return sql;

    const [, target, targetAlias, source, sourceAlias, joinCond, setClauses, whereCond] = matches;
    
    let mergeSql = `MERGE INTO ${target} ${targetAlias}\n`;
    mergeSql += `USING ${source} ${sourceAlias}\n`;
    mergeSql += `ON ${joinCond}\n`;
    mergeSql += `WHEN MATCHED ${whereCond ? `AND ${whereCond} ` : ''}THEN UPDATE SET ${setClauses}`;

    return mergeSql;
  }
}