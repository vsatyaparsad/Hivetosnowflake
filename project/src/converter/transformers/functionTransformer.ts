import { functionMappings } from '../../mappings/functions';

export class FunctionTransformer {
  transformFunction(functionName: string, args: string[]): string {
    const mapping = functionMappings.find(m => 
      m.hiveName.toLowerCase() === functionName.toLowerCase()
    );

    if (!mapping) return `${functionName}(${args.join(', ')})`;

    if (mapping.transform) {
      return mapping.transform(args);
    }

    return `${mapping.snowflakeName}(${args.join(', ')})`;
  }

  transformWindowFunction(sql: string): string {
    // Handle window function OVER clauses
    return sql.replace(
      /OVER\s*\((?:PARTITION\s+BY\s+([^)]+))?(?:\s*ORDER\s+BY\s+([^)]+))?\)/gi,
      (match, partition, order) => {
        let result = 'OVER (';
        if (partition) result += `PARTITION BY ${partition} `;
        if (order) result += `ORDER BY ${order}`;
        return result + ')';
      }
    );
  }

  transformAggregateFunction(sql: string): string {
    // Handle special aggregate functions
    return sql
      .replace(/COLLECT_SET\((.*?)\)/gi, 'ARRAY_AGG(DISTINCT $1)')
      .replace(/COLLECT_LIST\((.*?)\)/gi, 'ARRAY_AGG($1)')
      .replace(/HISTOGRAM_NUMERIC\((.*?)\)/gi, 'APPROX_TOP_K($1)');
  }
}