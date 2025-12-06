import { DatabaseClient } from '../../database/database-client.interface';
import {
  ICollectionStrategy,
  CollectionStrategyType,
  CollectionAggregationConfig,
  CollectionAggregationResult,
  SelectedField,
} from '../collection-strategy.interface';
import { QueryContext } from '../query-builder';

/**
 * CTE-based collection strategy
 *
 * This is the current/default strategy that uses PostgreSQL CTEs with jsonb_agg
 * to aggregate related records into JSONB arrays.
 *
 * Benefits:
 * - Single query execution
 * - No temp table management
 * - Works well for moderate data sizes
 *
 * SQL Pattern:
 * ```sql
 * WITH "cte_0" AS (
 *   SELECT
 *     "user_id" as parent_id,
 *     jsonb_agg(
 *       jsonb_build_object('id', "id", 'title', "title")
 *       ORDER BY "views" DESC
 *     ) as data
 *   FROM (
 *     SELECT "user_id", "id", "title", "views"
 *     FROM "posts"
 *     WHERE "views" > $1
 *     ORDER BY "views" DESC
 *   ) sub
 *   GROUP BY "user_id"
 * )
 * SELECT ... COALESCE("cte_0".data, '[]'::jsonb) as "posts" ...
 * ```
 */
export class CteCollectionStrategy implements ICollectionStrategy {
  getType(): CollectionStrategyType {
    return 'cte';
  }

  requiresParentIds(): boolean {
    // JSONB strategy doesn't need parent IDs upfront - it aggregates for all parents
    return false;
  }

  buildAggregation(
    config: CollectionAggregationConfig,
    context: QueryContext
  ): CollectionAggregationResult {
    const cteName = `cte_${config.counter}`;

    let cteSQL: string;
    let selectExpression: string;

    switch (config.aggregationType) {
      case 'jsonb':
        cteSQL = this.buildJsonbAggregation(config, cteName, context);
        selectExpression = `COALESCE("${cteName}".data, ${config.defaultValue})`;
        break;

      case 'array':
        cteSQL = this.buildArrayAggregation(config, cteName, context);
        selectExpression = `COALESCE("${cteName}".data, ${config.defaultValue})`;
        break;

      case 'count':
      case 'min':
      case 'max':
      case 'sum':
        cteSQL = this.buildScalarAggregation(config, cteName, context);
        selectExpression = `COALESCE("${cteName}".data, ${config.defaultValue})`;
        break;

      default:
        throw new Error(`Unknown aggregation type: ${config.aggregationType}`);
    }

    // Store CTE in context
    context.ctes.set(cteName, { sql: cteSQL, params: [] });

    return {
      sql: cteSQL,
      params: context.allParams,
      tableName: cteName,
      joinClause: `LEFT JOIN "${cteName}" ON "${config.sourceTable}"."id" = "${cteName}".parent_id`,
      selectExpression,
      isCTE: true,
    };
  }

  /**
   * Helper to collect all leaf fields from a potentially nested structure
   * Returns array of { alias, expression } for SELECT clause (flattened with unique aliases)
   */
  private collectLeafFields(fields: SelectedField[], prefix: string = ''): Array<{ alias: string; expression: string }> {
    const result: Array<{ alias: string; expression: string }> = [];
    for (const field of fields) {
      const fullAlias = prefix ? `${prefix}__${field.alias}` : field.alias;
      if (field.nested) {
        // Recurse into nested fields
        result.push(...this.collectLeafFields(field.nested, fullAlias));
      } else if (field.expression) {
        // Leaf field
        result.push({ alias: fullAlias, expression: field.expression });
      }
    }
    return result;
  }

  /**
   * Helper to build jsonb_build_object expression (handles nested structures)
   */
  private buildJsonbObject(fields: SelectedField[], prefix: string = ''): string {
    const parts: string[] = [];
    for (const field of fields) {
      if (field.nested) {
        // Nested object - recurse
        const nestedJsonb = this.buildJsonbObject(field.nested, prefix ? `${prefix}__${field.alias}` : field.alias);
        parts.push(`'${field.alias}', ${nestedJsonb}`);
      } else {
        // Leaf field - reference the aliased column from subquery
        const fullAlias = prefix ? `${prefix}__${field.alias}` : field.alias;
        parts.push(`'${field.alias}', "${fullAlias}"`);
      }
    }
    return `jsonb_build_object(${parts.join(', ')})`;
  }

  /**
   * Build JSONB aggregation CTE
   *
   * When LIMIT/OFFSET is specified, uses ROW_NUMBER() window function to correctly
   * apply pagination per parent row (not globally).
   */
  private buildJsonbAggregation(
    config: CollectionAggregationConfig,
    cteName: string,
    context: QueryContext
  ): string {
    const { selectedFields, targetTable, foreignKey, whereClause, orderByClause, limitValue, offsetValue, isDistinct } = config;

    // Collect all leaf fields for the SELECT clause
    const leafFields = this.collectLeafFields(selectedFields);

    // Build the JSONB fields for jsonb_build_object (handles nested structures)
    const jsonbObjectExpr = this.buildJsonbObject(selectedFields);

    // Build WHERE clause
    const whereSQL = whereClause ? `WHERE ${whereClause}` : '';

    // Build DISTINCT clause
    const distinctClause = isDistinct ? 'DISTINCT ' : '';

    // If LIMIT or OFFSET is specified, use ROW_NUMBER() for per-parent pagination
    if (limitValue !== undefined || offsetValue !== undefined) {
      return this.buildJsonbAggregationWithRowNumber(
        config, leafFields, jsonbObjectExpr, whereSQL, distinctClause
      );
    }

    // No LIMIT/OFFSET - use simple aggregation
    // Build the subquery SELECT fields
    const allSelectFields = [
      `"${foreignKey}" as "__fk_${foreignKey}"`,
      ...leafFields.map(f => {
        if (f.expression !== `"${f.alias}"`) {
          return `${f.expression} as "${f.alias}"`;
        }
        return f.expression;
      }),
    ];

    // Build ORDER BY clause
    const orderBySQL = orderByClause ? `ORDER BY ${orderByClause}` : '';

    // Build the jsonb_agg ORDER BY clause
    const jsonbAggOrderBy = orderByClause ? ` ORDER BY ${orderByClause}` : '';

    const cteSQL = `
SELECT
  "__fk_${foreignKey}" as parent_id,
  jsonb_agg(
    ${jsonbObjectExpr}${jsonbAggOrderBy}
  ) as data
FROM (
  SELECT ${distinctClause}${allSelectFields.join(', ')}
  FROM "${targetTable}"
  ${whereSQL}
  ${orderBySQL}
) sub
GROUP BY "__fk_${foreignKey}"
    `.trim();

    return cteSQL;
  }

  /**
   * Build JSONB aggregation with ROW_NUMBER() for per-parent LIMIT/OFFSET
   *
   * SQL Pattern:
   * ```sql
   * SELECT parent_id, jsonb_agg(jsonb_build_object(...)) as data
   * FROM (
   *   SELECT *, ROW_NUMBER() OVER (PARTITION BY foreign_key ORDER BY ...) as __rn
   *   FROM (SELECT ... FROM table WHERE ...) inner_sub
   * ) sub
   * WHERE __rn > offset AND __rn <= offset + limit
   * GROUP BY parent_id
   * ```
   */
  private buildJsonbAggregationWithRowNumber(
    config: CollectionAggregationConfig,
    leafFields: Array<{ alias: string; expression: string }>,
    jsonbObjectExpr: string,
    whereSQL: string,
    distinctClause: string
  ): string {
    const { targetTable, foreignKey, orderByClause, limitValue, offsetValue } = config;

    // Build the innermost SELECT fields
    const innerSelectFields = [
      `"${foreignKey}" as "__fk_${foreignKey}"`,
      ...leafFields.map(f => {
        if (f.expression !== `"${f.alias}"`) {
          return `${f.expression} as "${f.alias}"`;
        }
        return f.expression;
      }),
    ];

    // Build ORDER BY for ROW_NUMBER() - use the order clause or default to foreign key
    const rowNumberOrderBy = orderByClause || `"__fk_${foreignKey}"`;

    // Build the row number filter condition
    const offset = offsetValue || 0;
    let rowNumberFilter: string;
    if (limitValue !== undefined) {
      // Both LIMIT and potentially OFFSET
      rowNumberFilter = `WHERE "__rn" > ${offset} AND "__rn" <= ${offset + limitValue}`;
    } else {
      // Only OFFSET (no LIMIT)
      rowNumberFilter = `WHERE "__rn" > ${offset}`;
    }

    const cteSQL = `
SELECT
  "__fk_${foreignKey}" as parent_id,
  jsonb_agg(
    ${jsonbObjectExpr}
  ) as data
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "__fk_${foreignKey}" ORDER BY ${rowNumberOrderBy}) as "__rn"
  FROM (
    SELECT ${distinctClause}${innerSelectFields.join(', ')}
    FROM "${targetTable}"
    ${whereSQL}
  ) inner_sub
) sub
${rowNumberFilter}
GROUP BY "__fk_${foreignKey}"
    `.trim();

    return cteSQL;
  }

  /**
   * Build array aggregation CTE (for toNumberList/toStringList)
   *
   * When LIMIT/OFFSET is specified, uses ROW_NUMBER() window function to correctly
   * apply pagination per parent row (not globally).
   */
  private buildArrayAggregation(
    config: CollectionAggregationConfig,
    cteName: string,
    context: QueryContext
  ): string {
    const { arrayField, targetTable, foreignKey, whereClause, orderByClause, limitValue, offsetValue, isDistinct } = config;

    if (!arrayField) {
      throw new Error('arrayField is required for array aggregation');
    }

    // Build WHERE clause
    const whereSQL = whereClause ? `WHERE ${whereClause}` : '';

    // Build DISTINCT clause
    const distinctClause = isDistinct ? 'DISTINCT ' : '';

    // If LIMIT or OFFSET is specified, use ROW_NUMBER() for per-parent pagination
    if (limitValue !== undefined || offsetValue !== undefined) {
      return this.buildArrayAggregationWithRowNumber(config, whereSQL, distinctClause);
    }

    // No LIMIT/OFFSET - use simple aggregation
    // Build ORDER BY clause
    const orderBySQL = orderByClause ? `ORDER BY ${orderByClause}` : '';

    // Build the array_agg ORDER BY clause
    const arrayAggOrderBy = orderByClause ? ` ORDER BY ${orderByClause}` : '';

    const cteSQL = `
SELECT
  "__fk_${foreignKey}" as parent_id,
  array_agg(
    "${arrayField}"${arrayAggOrderBy}
  ) as data
FROM (
  SELECT ${distinctClause}"__fk_${foreignKey}", "${arrayField}"
  FROM (
    SELECT "${foreignKey}" as "__fk_${foreignKey}", "${arrayField}"
    FROM "${targetTable}"
    ${whereSQL}
    ${orderBySQL}
  ) inner_sub
) sub
GROUP BY "__fk_${foreignKey}"
    `.trim();

    return cteSQL;
  }

  /**
   * Build array aggregation with ROW_NUMBER() for per-parent LIMIT/OFFSET
   */
  private buildArrayAggregationWithRowNumber(
    config: CollectionAggregationConfig,
    whereSQL: string,
    distinctClause: string
  ): string {
    const { arrayField, targetTable, foreignKey, orderByClause, limitValue, offsetValue } = config;

    // Build ORDER BY for ROW_NUMBER() - use the order clause or default to foreign key
    const rowNumberOrderBy = orderByClause || `"__fk_${foreignKey}"`;

    // Build the row number filter condition
    const offset = offsetValue || 0;
    let rowNumberFilter: string;
    if (limitValue !== undefined) {
      rowNumberFilter = `WHERE "__rn" > ${offset} AND "__rn" <= ${offset + limitValue}`;
    } else {
      rowNumberFilter = `WHERE "__rn" > ${offset}`;
    }

    const cteSQL = `
SELECT
  "__fk_${foreignKey}" as parent_id,
  array_agg(
    "${arrayField}"
  ) as data
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "__fk_${foreignKey}" ORDER BY ${rowNumberOrderBy}) as "__rn"
  FROM (
    SELECT ${distinctClause}"${foreignKey}" as "__fk_${foreignKey}", "${arrayField}"
    FROM "${targetTable}"
    ${whereSQL}
  ) inner_sub
) sub
${rowNumberFilter}
GROUP BY "__fk_${foreignKey}"
    `.trim();

    return cteSQL;
  }

  /**
   * Build scalar aggregation CTE (COUNT, MIN, MAX, SUM)
   */
  private buildScalarAggregation(
    config: CollectionAggregationConfig,
    cteName: string,
    context: QueryContext
  ): string {
    const { aggregationType, aggregateField, targetTable, foreignKey, whereClause } = config;

    // Build WHERE clause
    const whereSQL = whereClause ? `WHERE ${whereClause}` : '';

    // Build aggregation expression
    let aggregateExpression: string;
    switch (aggregationType) {
      case 'count':
        aggregateExpression = 'COUNT(*)';
        break;
      case 'min':
      case 'max':
      case 'sum':
        if (!aggregateField) {
          throw new Error(`${aggregationType.toUpperCase()} requires an aggregate field`);
        }
        aggregateExpression = `${aggregationType.toUpperCase()}("${aggregateField}")`;
        break;
      default:
        throw new Error(`Unknown aggregation type: ${aggregationType}`);
    }

    const cteSQL = `
SELECT
  "${foreignKey}" as parent_id,
  ${aggregateExpression} as data
FROM "${targetTable}"
${whereSQL}
GROUP BY "${foreignKey}"
    `.trim();

    return cteSQL;
  }
}
