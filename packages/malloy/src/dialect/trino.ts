import {indent} from '../model/utils';
import {
  DialectFragment,
  Expr,
  ExpressionValueType,
  ExtractUnit,
  FieldAtomicTypeDef,
  Sampling,
  StructDef,
  TimeValue,
  TimestampUnit,
  TypecastFragment,
  getIdentifier,
  isDateUnit,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  isTimeFieldType,
  mkExpr,
} from '../model/malloy_types';
import {
  Dialect,
  DialectField,
  DialectFieldList,
  QueryInfo,
  qtz,
} from './dialect';
import {FUNCTIONS, DialectFunctionOverloadDef} from './functions';
import {
  anyExprType,
  arg,
  minScalar,
  overload,
  params,
  sql,
  spread,
  makeParam,
  param,
} from './functions/util';

const castMap: Record<string, string> = {
  number: 'double',
  string: 'varchar',
};

// These are the units that "TIMESTAMP_ADD" accepts
const timestampAddUnits = [
  'microsecond',
  'millisecond',
  'second',
  'minute',
  'hour',
  'day',
];

const extractMap: Record<string, string> = {};

const types: ExpressionValueType[] = [
  'string',
  'number',
  'timestamp',
  'date',
  'json',
];

const trinoToMalloyTypes: {[key: string]: FieldAtomicTypeDef} = {
  'boolean': {type: 'boolean'},
  'tinyint': {type: 'number', numberType: 'integer'},
  'smallint': {type: 'number', numberType: 'integer'},
  'integer': {type: 'number', numberType: 'integer'},
  'bigint': {type: 'number', numberType: 'integer'},
  'real': {type: 'number', numberType: 'float'},
  'double': {type: 'number', numberType: 'float'},
  'decimal': {type: 'number', numberType: 'float'},
  'varchar': {type: 'string'},
  'char': {type: 'string'},
  'varbinary': {type: 'string'},
  'json': {type: 'json'},
  'date': {type: 'date'},
  'timestamp': {type: 'timestamp'},
  'time': {type: 'string'},
  'uuid': {type: 'string'},
  'ipaddress': {type: 'string'},
};

function fnCoalesce(): DialectFunctionOverloadDef[] {
  return types.flatMap(type => [
    overload(
      minScalar(type),
      [params('values', anyExprType(type))],
      // Trino coalesce does not support 1 argument, so add extra NULL
      sql`COALESCE(${spread(arg('values'))}, NULL)`
    ),
  ]);
}

function fnIfnull(): DialectFunctionOverloadDef[] {
  return types.map(type =>
    overload(
      minScalar(type),
      [param('value', anyExprType(type)), param('default', anyExprType(type))],
      // Postgres doesn't have an IFNULL function, so we use COALESCE, which is equivalent.
      sql`COALESCE(${arg('value')}, ${arg('default')})`
    )
  );
}

function fnReverse(): DialectFunctionOverloadDef[] {
  const value = makeParam('value', anyExprType('string'));
  return [
    // explicit cast since Trino also supports varbinary
    overload(
      minScalar('string'),
      [value.param],
      sql`REVERSE(CAST(${value.arg} AS VARCHAR))`
    ),
  ];
}

function fnUnicode(): DialectFunctionOverloadDef[] {
  // Aparently the ASCII function also works for unicode code points...
  return [
    overload(
      minScalar('number'),
      [param('value', anyExprType('string'))],
      sql`CODEPOINT(${arg('value')})`
    ),
  ];
}

function fnLog(): DialectFunctionOverloadDef[] {
  const value = makeParam('value', anyExprType('number'));
  const base = makeParam('base', anyExprType('number'));
  return [
    overload(
      minScalar('number'),
      [value.param, base.param],
      // Parameter order is backwards in Trino
      sql`LOG(${base.arg}, ${value.arg})`
    ),
  ];
}

function fnDiv(): DialectFunctionOverloadDef[] {
  const dividend = makeParam('dividend', anyExprType('number'));
  const divisor = makeParam('divisor', anyExprType('number'));
  return [
    overload(
      minScalar('number'),
      [dividend.param, divisor.param],
      // Default division in Trino is integer
      sql`${dividend.arg} / ${divisor.arg}`
    ),
  ];
}

export const TRINO_FUNCTIONS = FUNCTIONS.clone();
TRINO_FUNCTIONS.add('reverse', fnReverse);
// Trino has one codepoint function
TRINO_FUNCTIONS.add('unicode', fnUnicode);
TRINO_FUNCTIONS.add('ascii', fnUnicode);
TRINO_FUNCTIONS.add('coalesce', fnCoalesce);
TRINO_FUNCTIONS.add('ifnull', fnIfnull);
TRINO_FUNCTIONS.add('log', fnLog);
TRINO_FUNCTIONS.add('div', fnDiv);
TRINO_FUNCTIONS.seal();

export class TrinoDialect extends Dialect {
  name = 'trino';

  // TODO check all of these
  defaultNumberType = 'DOUBLE';
  defaultDecimalType = 'DECIMAL';
  udfPrefix = '__udf';
  hasFinalStage = false;
  stringTypeName = 'VARCHAR';
  divisionIsInteger = true;
  supportsSumDistinctFunction = false;
  // whether unnest can automatically generate __row_id, fails due to some other issue
  unnestWithNumbers = false;
  defaultSampling = {enable: false};
  // true => leads into using sqlUnnestPipelineHead with potentially nested aggregation functions
  // which Trino does not appear to support
  // false => hit select * replace syntax for UDF in generatePipelinedStages
  supportUnnestArrayAgg = false;
  supportsCTEinCoorelatedSubQueries = false;
  dontUnionIndex = false;
  supportsQualify = true;
  supportsAggDistinct = false;
  supportsSafeCast = true;
  supportsNesting = true;

  quoteTablePath(tablePath: string): string {
    // TODO check rules
    return `${tablePath}`;
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (SELECT * FROM UNNEST(sequence(0,${groupSetCount},1)) as t(group_set))`;
  }

  sqlAnyValue(groupSet: number, fieldName: string): string {
    return `ARBITRARY(CASE WHEN group_set=${groupSet} THEN ${fieldName} END)`;
  }

  // can array agg or arbitrary a struct...
  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    limit: number | undefined
  ): string {
    let tail = '';
    if (limit !== undefined) {
      tail += ` LIMIT ${limit}`;
    }
    // TODO find solution for tail / limit
    return `COALESCE(ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN ${this.toRow(
      fieldList
    )}\n END ${orderBy}) FILTER (WHERE group_set=${groupSet}), ARRAY[])`;
  }

  castToString(expression: string): string {
    return `CAST(${expression} as VARCHAR)`;
  }

  concat(...values: string[]): string {
    return values.join(' || ');
  }

  sqlTypeToMalloyType(sqlType: string): FieldAtomicTypeDef | undefined {
    // Remove trailing params
    const baseSqlType = sqlType.match(/^([\w\s]+)/)?.at(0) ?? sqlType;
    return trinoToMalloyTypes[baseSqlType.trim().toLowerCase()];
  }

  malloyTypeToSQLType(malloyType: FieldAtomicTypeDef): string {
    if (malloyType.type === 'number') {
      if (malloyType.numberType === 'integer') {
        return 'integer';
      } else {
        return 'double';
      }
    } else if (malloyType.type === 'string') {
      return 'varchar';
    }
    return malloyType.type;
  }

  toRow(fieldList: DialectFieldList): string {
    const fields = fieldList.map(f => `${f.sqlExpression}`).join(', ');
    return `CAST(ROW(${fields}) AS ${this.toRowType(fieldList)})`;
  }

  toRowType(fieldList: DialectFieldList): string {
    return `ROW(${fieldList
      .map(f => f.sqlOutputName + ' ' + this.toFieldType(f))
      .join(', ')})`;
  }

  toFieldType(field: DialectField): string {
    if (field.type === 'struct') {
      if (field.nativeField) {
        // TODO is this always an array? how can we tell?
        return (
          'ARRAY(' +
          this.toRowType(
            (field.nativeField as StructDef).fields.map(f => {
              return {
                type: f.type,
                nativeField: f,
                sqlExpression: f.name,
                sqlOutputName: f.name,
              };
            })
          ) +
          ')'
        );
      } else {
        console.log('Nested field without native type', field);
        return 'struct';
      }
    } else {
      return castMap[field.type] || field.type;
    }
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    return `ARBITRARY(CASE WHEN group_set=${groupSet} THEN ${this.toRow(
      fieldList
    )})`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `ARBITRARY(CASE WHEN group_set=${groupSet} THEN ${name} END) as ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const nullValues = fieldList.map(f => `NULL`).join(', ');

    return `COALESCE(ARBITRARY(CASE WHEN group_set=${groupSet} THEN ${this.toRow(
      fieldList
    )} END), CAST(ROW(${nullValues}) as ${this.toRowType(fieldList)}))`;
  }

  dialectExpr(qi: QueryInfo, df: DialectFragment): Expr {
    switch (df.function) {
      case 'now':
        return this.sqlNow();
      case 'timeDiff':
        return this.sqlMeasureTime(df.left, df.right, df.units);
      case 'delta':
        return this.sqlAlterTime(df.op, df.base, df.delta, df.units);
      case 'trunc':
        return this.sqlTrunc(qi, df.expr, df.units);
      case 'extract':
        return this.sqlExtract(qi, df.expr, df.units);
      case 'cast':
        return this.sqlCast(qi, df);
      case 'regexpMatch':
        return this.sqlRegexpMatch(df.expr, df.regexp);
      case 'div': {
        if (this.divisionIsInteger) {
          return mkExpr`CAST(${df.numerator} AS DOUBLE)/${df.denominator}`;
        }
        return mkExpr`${df.numerator}/${df.denominator}`;
      }
      case 'timeLiteral':
        return [
          this.sqlLiteralTime(
            qi,
            df.literal,
            df.literalType,
            df.timezone || qtz(qi)
          ),
        ];
      case 'stringLiteral':
        return [this.sqlLiteralString(df.literal)];
      case 'numberLiteral':
        return [this.sqlLiteralNumber(df.literal)];
      case 'regexpLiteral':
        return [this.sqlLiteralRegexp(df.literal)];
    }
  }

  //
  // this code used to be:
  //
  //   from += `JOIN UNNEST(GENERATE_ARRAY(0,${this.maxGroupSet},1)) as group_set\n`;
  //
  // BigQuery will allocate more resources if we use a CROSS JOIN so we do that instead.
  //
  sqlUnnestAlias(
    source: string,
    alias: string,
    fieldList: DialectFieldList,
    needDistinctKey: boolean,
    isArray: boolean
  ): string {
    if (isArray) {
      if (needDistinctKey) {
        return `LEFT JOIN UNNEST(${source}) WITH ORDINALITY as ${alias}(value,__row_id) ON TRUE`;
      } else {
        return `LEFT JOIN UNNEST(${source}) as ${alias}(value) ON TRUE`;
      }
    } else if (needDistinctKey) {
      return `LEFT JOIN UNNEST(${source}) WITH ORDINALITY as ${alias}(${fieldList
        .map(f => `${f.sqlExpression}`)
        .join(', ')}, __row_id) ON TRUE`;
    } else {
      return `LEFT JOIN UNNEST(${source}) as ${alias} ON TRUE`;
    }
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    sqlDistinctKey = `CAST(${sqlDistinctKey} AS VARBINARY)`;
    const upperPart = `cast(from_base(substr(to_hex(md5(${sqlDistinctKey})), 1, 15), 16) as DECIMAL(38)) * 4294967296`;
    const lowerPart = `cast(from_base(substr(to_hex(md5(${sqlDistinctKey})), 16, 8), 16) as DECIMAL(38))`;
    // See the comment below on `sql_sum_distinct` for why we multiply by this decimal
    const precisionShiftMultiplier = '0.000000001';
    return `(${upperPart} + ${lowerPart}) * ${precisionShiftMultiplier}`;
  }

  sqlGenerateUUID(): string {
    // Trino has a separate UUID type distinct from VARCHAR, which cannot be used as a string for example in 'CONCAT'
    // hence cast to string upfront
    return 'CAST(UUID() AS VARCHAR)';
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    _fieldType: string,
    _isNested: boolean,
    _isArray: boolean
  ): string {
    return `${alias}.${fieldName}`;
  }

  sqlUnnestPipelineHead(
    isSingleton: boolean,
    sourceSQLExpression: string
  ): string {
    let p = sourceSQLExpression;
    if (isSingleton) {
      // TODO this is not correct syntax, but the alternatives often also don't work - especially when there is nested aggregation functions
      // e.g. FROM UNNEST(ARRAY[COALESCE(ARBITRARY(CASE WHEN group_set=1 THEN CAST(ROW(t__1) AS ROW(t double)) END), CAST(ROW(NULL) as ROW(t double)))])
      p = `ARRAY[${p}]`;
    }
    return `UNNEST(${p})`;
  }

  sqlCreateFunction(id: string, funcText: string): string {
    return `CREATE TEMPORARY FUNCTION ${id}(__param ANY TYPE) AS ((\n${indent(
      funcText
    )}));\n`;
  }

  sqlCreateTableAsSelect(tableName: string, sql: string): string {
    return `
  CREATE TABLE IF NOT EXISTS \`${tableName}\`
  OPTIONS (
      expiration_timestamp=TIMESTAMP_ADD(current_timestamp,  INTERVAL '1' hour)
  )
  AS (
  ${indent(sql)}
  );
  `;
  }

  sqlCreateFunctionCombineLastStage(
    lastStageName: string,
    structDef: StructDef
  ): string {
    return `SELECT ARRAY_AGG(CAST(ROW(${structDef.fields
      .map(fieldDef => this.sqlMaybeQuoteIdentifier(getIdentifier(fieldDef)))
      .join(',')}) AS ROW(${structDef.fields
      .map(fd => fd.name + ' ' + (castMap[fd.type] || fd.type))
      .join(',')}))) FROM ${lastStageName}\n`;
  }

  sqlSelectAliasAsStruct(alias: string, physicalFieldNames: string[]): string {
    return `ROW(${physicalFieldNames
      .map(name => `${alias}.${name}`)
      .join(', ')})`;
  }

  // TODO check on exact Trino keywords
  keywords = `
    ALL
    AND
    ANY
    ARRAY
    AS
    ASC
    AT
    BETWEEN
    BY
    CASE
    CAST
    COLLATE
    CONTAINS
    CREATE
    CROSS
    CUBE
    CURRENT
    DEFAULT
    DEFINE
    DESC
    DISTINCT
    ELSE
    END
    ENUM
    ESCAPE
    EXCEPT
    EXCLUDE
    EXISTS
    EXTRACT
    FALSE
    FETCH
    FOLLOWING
    FOR
    FROM
    FULL
    GROUP
    GROUPING
    GROUPS
    HASH
    HAVING
    IF
    IGNORE
    IN
    INNER
    INTERSECT
    INTERVAL
    INTO
    IS
    JOIN
    LATERAL
    LEFT
    LIKE
    LIMIT
    LOOKUP
    MERGE
    NATURAL
    NEW
    NO
    NOT
    NULL
    NULLS
    OF
    ON
    OR
    ORDER
    OUTER
    OVER
    PARTITION
    PRECEDING
    PROTO
    RANGE
    RECURSIVE
    RESPECT
    RIGHT
    ROLLUP
    ROWS
    SELECT
    SET
    SOME
    STRUCT
    TABLESAMPLE
    THEN
    TO
    TREAT
    TRUE
    UNBOUNDED
    UNION
    UNNEST
    USING
    VALUES
    WHEN
    WHERE
    WINDOW
    WITH
    WITHIN`.split(/\s/);

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  sqlNow(): Expr {
    return mkExpr`CURRENT_TIMESTAMP`;
  }

  sqlTrunc(qi: QueryInfo, sqlTime: TimeValue, units: TimestampUnit): Expr {
    if (sqlTime.valueType === 'date') {
      if (isDateUnit(units)) {
        let result = mkExpr`DATE_TRUNC('${units}', ${sqlTime.value})`;
        if (units === 'week') {
          result = mkExpr`(${result} - INTERVAL '1' DAY)`;
        }
        return result;
      }
      return mkExpr`(CAST ${sqlTime.value} AS TIMESTAMP)`;
    }
    let result = mkExpr`DATE_TRUNC('${units}', ${sqlTime.value})`;
    if (units === 'week') {
      result = mkExpr`(${result} - INTERVAL '1' DAY)`;
    }
    return result;
  }

  sqlExtract(qi: QueryInfo, expr: TimeValue, units: ExtractUnit): Expr {
    let extractFrom = expr.value;
    if (expr.valueType === 'timestamp') {
      const tz = qtz(qi);
      if (tz) {
        extractFrom = mkExpr`${extractFrom} AT TIME ZONE '${tz}'`;
      }
    }

    const extractTo = extractMap[units] || units;
    let extracted = mkExpr`EXTRACT(${extractTo} FROM ${extractFrom})`;
    return units === 'day_of_week' ? mkExpr`(${extracted}%7 + 1)` : extracted;
  }

  sqlAlterTime(
    op: '+' | '-',
    expr: TimeValue,
    n: Expr,
    timeframe: TimestampUnit
  ): Expr {
    let theTime = expr.value;
    return mkExpr`DATE_ADD('${timeframe}', ${
      op === '+' ? '' : '-'
    }${n}, ${theTime})`;
  }

  ignoreInProject(fieldName: string): boolean {
    return fieldName === '_PARTITIONTIME';
  }

  sqlCast(qi: QueryInfo, cast: TypecastFragment): Expr {
    if (cast.srcType !== cast.dstType) {
      const tz = qtz(qi);
      if (cast.srcType === 'timestamp' && cast.dstType === 'date' && tz) {
        return mkExpr`CAST(${cast.expr} AT TIME ZONE '${tz}' AS ${cast.dstType})`;
      }
      const dstType =
        typeof cast.dstType === 'string'
          ? this.malloyTypeToSQLType({type: cast.dstType})
          : cast.dstType.raw;
      const castFunc = cast.safe ? 'TRY_CAST' : 'CAST';
      return mkExpr`${castFunc}(${cast.expr}  AS ${dstType})`;
    }
    return cast.expr;
  }

  sqlRegexpMatch(expr: Expr, regexp: Expr): Expr {
    return mkExpr`REGEXP_LIKE(${expr}, ${regexp})`;
  }

  sqlLiteralTime(
    qi: QueryInfo,
    timeString: string,
    type: 'date' | 'timestamp',
    timezone?: string
  ): string {
    if (type === 'date') {
      return `(DATE '${timeString.split(' ')[0]}')`;
    } else if (type === 'timestamp') {
      if (timezone) {
        return `(TIMESTAMP '${timeString} ${timezone}')`;
      } else {
        return `(TIMESTAMP '${timeString}')`;
      }
    } else {
      throw new Error(`Unknown Liternal time format ${type}`);
    }
  }

  sqlMeasureTime(from: TimeValue, to: TimeValue, units: string): Expr {
    let lVal = from.value;
    let rVal = to.value;
    let diffUsing = 'DATE_DIFF';

    if (units === 'second' || units === 'minute' || units === 'hour') {
      if (from.valueType !== 'timestamp') {
        lVal = mkExpr`TIMESTAMP(${lVal})`;
      }
      if (to.valueType !== 'timestamp') {
        rVal = mkExpr`TIMESTAMP(${rVal})`;
      }
    }

    return mkExpr`${diffUsing}('${units}', ${lVal}, ${rVal})`;
  }

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        throw new Error(
          "StandardSQL doesn't support sampling by rows only percent"
        );
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL}  TABLESAMPLE SYSTEM (${sample.percent}))`;
      }
    }
    return tableSQL;
  }

  sqlLiteralString(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\');
    return "'" + noVirgule.replace(/'/g, "''") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    return "'" + literal.replace(/'/g, "''") + "'";
  }

  getGlobalFunctionDef(name: string): DialectFunctionOverloadDef[] | undefined {
    return TRINO_FUNCTIONS.get(name);
  }

  validateTypeName(sqlType: string): boolean {
    // Letters:              BIGINT
    // Numbers:              INT8
    // Spaces:               TIMESTAMP WITH TIME ZONE
    // Parentheses, Commas:  NUMERIC(5, 2)
    return sqlType.match(/^[A-Za-z\s(),0-9]*$/) !== null;
  }
}
