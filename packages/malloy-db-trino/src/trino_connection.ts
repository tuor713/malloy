import {
  MalloyQueryData,
  PersistSQLResults,
  RunSQLOptions,
  PooledConnection,
  StructDef,
  SQLBlock,
  QueryDataRow,
  NamedStructDefs,
  AtomicFieldTypeInner,
  StreamingConnection,
  Connection,
  FieldDef,
  FetchSchemaOptions,
  QueryRunStats
} from '@malloydata/malloy';
import {Trino, BasicAuth, QueryData} from 'trino-client';

const trinoToMalloyTypes: {[key: string]: AtomicFieldTypeInner} = {
  boolean: 'boolean',
  bigint: 'number',
  integer: 'number',
  tinyint: 'number',
  smallint: 'number',
  real: 'number',
  double: 'number',
  decimal: 'number',
  varchar: 'string',
  char: 'string',
  json: 'json',
  date: 'date',
  timestamp: 'timestamp',
  // TODO more Trino types
}

const DEFAULT_PAGE_SIZE = 1000;

interface TrinoConnectionConfiguration {
  server?: string;
  username?: string;
  password?: string;
  catalog?: string;
  schema?: string;
  source?: string;
  // TODO more Trino configuration options
}

export class TrinoConnection
  implements Connection
{
  public readonly name: string;

  private schemaCache = new Map<
    string,
    {schema: StructDef; error?: undefined} | {error: string; schema?: undefined}
  >();

  private config: TrinoConnectionConfiguration;

  constructor(
    name: string,
    queryOptions?: any,
    config: TrinoConnectionConfiguration = {}
  ) {
    this.name = name;
    this.config = config;
  }

  protected async getClient(): Promise<Trino> {
    return Trino.create({
      server: this.config.server,
      catalog: this.config.catalog,
      schema: this.config.schema,
      auth: new BasicAuth(this.config.username || 'anonymous', this.config.password),
      source: this.config.source,
      ssl: {
        rejectUnauthorized: false,
      }
    });
  }

  public async runSQL(
    sqlCommand: string,
    options: Partial<RunSQLOptions> = {}
  ): Promise<MalloyQueryData> {
    return await this.runTrinoSQL(sqlCommand, options.rowLimit || DEFAULT_PAGE_SIZE);
  }

  private arrayInnerType(type: any): any {
    return type['arguments'][0]['value'];
  }

  private extractRowColumns(type: any): {columns: string[], types: any[]} {
    let columns: string[] = [];
    let types: any[] = [];
    let idx = 1;

    for (let t of type['arguments']) {
      if (t['value']['fieldName']) {
        columns.push(t['value']['fieldName']['name']);
      } else {
        columns.push('_col'+idx);
        idx++;
      }

      types.push(t['value']['typeSignature']);
    }

    return {columns, types};
  }

  private trinoRowToMalloyRow(columns:string[], types:any[], row: QueryData): QueryDataRow {
    const res = {};
    for (let i = 0; i < columns.length; i++) {
      if (types[i]['rawType'] == 'array') {
        const innerType = this.arrayInnerType(types[i]);
        if (innerType['rawType'] == 'row') {
          const innerDetails = this.extractRowColumns(innerType);

          let res2 : QueryData = [];
          for (let r of row[i]) {
            res2.push(this.trinoRowToMalloyRow(innerDetails.columns, innerDetails.types, r));
          }

          res[columns[i]] = res2;
          continue;
        }
      }
      res[columns[i]] = row[i];
    }

    return res;
  }

  private async runTrinoSQL(
    sqlCommand: string,
    pageSize: number
  ) {
    const client = await this.getClient();
    const iter = await client.query(sqlCommand);
    const rows : QueryDataRow[] = [];
    let i = 0;
    outer: for await (const row of iter) {
      const columns = (row.columns || []).map(c => c.name);

      // typeSignature is in the result but not in the trino-client API
      // it's the parsed representation of the types
      const types = (row.columns || []).map(c => c['typeSignature']);

      for (const r of row.data || []) {
        rows.push(this.trinoRowToMalloyRow(columns, types, r));

        i++;
        if (i >= pageSize) {
          break outer;
        }
      }
    }

    return {rows, totalRows: i};
  }

  public async test(): Promise<void> {
    await this.runTrinoSQL('SELECT 1', 1);
  }

  public isPool(): this is PooledConnection {
    return false;
  }

  public canPersist(): this is PersistSQLResults {
    return false;
  }

  public canStream(): this is StreamingConnection {
    return true;
  }

  public async *runSQLStream(
    sqlCommand: string,
    {rowLimit, abortSignal}: RunSQLOptions = {}
  ): AsyncIterableIterator<QueryDataRow> {
    const client = await this.getClient();
    const iter = await client.query(sqlCommand);

    let i = 0;
    outer: for await (const row of iter) {
      const columns = (row.columns || []).map(c => c.name);

      // typeSignature is in the result but not in the trino-client API
      // it's the parsed representation of the types
      const types = (row.columns || []).map(c => c['typeSignature']);

      for (const r of row.data || []) {
        yield this.trinoRowToMalloyRow(columns, types, r);

        i++;
        if ((rowLimit !== undefined && i >= rowLimit) || abortSignal?.aborted) {
          break outer;
        }
      }
    }
  }

  async close(): Promise<void> {
    return;
  }

  public async fetchSchemaForTables(tables: Record<string, string>, options: FetchSchemaOptions): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: {[name: string]: string} = {};

    for (const tableKey in tables) {
      let inCache = this.schemaCache.get(tableKey);
      if (!inCache) {
        try {
          inCache = {
            schema: await this.getTableSchema(tableKey, tables[tableKey]),
          };
          this.schemaCache.set(tableKey, inCache);
        } catch (error) {
          inCache = {error: error.message};
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableKey] = inCache.schema;
      } else {
        errors[tableKey] = inCache.error;
      }
    }

    return {schemas, errors};
  }

  private async getTableSchema(tableKey: string, tablePath: string): Promise<StructDef> {
    const structDef : StructDef = {
      type: 'struct',
      name: tableKey,
      dialect: 'trino',
      structSource: {type: 'table', tablePath},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const infoQuery = "DESCRIBE " + tablePath;
    await this.schemaFromQuery(infoQuery, structDef);

    return structDef;
  }

private async schemaFromQuery(schemaQuery: string, structDef: StructDef, client?: Trino) {
    if (!client) {
      client = await this.getClient();
    }
    const iter = await client.query(schemaQuery);
    for await (const row of iter) {
      for (let col of (row.data || [])) {
        const nameIdx = 0;
        const typeIdx = row.columns?.findIndex(c => c.name === 'Type') || 1;

        let trinoBaseType = col[typeIdx].split('(')[0];
        let malloyType = trinoToMalloyTypes[trinoBaseType];
        if (malloyType) {
          structDef.fields.push({
            type: malloyType,
            name: col[nameIdx]
          });
        } else if (trinoBaseType == 'array') {
          let innerType = (/array\((.*)\)/.exec(col[typeIdx]) || ["",""])[1];
          let arType : FieldDef = {
            type: 'struct',
            name: col[nameIdx] as string,
            dialect: 'trino',
            structRelationship: {
              type: 'nested',
              fieldName: col[nameIdx] as string,
              isArray: true
            },
            structSource: {type: 'nested'},
            fields: [{name: 'value', type: trinoToMalloyTypes[innerType]}]
          };
          structDef.fields.push(arType);
        } else {
          structDef.fields.push({
            type: 'unsupported',
            rawType: col[typeIdx],
            name: col[nameIdx]
          });
        }
      }
    }
  }

  public async fetchSchemaForSQLBlock(block: SQLBlock): Promise<{
    structDef: StructDef; error?: undefined; } | { error: string; structDef?: undefined;
  }>
  {
    const client = await this.getClient();
    const tempId = "block" + Math.floor(Math.random()*100000000).toString();

    const iter = await client.query('PREPARE ' + tempId + ' FROM ' + block.selectStr);
    // consume iter to effect query
    for await (const row of iter) {
    }

    const structDef : StructDef = {
      type: 'struct',
      name: block.name,
      dialect: 'trino',
      structSource: {type: 'sql', method: 'subquery', sqlBlock: block},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name
      },
      fields: []
    };

    await this.schemaFromQuery('DESCRIBE OUTPUT ' + tempId, structDef, client);
    const iter2 = await client.query('DEALLOCATE PREPARE ' + tempId);
    // consume iter to effect query
    for await (const row of iter2) {
    }

    return {structDef};
  }

  public async estimateQueryCost(_: string): Promise<QueryRunStats> {
    return {};
  }

  get dialectName(): string {
    return 'trino';
  }
}