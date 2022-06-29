/*
 * Copyright 2022 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

import { RunSQLOptions } from "./malloy";
import { MalloyQueryData, QueryDataRow, SQLBlock, StructDef } from "./model";

/**
 * A URL.
 */
export class URL {
  private _url: string;

  constructor(stringURL: string) {
    this._url = stringURL;
  }

  /**
   * @returns The string form of this URL.
   */
  public toString(): string {
    return this._url;
  }

  /**
   * Construct a URL from string.
   *
   * @param stringURL The string form of the URL.
   * @returns A URL.
   */
  public static fromString(stringURL: string): URL {
    return new URL(stringURL);
  }
}

/**
 * The contents of a Malloy query document.
 */
export type QueryString = string;

/**
 * The contents of a Malloy model document.
 */
export type ModelString = string;

/**
 * A URL whose contents is a Malloy model.
 */
export type ModelURL = URL;

/**
 * A URL whose contents is a Malloy query.
 */
export type QueryURL = URL;

/**
 * An object capable of reading the contents of a URL in some context.
 */
export interface URLReader {
  /**
   * Read the contents of the given URL.
   *
   * @param url The URL to read.
   * @returns A promise to the contents of the URL.
   */
  readURL: (url: URL) => Promise<string>;
}

/**
 * An object capable of reading schemas for given table names.
 */
export interface InfoConnection {
  // TODO should we really be exposing StructDef like this?
  // TODO should this be a Map instead of a Record in the public interface?
  /**
   * Fetch schemas for multiple tables.
   *
   * @param tables The names of tables to fetch schemas for.
   * @returns A mapping of table names to schemas.
   */
  fetchSchemaForTables(tables: string[]): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }>;

  /**
   * Fetch schemas for multiple SQL blocks.
   *
   * @param tables The SQL blocks to fetch schemas for.
   * @returns A mapping of SQL block names to schemas.
   */
  fetchSchemaForSQLBlocks(sqlStructs: SQLBlock[]): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }>;

  /**
   * The name of the connection.
   */
  get name(): string;
}

/**
 * An object capable of running SQL.
 */
export interface Connection extends InfoConnection {
  /**
   * Run some SQL and yield results.
   *
   * @param sql The SQL to run.
   * @param options.pageSize Maximum number of results to return at once.
   * @returns The rows of data resulting from running the given SQL query
   * and the total number of rows available.
   */
  runSQL(sql: string, options?: RunSQLOptions): Promise<MalloyQueryData>;

  // TODO feature-sql-block Comment
  isPool(): this is PooledConnection;

  canPersist(): this is PersistSQLResults;

  canFetchSchemaAndRunSimultaneously(): this is FetchSchemaAndRunSimultaneously;

  canStream(): this is StreamingConnection;

  canFetchSchemaAndRunStreamSimultaneously(): this is FetchSchemaAndRunStreamSimultaneously;
}

// TODO feature-sql-block Comment
export interface TestableConnection extends Connection {
  // TODO feature-sql-block Comment
  test(): Promise<void>;
}

export interface PooledConnection extends Connection {
  // Most pool implementations require a specific call to release connection handles. If a Connection is a
  // PooledConnection, drain() should be called when connection usage is over
  drain(): Promise<void>;
  isPool(): true;
}

export interface PersistSQLResults extends Connection {
  manifestTemporaryTable(sqlCommand: string): Promise<string>;
}

export interface FetchSchemaAndRunSimultaneously extends Connection {
  runSQLBlockAndFetchResultSchema(
    sqlBlock: SQLBlock,
    options?: { rowLimit?: number }
  ): Promise<{ data: MalloyQueryData; schema: StructDef }>;
}

export interface StreamingConnection extends Connection {
  runSQLStream(
    sqlCommand: string,
    options?: { rowLimit?: number }
  ): AsyncIterableIterator<QueryDataRow>;
}

export interface FetchSchemaAndRunStreamSimultaneously
  extends StreamingConnection,
    FetchSchemaAndRunSimultaneously {
  runSQLBlockStreamAndFetchResultSchema(
    sqlCommand: string,
    options?: { rowLimit?: number }
  ): Promise<{
    stream: AsyncIterableIterator<QueryDataRow>;
    schema: StructDef;
  }>;
}

/**
 * A mapping of connection names to connections.
 */
export interface LookupConnection<T extends InfoConnection> {
  /**
   * @param connectionName The name of the connection for which a `Connection` is required.
   * @returns A promise to a `Connection` for the connection named `connectionName`.
   */
  lookupConnection(connectionName?: string): Promise<T>;
}
