/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import {Fragment} from '../../model';
import {
  overload,
  minAggregate,
  maxScalar,
  sql,
  DialectFunctionOverloadDef,
  makeParam,
  literal,
} from './util';

export function fnStringAgg(): DialectFunctionOverloadDef[] {
  const value = makeParam('value', maxScalar('string'));
  const separator = makeParam('separator', literal(maxScalar('string')));
  const orderBy: Fragment = {type: 'aggregate_order_by'};
  return [
    overload(
      minAggregate('string'),
      [value.param],
      sql`STRING_AGG(${value.arg}${orderBy})`,
      {supportsOrderBy: true, defaultOrderByArgIndex: 0}
    ),
    overload(
      minAggregate('string'),
      [value.param, separator.param],
      sql`STRING_AGG(${value.arg}, ${separator.arg}${orderBy})`,
      {supportsOrderBy: true, defaultOrderByArgIndex: 0}
    ),
  ];
}

export function fnStringAggDistinct(): DialectFunctionOverloadDef[] {
  const value = makeParam('value', maxScalar('string'));
  const separator = makeParam('separator', literal(maxScalar('string')));
  const orderBy: Fragment = {type: 'aggregate_order_by'};
  return [
    overload(
      minAggregate('string'),
      [value.param],
      sql`STRING_AGG(DISTINCT ${value.arg}${orderBy})`,
      {
        isSymmetric: true,
        supportsOrderBy: 'only_default',
        defaultOrderByArgIndex: 0,
      }
    ),
    overload(
      minAggregate('string'),
      [value.param, separator.param],
      sql`STRING_AGG(DISTINCT ${value.arg}, ${separator.arg}${orderBy})`,
      {
        isSymmetric: true,
        supportsOrderBy: 'only_default',
        defaultOrderByArgIndex: 0,
      }
    ),
  ];
}
