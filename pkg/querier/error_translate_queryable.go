// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/error_translate_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"

	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/validation"
)

// TranslateToPromqlAPIError converts error to one of promql.Errors for consumption in PromQL API.
// PromQL API only recognizes few errors, and converts everything else to HTTP status code 422.
//
// Specifically, it supports:
//
//   promql.ErrQueryCanceled, mapped to 503
//   promql.ErrQueryTimeout, mapped to 503
//   promql.ErrStorage mapped to 500
//   anything else is mapped to 422
//
// Querier code produces different kinds of errors, and we want to map them to above-mentioned HTTP status codes correctly.
//
// Details:
// - vendor/github.com/prometheus/prometheus/web/api/v1/api.go, respondError function only accepts *apiError types.
// - translation of error to *apiError happens in vendor/github.com/prometheus/prometheus/web/api/v1/api.go, returnAPIError method.
func TranslateToPromqlAPIError(err error) error {
	if err == nil {
		return err
	}

	//nolint:errorlint // We don't expect the cause error to be wrapped.
	switch errors.Cause(err).(type) {
	case promql.ErrStorage, promql.ErrTooManySamples, promql.ErrQueryCanceled, promql.ErrQueryTimeout:
		// Don't translate those, just in case we use them internally.
		return err
	case validation.LimitError:
		// This will be returned with status code 422 by Prometheus API.
		return err
	default:
		if errors.Is(err, context.Canceled) {
			return err // 422
		}

		s, ok := status.FromError(err)

		if !ok {
			s, ok = status.FromError(errors.Cause(err))
		}

		if ok {
			code := s.Code()

			// Treat these as HTTP status codes, even though they are supposed to be grpc codes.
			if code >= 400 && code < 500 {
				// Return directly, will be mapped to 422
				return err
			} else if code >= 500 && code < 599 {
				// Wrap into ErrStorage for mapping to 500
				return promql.ErrStorage{Err: err}
			}
		}

		// All other errors will be returned as 500.
		return promql.ErrStorage{Err: err}
	}
}

// ErrTranslateFn is used to translate or wrap error before returning it by functions in
// storage.SampleAndChunkQueryable interface.
// Input error may be nil.
type ErrTranslateFn func(err error) error

func NewErrorTranslateQueryable(q storage.Queryable) storage.Queryable {
	return NewErrorTranslateQueryableWithFn(q, TranslateToPromqlAPIError)
}

func NewErrorTranslateQueryableWithFn(q storage.Queryable, fn ErrTranslateFn) storage.Queryable {
	return errorTranslateQueryable{q: q, fn: fn}
}

func NewErrorTranslateSampleAndChunkQueryable(q storage.SampleAndChunkQueryable) storage.SampleAndChunkQueryable {
	return NewErrorTranslateSampleAndChunkQueryableWithFn(q, TranslateToPromqlAPIError)
}

func NewErrorTranslateSampleAndChunkQueryableWithFn(q storage.SampleAndChunkQueryable, fn ErrTranslateFn) storage.SampleAndChunkQueryable {
	return errorTranslateSampleAndChunkQueryable{q: q, fn: fn}
}

type errorTranslateQueryable struct {
	q  storage.Queryable
	fn ErrTranslateFn
}

func (e errorTranslateQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := e.q.Querier(ctx, mint, maxt)
	return errorTranslateQuerier{q: q, fn: e.fn}, e.fn(err)
}

type errorTranslateSampleAndChunkQueryable struct {
	q  storage.SampleAndChunkQueryable
	fn ErrTranslateFn
}

func (e errorTranslateSampleAndChunkQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := e.q.Querier(ctx, mint, maxt)
	return errorTranslateQuerier{q: q, fn: e.fn}, e.fn(err)
}

func (e errorTranslateSampleAndChunkQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := e.q.ChunkQuerier(ctx, mint, maxt)
	return errorTranslateChunkQuerier{q: q, fn: e.fn}, e.fn(err)
}

type errorTranslateQuerier struct {
	q  storage.Querier
	fn ErrTranslateFn
}

func (e errorTranslateQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelValues(name, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelNames(matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateQuerier) Close() error {
	return e.fn(e.q.Close())
}

func (e errorTranslateQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	s := e.q.Select(sortSeries, hints, matchers...)
	return errorTranslateSeriesSet{s: s, fn: e.fn}
}

type errorTranslateChunkQuerier struct {
	q  storage.ChunkQuerier
	fn ErrTranslateFn
}

func (e errorTranslateChunkQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelValues(name, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateChunkQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelNames(matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateChunkQuerier) Close() error {
	return e.fn(e.q.Close())
}

func (e errorTranslateChunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	s := e.q.Select(sortSeries, hints, matchers...)
	return errorTranslateChunkSeriesSet{s: s, fn: e.fn}
}

type errorTranslateSeriesSet struct {
	s  storage.SeriesSet
	fn ErrTranslateFn
}

func (e errorTranslateSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateSeriesSet) At() storage.Series {
	return e.s.At()
}

func (e errorTranslateSeriesSet) Err() error {
	return e.fn(e.s.Err())
}

func (e errorTranslateSeriesSet) Warnings() storage.Warnings {
	return e.s.Warnings()
}

type errorTranslateChunkSeriesSet struct {
	s  storage.ChunkSeriesSet
	fn ErrTranslateFn
}

func (e errorTranslateChunkSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateChunkSeriesSet) At() storage.ChunkSeries {
	return e.s.At()
}

func (e errorTranslateChunkSeriesSet) Err() error {
	return e.fn(e.s.Err())
}

func (e errorTranslateChunkSeriesSet) Warnings() storage.Warnings {
	return e.s.Warnings()
}
