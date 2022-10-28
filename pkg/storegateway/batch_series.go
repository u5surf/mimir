// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type batchedSeriesSet struct {
	ctx      context.Context
	postings []storage.SeriesRef

	batchSize int
	i         int // where we're at in the labels and chunks
	iOffset   int // if i==0, which index in postings does this correspond to
	preloaded []seriesEntry
	stats     *queryStats
	err       error

	indexr           *bucketIndexReader              // Index reader for block.
	chunkr           *bucketChunkReader              // Chunk reader for block.
	matchers         []*labels.Matcher               // Series matchers.
	shard            *sharding.ShardSelector         // Shard selector.
	seriesHashCache  *hashcache.BlockSeriesHashCache // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter    ChunksLimiter                   // Rate limiter for loading chunks.
	seriesLimiter    SeriesLimiter                   // Rate limiter for loading series.
	skipChunks       bool                            // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64                           // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates   []storepb.Aggr                  // List of aggregates to load when loading chunks.
	logger           log.Logger

	cleanupFuncs []func()
}

func batchedBlockSeries(
	ctx context.Context,
	batchSize int,
	indexr *bucketIndexReader, // Index reader for block.
	chunkr *bucketChunkReader, // Chunk reader for block.
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates []storepb.Aggr, // List of aggregates to load when loading chunks.
	logger log.Logger,
) (storepb.SeriesSet, error) {
	if batchSize <= 0 {
		return nil, errors.New("batch size must be a positive number")
	}
	if skipChunks {
		res, ok := fetchCachedSeries(ctx, indexr.block.userID, indexr.block.indexCache, indexr.block.meta.ULID, matchers, shard, logger)
		if ok {
			return newBucketSeriesSet(res), nil
		}
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers)
	if err != nil {
		return nil, errors.Wrap(err, "expanded matching posting")
	}

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	var seriesCacheStats queryStats
	if shard != nil {
		ps, seriesCacheStats = filterPostingsByCachedShardHash(ps, shard, seriesHashCache)
	}

	return &batchedSeriesSet{
		batchSize:       batchSize,
		preloaded:       make([]seriesEntry, 0, batchSize),
		stats:           &seriesCacheStats,
		iOffset:         -batchSize,
		ctx:             ctx,
		postings:        ps,
		indexr:          indexr,
		chunkr:          chunkr,
		matchers:        matchers,
		shard:           shard,
		seriesHashCache: seriesHashCache,
		chunksLimiter:   chunksLimiter,
		seriesLimiter:   seriesLimiter,
		skipChunks:      skipChunks,
		minTime:         minTime,
		maxTime:         maxTime,
		loadAggregates:  loadAggregates,
		logger:          logger,
	}, nil
}

func (s *batchedSeriesSet) Next() bool {
	if s.iOffset+s.i >= len(s.postings)-1 || s.err != nil {
		return false
	}
	s.i++
	if s.i >= len(s.preloaded) {
		return s.preload()
	}
	return true
}

func (s *batchedSeriesSet) preload() bool {
	s.resetPreloaded()
	s.iOffset += s.batchSize
	if s.iOffset > len(s.postings) {
		return false
	}

	end := s.iOffset + s.batchSize
	if end > len(s.postings) {
		end = len(s.postings)
	}
	nextBatch := s.postings[s.iOffset:end]

	if err := s.indexr.PreloadSeries(s.ctx, nextBatch); err != nil {
		s.err = errors.Wrap(err, "preload series")
		return false
	}

	var (
		symbolizedLset []symbolizedLabel
		chks           []chunks.Meta
	)
	for _, id := range nextBatch {
		ok, err := s.indexr.LoadSeriesForTime(id, &symbolizedLset, &chks, s.skipChunks, s.minTime, s.maxTime)
		if err != nil {
			s.err = errors.Wrap(err, "read series")
			return false
		}
		if !ok {
			// No matching chunks for this time duration, skip series and extend nextBatch
			continue
		}

		lset, err := s.indexr.LookupLabelsSymbols(symbolizedLset)
		if err != nil {
			s.err = errors.Wrap(err, "lookup labels symbols")
			return false
		}

		// Skip the series if it doesn't belong to the shard.
		if s.shard != nil {
			hash, ok := s.seriesHashCache.Fetch(id)
			s.stats.seriesHashCacheRequests++

			if !ok {
				hash = lset.Hash()
				s.seriesHashCache.Store(id, hash)
			} else {
				s.stats.seriesHashCacheHits++
			}

			if hash%s.shard.ShardCount != s.shard.ShardIndex {
				continue
			}
		}

		// Check series limit after filtering out series not belonging to the requested shard (if any).
		if err := s.seriesLimiter.Reserve(1); err != nil {
			s.err = errors.Wrap(err, "exceeded series limit")
			return false
		}

		entry := seriesEntry{lset: lset}

		if !s.skipChunks {
			// Schedule loading chunks.
			entry.refs = make([]chunks.ChunkRef, 0, len(chks))
			entry.chks = make([]storepb.AggrChunk, 0, len(chks))
			for j, meta := range chks {
				// seriesEntry s is appended to res, but not at every outer loop iteration,
				// therefore len(res) is the index we need here, not outer loop iteration number.
				if err := s.chunkr.addLoad(meta.Ref, len(s.preloaded), j); err != nil {
					s.err = errors.Wrap(err, "add chunk load")
					return false
				}
				entry.chks = append(entry.chks, storepb.AggrChunk{
					MinTime: meta.MinTime,
					MaxTime: meta.MaxTime,
				})
				entry.refs = append(entry.refs, meta.Ref)
			}

			// Ensure sample limit through chunksLimiter if we return chunks.
			if err := s.chunksLimiter.Reserve(uint64(len(entry.chks))); err != nil {
				s.err = errors.Wrap(err, "exceeded chunks limit")
				return false
			}
		}

		s.preloaded = append(s.preloaded, entry)
	}

	if len(s.preloaded) == 0 {
		return s.preload() // we didn't find any suitable series in this batch, try with the next one
	}

	if s.skipChunks {
		storeCachedSeries(s.ctx, s.indexr.block.indexCache, s.indexr.block.userID, s.indexr.block.meta.ULID, s.matchers, s.shard, s.preloaded, s.logger)
		return true
	}

	if err := s.chunkr.load(s.preloaded, s.loadAggregates); err != nil {
		s.err = errors.Wrap(err, "load chunks")
		return false
	}

	s.stats = s.stats.merge(s.indexr.stats).merge(s.chunkr.stats)
	return true
}

func (s *batchedSeriesSet) resetPreloaded() {
	s.preloaded = s.preloaded[:0]
	s.loadAggregates = s.loadAggregates[:0]
	s.cleanupFuncs = append(s.cleanupFuncs, s.indexr.unload)
	if s.chunkr != nil { // can be nil when the client didn't want to load chunks
		s.chunkr.reset()
		s.cleanupFuncs = append(s.cleanupFuncs, s.chunkr.unload)
	}
	s.i = 0
}

func (s *batchedSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if s.i >= len(s.preloaded) {
		return nil, nil
	}
	return s.preloaded[s.i].lset, s.preloaded[s.i].chks
}

func (s *batchedSeriesSet) Err() error {
	return s.err
}

func (s *batchedSeriesSet) CleanupFunc() func() {
	return func() {
		for _, cleanup := range s.cleanupFuncs {
			cleanup()
		}
		s.cleanupFuncs = s.cleanupFuncs[:0]
	}
}
