// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeQueryStats_export(t *testing.T) {
	orig := newSafeQueryStats()
	orig.update(func(stats *queryStats) {
		stats.blocksQueried = 10
	})

	exported := orig.export()
	assert.Equal(t, 10, exported.blocksQueried)

	orig.update(func(stats *queryStats) {
		stats.blocksQueried = 20
	})

	assert.Equal(t, 20, orig.unsafeStats.blocksQueried)
	assert.Equal(t, 10, exported.blocksQueried)
}
