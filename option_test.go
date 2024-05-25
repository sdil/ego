/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ego

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/tochemey/goakt/v2/discovery/kubernetes"
	"github.com/tochemey/goakt/v2/log"
	"github.com/tochemey/goakt/v2/telemetry"
)

func TestOptions(t *testing.T) {
	// use the default logger of GoAkt
	logger := log.DefaultLogger
	// create a discovery provider
	discoveryProvider := kubernetes.NewDiscovery(&kubernetes.Config{})
	tel := telemetry.New()

	testCases := []struct {
		name     string
		option   Option
		expected Engine
	}{
		{
			name:   "WithCluster",
			option: WithCluster(discoveryProvider, 30, 3, "localhost", 1334, 1335, 1336),
			expected: Engine{
				discoveryProvider:  discoveryProvider,
				minimumPeersQuorum: 3,
				hostName:           "localhost",
				gossipPort:         1335,
				peersPort:          1336,
				remotingPort:       1334,
				partitionsCount:    30,
				enableCluster:      atomic.NewBool(true),
			},
		},
		{
			name:     "WithLogger",
			option:   WithLogger(logger),
			expected: Engine{logger: logger},
		},
		{
			name:     "WithTelemetry",
			option:   WithTelemetry(tel),
			expected: Engine{telemetry: tel},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e Engine
			tc.option.Apply(&e)
			assert.Equal(t, tc.expected, e)
		})
	}
}
