// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/internal/cortex/chunk/cache"
	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
)

func TestDecodeEncode_PassthroughRetainsBody(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"]]}],"analysis":null}}`)

	decoded, err := PrometheusCodec.DecodeResponse(context.Background(), &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, body, encodedJSON(decoded))

	encoded, err := PrometheusCodec.EncodeResponse(context.Background(), decoded)
	require.NoError(t, err)
	got, err := io.ReadAll(encoded.Body)
	require.NoError(t, err)
	require.Equal(t, body, got, "EncodeResponse should pass through retained JSON bytes")
}

func TestResultsCache_StoresAndServesRetainedJSON(t *testing.T) {
	// Distinct JSON that would not match a re-marshal of the same struct
	// (e.g. spacing / field order). Passthrough must preserve exact bytes.
	rawBody := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[100,"1"],[110,"2"]]}],"analysis":null},"warnings":["w"]}`)

	cfg := ResultsCacheConfig{
		CacheConfig: cache.Config{
			Cache: cache.NewMockCache(),
		},
	}
	rcm, c, err := NewResultsCacheMiddleware(
		log.NewNopLogger(),
		cfg,
		constSplitter(day),
		mockLimits{},
		PrometheusCodec,
		PrometheusResponseExtractor{},
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	calls := 0
	downstream := HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		calls++
		var resp PrometheusResponse
		require.NoError(t, json.Unmarshal(rawBody, &resp))
		return withEncodedJSON(&resp, rawBody), nil
	})
	h := rcm.Wrap(downstream)

	ctx := user.InjectOrgID(context.Background(), "1")
	req := &PrometheusRequest{
		Path:  "/api/v1/query_range",
		Start: 100,
		End:   110,
		Step:  10,
		Query: "up",
	}

	// Miss: store retained JSON in cache.
	resp1, err := h.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, rawBody, encodedJSON(resp1))

	// Confirm cache blob contains the raw body, not a re-marshaled form.
	_, bufs, _ := c.Fetch(ctx, []string{cache.HashKey(constSplitter(day).GenerateCacheKey("1", req))})
	require.Len(t, bufs, 1)
	require.True(t, bytes.Contains(bufs[0], rawBody), "cache entry should embed original JSON body")

	// Hit: no downstream call; response keeps cache JSON for encode passthrough.
	resp2, err := h.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "second request should be a full cache hit")
	require.Equal(t, rawBody, encodedJSON(resp2))

	httpResp, err := PrometheusCodec.EncodeResponse(ctx, resp2)
	require.NoError(t, err)
	got, err := io.ReadAll(httpResp.Body)
	require.NoError(t, err)
	require.Equal(t, rawBody, got)
}

func TestPartition_FullExtentKeepsEncodedBody(t *testing.T) {
	raw, err := json.Marshal(&PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{{
				Labels:  []cortexpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []cortexpb.Sample{{TimestampMs: 100, Value: 1}, {TimestampMs: 200, Value: 2}},
			}},
		},
	})
	require.NoError(t, err)

	ext := Extent{Start: 100, End: 200, Body: raw}
	s := resultsCache{
		extractor:      PrometheusResponseExtractor{},
		minCacheExtent: 10,
	}
	reqs, resps, err := s.partition(&PrometheusRequest{Start: 100, End: 200, Step: 100}, []Extent{ext}, extractAnyStep)
	require.NoError(t, err)
	require.Empty(t, reqs)
	require.Len(t, resps, 1)
	require.Equal(t, raw, encodedJSON(resps[0]))
}

func TestPartition_PartialExtentDropsEncodedBody(t *testing.T) {
	raw, err := json.Marshal(&PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{{
				Labels:  []cortexpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []cortexpb.Sample{{TimestampMs: 100, Value: 1}, {TimestampMs: 200, Value: 2}},
			}},
		},
	})
	require.NoError(t, err)

	ext := Extent{Start: 100, End: 200, Body: raw}
	s := resultsCache{
		extractor:      PrometheusResponseExtractor{},
		minCacheExtent: 10,
	}
	// Request only half the extent → must Extract (no body passthrough).
	reqs, resps, err := s.partition(&PrometheusRequest{Start: 100, End: 150, Step: 50}, []Extent{ext}, extractAnyStep)
	require.NoError(t, err)
	require.Empty(t, reqs)
	require.Len(t, resps, 1)
	require.Nil(t, encodedJSON(resps[0]))
}
