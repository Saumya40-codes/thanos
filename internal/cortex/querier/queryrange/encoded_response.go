// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryrange

// encodedJSONCarrier is implemented by Response values that still hold
// original wire/cache JSON and can skip re-marshaling.
type encodedJSONCarrier interface {
	GetEncodedJSON() []byte
}

// encodedBodyResponse wraps a PrometheusResponse with the JSON bytes it was
// decoded from (or loaded from cache). Middlewares that mutate the payload
// must not preserve the encoded body.
type encodedBodyResponse struct {
	inner   *PrometheusResponse
	encoded []byte
}

func withEncodedJSON(inner *PrometheusResponse, encoded []byte) Response {
	if inner == nil {
		return nil
	}
	if len(encoded) == 0 {
		return inner
	}
	// Keep a copy so callers can reuse/recycle the original buffer safely.
	body := make([]byte, len(encoded))
	copy(body, encoded)
	return &encodedBodyResponse{inner: inner, encoded: body}
}

func (e *encodedBodyResponse) Reset()         { e.inner.Reset() }
func (e *encodedBodyResponse) String() string { return e.inner.String() }
func (e *encodedBodyResponse) ProtoMessage()  {}
func (e *encodedBodyResponse) GetHeaders() []*PrometheusResponseHeader {
	return e.inner.GetHeaders()
}
func (e *encodedBodyResponse) GetStats() *PrometheusResponseStats {
	return e.inner.GetStats()
}
func (e *encodedBodyResponse) GetEncodedJSON() []byte { return e.encoded }

// asPrometheusResponse unwraps wrappers and returns the concrete response.
func asPrometheusResponse(r Response) *PrometheusResponse {
	switch v := r.(type) {
	case *PrometheusResponse:
		return v
	case *encodedBodyResponse:
		return v.inner
	default:
		return nil
	}
}

// encodedJSON returns retained wire/cache JSON when available.
func encodedJSON(r Response) []byte {
	if c, ok := r.(encodedJSONCarrier); ok {
		return c.GetEncodedJSON()
	}
	return nil
}
