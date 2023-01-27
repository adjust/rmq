package rmq_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/adjust/rmq/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadWithHeader(t *testing.T) {
	p := `{"foo":"bar"}`

	h := make(http.Header)
	ph := rmq.PayloadWithHeader(p, h)
	assert.Equal(t, p, ph) // No change for empty header.
	h2, p2, err := rmq.ExtractHeaderAndPayload(ph)
	require.NoError(t, err)
	assert.Nil(t, h2)
	assert.Equal(t, p, p2)

	h.Set("X-Foo", "Bar")
	ph = rmq.PayloadWithHeader(p, h)
	assert.NotEqual(t, p, ph)

	h2, p2, err = rmq.ExtractHeaderAndPayload(ph)
	require.NoError(t, err)
	assert.Equal(t, h, h2)
	assert.Equal(t, p, p2)
}

func TestPayloadBytesWithHeader(t *testing.T) {
	p := `{"foo":"bar"}`

	h := make(http.Header)
	ph := rmq.PayloadBytesWithHeader([]byte(p), h)
	assert.Equal(t, p, string(ph)) // No change for empty header.
	h2, p2, err := rmq.ExtractHeaderAndPayload(string(ph))
	require.NoError(t, err)
	assert.Nil(t, h2)
	assert.Equal(t, p, p2)

	h.Set("X-Foo", "Bar")
	ph = rmq.PayloadBytesWithHeader([]byte(p), h)
	assert.NotEqual(t, p, ph)

	h2, p2, err = rmq.ExtractHeaderAndPayload(string(ph))
	require.NoError(t, err)
	assert.Equal(t, h, h2)
	assert.Equal(t, p, string(p2))
}

func TestExtractHeaderAndPayload(t *testing.T) {
	t.Run("missing_line_separator", func(t *testing.T) {
		ph := rmq.PayloadWithHeader("foo", http.Header{"foo": []string{"bar"}})
		ph = ph[0:7] // Truncating payload.
		h, p, err := rmq.ExtractHeaderAndPayload(ph)
		require.Error(t, err)
		assert.Nil(t, h)
		assert.Empty(t, p)
	})

	t.Run("invalid_json", func(t *testing.T) {
		ph := rmq.PayloadWithHeader("foo", http.Header{"foo": []string{"bar"}})
		ph = strings.Replace(ph, `"`, `'`, 1) // Corrupting JSON.
		h, p, err := rmq.ExtractHeaderAndPayload(ph)
		require.Error(t, err)
		assert.Nil(t, h)
		assert.Empty(t, p)
	})

	t.Run("ok", func(t *testing.T) {
		ph := rmq.PayloadWithHeader("foo", http.Header{"foo": []string{"bar"}})
		h, p, err := rmq.ExtractHeaderAndPayload(ph)
		require.NoError(t, err)
		assert.Equal(t, http.Header{"foo": []string{"bar"}}, h)
		assert.Equal(t, "foo", p)
	})

	t.Run("ok_line_breaks", func(t *testing.T) {
		ph := rmq.PayloadWithHeader("foo", http.Header{"foo": []string{"bar1\nbar2\nbar3"}})
		h, p, err := rmq.ExtractHeaderAndPayload(ph)
		require.NoError(t, err)
		assert.Equal(t, http.Header{"foo": []string{"bar1\nbar2\nbar3"}}, h)
		assert.Equal(t, "foo", p)
	})
}

func ExamplePayloadWithHeader() {
	var (
		pub, con rmq.Queue
	)

	// ....

	h := make(http.Header)
	h.Set("X-Baz", "quux")

	// You can add header to your payload during publish.
	_ = pub.Publish(rmq.PayloadWithHeader(`{"foo":"bar"}`, h))

	// ....

	_, _ = con.AddConsumerFunc("tag", func(delivery rmq.Delivery) {
		// And receive header back in consumer.
		delivery.(rmq.WithHeader).Header().Get("X-Baz") // "quux"

		// ....
	})
}
