package rmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Redis protocol does not define a specific way to pass additional data like header.
// However, there is often need to pass them (for example for traces propagation).
//
// This implementation injects optional header values marked with a signature into payload body
// during publishing. When message is consumed, if signature is present, header and original payload
// are extracted from augmented payload.
//
// Header is defined as http.Header for better interoperability with existing libraries,
// for example with go.opentelemetry.io/otel/propagation.HeaderCarrier.

// PayloadWithHeader creates a payload string with header.
func PayloadWithHeader(payload string, header http.Header) string {
	if len(header) == 0 {
		return payload
	}

	hd, _ := json.Marshal(header) // String map never fails marshaling.

	return jsonHeaderSignature + string(hd) + "\n" + payload
}

// PayloadBytesWithHeader creates payload bytes slice with header.
func PayloadBytesWithHeader(payload []byte, header http.Header) []byte {
	if len(header) == 0 {
		return payload
	}

	hd, _ := json.Marshal(header) // String map never fails marshaling.

	res := make([]byte, 0, len(jsonHeaderSignature)+len(hd)+1+len(payload))
	res = append(res, []byte(jsonHeaderSignature)...)
	res = append(res, hd...)
	res = append(res, '\n')
	res = append(res, payload...)

	return res
}

// ExtractHeaderAndPayload splits augmented payload into header and original payload if specific signature is present.
func ExtractHeaderAndPayload(payload string) (http.Header, string, error) {
	if !strings.HasPrefix(payload, jsonHeaderSignature) {
		return nil, payload, nil
	}

	lineEnd := strings.Index(payload, "\n")
	if lineEnd == -1 {
		return nil, "", errors.New("missing line separator")
	}

	first := payload[len(jsonHeaderSignature):lineEnd]
	rest := payload[lineEnd+1:]

	header := make(http.Header)

	if err := json.Unmarshal([]byte(first), &header); err != nil {
		return nil, "", fmt.Errorf("parsing header: %w", err)
	}

	return header, rest, nil
}

// WithHeader is a Delivery with Header.
type WithHeader interface {
	Header() http.Header
}

// jsonHeaderSignature is a signature marker to indicate JSON header presence.
// Do not change the value.
const jsonHeaderSignature = "\xFF\x00\xBE\xBEJ"
