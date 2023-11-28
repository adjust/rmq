package rmq

import (
	"math/rand"
	"time"
)

var (
	source = rand.NewSource(time.Now().UnixNano())
	random = rand.New(source)
)

// standard characters used by uniuri
const letterBytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// from https://stackoverflow.com/a/31832326
func RandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[random.Intn(len(letterBytes))]
	}
	return string(b)
}
