package rmq

import "log"

// By default rmq panics on various redis issues preferring to restart consumers and producers.
// However if this behaviour is not desired you can override this handler. Beware that rmq
// may be in an unusable state if sufficient recovery is not performed.
var Panicf = func(format string, v ...interface{}) {
	log.Panicf(format, v...)
}
