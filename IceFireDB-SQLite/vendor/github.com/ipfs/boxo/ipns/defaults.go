package ipns

import "time"

const (

	// DefaultRecordLifetime defines for how long IPNS record should be valid
	// when ValidityType is 0. The default here aims to match the record
	// expiration window of Amino DHT.
	DefaultRecordLifetime = 48 * time.Hour

	// DefaultRecordTTL specifies how long the record can be returned from
	// cache before checking for update again. The function of this TTL is
	// similar to TTL of DNS record, and the default here is a trade-off
	// between faster updates and benefiting from various types of caching.
	DefaultRecordTTL = 1 * time.Hour
)
