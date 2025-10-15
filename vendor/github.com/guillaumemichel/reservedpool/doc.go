// Package reservedpool implements a simple concurrency pool with per-category
// reservations. Each category of work can claim a number of slots that are
// reserved exclusively for it while any extra capacity is shared among all
// categories. This allows prioritizing certain categories without leaving the
// pool underutilized.
//
// Acquire blocks until a slot for the requested category is available. Release
// returns a previously acquired slot. The pool can be closed to release
// waiters and reject future acquisitions.

package reservedpool
