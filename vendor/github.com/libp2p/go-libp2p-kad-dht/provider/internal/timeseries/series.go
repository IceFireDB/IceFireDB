package timeseries

import (
	"sync"
	"time"

	"github.com/gammazero/deque"
)

// Numeric represents types that can be used in mathematical operations.
type Numeric interface {
	~int64 | ~float64
}

// timeEntry represents a timestamped value with optional weight.
type timeEntry[T Numeric] struct {
	timestamp time.Time
	value     T
	weight    int
}

// TimeSeries maintains a time-windowed series of numeric values with optional weights.
// It automatically removes entries older than the retention period.
type TimeSeries[T Numeric] struct {
	mutex     sync.Mutex
	data      deque.Deque[timeEntry[T]]
	retention time.Duration
	weighted  bool // whether this series uses weights
}

// NewTimeSeries creates a new TimeSeries for the given numeric type with the specified retention period.
func NewTimeSeries[T Numeric](retention time.Duration) TimeSeries[T] {
	return TimeSeries[T]{
		data:      deque.Deque[timeEntry[T]]{},
		retention: retention,
		weighted:  false,
	}
}

// NewWeightedTimeSeries creates a new TimeSeries that supports weighted values.
func NewWeightedTimeSeries[T Numeric](retention time.Duration) TimeSeries[T] {
	return TimeSeries[T]{
		data:      deque.Deque[timeEntry[T]]{},
		retention: retention,
		weighted:  true,
	}
}

// Add records a new value with the current timestamp and weight 1.
func (t *TimeSeries[T]) Add(value T) {
	t.AddWeighted(value, 1)
}

// AddWeighted records a new value with the current timestamp and specified weight.
// For non-weighted series, the weight parameter is ignored.
func (t *TimeSeries[T]) AddWeighted(value T, weight int) {
	now := time.Now()
	if !t.weighted {
		weight = 1
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.data.PushBack(timeEntry[T]{timestamp: now, value: value, weight: weight})
	t.gc(now)
}

// Sum returns the sum of all values within the retention window.
// For weighted series, returns the weighted sum (sum of value * weight).
func (t *TimeSeries[T]) Sum() T {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.sumUnlocked()
}

// sumUnlocked returns the sum without acquiring the mutex.
// The caller must hold the mutex.
func (t *TimeSeries[T]) sumUnlocked() T {
	t.gc(time.Now())

	var sum T
	for i := 0; i < t.data.Len(); i++ {
		entry := t.data.At(i)
		if t.weighted {
			sum += entry.value * T(entry.weight)
		} else {
			sum += entry.value
		}
	}
	return sum
}

// Avg returns the average of all values within the retention window.
// For weighted series, returns the weighted average (weighted sum / total weight).
// For non-weighted series, returns the arithmetic mean.
func (t *TimeSeries[T]) Avg() float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())

	length := t.data.Len()
	if length == 0 {
		return 0
	}

	if t.weighted {
		var weightedSum T
		var totalWeight int
		for i := range length {
			entry := t.data.At(i)
			weightedSum += entry.value * T(entry.weight)
			totalWeight += entry.weight
		}
		if totalWeight == 0 {
			return 0
		}
		return float64(weightedSum) / float64(totalWeight)
	} else {
		sum := t.sumUnlocked()
		return float64(sum) / float64(length)
	}
}

// Count returns the number of entries within the retention window.
func (t *TimeSeries[T]) Count() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.gc(time.Now())
	return t.data.Len()
}

// gc removes entries older than the retention period.
// The caller must hold the mutex.
func (t *TimeSeries[T]) gc(now time.Time) {
	cutoff := now.Add(-t.retention)
	for t.data.Len() > 0 {
		if t.data.Front().timestamp.Before(cutoff) {
			t.data.PopFront()
		} else {
			break
		}
	}
}

// IntTimeSeries is a convenience type alias for TimeSeries[int64].
type IntTimeSeries = TimeSeries[int64]

// FloatTimeSeries is a convenience type alias for TimeSeries[float64] with weighted support.
type FloatTimeSeries = TimeSeries[float64]

// NewIntTimeSeries creates a new TimeSeries for int64 values with the specified retention period.
// This is a convenience constructor for backward compatibility.
func NewIntTimeSeries(retention time.Duration) IntTimeSeries {
	return NewTimeSeries[int64](retention)
}

// NewFloatTimeSeries creates a new weighted TimeSeries for float64 values with the specified retention period.
// This series supports weighted values and calculates weighted averages.
// This is a convenience constructor for backward compatibility.
func NewFloatTimeSeries(retention time.Duration) FloatTimeSeries {
	return NewWeightedTimeSeries[float64](retention)
}
