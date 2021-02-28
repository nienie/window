package window

import "time"

//SlidingWindowAssigner ...
type SlidingWindowAssigner struct {
	name   string
	size   time.Duration
	offset time.Duration
	slide  time.Duration
}

//NewSlidingWindowAssigner ...
func NewSlidingWindowAssigner(name string, size, slide, offset time.Duration) *SlidingWindowAssigner {
	return &SlidingWindowAssigner{
		name:   name,
		size:   size,
		slide:  slide,
		offset: offset,
	}
}

//AssignWindows ...
func (o *SlidingWindowAssigner) AssignWindows(timestamp time.Duration) []*TimeWindow {
	windows := make([]*TimeWindow, 0, o.size/o.slide)
	lastStart := GetWindowStartWithOffset(timestamp, o.offset, o.slide)
	for start := lastStart; start > timestamp-o.size; start -= o.slide {
		windows = append(windows, NewTimeWindow(o.name, start, start+o.size))
	}
	return windows
}

var _ Assigner = (*SlidingWindowAssigner)(nil)
