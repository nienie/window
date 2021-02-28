package window

import "time"

//TumblingWindowAssigner ...
type TumblingWindowAssigner struct {
	name   string
	size   time.Duration
	offset time.Duration
}

//NewTumblingWindowAssigner ...
func NewTumblingWindowAssigner(name string, size, offset time.Duration) *TumblingWindowAssigner {
	return &TumblingWindowAssigner{
		name:   name,
		size:   size,
		offset: offset,
	}
}

//AssignWindows ...
func (o *TumblingWindowAssigner) AssignWindows(timestamp time.Duration) []*TimeWindow {
	start := GetWindowStartWithOffset(timestamp, o.offset, o.size)
	return []*TimeWindow{NewTimeWindow(o.name, start, start+o.size)}
}

var _ Assigner = (*TumblingWindowAssigner)(nil)
