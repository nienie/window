package window

import (
	"fmt"
	"time"
)

const (
	windowNameFormat = "%s:%d-%d"
)

//TimeWindow 事件窗口，精度ms
type TimeWindow struct {
	prefix  string
	start   time.Duration //窗口开始时间戳
	end     time.Duration //窗口结束时间戳
	precise time.Duration //窗口精度,ms
}

//NewTimeWindow ...
func NewTimeWindow(prefix string, start, end time.Duration) *TimeWindow {
	return &TimeWindow{
		prefix:  prefix,
		start:   start,
		end:     end,
		precise: time.Millisecond,
	}
}

//String ...
func (o TimeWindow) String() string {
	return fmt.Sprintf(`TimeWindow={"name":"%s"","start":%d,"end":%d,"size":"%s","precise":"%s"}`,
		o.prefix, o.start, o.end, o.GetWindowSize(), o.precise)
}

//GetStart 窗口开始时间戳，包括这个时间
func (o *TimeWindow) GetStart() time.Duration {
	return o.start
}

//GetEnd  窗口结束时间戳，不包括这个时间戳
func (o *TimeWindow) GetEnd() time.Duration {
	return o.end
}

//GetPrecise ...
func (o *TimeWindow) GetPrecise() time.Duration {
	return o.precise
}

//GetName ...
func (o *TimeWindow) GetName() string {
	return fmt.Sprintf(windowNameFormat, o.prefix, o.start/o.precise, o.end/o.precise)
}

//GetWindowSize 窗口的大小
func (o *TimeWindow) GetWindowSize() time.Duration {
	return o.end - o.start
}

//MaxTimestamp 窗口最大的时间戳
func (o *TimeWindow) MaxTimestamp() time.Duration {
	return o.end - o.precise
}

//Intersects Returns if this window intersects the given window.
func (o *TimeWindow) Intersects(other *TimeWindow) bool {
	return o.start <= other.end && o.end >= other.start
}

//Cover ...
func (o *TimeWindow) Cover(other *TimeWindow) *TimeWindow {
	return NewTimeWindow(o.prefix, time.Duration(minInt64(int64(o.start), int64(other.start))), time.Duration(maxInt64(int64(o.end), int64(other.end))))
}

//GetWindowStartWithOffset Method to get the window start for a timestamp
// timestamp epoch to get the window start.
// offset The offset which window start would be shifted by.
// windowSize The size of the generated windows.
func GetWindowStartWithOffset(timestamp, offset, windowSize time.Duration) time.Duration {
	return timestamp - (timestamp-offset+windowSize)%windowSize
}

func minInt64(first, second int64) int64 {
	if first <= second {
		return first
	}
	return second
}

func maxInt64(first, second int64) int64 {
	if first >= second {
		return first
	}
	return second
}
