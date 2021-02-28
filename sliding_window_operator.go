package window

import (
	"context"
	"fmt"
	"time"
)

//SlidingWindowOperator ...
type SlidingWindowOperator struct {
	name     string
	size     time.Duration
	slide    time.Duration
	offset   time.Duration
	backend  StateBackend
	assigner *SlidingWindowAssigner
}

//NewSlidingWindowOperator  ...
//size: 窗口大小
//slide: 滑动的时间
//offset: 偏移量，跟时区有关，北京东八区offset = -8h
func NewSlidingWindowOperator(name string, size, slide, offset time.Duration, backend StateBackend) *SlidingWindowOperator {
	return &SlidingWindowOperator{
		name:     name,
		size:     size,
		slide:    slide,
		offset:   offset,
		backend:  backend,
		assigner: NewSlidingWindowAssigner(name, size, slide, offset),
	}
}

//GetName ...
func (o *SlidingWindowOperator) GetName() string {
	return o.name
}

//GetSize ...
func (o *SlidingWindowOperator) GetSize() time.Duration {
	return o.size
}

//GetOffset ...
func (o *SlidingWindowOperator) GetOffset() time.Duration {
	return o.offset
}

//GetSlide ...
func (o *SlidingWindowOperator) GetSlide() time.Duration {
	return o.slide
}

//GetAssigner ...
func (o *SlidingWindowOperator) GetAssigner() Assigner {
	return o.assigner
}

//GetStateBackend ...
func (o *SlidingWindowOperator) GetStateBackend() StateBackend {
	return o.backend
}

//Process ...
// timestamp unix时间戳时间戳，单位ns
// event 处理的事件
func (o *SlidingWindowOperator) Process(ctx context.Context, timestamp time.Duration, event interface{}) error {
	windows := o.assigner.AssignWindows(timestamp)
	var (
		gerr error
		err  error
	)
	for _, window := range windows {
		windowState := NewTimeWindowState(window, o.backend)
		err = windowState.Update(ctx, event)
		if err != nil {
			gerr = err
			continue
		}
	}
	return gerr
}

//GetWindow 获取某一时刻的时间窗口
func (o *SlidingWindowOperator) GetWindow(ctx context.Context, timestamp time.Duration) *TimeWindow {
	windows := o.assigner.AssignWindows(timestamp)
	for i := len(windows) - 1; i >= 0; i-- {
		if windows[i].GetEnd() > timestamp {
			return windows[i]
		}
	}
	return windows[0]
}

//GetState 获取某一时刻的状态
func (o *SlidingWindowOperator) GetState(ctx context.Context, timestamp time.Duration) (State, error) {
	window := o.GetWindow(ctx, timestamp)
	windowState := NewTimeWindowState(window, o.backend)
	return windowState.Get(ctx)
}

//GetCurrentState 获取当前时刻的状态
func (o *SlidingWindowOperator) GetCurrentState(ctx context.Context) (State, error) {
	return o.GetState(ctx, time.Duration(time.Now().UnixNano()))
}

//String ...
func (o SlidingWindowOperator) String() string {
	return fmt.Sprintf(`SlidingWindowOperator={"type":%v,"name":"%s","size":"%s","slide":"%s","offset":"%s"}`,
		Sliding, o.name, o.size, o.slide, o.offset)
}

var _ Operator = (*SlidingWindowOperator)(nil)
