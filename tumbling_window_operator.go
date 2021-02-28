package window

import (
	"context"
	"fmt"
	"time"
)

//TumblingWindowOperator ...
type TumblingWindowOperator struct {
	name     string
	size     time.Duration
	offset   time.Duration
	backend  StateBackend
	assigner *TumblingWindowAssigner
}

//NewTumblingWindowOperator ...
//size: 窗口大小
//offset: 偏移量，跟时区有关，北京东八区offset = -8h
func NewTumblingWindowOperator(name string, size, offset time.Duration, backend StateBackend) *TumblingWindowOperator {
	return &TumblingWindowOperator{
		name:     name,
		size:     size,
		offset:   offset,
		backend:  backend,
		assigner: NewTumblingWindowAssigner(name, size, offset),
	}
}

//GetName ...
func (o *TumblingWindowOperator) GetName() string {
	return o.name
}

//GetSize ...
func (o *TumblingWindowOperator) GetSize() time.Duration {
	return o.size
}

//GetOffset ...
func (o *TumblingWindowOperator) GetOffset() time.Duration {
	return o.offset
}

//GetAssigner ...
func (o *TumblingWindowOperator) GetAssigner() Assigner {
	return o.assigner
}

//GetStateBackend ...
func (o *TumblingWindowOperator) GetStateBackend() StateBackend {
	return o.backend
}

//Process ...
// timestamp unix时间戳时间戳，单位ns
// event 处理的事件
func (o *TumblingWindowOperator) Process(ctx context.Context, timestamp time.Duration, event interface{}) error {
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
func (o *TumblingWindowOperator) GetWindow(ctx context.Context, timestamp time.Duration) *TimeWindow {
	windows := o.assigner.AssignWindows(timestamp)
	return windows[0]
}

//GetState 获取某一时刻的状态
func (o *TumblingWindowOperator) GetState(ctx context.Context, timestamp time.Duration) (State, error) {
	window := o.GetWindow(ctx, timestamp)
	windowState := NewTimeWindowState(window, o.backend)
	return windowState.Get(ctx)
}

//GetCurrentState 获取当前时刻的状态
func (o *TumblingWindowOperator) GetCurrentState(ctx context.Context) (State, error) {
	return o.GetState(ctx, time.Duration(time.Now().UnixNano()))
}

//String ...
func (o TumblingWindowOperator) String() string {
	return fmt.Sprintf(`TumblingWindowOperator={"type":%v,"name":"%s","size":"%s","offset":"%s"}`,
		Tumbling, o.name, o.size, o.offset)
}

var _ Operator = (*TumblingWindowOperator)(nil)
