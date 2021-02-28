package window

import (
	"context"
	"time"
)

//TimeWindowState ...
type TimeWindowState struct {
	window  *TimeWindow
	backend StateBackend
}

//NewTimeWindowState ...
func NewTimeWindowState(window *TimeWindow, backend StateBackend) *TimeWindowState {
	return &TimeWindowState{
		window:  window,
		backend: backend,
	}
}

//Get ...
func (o *TimeWindowState) Get(ctx context.Context) (State, error) {
	return o.backend.Get(ctx, o.window.GetName())
}

//Update ...
func (o *TimeWindowState) Update(ctx context.Context, event interface{}) error {
	_, err := o.backend.Update(ctx, o.window.GetName(), event)
	if err != nil {
		return err
	}
	return o.backend.Expire(ctx, o.window.GetName(), int64(o.window.GetWindowSize()/time.Second))
}

//Del ...
func (o *TimeWindowState) Del(ctx context.Context) error {
	return o.backend.Del(ctx, o.window.GetName())
}
