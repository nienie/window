package window

import (
	"context"
	"time"
)

//Operator ...
type Operator interface {

	//Process 窗口统计处理
	//timestamp unix的时间戳，单位ns
	//event: 事件
	Process(ctx context.Context, timestamp time.Duration, event interface{}) error

	//GetState 获取某一时刻窗口状态
	//timestamp unix时间戳，单位ns
	GetState(ctx context.Context, timestamp time.Duration) (State, error)

	//GetCurrentState 获取当前时刻的窗口状态
	GetCurrentState(ctx context.Context) (State, error)

	//GetWindow 获取某一时刻的时间窗口
	GetWindow(ctx context.Context, timestamp time.Duration) *TimeWindow
}
