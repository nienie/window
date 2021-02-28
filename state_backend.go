package window

import "context"

//StateBackend ...
type StateBackend interface {

	//Update 更新窗口的状态
	Update(ctx context.Context, key string, event interface{}) (State, error)

	//Expire 给窗口状态设置过期时间
	Expire(ctx context.Context, key string, expireSeconds int64) error

	//Get 获取窗口的状态
	Get(ctx context.Context, key string) (State, error)

	//Del 删除窗口的状态
	Del(ctx context.Context, key string) error
}

//DummyStateBackend 测试用
type DummyStateBackend struct {
}

//Update ...
func (o *DummyStateBackend) Update(ctx context.Context, key string, event interface{}) (State, error) {
	return &DummyState{}, nil
}

//Expire ...
func (o *DummyStateBackend) Expire(ctx context.Context, key string, expireSeconds int64) error {
	return nil
}

//Get ...
func (o *DummyStateBackend) Get(ctx context.Context, key string) (State, error) {
	return &DummyState{}, nil
}

//Del ...
func (o *DummyStateBackend) Del(ctx context.Context, key string) error {
	return nil
}

var _ StateBackend = (*DummyStateBackend)(nil)
