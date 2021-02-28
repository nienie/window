package window

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

//ChargeEvent 用户充值事件
type ChargeEvent struct {
	UID    int64 `json:"uid"`    //用户Uid
	Amount int64 `json:"amount"` //充值金额
	Ts     int64 `json:"ts"`     //充值时间戳，单位s
}

//ChargeState 用户充值的金额统计结果
type ChargeState struct {
	TotalAmount uint64 `json:"total_amount"`
}

//String ...
func (o *ChargeState) String() string {
	data, _ := json.Marshal(o)
	return string(data)
}

var _ State = (*ChargeState)(nil)

//ChargeStateBackend ...
type ChargeStateBackend struct {
	client *redis.Client
}

//NewChargeStateBackend ...
func NewChargeStateBackend(client *redis.Client) *ChargeStateBackend {
	return &ChargeStateBackend{
		client: client,
	}
}

//Get 获取窗口的状态
func (o *ChargeStateBackend) Get(ctx context.Context, key string) (State, error) {
	count, err := o.client.Get(ctx, key).Uint64()
	if err != nil {
		return nil, err
	}
	return &ChargeState{
		TotalAmount: count,
	}, nil
}

//Update 更新窗口的状态
func (o *ChargeStateBackend) Update(ctx context.Context, key string, ev interface{}) (State, error) {
	event, ok := ev.(*ChargeEvent)
	if !ok {
		return nil, fmt.Errorf("invalid event")
	}
	count, err := o.client.IncrBy(ctx, key, event.Amount).Uint64()
	return &ChargeState{
		TotalAmount: count,
	}, err
}

//Expire 给窗口状态设置过期时间
func (o *ChargeStateBackend) Expire(ctx context.Context, key string, expireSeconds int64) error {
	return o.client.Expire(ctx, key, time.Duration(expireSeconds)*time.Second).Err()
}

//Del 删除窗口状态
func (o *ChargeStateBackend) Del(ctx context.Context, key string) error {
	return o.client.Del(ctx, key).Err()
}

var _ StateBackend = (*ChargeStateBackend)(nil)
