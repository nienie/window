package window

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

//TumblingWindowOperatorTestSuite ...
type TumblingWindowOperatorTestSuite struct {
	suite.Suite
	redisClient *redis.Client
}

func (o *TumblingWindowOperatorTestSuite) SetupSuite() {
	o.redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	err := o.redisClient.Ping(context.TODO()).Err()
	o.Require().Nil(err)
}

func (o *TumblingWindowOperatorTestSuite) Test() {
	var (
		uid            int64 //用户ID
		windowOperator *TumblingWindowOperator
		size           time.Duration //窗口大小
		offset         time.Duration //时间偏移量，跟当前所在时区有关
	)
	ctx := context.TODO()
	uid = 100000
	operatorName := fmt.Sprintf("one-day-charge-%d", uid) //窗口算子名字
	size = 24 * time.Hour                                 //窗口大小， 1天，统计一天内，用户uid=100000 充值金额
	offset = -8 * time.Hour                               //偏移量，北京东8区，所以要减去8小时
	//一天大小的滚动窗口
	windowOperator = NewTumblingWindowOperator(operatorName, size, offset, NewChargeStateBackend(o.redisClient))
	//用户充值的事件
	now := time.Now()
	events := []*ChargeEvent{
		//昨天第一次充值，99
		{
			UID:    uid,
			Amount: 99,
			Ts:     now.Add(-24 * time.Hour).Unix(),
		},
		//昨天第二次充值，199
		{
			UID:    uid,
			Amount: 199,
			Ts:     now.Add(-24 * time.Hour).Unix(),
		},
		//今天第一次充值，92
		{
			UID:    uid,
			Amount: 92,
			Ts:     now.Unix(),
		},
		//今天第二次充值，180
		{
			UID:    uid,
			Amount: 180,
			Ts:     now.Unix(),
		},
		//今天第二次充值，36
		{
			UID:    uid,
			Amount: 36,
			Ts:     now.Unix(),
		},
	}
	//每个事件都丢进窗口算子处理
	for _, event := range events {
		windowOperator.Process(ctx, time.Duration(event.Ts)*time.Second, event)
	}
	//昨天充值金额统计结果
	timestamp := now.Add(-24 * time.Hour)
	window := windowOperator.GetWindow(ctx, time.Duration(timestamp.UnixNano()))
	state, err := windowOperator.GetState(ctx, time.Duration(timestamp.UnixNano()))
	o.T().Logf("uid=%d||timestamp=%s||window_size=%s||window_start=%s||window_end=%s||state=%s||err=%v",
		uid, timestamp.Format("2006-01-02 15:04:05"), size,
		time.Unix(int64(window.GetStart()/time.Second), int64(window.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
		time.Unix(int64(window.GetEnd()/time.Second), int64(window.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"),
		state, err)
	o.Require().Nil(err)
	o.Require().Equal(uint64(298), state.(*ChargeState).TotalAmount)
	//清理结果，防止重复执行会失败
	windowOperator.GetStateBackend().Del(ctx, window.GetName())

	//今天充值的金额统计结果
	timestamp = now
	window = windowOperator.GetWindow(ctx, time.Duration(timestamp.UnixNano()))
	state, err = windowOperator.GetState(ctx, time.Duration(timestamp.UnixNano()))
	o.T().Logf("uid=%d||timestamp=%s||window_size=%s||window_start=%s||window_end=%s||state=%s||err=%v",
		uid, timestamp.Format("2006-01-02 15:04:05"), size,
		time.Unix(int64(window.GetStart()/time.Second), int64(window.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
		time.Unix(int64(window.GetEnd()/time.Second), int64(window.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"),
		state, err)
	o.Require().Nil(err)
	o.Require().Equal(uint64(308), state.(*ChargeState).TotalAmount)
	//清理结果，防止重复执行会失败
	windowOperator.GetStateBackend().Del(ctx, window.GetName())
}

//TestTumblingWindowOperator ...
func TestTumblingWindowOperator(t *testing.T) {
	suite.Run(t, new(TumblingWindowOperatorTestSuite))
}

//结果：
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:80: uid=100000||timestamp=2020-11-10 11:37:15||window_size=24h0m0s||window_start=2020-11-10 00:00:00||window_end=2020-11-11 00:00:00||state={"TotalAmount":298}||err=<nil>
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:95: uid=100000||timestamp=2020-11-11 11:37:15||window_size=24h0m0s||window_start=2020-11-11 00:00:00||window_end=2020-11-12 00:00:00||state={"TotalAmount":308}||err=<nil>
