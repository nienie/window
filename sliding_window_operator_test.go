package window

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

//SlidingWindowOperatorTestSuite ....
type SlidingWindowOperatorTestSuite struct {
	suite.Suite
	redisClient *redis.Client
}

//SetupSuite ....
func (o *SlidingWindowOperatorTestSuite) SetupSuite() {
	o.redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	err := o.redisClient.Ping(context.TODO()).Err()
	o.Require().Nil(err)
}

func (o *SlidingWindowOperatorTestSuite) Test() {
	var (
		uid            int64 //用户ID
		windowOperator *SlidingWindowOperator
		size           time.Duration //窗口大小
		slide          time.Duration //滑动距离
		offset         time.Duration //时间偏移量，跟当前所在时区有关
	)
	ctx := context.TODO()
	uid = 100000
	operatorName := fmt.Sprintf("three-day-charge-%d", uid) //窗口算子名字
	size = 3 * 24 * time.Hour                               //窗口大小， 3天，统计3天内，用户uid=100000 充值金额
	slide = 24 * time.Hour                                  //滑动距离
	offset = -8 * time.Hour                                 //偏移量，北京东8区，所以要减去8小时
	//三天时长的滑动窗口，一天滑动一次
	windowOperator = NewSlidingWindowOperator(operatorName, size, slide, offset, NewChargeStateBackend(o.redisClient))
	//用户充值的事件
	now := time.Now()
	events := []*ChargeEvent{
		//4天前充值99
		{
			UID:    uid,
			Amount: 99,
			Ts:     now.Add(-4 * 24 * time.Hour).Unix(),
		},
		//三天前充值199
		{
			UID:    uid,
			Amount: 199,
			Ts:     now.Add(-3 * 24 * time.Hour).Unix(),
		},
		//两天前充值，92
		{
			UID:    uid,
			Amount: 92,
			Ts:     now.Add(-2 * 24 * time.Hour).Unix(),
		},
		//一天前充值180
		{
			UID:    uid,
			Amount: 180,
			Ts:     now.Add(-1 * 24 * time.Hour).Unix(),
		},
		//今天充值36
		{
			UID:    uid,
			Amount: 36,
			Ts:     now.Unix(),
		},
	}
	//每个充值事件都丢进窗口算子进行处理
	for _, event := range events {
		windowOperator.Process(ctx, time.Duration(event.Ts)*time.Second, event)
	}
	//昨天开始，3天内的充值金额统计结果
	timestamp := now.Add(-24 * time.Hour)
	window := windowOperator.GetWindow(ctx, time.Duration(timestamp.UnixNano()))
	state, err := windowOperator.GetState(ctx, time.Duration(timestamp.UnixNano()))
	o.T().Logf("uid=%d||timestamp=%s||window_size=%s||window_start=%s||window_end=%s||state=%s||err=%v",
		uid, timestamp.Format("2006-01-02 15:04:05"), size,
		time.Unix(int64(window.GetStart()/time.Second), int64(window.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
		time.Unix(int64(window.GetEnd()/time.Second), int64(window.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"),
		state, err)
	o.Require().Nil(err)
	//471 = 199 + 92 + 180
	o.Require().Equal(uint64(471), state.(*ChargeState).TotalAmount)
	//清理结果，防止重复执行会失败
	windowOperator.GetStateBackend().Del(ctx, window.GetName())

	//最近3天充值的金额统计结果
	timestamp = now
	window = windowOperator.GetWindow(ctx, time.Duration(timestamp.UnixNano()))
	state, err = windowOperator.GetState(ctx, time.Duration(timestamp.UnixNano()))
	o.T().Logf("uid=%d||timestamp=%s||window_size=%s||window_start=%s||window_end=%s||state=%s||err=%v",
		uid, timestamp.Format("2006-01-02 15:04:05"), size,
		time.Unix(int64(window.GetStart()/time.Second), int64(window.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
		time.Unix(int64(window.GetEnd()/time.Second), int64(window.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"),
		state, err)
	o.Require().Nil(err)
	//308 = 92 + 180 + 36
	o.Require().Equal(uint64(308), state.(*ChargeState).TotalAmount)
	//清理结果，防止重复执行会失败
	windowOperator.GetStateBackend().Del(ctx, window.GetName())
}

func TestSlidingWindowOperator(t *testing.T) {
	suite.Run(t, new(SlidingWindowOperatorTestSuite))
}

//结果：
//TestSlidingWindowOperator/Test: sliding_window_operator_test.go:81: uid=100000||timestamp=2020-11-10 12:39:34||window_size=72h0m0s||window_start=2020-11-08 00:00:00||window_end=2020-11-11 00:00:00||state={"TotalAmount":471}||err=<nil>
//TestSlidingWindowOperator/Test: sliding_window_operator_test.go:95: uid=100000||timestamp=2020-11-11 12:39:34||window_size=72h0m0s||window_start=2020-11-09 00:00:00||window_end=2020-11-12 00:00:00||state={"TotalAmount":308}||err=<nil>
