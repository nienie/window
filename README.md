# window

## 背景

在我们业务开发中，经常遇到类似"用户当天累计消费金额"、"用户最近三天消费金额"等统计的需求。

本质上来说，这都是窗口统计(第一种是窗口大小为一天的滚动窗口，第二种是窗口大小为3天，滑动大小为一天的滑动窗口)。

通常，我们可以保存所有的消费记录到MySQL，然后通过MySQL的查询，来统计出来。

但是，如果数据量非常大，且访问量非常高，MySQL无法支撑起线上业务如此庞大的实时查询，我们该如何实时的统计出用户时间窗口内的值呢？

Flink提供了非常丰富的窗口算子，利用Flink提供的API，就可以非常简单的完成窗口计算。

但是，很多业务都是非Java业务。另外为了一个简单的统计功能，引入非常复杂的Flink，实现过于重。

那么，有没有简洁的方法实现，类似Flink的窗口计算功能呢。

[Window](https://github.com/nienie/window) 提供了Golang的解决方案。

## 特点

提供了滑动窗口和滚动窗口算子，让Golang很容易实现类似Flink的窗口统计的逻辑。

## 基本概念

- **Time Window**

时间窗口，时间在窗口开始时间和结束时间之内的元素，都会属于这个窗口。时间窗口主要有两种类型：1）**滚动窗口**(**Tumbling Window**)，2）**滑动窗口**(**Sliding Window**)。

- **Tumbling Window**

**滚动窗口**，某个时刻只属于一个窗口。例如，1天大小的滚动窗口，时间段[2020-11-11 00:00:00, 2020-11-12 00:00:00)是一个窗口，
时间段[2020-11-12 00:00:00, 2020-11-13 00:00:00)是另外一个窗口。2020-11-11 10:52:48 只会属于[2020-11-11 00:00:00, 2020-11-12 00:00:00) 这个窗口。
而不会属于[2020-11-12 00:00:00, 2020-11-13 00:00:00)的窗口。

- **Sliding Window**

**滑动窗口**，某个时刻会属于多个窗口。例如，1天大小的滚动窗口，12h滑动一次。时间段[2020-11-10 12:00:00, 2020-11-11 12:00:00)和[2020-11-11 00:00:00, 2020-11-12 00:00:00)都是一个窗口。
窗口之间有重合，时间2020-11-11 10:44:45，会同时属于这个窗口。

- **State**

**状态**，有状态流式计算中的概念，是统计的中间结果也可以是最终的结果。例如，要统计用户充值总金额，State就是用户这个充值的总金额。

- **StateBackend**

**StateBackend**，状态存储的抽象。

## 使用与示例

详见[一天内用户充值金额统计(滚动窗口)](https://github.com/nienie/window/blob/master/tumbling_window_operator_test.go)

详见[三天内用户充值金额统计(滑动窗口)](https://github.com/nienie/window/blob/master/sliding_window_operator_test.go)

1) 定义State。

```golang
//State就是我们要统计的东西。
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
```

2) 定义StateBackend.

```golang
//ChargeEvent 用户充值事件
type ChargeEvent struct {
	UID    int64 `json:"uid"`    //用户Uid
	Amount int64 `json:"amount"` //充值金额
	Ts     int64 `json:"ts"`     //充值时间戳，单位s
}

//ChargeStateBackend 充值状态的StateBackend
type ChargeStateBackend struct {
	client *redis.Client
}

//NewChargeState ...
func NewChargeStateBackend(client *redis.Client) *ChargeStateBackend {
	return &ChargeStateBackend{
		client: client,
	}
}

//Get 获取充值统计结果
func (o *ChargeStateBackend) Get(ctx context.Context, key string) (State, error) {
	count, err := o.client.Get(ctx, key).Uint64()
	if err != nil {
		return nil, err
	}
	return &ChargeState{
		TotalAmount: count,
	}, nil
}

//Update 当有新的充值事件发生，更新充值结果
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

//Expire 给充值结果设置过期时间
func (o *ChargeStateBackend) Expire(ctx context.Context, key string, expireSeconds int64) error {
	return o.client.Expire(ctx, key, time.Duration(expireSeconds)*time.Second).Err()
}

//Del 删除充值结果的State
func (o *ChargeStateBackend) Del(ctx context.Context, key string) error {
	return o.client.Del(ctx, key).Err()
}

```

3) 定义滚动窗口或者滑动窗口。

```golang
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
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:80: uid=100000||timestamp=2020-11-10 11:37:15||window_size=24h0m0s||window_start=2020-11-10 00:00:00||window_end=2020-11-11 00:00:00||state={"TotalAmount":298}||err=<nil>
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:95: uid=100000||timestamp=2020-11-11 11:37:15||window_size=24h0m0s||window_start=2020-11-11 00:00:00||window_end=2020-11-12 00:00:00||state={"TotalAmount":308}||err=<nil>
```

```golang
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
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:80: uid=100000||timestamp=2020-11-10 11:37:15||window_size=24h0m0s||window_start=2020-11-10 00:00:00||window_end=2020-11-11 00:00:00||state={"TotalAmount":298}||err=<nil>
//TestTumblingWindowOperator/Test: tumbling_window_operator_test.go:95: uid=100000||timestamp=2020-11-11 11:37:15||window_size=24h0m0s||window_start=2020-11-11 00:00:00||window_end=2020-11-12 00:00:00||state={"TotalAmount":308}||err=<nil>
```
