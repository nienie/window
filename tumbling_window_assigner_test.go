package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

//TumblingWindowAssignerTestSuite ...
type TumblingWindowAssignerTestSuite struct {
	suite.Suite
}

//Test ...
func (o *TumblingWindowAssignerTestSuite) Test() {
	now := time.Now()
	var (
		window          *TumblingWindowAssigner
		assignedWindows []*TimeWindow
	)
	//7天窗口
	window = NewTumblingWindowAssigner("SevenDayTW", 7*24*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1天的窗口，offset = 0
	window = NewTumblingWindowAssigner("OneDayTW", 24*time.Hour, 0)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1天的窗口，北京东8区，快8小时，offset = -8h
	window = NewTumblingWindowAssigner("OneDayTW", 24*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1天的窗口，美国西8区时间
	window = NewTumblingWindowAssigner("OneDayTW", 24*time.Hour, 8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(),
			now.In(time.FixedZone("GMT-8", -8*60*60)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).In(time.FixedZone("GMT-8", -8*60*60)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).In(time.FixedZone("GMT-8", -8*60*60)).Format("2006-01-02 15:04:05"))
	}
	//12小时一个窗口
	window = NewTumblingWindowAssigner("TwelveHourTW", 12*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//8小时一个窗口
	window = NewTumblingWindowAssigner("EightHourTW", 8*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1小时一个窗口
	window = NewTumblingWindowAssigner("OneHourTW", 1*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
}

//TestTumblingWindowAssigner ...
func TestTumblingWindowAssigner(t *testing.T) {
	suite.Run(t, new(TumblingWindowAssignerTestSuite))
}

//结果：
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:26: name=SevenDayTW:1605110400000-1605715200000||window_size=168h0m0s||now=2020-11-13 16:21:59||start=2020-11-12 00:00:00||end=2020-11-19 00:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:34: name=OneDayTW:1605225600000-1605312000000||window_size=24h0m0s||now=2020-11-13 16:21:59||start=2020-11-13 08:00:00||end=2020-11-14 08:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:42: name=OneDayTW:1605196800000-1605283200000||window_size=24h0m0s||now=2020-11-13 16:21:59||start=2020-11-13 00:00:00||end=2020-11-14 00:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:50: name=OneDayTW:1605254400000-1605340800000||window_size=24h0m0s||now=2020-11-13 00:21:59||start=2020-11-13 00:00:00||end=2020-11-14 00:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:59: name=TwelveHourTW:1605240000000-1605283200000||window_size=12h0m0s||now=2020-11-13 16:21:59||start=2020-11-13 12:00:00||end=2020-11-14 00:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:67: name=EightHourTW:1605254400000-1605283200000||window_size=8h0m0s||now=2020-11-13 16:21:59||start=2020-11-13 16:00:00||end=2020-11-14 00:00:00
//TestTumblingWindowAssigner/Test: tumbling_window_assigner_test.go:75: name=OneHourTW:1605254400000-1605258000000||window_size=1h0m0s||now=2020-11-13 16:21:59||start=2020-11-13 16:00:00||end=2020-11-13 17:00:00
