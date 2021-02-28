package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

//SlidingWindowAssignerTestSuite ...
type SlidingWindowAssignerTestSuite struct {
	suite.Suite
}

//Test ...
func (o *SlidingWindowAssignerTestSuite) Test() {
	now := time.Now()
	var (
		window          *SlidingWindowAssigner
		assignedWindows []*TimeWindow
	)
	//7天窗口
	window = NewSlidingWindowAssigner("SevenDaySW", 7*24*time.Hour, 24*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1天的窗口，offset = 0
	window = NewSlidingWindowAssigner("OneDaySW", 24*time.Hour, 12*time.Hour, 0)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1天的窗口，北京东8区，快8小时，offset = -8h
	window = NewSlidingWindowAssigner("OneDaySW", 24*time.Hour, 12*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//12小时一个窗口
	window = NewSlidingWindowAssigner("TwelveHourSW", 12*time.Hour, 3*time.Hour, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
	//1小时一个窗口
	window = NewSlidingWindowAssigner("OneHourSW", 1*time.Hour, 10*time.Minute, -8*time.Hour)
	assignedWindows = window.AssignWindows(time.Duration(now.UnixNano()))
	for _, w := range assignedWindows {
		o.T().Logf("name=%s||window_size=%s||now=%s||start=%s||end=%s", w.GetName(), w.GetWindowSize(), now.Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetStart()/time.Second), int64(w.GetStart()%time.Second)).Format("2006-01-02 15:04:05"),
			time.Unix(int64(w.GetEnd()/time.Second), int64(w.GetEnd()%time.Second)).Format("2006-01-02 15:04:05"))
	}
}

//TestSlidingWindowAssigner ...
func TestSlidingWindowAssigner(t *testing.T) {
	suite.Run(t, new(SlidingWindowAssignerTestSuite))
}

//结果：
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1605024000000-1605628800000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 00:00:00||end=2020-11-18 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604937600000-1605542400000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-10 00:00:00||end=2020-11-17 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604851200000-1605456000000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-09 00:00:00||end=2020-11-16 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604764800000-1605369600000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-08 00:00:00||end=2020-11-15 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604678400000-1605283200000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-07 00:00:00||end=2020-11-14 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604592000000-1605196800000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-06 00:00:00||end=2020-11-13 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:26: name=SevenDaySW:1604505600000-1605110400000||window_size=168h0m0s||now=2020-11-11 10:44:45||start=2020-11-05 00:00:00||end=2020-11-12 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:34: name=OneDaySW:1605052800000-1605139200000||window_size=24h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 08:00:00||end=2020-11-12 08:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:34: name=OneDaySW:1605009600000-1605096000000||window_size=24h0m0s||now=2020-11-11 10:44:45||start=2020-11-10 20:00:00||end=2020-11-11 20:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:42: name=OneDaySW:1605024000000-1605110400000||window_size=24h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 00:00:00||end=2020-11-12 00:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:42: name=OneDaySW:1604980800000-1605067200000||window_size=24h0m0s||now=2020-11-11 10:44:45||start=2020-11-10 12:00:00||end=2020-11-11 12:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:50: name=TwelveHourSW:1605056400000-1605099600000||window_size=12h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 09:00:00||end=2020-11-11 21:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:50: name=TwelveHourSW:1605045600000-1605088800000||window_size=12h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 06:00:00||end=2020-11-11 18:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:50: name=TwelveHourSW:1605034800000-1605078000000||window_size=12h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 03:00:00||end=2020-11-11 15:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:50: name=TwelveHourSW:1605024000000-1605067200000||window_size=12h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 00:00:00||end=2020-11-11 12:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605062400000-1605066000000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 10:40:00||end=2020-11-11 11:40:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605061800000-1605065400000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 10:30:00||end=2020-11-11 11:30:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605061200000-1605064800000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 10:20:00||end=2020-11-11 11:20:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605060600000-1605064200000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 10:10:00||end=2020-11-11 11:10:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605060000000-1605063600000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 10:00:00||end=2020-11-11 11:00:00
//TestSlidingWindowAssigner/Test: sliding_window_assigner_test.go:58: name=OneHourSW:1605059400000-1605063000000||window_size=1h0m0s||now=2020-11-11 10:44:45||start=2020-11-11 09:50:00||end=2020-11-11 10:50:00
