package window

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type WindowsTestSuite struct {
	suite.Suite
}

func (o *WindowsTestSuite) TestAll() {
	var (
		w *TimeWindow
		added bool
	)
	windows := NewWindows()
	prefix := "test-windows"
	w1 := NewTimeWindow(prefix, 1*time.Second, 5*time.Second)
	w2 := NewTimeWindow(prefix, 3*time.Second, 7*time.Second)
	w3 := NewTimeWindow(prefix, 2*time.Second, 6*time.Second)

	added = windows.Add(w1)
	o.T().Logf("w=%s||added=%v", w1, added)
	o.Require().True(added)
	o.Require().Len(windows, 1)

	added = windows.Add(w1)
	o.T().Logf("w=%s||added=%v", w1, added)
	o.Require().False(added)
	o.Require().Len(windows, 1)

	added = windows.Add(w2)
	o.T().Logf("w=%s||added=%v", w2, added)
	o.Require().True(added)
	o.Require().Len(windows, 2)
	w = windows.Peek()
	o.Require().Equal(w1, w)

	added = windows.Add(w3)
	o.T().Logf("w=%s||added=%v", w3, added)
	o.Require().True(added)
	o.Require().Len(windows, 3)
	w = windows.Peek()
	o.Require().Equal(w1, w)

	//从小到大 pop
	w = windows.PopFront()
	o.Require().Equal(w1, w)
	w= windows.PopFront()
	o.Require().Equal(w3, w)
	w= windows.PopFront()
	o.Require().Equal(w2, w)
}

func TestWindows(t *testing.T) {
	suite.Run(t, new(WindowsTestSuite))
}