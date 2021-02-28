package window

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

//WindowStateTestSuite ...
type WindowStateTestSuite struct {
	suite.Suite
}

func (o *WindowStateTestSuite) Test() {
	window := NewTimeWindow("window-state-test", time.Duration(time.Now().UnixNano()), time.Duration(time.Now().Add(24*time.Hour).UnixNano()))
	windowState := NewTimeWindowState(window, new(DummyStateBackend))
	ctx := context.TODO()
	err := windowState.Update(ctx, nil)
	o.Require().Nil(err)
	state, err := windowState.Get(ctx)
	o.Require().Nil(err)
	o.T().Logf("state=%s", state)
	err = windowState.Del(ctx)
	o.Require().Nil(err)
}

//TestWindowState ...
func TestWindowState(t *testing.T) {
	suite.Run(t, new(WindowStateTestSuite))
}
