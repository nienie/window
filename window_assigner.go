package window

import "time"

//Assigner ...
type Assigner interface {

	//AssignWindows ...
	AssignWindows(timestamp time.Duration) []*TimeWindow
}
