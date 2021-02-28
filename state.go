package window

//State ...
type State interface {
	//String ...
	String() string
}

//DummyState 测试用
type DummyState struct {
}

//String ...
func (o *DummyState) String() string {
	return "DummyState"
}

var _ State = (*DummyState)(nil)
