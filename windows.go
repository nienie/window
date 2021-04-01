package window

import "sort"

//Windows ...
type Windows []*TimeWindow

//NewWindows ...
func NewWindows() Windows {
	return make([]*TimeWindow, 0)
}

//Len ...
func (o Windows) Len() int {
	return len(o)
}

//Swap ...
func (o Windows) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

//Less ...
func (o Windows) Less(i, j int) bool {
	return o[i].GetStart() <= o[j].GetStart()
}

//Sort ...
func (o *Windows) Sort() {
	sort.Stable(o)
}

//Add ...
func (o *Windows) Add(w *TimeWindow) bool {
	for _, tw := range *o {
		//w已经在了，直接返回
		if tw.GetName() == w.GetName() {
			return false
		}
	}
	//添加到windows中
	*o = append(*o, w)
	//从小到大排列
	o.Sort()
	return true
}

//Peek ...
func (o Windows) Peek() *TimeWindow {
	if len(o) == 0 {
		return nil
	}
	return o[0]
}

//PopFront ...
func (o *Windows) PopFront() *TimeWindow {
	if len(*o) == 0 {
		return nil
	}
	tw := (*o)[0]
	*o = (*o)[1:]
	return tw
}