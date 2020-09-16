package excel

import (
	"sync"
	"sync/atomic"

	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize"
)

/*
f := excelize.NewFile()

f.SetCellStr("Sheet1", "A1", "BundleKey")
f.SetCellStr("Sheet1", "B1", "Name")
f.SetCellStr("Sheet1", "C1", "Reason")
f.SetCellStr("Sheet1", "D1", "Sentence")

var ch = make(chan BadCase, 1)
go consumerKakfa(ch)
var i = 2
for c := range ch {
    fmt.Println(c, i)
    f.SetCellStr("Sheet1", fmt.Sprintf("A%d", i), c.BundleKey)
    f.SetCellStr("Sheet1", fmt.Sprintf("B%d", i), c.Name)
    f.SetCellStr("Sheet1", fmt.Sprintf("C%d", i), c.Reason)
    f.SetCellStr("Sheet1", fmt.Sprintf("D%d", i), c.Sentence)
    i++
}

if err := f.SaveAs("bad_case.xlsx"); err != nil {
    panic(err)
}
*/

type Excel struct {
	filepath string
	*excelize.File
}

func NewExcel(filepath string) *Excel {
	return &Excel{
		filepath: filepath,
		File:     excelize.NewFile(),
	}
}

func (e *Excel) GetSheet(name string) *Sheet {
	var exist bool
	for _, existsName := range e.GetSheetList() {
		if name == existsName {
			exist = true
			break
		}
	}

	if !exist {
		e.File.NewSheet(name)
	}

	return &Sheet{f: e.File, name: name}
}

func (e *Excel) Save() error {
	return e.SaveAs(e.filepath)
}

type Sheet struct {
	name  string
	index int64
	f     *excelize.File
	lock  sync.Mutex
}

func (s *Sheet) Append(vals ...interface{}) {
	line := atomic.AddInt64(&s.index, 1)
	for i, val := range vals {
		col := generateColumnPrefix(i)
		s.f.SetCellValue(s.name, fmt.Sprintf("%s%d", col, line), val)
	}
}

func generateColumnPrefix(n int) string {
	n += 1
	var s string
	for n > 0 {
		n -= 1
		s = string(byte(n%26+65)) + s
		n /= 26
	}
	return s
}
