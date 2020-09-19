package excel

import (
	"os"
	"sync"
	"sync/atomic"

	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize"
)

type Excel struct {
	*excelize.File
	filepath string
	lock     *sync.Mutex
}

func NewExcel(filepath string) (*Excel, error) {
	_, err := os.Stat(filepath)
	if err == nil {
		return nil, fmt.Errorf("%s is exists", filepath)
	}
	return &Excel{
		filepath: filepath,
		File:     excelize.NewFile(),
		lock:     &sync.Mutex{},
	}, nil
}

func OpenExcel(filepath string) (*Excel, error) {
	f, err := excelize.OpenFile(filepath)
	if err != nil {
		return nil, err
	}

	return &Excel{
		filepath: filepath,
		File:     f,
		lock:     &sync.Mutex{},
	}, nil
}

func (e *Excel) GetSheet(name string) *Sheet {
	var exist bool
	for _, existsName := range e.GetSheetMap() {
		if name == existsName {
			exist = true
			break
		}
	}

	if !exist {
		e.File.NewSheet(name)
	}

	return &Sheet{f: e.File, name: name, lock: e.lock}
}

func (e *Excel) Save() error {
	return e.SaveAs(e.filepath)
}

type Sheet struct {
	name  string
	index int64
	f     *excelize.File
	lock  *sync.Mutex
}

func (s *Sheet) Append(vals ...interface{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	line := atomic.AddInt64(&s.index, 1)
	for i, val := range vals {
		col := GenerateExcelColumnPrefix(i)
		s.f.SetCellValue(s.name, fmt.Sprintf("%s%d", col, line), val)
	}
	return nil
}

func GenerateExcelColumnPrefix(n int) string {
	n += 1
	var s string
	for n > 0 {
		n -= 1
		s = string(byte(n%26+65)) + s
		n /= 26
	}

	return s
}
