package pdf

import (
	"github.com/ledongthuc/pdf"
	"os"
	"strings"
)

type Page struct {
	Num  int
	Text string
}

func (p Page) String() string {
	return strings.TrimSpace(p.Text)
}

type Pages []Page

func (pages Pages) String() string {
	var b strings.Builder
	for _, page := range pages {
		t := page.String()
		if t != "" {
			b.WriteString(t)
		}
	}
	return b.String()
}

func ExtractPages(filepath string) ([]Page, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	r, err := pdf.NewReader(f, fi.Size())
	if err != nil {
		return nil, err
	}

	pageNums := r.NumPage()
	var pages []Page
	fonts := make(map[string]*pdf.Font)
	for i := 1; i <= pageNums; i++ {
		p := r.Page(i)
		for _, name := range p.Fonts() { // cache fonts so we don't continually parse charmap
			if _, ok := fonts[name]; !ok {
				f := p.Font(name)
				fonts[name] = &f
			}
		}
		text, err := p.GetPlainText(fonts)
		if err != nil {
			return nil, err
		}
		pages = append(pages, Page{Num: i, Text: text})
	}
	return pages, nil
}
