package pdf

import (
	"bytes"
	"errors"
	"io/ioutil"
	"strings"

	"github.com/lu4p/unipdf/v3/extractor"
	pdf "github.com/lu4p/unipdf/v3/model"
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
	return extractPages(filepath, nil)
}

func ExtractPagesWithPassword(filepath string, password []byte) ([]Page, error) {
	return extractPages(filepath, password)
}

func extractPages(filepath string, password []byte) ([]Page, error) {
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	pdfReader, err := pdf.NewPdfReader(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	isEncrypted, err := pdfReader.IsEncrypted()
	if err != nil {
		return nil, err
	}

	if isEncrypted {
		ok, err := pdfReader.Decrypt(password)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("This pdf file need be decrypt first")
		}
	}

	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return nil, err
	}

	var pages []Page
	for i := 0; i < numPages; i++ {
		pageNum := i + 1

		page, err := pdfReader.GetPage(pageNum)
		if err != nil {
			return nil, err
		}

		ex, err := extractor.New(page)
		if err != nil {
			return nil, err
		}

		text, err := ex.ExtractText()
		if err != nil {
			return nil, err
		}

		pages = append(pages, Page{Num: pageNum, Text: text})
	}
	return pages, nil
}
