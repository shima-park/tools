package ioutil

import (
	"bufio"
	"context"
	"io"
	"os"
)

type Line struct {
	Bytes []byte
	Err   error
}

func (l Line) String() string {
	return string(l.Bytes)
}

func ScanLines(ctx context.Context, r io.Reader) chan Line {
	if ctx == nil {
		ctx = context.Background()
	}

	ch := make(chan Line)
	go func() {
		defer func() {
			close(ch)
			if closer, ok := r.(io.ReadCloser); ok {
				closer.Close()
			}
		}()

		scanner := bufio.NewScanner(r)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if scanner.Scan() {
					l := Line{Bytes: scanner.Bytes(), Err: scanner.Err()}
					select {
					case ch <- l:
						continue
					case <-ctx.Done():
						return
					}
				}
				return
			}
		}

	}()
	return ch
}

func ScanFileLines(ctx context.Context, filepath string) chan Line {
	f, err := os.Open(filepath)
	if err != nil {
		ch := make(chan Line, 1)
		ch <- Line{Err: err}
		close(ch)
		return ch
	}
	return ScanLines(ctx, f)
}
