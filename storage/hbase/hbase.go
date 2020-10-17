package hbase

import (
	"context"
	"errors"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/shima-park/tools/storage/hbase/gen-go/hbase"
)

type HBaseClient struct {
	lock   *sync.Mutex
	addr   string
	closer func() error
	*hbase.HbaseClient
}

func NewHBaseClient(addr string) (*HBaseClient, error) {
	client, closer, err := newHBaseClient(addr)
	if err != nil {
		return nil, err
	}

	return &HBaseClient{
		lock:        &sync.Mutex{},
		addr:        addr,
		closer:      closer,
		HbaseClient: client,
	}, nil
}

func newHBaseClient(addr string) (*hbase.HbaseClient, func() error, error) {
	trans, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client := thrift.NewTStandardClient(iprot, oprot)
	if err := trans.Open(); err != nil {
		return nil, nil, err
	}
	return hbase.NewHbaseClient(client), trans.Close, nil
}

type ScanMessage struct {
	Results []*hbase.TRowResult_
	Err     error
}

func (c *HBaseClient) Scan(ctx context.Context, table, start, end string,
	columns []hbase.Text, attributes map[string]hbase.Text) chan *ScanMessage {

	ch := make(chan *ScanMessage, 1)

	errHandle := func(err error) {
		select {
		case <-ctx.Done():
			return
		case ch <- &ScanMessage{
			Err: err,
		}:
		}
	}

	resultHandle := func(results []*hbase.TRowResult_) {
		if len(results) == 0 {
			return
		}

		select {
		case <-ctx.Done():
			return
		case ch <- &ScanMessage{
			Results: results,
		}:
		}
	}

	go func() {
		defer close(ch)

		var caching int32 = 1000
		scan := &hbase.TScan{
			StartRow: []byte(start),
			StopRow:  []byte(end),
			Caching:  &caching,
			Columns:  columns,
		}

		for {
			scannerID, err := c.ScannerOpenWithScan(ctx, []byte(table), scan, attributes)
			if err != nil {
				if err == io.EOF || errors.Is(err, syscall.EPIPE) {
					for retries := 0; retries < 30; retries++ {
						client, closer, err := newHBaseClient(c.addr)
						if err != nil {
							time.Sleep(time.Second)
							continue
						}
						c.lock.Lock()
						c.closer()
						c.HbaseClient = client
						c.closer = closer
						c.lock.Unlock()
						break
					}
				}
				errHandle(err)
				continue
			}

			for {
				results, err := c.ScannerGetList(ctx, scannerID, 1000)
				if err != nil {
					errHandle(err)
					scan.StartRow = createClosestRowAfter(scan.StartRow)
					c.ScannerClose(context.Background(), scannerID) //不传入当前上下文，防止未Close就退出了
					break
				}

				if len(results) > 0 {
					resultHandle(results)
					scan.StartRow = results[len(results)-1].Row
				} else {
					return
				}
			}
		}

	}()
	return ch
}

func createClosestRowAfter(row []byte) []byte {
	var nextRow []byte
	var i int
	for i = 0; i < len(row); i++ {
		nextRow = append(nextRow, row[i])
	}
	nextRow = append(nextRow, 0x00)
	return nextRow
}
