package hbase

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/shima-park/tools/storage/hbase/gen-go/hbase"
)

type HBaseClient struct {
	*hbase.HbaseClient
}

func NewHBaseClient(addr string) (*HBaseClient, error) {
	trans, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client := thrift.NewTStandardClient(iprot, oprot)
	if err := trans.Open(); err != nil {
		return nil, err
	}

	return &HBaseClient{
		hbase.NewHbaseClient(client),
	}, nil
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
				errHandle(err)
				return
			}
			defer c.ScannerClose(context.Background(), scannerID)

		Loop:
			for {
				results, err := c.ScannerGet(ctx, scannerID)
				if err != nil {
					errHandle(err)
					break Loop
				}

				resultHandle(results)

				if len(results) == 0 {
					return
				} else {
					nextStartRow := createClosestRowAfter(results[len(results)-1].Row)
					scan.StartRow = nextStartRow
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
