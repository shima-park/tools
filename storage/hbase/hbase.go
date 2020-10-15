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

	ch := make(chan *ScanMessage, 64)

	scannerID, err := c.ScannerOpenWithStop(
		ctx, []byte(table), []byte(start), []byte(end), columns, attributes,
	)
	if err != nil {
		ch <- &ScanMessage{
			Err: err,
		}
		return ch
	}

	go func() {
		defer c.ScannerClose(context.Background(), scannerID)

		var caching int32 = 1000
		for {
			results, err := c.ScannerGetList(ctx, scannerID, caching)
			if err != nil {
				select {
				case <-ctx.Done():
					break
				case ch <- &ScanMessage{
					Err: err,
				}:
				}
				break
			}

			if len(results) == 0 {
				break
			}
			select {
			case <-ctx.Done():
				break
			case ch <- &ScanMessage{
				Results: results,
			}:
			}
		}

		close(ch)
	}()
	return ch
}
