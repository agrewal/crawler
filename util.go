package fetcher

import (
	"bufio"
	"context"
	"io"
)

func ReaderToStringFetchable(ctx context.Context, reader io.Reader, channelBufferSize int) <-chan Fetchable {
	strChannel := make(chan Fetchable, channelBufferSize)
	go func() {
		scanner := bufio.NewScanner(reader)
		defer close(strChannel)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case strChannel <- StringFetchable(scanner.Text()):
				continue
			}
		}
	}()
	return strChannel
}
