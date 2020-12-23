package crawler

import (
	"bufio"
	"context"
)

func ScannerToChannel(ctx context.Context, scanner *bufio.Scanner, channelBufferSize int) <-chan string {
	strChannel := make(chan string, channelBufferSize)
	go func() {
		defer close(strChannel)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case strChannel <- scanner.Text():
				continue
			}
		}
	}()
	return strChannel
}
