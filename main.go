package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	maxStreamsPerDeal = 16
)

type AtomicWriter struct {
	file *os.File
	mu   sync.Mutex
}

func (aw *AtomicWriter) WriteAt(data []byte, offset int64) (int, error) {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	return aw.file.WriteAt(data, offset)
}

type Downloader struct {
	dealURL       string
	contentLength int64
	numStreams    int
}

func NewDownloader(dealURL string, contentLength, numStreams int64) *Downloader {
	return &Downloader{
		dealURL:       dealURL,
		contentLength: contentLength,
		numStreams:    int(numStreams),
	}
}

type DownloadStatus struct {
	StreamID int
	Success  bool
	Message  string
}

func (d *Downloader) DownloadChunk(start, end int64, streamID int, writer *AtomicWriter, statusChan chan<- DownloadStatus, wg *sync.WaitGroup) {
	defer wg.Done()

	status := DownloadStatus{StreamID: streamID}

	client := &http.Client{}
	req, err := http.NewRequest("GET", d.dealURL, nil)
	if err != nil {
		status.Success = false
		status.Message = fmt.Sprintf("Error creating request: %s", err)
		statusChan <- status
		return
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	for retries := 3; retries > 0; retries-- {
		resp, err := client.Do(req)
		if err != nil {
			status.Success = false
			status.Message = fmt.Sprintf("Error downloading chunk: %s", err)
			time.Sleep(time.Second * 5) // Backoff
			continue
		}
		defer resp.Body.Close()

		data := make([]byte, end-start+1)
		_, err = resp.Body.Read(data)
		if err != nil {
			status.Success = false
			status.Message = fmt.Sprintf("Error reading chunk data: %s", err)
			time.Sleep(time.Second * 5) // Backoff
			continue
		}

		_, err = writer.WriteAt(data, start)
		if err != nil {
			status.Success = false
			status.Message = fmt.Sprintf("Error writing chunk to disk: %s", err)
			time.Sleep(time.Second * 5) // Backoff
			continue
		}
		status.Success = true
		statusChan <- status
		break
	}
}

func getContentLength(url string) (int64, error) {
	resp, err := http.Head(url)
	if err != nil {
		fmt.Println("Error getting content length:", err)
		return 0, err
	}
	defer resp.Body.Close()

	contentLength := resp.ContentLength
	if contentLength == -1 {
		return 0, fmt.Errorf("content length not provided in response")
	}
	return contentLength, nil
}

func main() {
	// Example URL for downloading a car file
	exampleDealURL := "https://storage.web3ph.dev/gw/bafybeif2ttg63z6x2kng26xxvsbic54wkc66e2jrnuva3vuhx4wep5n56i"
	contentLength := 4534421770 // size of the file in bytes
	numStreams := 16

	file, err := os.Create("output.car")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer file.Close()

	atomicWriter := &AtomicWriter{file: file}

	downloader := NewDownloader(exampleDealURL, int64(contentLength), int64(numStreams))

	chunkSize := downloader.contentLength / int64(downloader.numStreams-1)
	remainingSize := downloader.contentLength % int64(downloader.numStreams-1)

	statusChan := make(chan DownloadStatus)

	var wg sync.WaitGroup
	for i := 0; i < downloader.numStreams-1; i++ {
		wg.Add(1)
		go downloader.DownloadChunk(int64(i)*chunkSize, int64(i+1)*chunkSize-1, i, atomicWriter, statusChan, &wg)
	}
	// Download the remaining chunk using the last stream
	wg.Add(1)
	go downloader.DownloadChunk(int64(downloader.numStreams-1)*chunkSize, int64(downloader.numStreams-1)*chunkSize+remainingSize-1, downloader.numStreams-1, atomicWriter, statusChan, &wg)

	// Wait for all downloads to complete
	go func() {
		wg.Wait()
		close(statusChan)
	}()

	// Print download statuses
	for status := range statusChan {
		fmt.Printf("Stream %d download: Success - %t, Message - %s\n", status.StreamID, status.Success, status.Message)
	}
}
