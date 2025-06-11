package nopfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"time"
)

// HTTPSubscriber represents a type that subscribes to a remote URL and appends data to a local file.
type HTTPSubscriber struct {
	remoteURL   string
	localFile   string
	interval    time.Duration
	stopChannel chan struct{}
}

// NewHTTPSubscriber creates a new Subscriber instance with the given parameters.
func NewHTTPSubscriber(remoteURL, localFile string, interval time.Duration) (*HTTPSubscriber, error) {
	logger.Infof("Subscribing to remote denylist: %s", remoteURL)

	sub := HTTPSubscriber{
		remoteURL:   remoteURL,
		localFile:   localFile,
		interval:    interval,
		stopChannel: make(chan struct{}, 1),
	}

	_, err := os.Stat(localFile)
	// if not found, we perform a first sync before returning.
	// this is necessary as otherwise the Blocker does not find much
	// of the file
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		logger.Infof("Performing first sync on: %s", localFile)
		err := sub.downloadAndAppend()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	go sub.subscribe()

	return &sub, nil
}

// subscribe starts the subscription process.
func (s *HTTPSubscriber) subscribe() {
	timer := time.NewTimer(0)

	for {
		select {
		case <-s.stopChannel:
			logger.Infof("Stopping subscription on: %s", s.localFile)
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			err := s.downloadAndAppend()
			if err != nil {
				logger.Error(err)
			}
			timer.Reset(s.interval)
		}
	}
}

// Stop stops the subscription process.
func (s *HTTPSubscriber) Stop() {
	close(s.stopChannel)
}

func (s *HTTPSubscriber) downloadAndAppend() error {
	localFile, err := os.OpenFile(s.localFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer localFile.Close()

	// Get the file size of the local file
	localFileInfo, err := localFile.Stat()
	if err != nil {
		return err
	}

	localFileSize := localFileInfo.Size()

	// Create a HTTP GET request with the Range header to download only the missing bytes
	req, err := http.NewRequest("GET", s.remoteURL, nil)
	if err != nil {
		return err
	}

	rangeHeader := fmt.Sprintf("bytes=%d-", localFileSize)
	req.Header.Set("Range", rangeHeader)

	logger.Debugf("%s: requesting bytes from %d: %s", s.localFile, localFileSize, req.URL)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusPartialContent:
		_, err = io.Copy(localFile, resp.Body)
		if err != nil {
			return err
		}
		logger.Infof("%s: appended %d bytes", s.localFile, resp.ContentLength)
	case (resp.StatusCode >= http.StatusBadRequest &&
		resp.StatusCode != http.StatusRequestedRangeNotSatisfiable) ||
		resp.StatusCode >= http.StatusInternalServerError:
		return fmt.Errorf("%s: server returned with unexpected code %d", s.localFile, resp.StatusCode)
		// error is ignored, we continued subscribed
	}
	return nil
}
