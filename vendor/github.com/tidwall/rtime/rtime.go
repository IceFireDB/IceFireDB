package rtime

import (
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"
)

var sites = []string{
	"facebook.com", "microsoft.com", "amazon.com", "google.com",
	"youtube.com", "twitter.com", "reddit.com", "netflix.com",
	"bing.com", "twitch.tv", "myshopify.com", "wikipedia.org",
}

// Now returns the current remote time. If the remote time cannot be
// retrieved then the zero value for Time is returned. It's a good idea to
// test for zero after every call, such as:
//
//    now := rtime.Now()
//    if now.IsZero() {
//        ... handle failure ...
//    }
//
func Now() time.Time {
	smu.Lock()
	if synced {
		tm := sremote.Add(time.Since(slocal))
		smu.Unlock()
		return tm
	}
	smu.Unlock()
	return now()
}

var rmu sync.Mutex
var rtime time.Time

func now() time.Time {
	res := make([]time.Time, 0, len(sites))
	results := make(chan time.Time, len(sites))

	// get as many dates as quickly as possible
	client := http.Client{
		Timeout: time.Duration(time.Second * 2),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for _, site := range sites {
		go func(site string) {
			resp, err := client.Head("https://" + site)
			if err == nil {
				tm, err := time.Parse(time.RFC1123, resp.Header.Get("Date"))
				resp.Body.Close()
				if err == nil {
					results <- tm
				}
			}
		}(site)
	}

	for {
		select {
		case <-time.After(2 * time.Second):
			return time.Time{}
		case tm := <-results:
			res = append(res, tm)
			if len(res) < 3 {
				continue
			}
			// We must have a minimum of three results. Find the two of three
			// that have the least difference in time and take the smaller of
			// the two.
			type pair struct {
				tm0  time.Time
				tm1  time.Time
				diff time.Duration
			}
			var list []pair
			for i := 0; i < len(res); i++ {
				for j := i + 1; j < len(res); j++ {
					if i != j {
						tm0, tm1 := res[i], res[j]
						if tm0.After(tm1) {
							tm0, tm1 = tm1, tm0
						}
						list = append(list, pair{tm0, tm1, tm1.Sub(tm0)})
					}
				}
			}
			sort.Slice(list, func(i, j int) bool {
				if list[i].diff < list[j].diff {
					return true
				}
				if list[i].diff > list[j].diff {
					return false
				}
				return list[i].tm0.Before(list[j].tm0)
			})
			res := list[0].tm0.Local()
			// Ensure that the new time is after the previous time.
			rmu.Lock()
			defer rmu.Unlock()
			if res.After(rtime) {
				rtime = res
			}
			return rtime
		}
	}
}

var smu sync.Mutex
var sid int
var synced bool
var sremote time.Time
var slocal time.Time

// Sync tells the application to keep rtime in sync with internet time. This
// ensures that all following rtime.Now() calls are fast, accurate, and without
// the need to check the result.
//
// Ideally you would call this at the top of your main() or init() function.
//
//    if err := rtime.Sync(); err != nil {
//        ... internet offline, handle error or try again ...
//        return
//    }
//    rtime.Now() // guaranteed to be a valid time
//
func Sync() error {
	smu.Lock()
	defer smu.Unlock()
	if synced {
		return nil
	}
	start := time.Now()
	for {
		tm := now()
		if !tm.IsZero() {
			sremote = tm
			break
		}
		if time.Since(start) > time.Second*15 {
			return errors.New("internet offline")
		}
		time.Sleep(time.Second / 15)
	}
	sid++
	gsid := sid
	synced = true
	slocal = time.Now()
	go func() {
		for {
			time.Sleep(time.Second * 15)
			tm := now()
			if !tm.IsZero() {
				smu.Lock()
				if gsid != sid {
					smu.Unlock()
					return
				}
				if tm.After(sremote) {
					sremote = tm
					slocal = time.Now()
				}
				smu.Unlock()
			}
		}
	}()
	return nil
}

// MustSync is like Sync but panics if the internet is offline.
func MustSync() {
	if err := Sync(); err != nil {
		panic(err)
	}
}
