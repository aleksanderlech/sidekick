// Copyright (c) 2020 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/pkg/v3/console"
)

// hangReportInterval is how often the tracker scans for hanging requests.
const hangReportInterval = 30 * time.Second

// hangSeq keys in-flight requests. A request id is only available in
// "requests" trace mode; elsewhere a monotonically increasing counter is
// used, so the tracker works with any --trace value.
var hangSeq atomic.Uint64

// hangTracker records in-flight requests so that requests which do not
// finish within a configured threshold can be reported periodically.
// Only request metadata is stored (no bodies); entries are removed as
// soon as a request finishes, so the map size equals the number of
// in-flight requests.
type hangTracker struct {
	mu        sync.Mutex
	threshold time.Duration
	reqs      map[uint64]*hangInfo
}

// hangInfo is the metadata of an in-flight request.
type hangInfo struct {
	host      string
	method    string
	path      string
	query     string
	client    string
	requestID string
	startTime time.Time
}

// globalHangTracker is active only when request tracing is enabled.
var globalHangTracker *hangTracker

// newHangTracker returns a tracker that reports requests older than
// threshold. A zero threshold disables reporting.
func newHangTracker(threshold time.Duration) *hangTracker {
	t := &hangTracker{
		threshold: threshold,
		reqs:      make(map[uint64]*hangInfo),
	}
	if threshold > 0 {
		go t.run()
	}
	return t
}

// run periodically reports requests that are still in flight past the
// configured threshold.
func (t *hangTracker) run() {
	ticker := time.NewTicker(hangReportInterval)
	defer ticker.Stop()
	for range ticker.C {
		t.report()
	}
}

// track registers a request as in-flight and returns a function that
// must be called (typically via defer) when the request finishes.
func (t *hangTracker) track(host, method, path, query, client, requestID string, startTime time.Time) func() {
	if t == nil || t.threshold <= 0 {
		return func() {}
	}
	if i := strings.IndexByte(client, ':'); i >= 0 {
		client = client[:i]
	}
	key := hangSeq.Add(1)
	t.mu.Lock()
	t.reqs[key] = &hangInfo{
		host:      host,
		method:    method,
		path:      path,
		query:     query,
		client:    client,
		requestID: requestID,
		startTime: startTime,
	}
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		delete(t.reqs, key)
		t.mu.Unlock()
	}
}

// report emits a report entry for every in-flight request that has
// exceeded the threshold.
func (t *hangTracker) report() {
	if t == nil || t.threshold <= 0 {
		return
	}
	now := time.Now().UTC()
	t.mu.Lock()
	hanging := make([]hangInfo, 0, len(t.reqs))
	for _, h := range t.reqs {
		if now.Sub(h.startTime) >= t.threshold {
			hanging = append(hanging, *h)
		}
	}
	t.mu.Unlock()
	if len(hanging) == 0 {
		return
	}
	// Report the oldest first.
	slices.SortFunc(hanging, func(a, b hangInfo) int { return a.startTime.Compare(b.startTime) })
	for _, h := range hanging {
		logHang(hangMsg{
			Type:      console.Colorize("TraceMsgType", HangingMsgType),
			Host:      h.host,
			Time:      now,
			Client:    h.client,
			RequestID: h.requestID,
			Age:       now.Sub(h.startTime),
			Method:    h.method,
			Path:      h.path,
			Query:     h.query,
		})
	}
}

// hangMsg is a report record for a request that has not finished
// within the configured threshold.
type hangMsg struct {
	Type      string        `json:"type"`
	Host      string        `json:"host"`
	Time      time.Time     `json:"time"`
	Client    string        `json:"client"`
	RequestID string        `json:"requestId,omitempty"`
	Age       time.Duration `json:"age"`
	Method    string        `json:"method"`
	Path      string        `json:"path"`
	Query     string        `json:"query"`
}

func (m hangMsg) String() string {
	b := &strings.Builder{}
	fmt.Fprintf(b, " %5s: ", LogMsgType)
	fmt.Fprintf(b, "%s ", m.Time.Format(timeFormat))
	fmt.Fprintf(b, "[%s] ", m.Type)
	fmt.Fprintf(b, " %s ", m.Host)
	if m.Query != "" {
		fmt.Fprintf(b, "%s %s?%s", m.Method, m.Path, m.Query)
	} else {
		fmt.Fprintf(b, "%s %s", m.Method, m.Path)
	}
	fmt.Fprintf(b, " %s ", m.Client)
	if m.RequestID != "" {
		fmt.Fprintf(b, "%s ", m.RequestID)
	}
	fmt.Fprint(b, console.Colorize("HeaderValue", m.Age.Round(time.Second).String()))
	return b.String()
}

// debugString returns the verbose text representation of a hang report.
func (m hangMsg) debugString() string {
	var nodeNameStr string
	b := &strings.Builder{}

	if m.Host != "" {
		nodeNameStr = fmt.Sprintf("%s: %s ", console.Colorize("TraceMsgType", HangingMsgType), m.Host)
	}

	fmt.Fprintf(b, "%s%s", nodeNameStr, console.Colorize("Request", "[REQUEST] "))
	fmt.Fprintf(b, "%s", m.Time.Format(timeFormat))
	if m.RequestID != "" {
		fmt.Fprintf(b, " %s", m.RequestID)
	}
	fmt.Fprint(b, "\n")
	if m.Query != "" {
		fmt.Fprintf(b, "%s%s", nodeNameStr, console.Colorize("Method", fmt.Sprintf("%s %s?%s", m.Method, m.Path, m.Query)))
	} else {
		fmt.Fprintf(b, "%s%s", nodeNameStr, console.Colorize("Method", fmt.Sprintf("%s %s", m.Method, m.Path)))
	}
	fmt.Fprint(b, "\n")
	fmt.Fprintf(b, "%s%s", nodeNameStr, console.Colorize("Stat", fmt.Sprintf("Age: %s", m.Age.Round(time.Second).String())))
	fmt.Fprint(b, "\n")
	fmt.Fprint(b, nodeNameStr)
	return b.String()
}

// logHang prints a hang report entry using the same output mode
// (json / debug / console) as the other trace records.
func logHang(m hangMsg) {
	if globalQuietEnabled || globalTrace == "" {
		return
	}

	if globalJSONEnabled {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		// Disable escaping special chars to display XML tags correctly
		enc.SetEscapeHTML(false)
		if err := enc.Encode(m); err != nil {
			console.Fatalln(err)
		}
		console.Println(strings.TrimSuffix(buf.String(), "\n"))
		return
	}
	if globalConsoleDisplay {
		if globalDebugEnabled {
			console.Println(m.debugString())
			return
		}
		console.Println(m)
		return
	}
}
