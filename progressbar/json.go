package progressbar

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Accounting is implemented by progress reporters that distinguish how bytes were completed, so
// orchestrators can compute real throughput (rclone/AzCopy exclude skipped and resumed data from it).
type Accounting interface {
	AddSkippedBytes(n int64)  // already present at the destination (e.g. -n -s), not transferred
	AddResumedBytes(n int64)  // restored from a checkpoint, not transferred in this run
	AddInflightBytes(n int64) // bytes of parts still in flight; negative to roll back
	SetListingDone()          // source enumeration finished: totals are final
}

// AddSkipped records bytes completed without transfer; falls back to plain completion.
func AddSkipped(pb ProgressBar, n int64) {
	if a, ok := pb.(Accounting); ok {
		a.AddSkippedBytes(n)
		return
	}
	pb.AddCompletedBytes(n)
}

// AddResumed records bytes restored from a checkpoint; falls back to plain completion.
func AddResumed(pb ProgressBar, n int64) {
	if a, ok := pb.(Accounting); ok {
		a.AddResumedBytes(n)
		return
	}
	pb.AddCompletedBytes(n)
}

// AddInflight records bytes of an unfinished part (no-op for reporters without accounting).
func AddInflight(pb ProgressBar, n int64) {
	if a, ok := pb.(Accounting); ok {
		a.AddInflightBytes(n)
	}
}

// Planner is implemented by reporters that show a check-first plan (cp --check-first).
type Planner interface {
	SetPhase(phase string)       // "listing", "checking", "transferring"
	AddChecked(n int64)          // source objects compared with the destination
	AddDestListed(n int64)       // destination objects listed for the comparison
	AddSkippedObject(size int64) // one object already at the destination: completed without transfer
	SetPlan()                    // the comparison is complete: the plan below is final
}

// SetPhase reports the current phase of a check-first copy.
func SetPhase(pb ProgressBar, phase string) {
	if p, ok := pb.(Planner); ok {
		p.SetPhase(phase)
	}
}

// AddChecked counts source objects compared with the destination.
func AddChecked(pb ProgressBar, n int64) {
	if p, ok := pb.(Planner); ok {
		p.AddChecked(n)
	}
}

// AddDestListed counts destination objects listed for the comparison.
func AddDestListed(pb ProgressBar, n int64) {
	if p, ok := pb.(Planner); ok {
		p.AddDestListed(n)
	}
}

// AddSkippedObject records one object found already at the destination by the plan.
func AddSkippedObject(pb ProgressBar, size int64) {
	if p, ok := pb.(Planner); ok {
		p.AddSkippedObject(size)
		return
	}
	AddSkipped(pb, size)
	pb.IncrementCompletedObjects()
}

// PlanDone marks the plan of a check-first copy as final.
func PlanDone(pb ProgressBar) {
	if p, ok := pb.(Planner); ok {
		p.SetPlan()
	}
}

// ListingDone marks source enumeration as complete.
func ListingDone(pb ProgressBar) {
	if a, ok := pb.(Accounting); ok {
		a.SetListingDone()
	}
}

// JSONProgress emits exact, machine-readable progress as one JSON line on stdout every interval
// (the model of rclone's --use-json-log --stats and AzCopy's --output-type json), instead of a
// rounded text bar on stderr.
type JSONProgress struct {
	interval time.Duration

	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64 // transferred + skipped + resumed (finished parts/files only)
	skippedBytes     int64
	resumedBytes     int64
	inflightBytes    int64
	listingDone      int32

	// check-first plan
	phase         atomic.Value // string
	checked       int64
	destListed    int64
	skippedFiles  int64
	planDone      int32
	planSendFiles int64
	planSendBytes int64
	planSkipFiles int64
	planSkipBytes int64

	start    time.Time
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

var (
	_ ProgressBar = (*JSONProgress)(nil)
	_ Accounting  = (*JSONProgress)(nil)
	_ Planner     = (*JSONProgress)(nil)
)

func NewJSON(interval time.Duration) *JSONProgress {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &JSONProgress{interval: interval, stop: make(chan struct{}), done: make(chan struct{})}
}

func (p *JSONProgress) Start() {
	p.start = time.Now()
	go func() {
		defer close(p.done)
		t := time.NewTicker(p.interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				p.emit(false)
			case <-p.stop:
				return
			}
		}
	}()
}

func (p *JSONProgress) Finish() {
	p.stopOnce.Do(func() {
		close(p.stop)
		<-p.done
		p.emit(true)
	})
}

func (p *JSONProgress) IncrementCompletedObjects() { atomic.AddInt64(&p.completedObjects, 1) }
func (p *JSONProgress) IncrementTotalObjects()     { atomic.AddInt64(&p.totalObjects, 1) }
func (p *JSONProgress) AddCompletedBytes(n int64)  { atomic.AddInt64(&p.completedBytes, n) }
func (p *JSONProgress) AddTotalBytes(n int64)      { atomic.AddInt64(&p.totalBytes, n) }

func (p *JSONProgress) AddSkippedBytes(n int64) {
	atomic.AddInt64(&p.skippedBytes, n)
	atomic.AddInt64(&p.completedBytes, n)
}

func (p *JSONProgress) AddResumedBytes(n int64) {
	atomic.AddInt64(&p.resumedBytes, n)
	atomic.AddInt64(&p.completedBytes, n)
}

func (p *JSONProgress) AddInflightBytes(n int64) { atomic.AddInt64(&p.inflightBytes, n) }

func (p *JSONProgress) SetPhase(phase string) { p.phase.Store(phase) }
func (p *JSONProgress) AddChecked(n int64)    { atomic.AddInt64(&p.checked, n) }
func (p *JSONProgress) AddDestListed(n int64) { atomic.AddInt64(&p.destListed, n) }

func (p *JSONProgress) AddSkippedObject(size int64) {
	atomic.AddInt64(&p.skippedFiles, 1)
	p.AddSkippedBytes(size)
	p.IncrementCompletedObjects()
}

// SetPlan freezes the plan: everything listed minus what the comparison found already at the destination.
func (p *JSONProgress) SetPlan() {
	skipFiles := atomic.LoadInt64(&p.skippedFiles)
	skipBytes := atomic.LoadInt64(&p.skippedBytes)
	atomic.StoreInt64(&p.planSkipFiles, skipFiles)
	atomic.StoreInt64(&p.planSkipBytes, skipBytes)
	atomic.StoreInt64(&p.planSendFiles, atomic.LoadInt64(&p.totalObjects)-skipFiles)
	atomic.StoreInt64(&p.planSendBytes, atomic.LoadInt64(&p.totalBytes)-skipBytes)
	atomic.StoreInt32(&p.planDone, 1)
}
func (p *JSONProgress) SetListingDone() { atomic.StoreInt32(&p.listingDone, 1) }

type progressLine struct {
	Operation        string `json:"operation"`
	Bytes            int64  `json:"bytes"`
	TotalBytes       int64  `json:"total_bytes"`
	Files            int64  `json:"files"`
	TotalFiles       int64  `json:"total_files"`
	TransferredBytes int64  `json:"transferred_bytes"`
	SkippedBytes     int64  `json:"skipped_bytes"`
	ResumedBytes     int64  `json:"resumed_bytes"`
	InflightBytes    int64  `json:"inflight_bytes"` // part of bytes/transferred_bytes not yet acknowledged
	ListingDone      bool   `json:"listing_done"`
	ElapsedMs        int64  `json:"elapsed_ms"`
	Final            bool   `json:"final,omitempty"`

	// check-first copies only
	Phase        string    `json:"phase,omitempty"`
	CheckedFiles int64     `json:"checked_files,omitempty"`
	DestListed   int64     `json:"dest_listed,omitempty"`
	SkippedFiles int64     `json:"skipped_files,omitempty"`
	Plan         *planLine `json:"plan,omitempty"`
}

// planLine is the final plan of a check-first copy: what is sent, and what was already at the destination.
type planLine struct {
	SendFiles int64 `json:"send_files"`
	SendBytes int64 `json:"send_bytes"`
	SkipFiles int64 `json:"skip_files"`
	SkipBytes int64 `json:"skip_bytes"`
}

// Snapshot returns the current counters (exported for tests).
func (p *JSONProgress) snapshot(final bool) progressLine {
	completed := atomic.LoadInt64(&p.completedBytes)
	skipped := atomic.LoadInt64(&p.skippedBytes)
	resumed := atomic.LoadInt64(&p.resumedBytes)
	inflight := atomic.LoadInt64(&p.inflightBytes)
	if inflight < 0 {
		inflight = 0
	}
	total := atomic.LoadInt64(&p.totalBytes)
	done := completed + inflight
	if total > 0 && done > total {
		done = total
	}
	transferred := completed - skipped - resumed + inflight
	if transferred < 0 {
		transferred = 0
	}
	var plan *planLine
	if atomic.LoadInt32(&p.planDone) == 1 {
		plan = &planLine{
			SendFiles: atomic.LoadInt64(&p.planSendFiles),
			SendBytes: atomic.LoadInt64(&p.planSendBytes),
			SkipFiles: atomic.LoadInt64(&p.planSkipFiles),
			SkipBytes: atomic.LoadInt64(&p.planSkipBytes),
		}
	}
	phase, _ := p.phase.Load().(string)
	return progressLine{
		Phase:            phase,
		CheckedFiles:     atomic.LoadInt64(&p.checked),
		DestListed:       atomic.LoadInt64(&p.destListed),
		SkippedFiles:     atomic.LoadInt64(&p.skippedFiles),
		Plan:             plan,
		Operation:        "progress",
		Bytes:            done,
		TotalBytes:       total,
		Files:            atomic.LoadInt64(&p.completedObjects),
		TotalFiles:       atomic.LoadInt64(&p.totalObjects),
		TransferredBytes: transferred,
		SkippedBytes:     skipped,
		ResumedBytes:     resumed,
		InflightBytes:    inflight,
		ListingDone:      atomic.LoadInt32(&p.listingDone) == 1,
		ElapsedMs:        time.Since(p.start).Milliseconds(),
		Final:            final,
	}
}

func (p *JSONProgress) emit(final bool) {
	if b, err := json.Marshal(p.snapshot(final)); err == nil {
		fmt.Println(string(b))
	}
}
