package progressbar

import "testing"

func TestJSONProgressAccounting(t *testing.T) {
	p := NewJSON(0)
	p.AddTotalBytes(1000)
	p.IncrementTotalObjects()
	p.IncrementTotalObjects()

	AddSkipped(p, 100) // already at destination
	AddResumed(p, 300) // from a checkpoint
	p.AddCompletedBytes(200)
	AddInflight(p, 50)
	p.IncrementCompletedObjects()

	s := p.snapshot(false)
	if s.Bytes != 650 {
		t.Errorf("bytes = %d, want 650 (100 skipped + 300 resumed + 200 sent + 50 in flight)", s.Bytes)
	}
	if s.TransferredBytes != 250 {
		t.Errorf("transferred = %d, want 250 (sent + in flight only)", s.TransferredBytes)
	}
	if s.SkippedBytes != 100 || s.ResumedBytes != 300 {
		t.Errorf("skipped/resumed = %d/%d, want 100/300", s.SkippedBytes, s.ResumedBytes)
	}
	if s.Files != 1 || s.TotalFiles != 2 || s.ListingDone {
		t.Errorf("files %d/%d listing_done=%v, want 1/2 false", s.Files, s.TotalFiles, s.ListingDone)
	}

	AddInflight(p, -50) // part settled
	p.AddCompletedBytes(50)
	ListingDone(p)
	s = p.snapshot(true)
	if s.Bytes != 650 || s.TransferredBytes != 250 || !s.ListingDone || !s.Final {
		t.Errorf("after settle: %+v", s)
	}
}

func TestFallbacksForPlainBar(t *testing.T) {
	var n NoOp
	AddSkipped(&n, 10) // must not panic on reporters without accounting
	AddResumed(&n, 10)
	AddInflight(&n, 10)
	ListingDone(&n)
}
