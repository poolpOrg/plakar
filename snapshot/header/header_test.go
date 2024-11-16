package header

import (
	"reflect"
	"testing"
	"time"
)

func TestSortHeaders(t *testing.T) {
	// Define base test data for consistent resetting
	baseHeaders := []Header{
		{CreationTime: time.Now().Add(-1 * time.Hour), Hostname: "server1", FilesCount: 100, SnapshotID: [32]byte{0x1}},
		{CreationTime: time.Now().Add(-2 * time.Hour), Hostname: "server2", FilesCount: 200, SnapshotID: [32]byte{0x3}},
		{CreationTime: time.Now(), Hostname: "server3", FilesCount: 150, SnapshotID: [32]byte{0x2}},
	}

	// Helper function to reset headers before each test
	resetHeaders := func() []Header {
		return append([]Header(nil), baseHeaders...)
	}

	// Test 1: Sort by CreationTime, ascending
	headers := resetHeaders()
	expected1 := []Header{headers[0], headers[1], headers[2]}
	if err := SortHeaders(headers, []string{"CreationTime"}); err != nil {
		t.Fatalf("Test 1 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected1) {
		t.Errorf("Test 1 failed: expected %v, got %v", expected1, headers)
	}

	// Test 2: Sort by CreationTime, descending
	headers = resetHeaders()
	expected2 := []Header{headers[2], headers[1], headers[0]}
	if err := SortHeaders(headers, []string{"-CreationTime"}); err != nil {
		t.Fatalf("Test 2 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected2) {
		t.Errorf("Test 2 failed: expected %v, got %v", expected2, headers)
	}

	// Test 3: Sort by SnapshotID, ascending (lexicographical comparison of [32]byte)
	headers = resetHeaders()
	expected3 := []Header{headers[0], headers[2], headers[1]}
	if err := SortHeaders(headers, []string{"SnapshotID"}); err != nil {
		t.Fatalf("Test 3 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected3) {
		t.Errorf("Test 3 failed: expected %v, got %v", expected3, headers)
	}

	// Test 4: Sort by SnapshotID, descending
	headers = resetHeaders()
	expected4 := []Header{headers[1], headers[2], headers[0]}
	if err := SortHeaders(headers, []string{"-SnapshotID"}); err != nil {
		t.Fatalf("Test 4 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected4) {
		t.Errorf("Test 4 failed: expected %v, got %v", expected4, headers)
	}

	// Test 5: Invalid sort key (should return error)
	headers = resetHeaders()
	err := SortHeaders(headers, []string{"InvalidKey"})
	if err == nil || err.Error() != "invalid sort key: InvalidKey" {
		t.Errorf("Test 5 failed: expected error 'invalid sort key: InvalidKey', got %v", err)
	}

	// Test 6: Sort by FilesCount, ascending
	headers = resetHeaders()
	expected6 := []Header{headers[0], headers[2], headers[1]}
	if err := SortHeaders(headers, []string{"FilesCount"}); err != nil {
		t.Fatalf("Test 6 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected6) {
		t.Errorf("Test 6 failed: expected %v, got %v", expected6, headers)
	}

	// Test 7: Sort by FilesCount, descending
	headers = resetHeaders()
	expected7 := []Header{headers[1], headers[2], headers[0]}
	if err := SortHeaders(headers, []string{"-FilesCount"}); err != nil {
		t.Fatalf("Test 7 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected7) {
		t.Errorf("Test 7 failed: expected %v, got %v", expected7, headers)
	}

	// Test 8: Sort by Hostname, ascending
	headers = resetHeaders()
	expected8 := []Header{headers[0], headers[1], headers[2]}
	if err := SortHeaders(headers, []string{"Hostname"}); err != nil {
		t.Fatalf("Test 8 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected8) {
		t.Errorf("Test 8 failed: expected %v, got %v", expected8, headers)
	}

	// Test 9: Sort by Hostname, descending
	headers = resetHeaders()
	expected9 := []Header{headers[2], headers[1], headers[0]}
	if err := SortHeaders(headers, []string{"-Hostname"}); err != nil {
		t.Fatalf("Test 9 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected9) {
		t.Errorf("Test 9 failed: expected %v, got %v", expected9, headers)
	}

	// Multi-key test: Sort by FilesCount ascending, then CreationTime ascending
	headers = resetHeaders()
	expected10 := []Header{headers[0], headers[2], headers[1]} // FilesCount orders, then CreationTime as tie-breaker
	if err := SortHeaders(headers, []string{"FilesCount", "CreationTime"}); err != nil {
		t.Fatalf("Test 10 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected10) {
		t.Errorf("Test 10 failed: expected %v, got %v", expected10, headers)
	}

	// Multi-key test: Sort by FilesCount, then CreationTime descending
	headers = resetHeaders()
	expected11 := []Header{headers[1], headers[2], headers[0]} // FilesCount orders, then CreationTime as tie-breaker
	if err := SortHeaders(headers, []string{"-FilesCount", "-CreationTime"}); err != nil {
		t.Fatalf("Test 10 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected11) {
		t.Errorf("Test 10 failed: expected %v, got %v", expected10, headers)
	}
}
