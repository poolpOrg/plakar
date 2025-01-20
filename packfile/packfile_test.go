package packfile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackFile(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test GetBlob
	retrievedChunk1, exists := p.GetBlob(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p.GetBlob(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}

	// this blob should not exist
	_, exists = p.GetBlob([32]byte{200})
	require.Equal(t, false, exists)

	// Check PackFile Metadata
	if p.Footer.Count != 2 {
		t.Fatalf("Expected Footer.Count to be 2 but got %d", p.Footer.Count)
	}
	if p.Footer.IndexOffset != uint32(len(p.Blobs)) {
		t.Fatalf("Expected Footer.Length to be %d but got %d", len(p.Blobs), p.Footer.IndexOffset)
	}
}

func TestPackFileSerialization(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test Serialize and NewFromBytes
	serialized, err := p.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize PackFile: %v", err)
	}

	p2, err := NewFromBytes(serialized)
	if err != nil {
		t.Fatalf("Failed to create PackFile from bytes: %v", err)
	}

	// Check that metadata is correctly restored after deserialization
	if p2.Footer.Version != p.Footer.Version {
		t.Fatalf("Expected Footer.Version to be %d but got %d", p.Footer.Version, p2.Footer.Version)
	}
	if p2.Footer.Count != p.Footer.Count {
		t.Fatalf("Expected Footer.Count to be %d but got %d", p.Footer.Count, p2.Footer.Count)
	}
	if p2.Footer.IndexOffset != p.Footer.IndexOffset {
		t.Fatalf("Expected Footer.Length to be %d but got %d", p.Footer.IndexOffset, p2.Footer.IndexOffset)
	}
	if p2.Footer.Timestamp != p.Footer.Timestamp {
		t.Fatalf("Expected Footer.Timestamp to be %d but got %d", p.Footer.Timestamp, p2.Footer.Timestamp)
	}

	// Test that chunks are still retrievable after serialization and deserialization
	retrievedChunk1, exists := p2.GetBlob(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p2.GetBlob(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}
}

func TestPackFileSerializeIndex(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test packfile Size
	require.Equal(t, p.Size(), uint32(44), "Expected 2 blobs but got %q", p.Size())

	// Test Serialize and NewFromBytes
	serialized, err := p.SerializeIndex()
	require.NoError(t, err, "Failed to serialize PackFile index")

	p2, err := NewIndexFromBytes(serialized)
	require.NoError(t, err, "Failed to create PackFile index from bytes")

	require.Equal(t, len(p2), 2, "Expected 2 blobs but got %d", len(p2))

	// Test that both chunks are equal after serialization and deserialization
	blob1, blob2 := p2[0], p2[1]

	require.Equal(t, blob1.Type, TYPE_CHUNK)
	require.Equal(t, blob2.Type, TYPE_CHUNK)

	require.Equal(t, blob1.Length, uint32(len(chunk1)))
	require.Equal(t, blob2.Length, uint32(len(chunk2)))

	require.Equal(t, blob1.Checksum, checksum1, "Expected %q but got %q", checksum1, blob1.Checksum)
	require.Equal(t, blob2.Checksum, checksum2, "Expected %q but got %q", checksum1, blob2.Checksum)
}

func TestPackFileSerializeFooter(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test Serialize and NewFromBytes
	serialized, err := p.SerializeFooter()
	require.NoError(t, err, "Failed to serialize PackFile footer")

	p2, err := NewFooterFromBytes(serialized)
	require.NoError(t, err, "Failed to create PackFile footer from bytes")

	require.Equal(t, p2.Count, uint32(2), "Expected 2 blobs but got %d", uint32(p2.Count))

	require.Equal(t, p2.IndexOffset, uint32(len(chunk1)+len(chunk2)), "Expected IndexOffset to be %d but got %d", len(chunk1)+len(chunk2), p2.IndexOffset)
}

func TestPackFileSerializeData(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test SerializeData
	serialized, err := p.SerializeData()
	require.NoError(t, err, "Failed to serialize PackFile data")

	// Check that the serialized data is correct
	expected := append(chunk1, chunk2...)
	require.Equal(t, expected, serialized, "Serialized data does not match expected data")
}

func TestDefaultConfiguration(t *testing.T) {
	c := DefaultConfiguration()

	require.Equal(t, c.MaxSize, uint32(20971520))
}

func TestTypes(t *testing.T) {
	list := Types()

	require.Equal(t, 8, len(list))
	require.Equal(t, []Type{TYPE_SNAPSHOT, TYPE_CHUNK, TYPE_OBJECT, TYPE_VFS, TYPE_CHILD, TYPE_DATA, TYPE_SIGNATURE, TYPE_ERROR}, list)
}

func TestBlobTypeName(t *testing.T) {
	tests := []struct {
		blobType Type
		expected string
	}{
		{TYPE_SNAPSHOT, "snapshot"},
		{TYPE_CHUNK, "chunk"},
		{TYPE_OBJECT, "object"},
		{TYPE_VFS, "vfs"},
		{TYPE_CHILD, "directory"},
		{TYPE_DATA, "data"},
		{TYPE_SIGNATURE, "signature"},
		{TYPE_ERROR, "error"},
		{255, "unknown"}, // test default case
	}

	for _, test := range tests {
		blob := Blob{Type: test.blobType}
		actual := blob.TypeName()
		require.Equal(t, actual, test.expected)
	}
}
