package objects

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestChecksumMarshalJSON(t *testing.T) {
	checksum := Checksum{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	expected := `"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"`

	jsonBytes, err := checksum.MarshalJSON()
	require.NoError(t, err)

	require.Equal(t, expected, string(jsonBytes))
}

func TestChecksumUnMarshalJSON(t *testing.T) {
	brokenValue := `"010203"`

	var c Checksum
	err := json.Unmarshal([]byte(brokenValue), &c)
	require.Error(t, err)

	// working
	expected := Checksum{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	marshalled := `"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"`

	var checksum Checksum
	err = json.Unmarshal([]byte(marshalled), &checksum)
	require.NoError(t, err)

	require.Equal(t, expected, checksum)
}

func TestClassificationMarshalJSON(t *testing.T) {
	classification := Classification{
		Analyzer: "test-analyzer",
		Classes:  []string{"class1", "class2"},
	}

	expected := `{"analyzer":"test-analyzer","classes":["class1","class2"]}`

	jsonBytes, err := json.Marshal(classification)
	require.NoError(t, err)

	require.Equal(t, expected, string(jsonBytes))
}

func TestCustomMetadataMarshalJSON(t *testing.T) {
	customMetadata := CustomMetadata{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	expected := `{"key":"test-key","value":"dGVzdC12YWx1ZQ=="}`

	jsonBytes, err := json.Marshal(customMetadata)
	require.NoError(t, err)

	require.Equal(t, expected, string(jsonBytes))
}

func TestObjectNew(t *testing.T) {
	object := NewObject()

	require.NotNil(t, object)
	require.NotNil(t, object.CustomMetadata)
	require.NotNil(t, object.Checksum)
	require.Nil(t, object.Chunks)
	require.Equal(t, "", object.ContentType)
	require.Nil(t, object.Classifications)
	require.Nil(t, object.Tags)
	require.Equal(t, float64(0), object.Entropy)
	require.NotNil(t, object.Distribution)
	require.Equal(t, uint32(0), object.Flags)
}

func TestObjectNewFromBytes(t *testing.T) {
	serialized := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// this must fail
	_, err := NewObjectFromBytes(serialized)
	require.Error(t, err)

	// this one will work
	serialized = []byte("\x85\xa8checksum\xc4 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa6chunks\xc0\xafcustom_metadata\x91\x82\xa3key\xa4test\xa5value\xc4\x05value\xacdistribution\xc5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa5flags\xce\x00\x00\x00\x00")

	object, err := NewObjectFromBytes(serialized)
	require.NoError(t, err)

	require.NotNil(t, object)
	require.Equal(t, []CustomMetadata{{Key: "test", Value: []byte("value")}}, object.CustomMetadata)

	serialized = []byte("\x84\xa8checksum\xc4 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa6chunks\xc0\xacdistribution\xc5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa5flags\xce\x00\x00\x00\x00")
	object, err = NewObjectFromBytes(serialized)
	require.NoError(t, err)

	require.NotNil(t, object)
	require.Equal(t, []CustomMetadata{}, object.CustomMetadata)
}

func TestObjectSerialize(t *testing.T) {
	object := NewObject()
	require.NotNil(t, object)

	// we set a value for CustomMetadata to avoid having msgpack.Unmarshal reset empty slices to nil and make the test fail
	object.CustomMetadata = append(object.CustomMetadata, CustomMetadata{Key: "test", Value: []byte("value")})

	serialized, err := object.Serialize()
	require.NoError(t, err)
	require.NotNil(t, serialized)

	var deserialized Object
	err = msgpack.Unmarshal(serialized, &deserialized)
	require.NoError(t, err)

	require.Equal(t, *object, deserialized)
}

func TestObjectAddClassification(t *testing.T) {
	object := NewObject()

	analyzer := "test-analyzer"
	classes := []string{"class1", "class2"}

	object.AddClassification(analyzer, classes)

	require.Equal(t, 1, len(object.Classifications))

	classification := object.Classifications[0]
	require.Equal(t, analyzer, classification.Analyzer)
	require.Equal(t, classes, classification.Classes)
}
