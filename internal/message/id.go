package message

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"time"
)

const (
	timestampBytes = 6
	randomBytes    = 10
	idBytes        = timestampBytes + randomBytes
)

var defaultEntropy io.Reader = rand.Reader

func NewID() string {
	return newIDAt(time.Now(), defaultEntropy)
}

func newIDAt(t time.Time, entropy io.Reader) string {
	raw := make([]byte, idBytes)

	var stamp [8]byte
	binary.BigEndian.PutUint64(stamp[:], uint64(t.UTC().UnixMilli()))
	copy(raw[:timestampBytes], stamp[8-timestampBytes:])

	if _, err := io.ReadFull(entropy, raw[timestampBytes:]); err != nil {
		binary.BigEndian.PutUint64(raw[timestampBytes:], uint64(t.UnixNano()))
	}

	return hex.EncodeToString(raw)
}

func TimestampOf(id string) (time.Time, bool) {
	raw, err := hex.DecodeString(id)
	if err != nil || len(raw) != idBytes {
		return time.Time{}, false
	}

	var stamp [8]byte
	copy(stamp[8-timestampBytes:], raw[:timestampBytes])

	return time.UnixMilli(int64(binary.BigEndian.Uint64(stamp[:]))).UTC(), true
}
