package gossip

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
)

// hash calculates the hash value as a string of a struct.
// Calculating hashes of a generic struct involves two steps:
//   - serialization of the object to a byte array. Specifically this function uses
//     json serialization for this purpose, though other serialization libraries might
//     be more efficient.
//   - hashing of the serialized data by creating a digest
func hash(v any) (string, error) {

	buf := bytes.NewBuffer(make([]byte, 0))
	err := json.NewEncoder(buf).Encode(v)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(buf.Bytes())
	return string(hash[:]), nil
}
