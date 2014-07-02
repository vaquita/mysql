package mysql

import (
	"crypto/sha1"
)

// formula :
// SHA1(password) XOR SHA1("20-byte public seed from server" <concat> SHA1( SHA1( password)))

func scramble41(password string, seed []byte) [20]byte {
	var final [sha1.Size]byte

	h1 := sha1.New()
	h1.Write([]byte(password))
	hashStage1 := h1.Sum(nil)

	h2 := sha1.New()
	h2.Write(hashStage1)
	hashStage2 := h2.Sum(nil)

	h := sha1.New()
	h.Write(seed)
	tmp := h.Sum(hashStage2)

	for i := 0; i < sha1.Size; i++ {
		final[i] = hashStage1[i] ^ tmp[i]
	}

	return final
}
