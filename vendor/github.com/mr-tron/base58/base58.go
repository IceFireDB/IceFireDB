package base58

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

const (
	base58EncodeChunkDigits = 10
	base58EncodeChunkBase   = uint64(430804206899405824) // 58^10
)

var base58ChunkPowers = [...]uint64{
	1,
	58,
	3364,
	195112,
	11316496,
	656356768,
	38068692544,
	2207984167552,
	128063081718016,
	7427658739644928,
	base58EncodeChunkBase,
}

// Encode encodes the passed bytes into a base58 encoded string.
func Encode(bin []byte) string {
	return FastBase58EncodingAlphabet(bin, BTCAlphabet)
}

// EncodeAlphabet encodes the passed bytes into a base58 encoded string with the
// passed alphabet.
func EncodeAlphabet(bin []byte, alphabet *Alphabet) string {
	return FastBase58EncodingAlphabet(bin, alphabet)
}

// FastBase58Encoding encodes the passed bytes into a base58 encoded string.
func FastBase58Encoding(bin []byte) string {
	return FastBase58EncodingAlphabet(bin, BTCAlphabet)
}

// FastBase58EncodingAlphabet encodes the passed bytes into a base58 encoded
// string with the passed alphabet.
func FastBase58EncodingAlphabet(bin []byte, alphabet *Alphabet) string {
	if len(bin) == 0 {
		return ""
	}

	zcount := 0
	for zcount < len(bin) && bin[zcount] == 0 {
		zcount++
	}
	if zcount == len(bin) {
		out := make([]byte, zcount)
		for i := range out {
			out[i] = alphabet.encode[0]
		}
		return string(out)
	}

	payload := bin[zcount:]
	wordCount := (len(payload) + 7) / 8

	var smallWords [8]uint64
	var smallScratch [8]uint64
	words := smallWords[:0]
	scratch := smallScratch[:0]
	if wordCount <= len(smallWords) {
		words = smallWords[:wordCount]
		scratch = smallScratch[:wordCount]
	} else {
		words = make([]uint64, wordCount)
		scratch = make([]uint64, wordCount)
	}
	loadBase256Words(words, payload)

	chunkEstimate := (len(payload)*555/406 + base58EncodeChunkDigits - 1) / base58EncodeChunkDigits
	var smallChunks [12]uint64
	chunks := smallChunks[:0]
	if chunkEstimate <= len(smallChunks) {
		chunks = smallChunks[:0]
	} else {
		chunks = make([]uint64, 0, chunkEstimate)
	}

	start := 0
	for {
		remainder := uint64(0)
		nextStart := len(words)
		for i := start; i < len(words); i++ {
			quotient, rem := bits.Div64(remainder, words[i], base58EncodeChunkBase)
			scratch[i] = quotient
			remainder = rem
			if quotient != 0 && nextStart == len(words) {
				nextStart = i
			}
		}
		chunks = append(chunks, remainder)
		if nextStart == len(words) {
			break
		}
		words, scratch = scratch, words
		start = nextStart
	}

	msDigits := countBase58Digits(chunks[len(chunks)-1])
	out := make([]byte, zcount+msDigits+(len(chunks)-1)*base58EncodeChunkDigits)
	for i := 0; i < zcount; i++ {
		out[i] = alphabet.encode[0]
	}

	pos := len(out)
	for i := 0; i < len(chunks)-1; i++ {
		chunk := chunks[i]
		for j := 0; j < base58EncodeChunkDigits; j++ {
			pos--
			out[pos] = alphabet.encode[chunk%58]
			chunk /= 58
		}
	}

	chunk := chunks[len(chunks)-1]
	for chunk > 0 {
		pos--
		out[pos] = alphabet.encode[chunk%58]
		chunk /= 58
	}

	return string(out)
}

// Decode decodes the base58 encoded bytes.
func Decode(str string) ([]byte, error) {
	return FastBase58DecodingAlphabet(str, BTCAlphabet)
}

// DecodeAlphabet decodes the base58 encoded bytes using the given b58 alphabet.
func DecodeAlphabet(str string, alphabet *Alphabet) ([]byte, error) {
	return FastBase58DecodingAlphabet(str, alphabet)
}

// FastBase58Decoding decodes the base58 encoded bytes.
func FastBase58Decoding(str string) ([]byte, error) {
	return FastBase58DecodingAlphabet(str, BTCAlphabet)
}

// FastBase58DecodingAlphabet decodes the base58 encoded bytes using the given
// b58 alphabet.
func FastBase58DecodingAlphabet(str string, alphabet *Alphabet) ([]byte, error) {
	if len(str) == 0 {
		return nil, fmt.Errorf("zero length string")
	}

	zero := alphabet.encode[0]
	b58sz := len(str)

	var zcount int
	for i := 0; i < b58sz && str[i] == zero; i++ {
		zcount++
	}

	if zcount == b58sz {
		return make([]byte, zcount), nil
	}

	payload := str[zcount:]
	byteEstimate := (len(payload)*406/555 + 1)
	wordCap := (byteEstimate + 7) / 8

	var smallWords [8]uint64
	words := smallWords[:0]
	if wordCap > len(smallWords) {
		words = make([]uint64, 0, wordCap)
	}

	firstChunkDigits := len(payload) % base58EncodeChunkDigits
	if firstChunkDigits == 0 {
		firstChunkDigits = base58EncodeChunkDigits
	}

	for offset := 0; offset < len(payload); {
		chunkDigits := base58EncodeChunkDigits
		if offset == 0 {
			chunkDigits = firstChunkDigits
		}

		chunk := uint64(0)
		for i := 0; i < chunkDigits; i++ {
			ch := payload[offset+i]
			if ch > 127 {
				return nil, fmt.Errorf("high-bit set on invalid digit")
			}
			val := alphabet.decode[ch]
			if val == -1 {
				return nil, fmt.Errorf("invalid base58 digit (%q)", ch)
			}
			chunk = chunk*58 + uint64(val)
		}
		mulAddBase58WordsLE(&words, base58ChunkPowers[chunkDigits], chunk)
		offset += chunkDigits
	}

	return unpackBase58WordsLE(words, zcount), nil
}

func loadBase256Words(dst []uint64, src []byte) {
	first := len(src) % 8
	if first == 0 {
		first = 8
	}

	word := uint64(0)
	for _, b := range src[:first] {
		word = (word << 8) | uint64(b)
	}
	dst[0] = word

	offset := first
	for i := 1; offset < len(src); i++ {
		dst[i] = binary.BigEndian.Uint64(src[offset : offset+8])
		offset += 8
	}
}

func countBase58Digits(v uint64) int {
	digits := 0
	for v > 0 {
		digits++
		v /= 58
	}
	if digits == 0 {
		return 1
	}
	return digits
}

func mulAddBase58WordsLE(words *[]uint64, mul, add uint64) {
	if len(*words) == 0 {
		if add != 0 {
			*words = append(*words, add)
		}
		return
	}

	carry := add
	for i := 0; i < len(*words); i++ {
		hi, lo := bits.Mul64((*words)[i], mul)
		lo, c := bits.Add64(lo, carry, 0)
		(*words)[i] = lo
		carry = hi + c
	}
	if carry != 0 {
		*words = append(*words, carry)
	}
}

func unpackBase58WordsLE(words []uint64, zcount int) []byte {
	high := len(words) - 1
	for high >= 0 && words[high] == 0 {
		high--
	}
	if high < 0 {
		return make([]byte, zcount)
	}

	msBytes := (bits.Len64(words[high]) + 7) / 8
	out := make([]byte, zcount+msBytes+8*high)
	pos := zcount

	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], words[high])
	copy(out[pos:], tmp[8-msBytes:])
	pos += msBytes

	for i := high - 1; i >= 0; i-- {
		binary.BigEndian.PutUint64(out[pos:pos+8], words[i])
		pos += 8
	}

	return out
}
