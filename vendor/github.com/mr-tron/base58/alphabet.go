package base58

// Alphabet is a a b58 alphabet.
type Alphabet struct {
	decode [128]int8
	encode [58]byte
}

// NewAlphabet creates a new alphabet from the passed string.
//
// It panics if the passed string is not 58 bytes long, isn't valid ASCII,
// or does not contain 58 distinct characters.
func NewAlphabet(s string) *Alphabet {
	if len(s) != 58 {
		panic("base58 alphabets must be 58 bytes long")
	}
	ret := new(Alphabet)
	copy(ret.encode[:], s)
	for i := range ret.decode {
		ret.decode[i] = -1
	}

	distinct := 0
	for i, b := range ret.encode {
		if ret.decode[b] == -1 {
			distinct++
		}
		ret.decode[b] = int8(i)
	}

	if distinct != 58 {
		panic("provided alphabet does not consist of 58 distinct characters")
	}

	return ret
}

// BTCAlphabet is the bitcoin base58 alphabet.
var BTCAlphabet = NewAlphabet("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

// FlickrAlphabet is the flickr base58 alphabet.
var FlickrAlphabet = NewAlphabet("123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ")
