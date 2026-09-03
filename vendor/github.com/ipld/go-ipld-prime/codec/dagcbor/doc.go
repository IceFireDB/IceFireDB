/*
The dagcbor package provides a DAG-CBOR codec implementation.

The Encode and Decode functions match the codec.Encoder and codec.Decoder function interfaces,
and can be registered with the go-ipld-prime/multicodec package for easy usage with systems such as CIDs.

Importing this package will automatically have the side-effect of registering Encode and Decode
with the go-ipld-prime/multicodec registry, associating them with the standard multicodec indicator numbers for DAG-CBOR.

This implementation follows the rules of DAG-CBOR, namely:

- by and large, it does emit and parse CBOR!

- only explicit-length maps and lists will be emitted by Encode;

- only explicit-length strings, bytes, maps and lists will be accepted by Decode;

- only tag 42 is accepted, and it must parse as a CID;

- only 64 bit floats will be emitted by Encode.

DecodeOptions.RelaxedDecode can be used to accept some legacy non-canonical
CBOR forms and can disable some strictness checks. Indefinite-length CBOR is
rejected even in relaxed mode.

Decode strictness is part of ongoing cross-implementation DAG-CBOR correctness
work: non-minimal integer and other CBOR header encodings are rejected, NaN
and Infinity are rejected, duplicate map keys are rejected, and undefined is
coerced to null.

Some DAG-CBOR strictness rules are not yet enforced on decode: map keys are not
required to be sorted, and non-64-bit floats are accepted. With RelaxedDecode,
duplicate map keys are also accepted by this decoder.

A note for future contributors: some functions in this package expose references to packages from the refmt module, and/or use them internally.
Please avoid adding new code which expands the visibility of these references.
In future work, we'd like to reduce or break this relationship entirely.
*/
package dagcbor
