package pubsub_pb

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	rpcPublishFieldNumber protowire.Number = 2
	rpcPartialFieldNumber protowire.Number = 10
)

// ValidateRawRPCControlMessageSize validates that all non-publish,
// non-PartialMessagesExtension fields in a raw RPC are no larger than
// maxControlMessageSize.
func ValidateRawRPCControlMessageSize(msg []byte, maxControlMessageSize int) error {
	controlSize := 0
	for len(msg) > 0 {
		field, typ, tagLen := protowire.ConsumeTag(msg)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}

		fieldValue := msg[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(field, typ, fieldValue)
		if fieldLen < 0 {
			return protowire.ParseError(fieldLen)
		}

		switch field {
		case rpcPartialFieldNumber:
			// There can only be a single partial message per rpc. So the
			// overhead size is small per RPC.
		case rpcPublishFieldNumber:
			payloadLen, err := dataFieldLength(fieldValue)
			if err != nil {
				return err
			}
			// Add the message overhead as part of the control size
			controlSize += tagLen + fieldLen - payloadLen

		default:
			controlSize += tagLen + fieldLen
		}
		msg = fieldValue[fieldLen:]

		if controlSize > maxControlMessageSize {
			return fmt.Errorf("rpc control size exceeds max control message size: %d > %d", controlSize, maxControlMessageSize)
		}
	}
	return nil
}

func dataFieldLength(msg []byte) (int, error) {
	const dataFieldNumber protowire.Number = 2
	msg, n := protowire.ConsumeBytes(msg)
	if n < 0 {
		return 0, protowire.ParseError(n)
	}

	for len(msg) > 0 {
		field, typ, tagLen := protowire.ConsumeTag(msg)
		if tagLen < 0 {
			return 0, protowire.ParseError(tagLen)
		}

		fieldValue := msg[tagLen:]
		fieldLen := protowire.ConsumeFieldValue(field, typ, fieldValue)
		if fieldLen < 0 {
			return 0, protowire.ParseError(fieldLen)
		}
		msg = fieldValue[fieldLen:]

		if field == dataFieldNumber && typ == protowire.BytesType {
			return fieldLen, nil
		}
	}

	return 0, nil
}
