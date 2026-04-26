package pubsub_pb

import "log/slog"

var _ slog.LogValuer = (*RPC)(nil)

func (m *RPC) LogValue() slog.Value {
	// Messages
	var msgs []any
	for _, msg := range m.Publish {
		msgs = append(msgs, slog.Group(
			"message",
			slog.String("topic", msg.GetTopic()),
			slog.Int("dataLen", len(msg.Data)),
			// For debugging
			// slog.Any("dataPrefix", msg.Data[0:min(len(msg.Data), 32)]),
		))
	}

	var fields []slog.Attr
	if len(msgs) > 0 {
		fields = append(fields, slog.Group("publish", msgs...))
	}
	if m.Control != nil {
		fields = append(fields, slog.Any("control", m.Control))
	}
	if m.Subscriptions != nil {
		fields = append(fields, slog.Any("subscriptions", m.Subscriptions))
	}
	return slog.GroupValue(fields...)
}

var _ slog.LogValuer = (*PartialMessagesExtension)(nil)

func (e *PartialMessagesExtension) LogValue() slog.Value {
	fields := make([]slog.Attr, 0, 4)
	fields = append(fields, slog.String("topic", e.GetTopicID()))
	fields = append(fields, slog.Any("groupID", e.GetGroupID()))

	// Message
	if e.PartialMessage != nil {
		fields = append(fields, slog.Group(
			"message",
			slog.Any("dataLen", len(e.PartialMessage)),
		))
	}

	if e.PartsMetadata != nil {
		fields = append(fields, slog.Any("partsMetadata", e.PartsMetadata))
	}
	return slog.GroupValue(fields...)
}
