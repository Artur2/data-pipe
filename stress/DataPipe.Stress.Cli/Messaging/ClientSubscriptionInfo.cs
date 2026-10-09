using MessagePack;

namespace DataPipe.Stress.Cli.Messaging;

public record ClientSubscriptionInfo([property: Key("group")] string Group, [property: Key("topic")] string Topic);