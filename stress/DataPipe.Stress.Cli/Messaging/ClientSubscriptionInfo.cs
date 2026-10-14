using MessagePack;

namespace DataPipe.Stress.Cli.Messaging;

[MessagePackObject]
public record ClientSubscriptionInfo([property: Key(0)] string Group, [property: Key(1)] string Topic);