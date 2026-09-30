using System.Text.Json.Serialization;

namespace DataPipe.Stress.Cli.Messaging;

public enum DataPipeMessageType
{
    [JsonStringEnumMemberName(nameof(Default))]
    Default,
    [JsonStringEnumMemberName(nameof(Subscribe))]
    Subscribe,
    [JsonStringEnumMemberName(nameof(Unsubscribe))]
    Unsubscribe
}