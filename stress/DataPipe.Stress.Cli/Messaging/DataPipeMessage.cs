using System.Text.Json.Serialization;

namespace DataPipe.Stress.Cli.Messaging;

public record DataPipeMessage(
    [property: JsonPropertyName("client_identifier")]
    string ClientIdentifier,
    [property: JsonPropertyName("message_identifier")]
    string MessageIdentifier,
    [property: JsonPropertyName("headers")]
    KeyValuePair<string, string>[] Headers,
    [property: JsonPropertyName("message_type")]
    DataPipeMessageType MessageType,
    [property: JsonPropertyName("data")] byte[] Data,
    [property: JsonPropertyName("topic")] string? Topic);