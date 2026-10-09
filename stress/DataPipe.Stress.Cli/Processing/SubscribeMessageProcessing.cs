using System.Buffers;
using System.CommandLine;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using DataPipe.Stress.Cli.Messaging;
using MessagePack;

namespace DataPipe.Stress.Cli.Processing;

public class SubscribeMessageProcessing(JsonSerializerOptions options) : IMessageProcessing
{
    public ProcessingType Type => ProcessingType.Subscribe;

    public async Task Process(ParseResult parsedResult, CancellationToken cancellationToken)
    {
        using var client = new ClientWebSocket();
        var clientId = parsedResult.GetRequiredValue<string>(Constants.ClientIdOption);
        var uriBuilder = new UriBuilder(parsedResult.GetRequiredValue<string>(Constants.UriArgument))
        {
            Query = "?clientId=" + clientId
        };
        var uri = uriBuilder.Uri;
        var topic = parsedResult.GetRequiredValue<string>(Constants.TopicOption);
        var groupId = parsedResult.GetRequiredValue<string>(Constants.GroupIdOption);

        await client.ConnectAsync(uri, cancellationToken);

        _ = Task.Factory.StartNew(async () =>
        {
            var subscriptionMessage = CreateSubscriptionMessage(topic, groupId, clientId);
            var serialize = JsonSerializer.Serialize(subscriptionMessage);
            var utf8Bytes = Encoding.UTF8.GetBytes(serialize);
            await client.SendAsync(utf8Bytes, WebSocketMessageType.Text, true, cancellationToken);

            while (client.State == WebSocketState.Open)
            {
                var bytes = new List<byte>();
                var buffer = ArrayPool<byte>.Shared.Rent(1024 * 10);
                try
                {
                    var isEnd = false;
                    while (!isEnd)
                    {
                        var result = await client.ReceiveAsync(buffer, cancellationToken);
                        bytes.AddRange(buffer.Take(result.Count));
                        if (result.EndOfMessage)
                        {
                            isEnd = true;
                        }
                    }

                    var deserializedMessage = JsonSerializer.Deserialize<DataPipeMessage>(bytes.ToArray(), options);
                    if (deserializedMessage != null)
                    {
                        Console.WriteLine($"Received message: {deserializedMessage.MessageIdentifier}");
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer, true);
                }
            }
        }, TaskCreationOptions.LongRunning);

        while (true)
        {
            var key = Console.ReadLine();
            if (key == "q")
            {
                await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnected",
                    cancellationToken);
                break;
            }

            await Task.Delay(1000, cancellationToken);
        }
    }

    private static DataPipeMessage CreateSubscriptionMessage(string topic, string group, string clientId)
    {
        var messageIdentifier = Guid.NewGuid().ToString();
        var subscriptionInfo = new List<ClientSubscriptionInfo>()
        {
            new(group, topic)
        };

        var serialized = MessagePackSerializer.Serialize(subscriptionInfo);

        var message = new DataPipeMessage(clientId,
            messageIdentifier,
            [],
            DataPipeMessageType.Subscribe,
            serialized,
            topic);

        return message;
    }
}