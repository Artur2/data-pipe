using System.Buffers;
using System.CommandLine;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using DataPipe.Stress.Cli.Messaging;

namespace DataPipe.Stress.Cli.Processing;

public class PublishMessageProcessing(RandomMessageFactory randomMessageFactory, JsonSerializerOptions options)
    : IMessageProcessing
{
    public ProcessingType Type => ProcessingType.Publish;

    public async Task Process(ParseResult parsedResult, CancellationToken cancellationToken)
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        using var client = new ClientWebSocket();
        var clientId = parsedResult.GetRequiredValue<string>(Constants.ClientIdOption);
        var uriBuilder = new UriBuilder(parsedResult.GetRequiredValue<string>(Constants.UriArgument))
        {
            Query = "?clientId=" + clientId
        };
        var uri = uriBuilder.Uri;
        var sleep = parsedResult.GetRequiredValue<int>(Constants.SleepOption);
        var topic = parsedResult.GetRequiredValue<string>(Constants.TopicOption);

        await client.ConnectAsync(uri, cancellationTokenSource.Token);

        _ = Task.Factory.StartNew(async () =>
        {
            while (client.State == WebSocketState.Open)
            {
                var dataBuffer = ArrayPool<byte>.Shared.Rent(1024);
                var randomMessage = randomMessageFactory.CreateMessage(clientId, topic, dataBuffer);
                var serializedMessage = JsonSerializer.Serialize(randomMessage, options);
                var utf8BufferLength = Encoding.UTF8.GetByteCount(serializedMessage);
                var bufferForSerializedMessage = ArrayPool<byte>.Shared.Rent(utf8BufferLength);
                try
                {
                    Encoding.UTF8.GetBytes(serializedMessage, bufferForSerializedMessage);
                    var memoryBlock = bufferForSerializedMessage.AsMemory(0, utf8BufferLength);
                    await client.SendAsync(memoryBlock, WebSocketMessageType.Text, true,
                        cancellationTokenSource.Token);

                    await Task.Delay(sleep, cancellationTokenSource.Token);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(dataBuffer);
                    ArrayPool<byte>.Shared.Return(bufferForSerializedMessage, true);
                }
            }
        }, TaskCreationOptions.LongRunning);

        while (true)
        {
            var key = Console.ReadLine();
            if (key == "q")
            {
                await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnected",
                    cancellationTokenSource.Token);
                break;
            }
        }
    }
}