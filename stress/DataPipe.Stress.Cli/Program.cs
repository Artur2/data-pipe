using System.Buffers;
using System.CommandLine;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataPipe.Stress.Cli.Messaging;

namespace DataPipe.Stress.Cli;

public class Program
{
    private const string UriArgument = "--uri";
    private const string SleepOption = "--sleep";
    private const string TopicOption = "--topic";
    private const string ClientIdOption = "--client-id";

    public static async Task<int> Main(string[] args)
    {
        var options = CreateOptions();

        var rootCommand = new RootCommand("Tool for stress testing data-pipe server");
        foreach (var option in options)
        {
            rootCommand.Add(option);
        }

        rootCommand.SetAction(async (parsedResult) =>
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            using var client = new ClientWebSocket();
            var clientId = parsedResult.GetRequiredValue<string>(ClientIdOption);
            var uriBuilder = new UriBuilder(parsedResult.GetRequiredValue<string>(UriArgument))
            {
                Query = "?clientId=" + clientId
            };
            var uri = uriBuilder.Uri;
            var sleep = parsedResult.GetRequiredValue<int>(SleepOption);
            var topic = parsedResult.GetRequiredValue<string>(TopicOption);

            await client.ConnectAsync(uri, cancellationTokenSource.Token);

            _ = Task.Factory.StartNew(async () =>
            {
                while (client.State == WebSocketState.Open)
                {
                    var dataBuffer = ArrayPool<byte>.Shared.Rent(1024);
                    var randomMessage = CreateRandomMessage(clientId, topic, dataBuffer);
                    var serializedMessage = JsonSerializer.Serialize(randomMessage, Options());
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

                await Task.Delay(1000, cancellationTokenSource.Token);
            }
        });

        return await rootCommand.Parse(args).InvokeAsync();
    }

    private static JsonSerializerOptions Options() => new()
    {
        Converters = {new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)},
    };

    private static DataPipeMessage CreateRandomMessage(string clientId, string topic, byte[] buffer)
    {
        var messageIdentifier = Guid.NewGuid().ToString();
        var rng = new Random();
        rng.NextBytes(buffer);

        var message = new DataPipeMessage(clientId,
            messageIdentifier,
            [],
            DataPipeMessageType.Default,
            buffer,
            topic);

        return message;
    }

    private static IEnumerable<Option> CreateOptions()
    {
        var urlOption = new Option<string>(UriArgument)
        {
            Description = "WebSocket server url",
        };

        var sleepOption = new Option<int>(SleepOption)
        {
            Description = "Sleep time in ms between message sending",
            DefaultValueFactory = (_) => 100
        };

        var topicOption = new Option<string>(TopicOption)
        {
            Description = "Topic name",
            DefaultValueFactory = (_) => "test"
        };

        var clientIdOption = new Option<string>(ClientIdOption)
        {
            Description = "Client ID",
            DefaultValueFactory = (_) => "Identity"
        };

        yield return urlOption;
        yield return sleepOption;
        yield return topicOption;
        yield return clientIdOption;
    }
}