using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Bogus;
using DataPipe.Stress.Cli.Messaging;

namespace DataPipe.Stress.Cli;

public class Program
{
    static Faker _faker = new Faker();

    public static async Task Main(string[] args)
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        using var client = new ClientWebSocket();
        var uri = new Uri("ws://127.0.0.1:7878/?clientId=identifier");

        await client.ConnectAsync(uri, cancellationTokenSource.Token);

        _ = Task.Factory.StartNew(async () =>
        {
            while (client.State == WebSocketState.Open)
            {
                var randomMessage = CreateRandomMessage();
                var serializedMessage = JsonSerializer.Serialize(randomMessage, Options());
                var utf8Buffer = Encoding.UTF8.GetBytes(serializedMessage);

                await client.SendAsync(utf8Buffer, WebSocketMessageType.Text, true, cancellationTokenSource.Token);
                await Task.Delay(100);
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
    }

    private static JsonSerializerOptions Options() => new()
    {
        Converters = {new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)},
    };

    private static DataPipeMessage CreateRandomMessage()
    {
        const string clientId = "identifier";
        const string topic = "test";
        var messageIdentifier = Guid.NewGuid().ToString();
        var amountOfBytes = _faker.Random.Int(0, 500);
        var randomBuffer = _faker.Random.Bytes(amountOfBytes);
        var message = new DataPipeMessage(clientId,
            messageIdentifier,
            [],
            DataPipeMessageType.Default,
            randomBuffer,
            topic);

        return message;
    }
}