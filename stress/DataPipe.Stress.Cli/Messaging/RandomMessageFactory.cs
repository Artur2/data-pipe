namespace DataPipe.Stress.Cli.Messaging;

public class RandomMessageFactory
{
    public DataPipeMessage CreateMessage(string clientId, string topic, byte[] buffer)
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
}