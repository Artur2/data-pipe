using System.CommandLine;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataPipe.Stress.Cli.Messaging;
using DataPipe.Stress.Cli.Options;
using DataPipe.Stress.Cli.Processing;
using MessagePack;
using MessagePack.Resolvers;
using Microsoft.Extensions.DependencyInjection;

namespace DataPipe.Stress.Cli;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var serviceCollection = new ServiceCollection();
        var cancellationTokenSource = new CancellationTokenSource();
        serviceCollection.AddScoped<OptionsProvider>();
        serviceCollection.AddScoped<RandomMessageFactory>();
        serviceCollection.AddSingleton<JsonSerializerOptions>(_ => new JsonSerializerOptions
        {
            Converters = {new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)},
        });
        serviceCollection.AddSingleton<MessagePackSerializerOptions>(_ =>
            MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));

        serviceCollection.AddScoped<IMessageProcessing, PublishMessageProcessing>();
        serviceCollection.AddScoped<IMessageProcessing, SubscribeMessageProcessing>();

        var serviceProvider = serviceCollection.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<OptionsProvider>().GetOptions();

        var rootCommand = new RootCommand("Tool for stress testing data-pipe server");
        foreach (var option in options)
        {
            rootCommand.Add(option);
        }

        rootCommand.SetAction(async (parsedResult) =>
        {
            var messageProcessing = serviceProvider.GetRequiredService<IEnumerable<IMessageProcessing>>();
            var mode = parsedResult.GetRequiredValue<ProcessingType>(Constants.Mode);
            var processing = messageProcessing.First(x => x.Type == mode);
            await processing.Process(parsedResult, cancellationTokenSource.Token);
        });

        return await rootCommand.Parse(args).InvokeAsync(cancellationToken: cancellationTokenSource.Token);
    }
}