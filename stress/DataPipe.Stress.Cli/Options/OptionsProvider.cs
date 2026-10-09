using System.CommandLine;
using DataPipe.Stress.Cli.Processing;

namespace DataPipe.Stress.Cli.Options;

public class OptionsProvider
{
    public IEnumerable<Option> GetOptions()
    {
        var urlOption = new Option<string>(Constants.UriArgument)
        {
            Description = "WebSocket server url",
        };

        var sleepOption = new Option<int>(Constants.SleepOption)
        {
            Description = "Sleep time in ms between message sending",
            DefaultValueFactory = (_) => 100
        };

        var topicOption = new Option<string>(Constants.TopicOption)
        {
            Description = "Topic name",
            DefaultValueFactory = (_) => "test"
        };

        var clientIdOption = new Option<string>(Constants.ClientIdOption)
        {
            Description = "Client ID",
            DefaultValueFactory = (_) => "Identity"
        };

        var mode = new Option<ProcessingType>(Constants.Mode)
        {
            Description = "Mode"
        };
        var groupIdOption = new Option<string?>(Constants.GroupIdOption)
        {
            Description = "Group ID",
        };

        yield return urlOption;
        yield return sleepOption;
        yield return topicOption;
        yield return clientIdOption;
        yield return mode;
        yield return groupIdOption;
    }
}