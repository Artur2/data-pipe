using System.CommandLine;

namespace DataPipe.Stress.Cli.Processing;

public interface IMessageProcessing
{
    ProcessingType Type { get; }
    
    Task Process(ParseResult parsedResult,CancellationToken cancellationToken);
}