using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Text.Json.Serialization;
using Amazon.S3;
using System.Text.Json;
using System.Diagnostics.CodeAnalysis;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace telemetry_sqs_to_s3_dotnet;

public class Function
{
    private static JsonSerializerOptions jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {

    }

    private static async Task Main()
    {
        Func<SQSEvent, ILambdaContext, Task> handler = FunctionHandler;
        await LambdaBootstrapBuilder.Create(handler, new SourceGeneratorLambdaJsonSerializer<LambdaFunctionJsonSerializerContext>())
            .Build()
            .RunAsync();
    }


    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
    /// to respond to SQS messages.
    /// </summary>
    /// <param name="evnt">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public static async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        Console.WriteLine($"function handler invoked");
        await ProcessMessageAsync(evnt.Records, context);
    }

    // public static string FunctionHandler(string input, ILambdaContext context)
    // {
    //     return input.ToUpper();
    // }

    // Declare enum with values 'Error', 'Trace', 'Performance'
    public enum TelemetryType
    {
        Error,
        Trace,
        Performance
    }

    struct TelemetryEvent
    {
        public string Application { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public TelemetryType Type { get; set; }
        // public Dictionary<string, string> Data { get; set; }
    }

    [RequiresUnreferencedCode("Calls System.Text.Json.JsonSerializer.Deserialize<TValue>(String, JsonSerializerOptions)")]
    private static async Task ProcessMessageAsync(List<SQSEvent.SQSMessage> records, ILambdaContext context)
    {
        Console.WriteLine($"Processing {records.Count} records");
        // context.Logger.LogInformation($"Processed message {message.Body}");

        var compiledRecords = "[";
        for (int i = 0; i < records.Count; i++)
        {
            compiledRecords += records[i].Body;
            if (i < records.Count - 1)
            {
                compiledRecords += ",";
            }
        }
        compiledRecords += "]";
        Console.WriteLine($"Compiled Records: {compiledRecords}");
        //Parse body as JSON
        // var telemetryEvents = JsonSerializer.Deserialize<List<TelemetryEvent>>(compiledRecords, jsonSerializerOptions);

        // Console.WriteLine($"Parsed JSON: {telemetryEvents}");

        // generate random alphanumeric of length 5
        var random = new Random();
        var randomString = "";
        for (int i = 0; i < 5; i++)
        {
            randomString += random.Next(1, 10);
        }

        var s3Client = new AmazonS3Client();
        var bucketName = Environment.GetEnvironmentVariable("TELEMETRY_TEMPORARY_BUCKET_NAME");
        var key = $"telemetry-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}-{randomString}.json";
        var putRequest = new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentBody = compiledRecords
        };
        await s3Client.PutObjectAsync(putRequest);
        Console.WriteLine($"Successfully put object in S3 with key: {key} in bucket: {bucketName}");
    }
}
/// <summary>
/// This class is used to register the input event and return type for the FunctionHandler method with the System.Text.Json source generator.
/// There must be a JsonSerializable attribute for each type used as the input and return type or a runtime error will occur 
/// from the JSON serializer unable to find the serialization information for unknown types.
/// </summary>
[JsonSerializable(typeof(string))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
    // By using this partial class derived from JsonSerializerContext, we can generate reflection free JSON Serializer code at compile time
    // which can deserialize our class and properties. However, we must attribute this class to tell it what types to generate serialization code for.
    // See https://docs.microsoft.com/en-us/dotnet/standard/serialization/system-text-json-source-generation
}