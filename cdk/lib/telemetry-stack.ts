import * as cdk from "aws-cdk-lib";
import * as S3 from "aws-cdk-lib/aws-s3";
import * as Lambda from "aws-cdk-lib/aws-lambda";
import * as ApiGateway from "aws-cdk-lib/aws-apigateway";
import * as Logs from "aws-cdk-lib/aws-logs";
import * as SQS from "aws-cdk-lib/aws-sqs";
import * as IAM from "aws-cdk-lib/aws-iam";
import { RustFunction } from "cargo-lambda-cdk";
import { Construct } from "constructs";
import * as LambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";

export class TelemetryStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create a new S3 bucket for telemetry data
    const telemetryTemporaryBucket = new S3.Bucket(
      this,
      "TelemetryTemporaryBucket",
      {
        bucketName: "telemetry-temporary-data",
      },
    );

    const telemetryQueue = new SQS.Queue(this, "TelemetryQueue", {
      queueName: "telemetry-data-queue",
    });

    const telemetryApiLogGroup = new Logs.LogGroup(
      this,
      "TelemetryApiLogGroup",
      {
        logGroupName: "/aws/apigateway/telemetry-api",
        retention: Logs.RetentionDays.ONE_MONTH,
      },
    );

    const telemetryApiGateway = new ApiGateway.RestApi(
      this,
      "TelemetryApiGateway",
      {
        restApiName: "telemetry-api",
        deployOptions: {
          accessLogDestination: new ApiGateway.LogGroupLogDestination(
            telemetryApiLogGroup,
          ),
          accessLogFormat: ApiGateway.AccessLogFormat.custom(
            JSON.stringify({
              requestId: "$context.requestId",
              ip: "$context.identity.sourceIp",
              user: "$context.identity.user",
              caller: "$context.identity.caller",
              requestTime: "$context.requestTime",
              httpMethod: "$context.httpMethod",
              resourcePath: "$context.resourcePath",
              status: "$context.status",
              protocol: "$context.protocol",
              responseLength: "$context.responseLength",
              errorMessage: "$context.error.message",
              errorMessageString: "$context.error.messageString",
              errorType: "$context.error.type",
              errorCause: "$context.error.cause",
              integrationError: "$context.integration.error",
              integrationStatus: "$context.integration.status",
            }),
          ),
          loggingLevel: ApiGateway.MethodLoggingLevel.INFO,
          metricsEnabled: true,
        },
      },
    );

    const telemetryApiGatewayRole = new IAM.Role(
      this,
      "TelemetryApiGatewayRole",
      {
        assumedBy: new IAM.ServicePrincipal("apigateway.amazonaws.com"),
      },
    );
    telemetryQueue.grantSendMessages(telemetryApiGatewayRole);

    const telemetryAPIGatewaySQSIntegration = new ApiGateway.AwsIntegration({
      service: "sqs",
      path: `${this.account}/${telemetryQueue.queueName}`,
      options: {
        credentialsRole: telemetryApiGatewayRole,
        requestParameters: {
          "integration.request.header.Content-Type": `'application/x-www-form-urlencoded'`, // Required for the VTL template to work
        },
        requestTemplates: {
          // VTL template to transform the JSON body into a URL-encoded format
          "application/json": `Action=SendMessage&MessageBody=$util.urlEncode($input.body)`,
        },
        integrationResponses: [
          {
            statusCode: "200",
          },
        ],
      },
    });

    telemetryApiGateway.root
      .addResource("telemetry")
      .addMethod("POST", telemetryAPIGatewaySQSIntegration);

    new cdk.CfnOutput(this, "TelemetryApiGatewayUrl", {
      value: telemetryApiGateway.url,
    });

    const telemetrySQSToS3RustFunction = new RustFunction(
      this,
      "TelemetrySQSToS3RustFunction",
      {
        manifestPath: "../telemetry-sqs-to-s3/Cargo.toml",
      },
    );

    telemetrySQSToS3RustFunction.addEnvironment(
      "TELEMETRY_TEMPORARY_BUCKET_NAME",
      telemetryTemporaryBucket.bucketName,
    );

    telemetryTemporaryBucket.grantReadWrite(telemetrySQSToS3RustFunction);

    // Add sqs trigger to the function
    telemetrySQSToS3RustFunction.addEventSource(
      new LambdaEventSources.SqsEventSource(telemetryQueue, {
        batchSize: 1000,
        maxBatchingWindow: cdk.Duration.seconds(1), // Change it to 1hr later
      }),
    );
  }
}
