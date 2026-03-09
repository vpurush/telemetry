import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as S3 from "aws-cdk-lib/aws-s3";
import * as S3Assets from "aws-cdk-lib/aws-s3-assets";
import * as Lambda from "aws-cdk-lib/aws-lambda";
import * as ApiGateway from "aws-cdk-lib/aws-apigateway";
import * as Logs from "aws-cdk-lib/aws-logs";
import * as SQS from "aws-cdk-lib/aws-sqs";
import * as IAM from "aws-cdk-lib/aws-iam";
import * as GlueAlpha from "@aws-cdk/aws-glue-alpha";
import { RustFunction } from "cargo-lambda-cdk";
import { Construct } from "constructs";
import * as LambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as LambdaGoAlpha from "@aws-cdk/aws-lambda-go-alpha";
import * as LambdaDotnet from "@aws-cdk/aws-lambda-dotnet";

export class TelemetryStack extends cdk.Stack {
  telemetryTemporaryBucket: S3.Bucket;
  telemetryPermanentBucket: S3.Bucket;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create a new S3 bucket for telemetry data
    this.telemetryTemporaryBucket = new S3.Bucket(
      this,
      "TelemetryTemporaryBucket",
      {
        bucketName: "telemetry-temporary-data",
      },
    );

    this.telemetryPermanentBucket = new S3.Bucket(
      this,
      "TelemetryPermanentBucket",
      {
        bucketName: "telemetry-permanent-data",
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
      .addMethod("POST", telemetryAPIGatewaySQSIntegration, {
        methodResponses: [
          {
            statusCode: "200",
          },
        ],
      });

    new cdk.CfnOutput(this, "TelemetryApiGatewayUrl", {
      value: telemetryApiGateway.url,
    });

    // const telemetrySQSToS3RustFunction = new RustFunction(
    //   this,
    //   "TelemetrySQSToS3RustFunction",
    //   {
    //     manifestPath: "../telemetry-sqs-to-s3/Cargo.toml",
    //   },
    // );

    // telemetrySQSToS3RustFunction.addEnvironment(
    //   "TELEMETRY_TEMPORARY_BUCKET_NAME",
    //   telemetryTemporaryBucket.bucketName,
    // );

    // telemetryTemporaryBucket.grantReadWrite(telemetrySQSToS3RustFunction);

    // Add sqs trigger to the function
    // telemetrySQSToS3RustFunction.addEventSource(
    //   new LambdaEventSources.SqsEventSource(telemetryQueue, {
    //     batchSize: 1000,
    //     maxBatchingWindow: cdk.Duration.seconds(1), // Change it to 1hr later
    //   }),
    // );

    // const goHandlerPath = path.join(__dirname, "../../telemetry-sqs-to-s3-go/main.go");

    // const telemetrySQSToS3GoFunction = new LambdaGoAlpha.GoFunction(this, "TelemetrySQSToS3GoFunction", {
    //   entry: goHandlerPath
    // })

    // telemetryTemporaryBucket.grantReadWrite(telemetrySQSToS3GoFunction);
    // telemetrySQSToS3GoFunction.addEventSource(
    //   new LambdaEventSources.SqsEventSource(telemetryQueue, {
    //     batchSize: 10,
    //     maxBatchingWindow: cdk.Duration.seconds(1),
    //   }),
    // );

    const telemetrySQSToS3DotnetFunction = new LambdaDotnet.DotNetFunction(
      this,
      "TelemetrySQSToS3DotnetFunction",
      {
        projectDir: "../telemetry-sqs-to-s3-dotnet",
        runtime: Lambda.Runtime.DOTNET_10,
        memorySize: 512,
        environment: {
          TELEMETRY_TEMPORARY_BUCKET_NAME:
            this.telemetryTemporaryBucket.bucketName,
        },
        bundling: {
          // msbuildParameters: ['/p:PublishAot=true'],
          dockerImage: cdk.DockerImage.fromRegistry(
            "public.ecr.aws/sam/build-dotnet10:latest",
          ),
        },
      },
    );
    telemetrySQSToS3DotnetFunction.addEventSource(
      new LambdaEventSources.SqsEventSource(telemetryQueue, {
        batchSize: 1000,
        maxBatchingWindow: cdk.Duration.minutes(5),
      }),
    );
    this.telemetryTemporaryBucket.grantReadWrite(
      telemetrySQSToS3DotnetFunction,
    );

    this.createTempToPermS3TransferGlueJob();
  }

  createTempToPermS3TransferGlueJob() {
    const telemetryTempToPermS3TransferScript = new S3Assets.Asset(
      this,
      "TelemetryTempToPermS3TransferScript",
      {
        path: "../s3-to-s3-for-analytics/src/main.py",
      },
    );

    const telemetryTempToPermS3TransferGlueRole = new IAM.Role(
      this,
      "TelemetryTempToPermS3TransferGlueRole",
      {
        assumedBy: new IAM.ServicePrincipal("glue.amazonaws.com"),
      },
    );

    this.telemetryTemporaryBucket.grantRead(
      telemetryTempToPermS3TransferGlueRole,
    );
    this.telemetryPermanentBucket.grantWrite(
      telemetryTempToPermS3TransferGlueRole,
    );

    const telemetryGlueLoggingBucket = new S3.Bucket(
      this,
      "TelemetryGlueLoggingBucket",
      {
        bucketName: "telemetry-glue-logging-bucket",
      },
    );

    new GlueAlpha.PySparkFlexEtlJob(this, "TelemetryTempToPermETLJob", {
      script: GlueAlpha.Code.fromBucket(
        telemetryTempToPermS3TransferScript.bucket,
        telemetryTempToPermS3TransferScript.s3ObjectKey,
      ),
      glueVersion: GlueAlpha.GlueVersion.V5_1, // Specify desired Glue version (e.g., 4.0 for Spark 3.3)
      // runtime: GlueAlpha.GlueRuntime.V4_0,
      role: telemetryTempToPermS3TransferGlueRole,
      // Optional: configure job capacity, security, connections, etc.
      workerType: GlueAlpha.WorkerType.G_1X,
      numberOfWorkers: 2,
      sparkUI: {
        bucket: telemetryGlueLoggingBucket,
        prefix: "/glue-spark-ui-logs",
      },
      // workerType: glue.WorkerType.G_1X,
    });
  }
}
