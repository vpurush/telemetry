import * as cdk from "aws-cdk-lib";
import * as S3 from "aws-cdk-lib/aws-s3";
import * as Lambda from "aws-cdk-lib/aws-lambda";
import * as ApiGateway from "aws-cdk-lib/aws-apigateway";
import * as SQS from "aws-cdk-lib/aws-sqs";
import * as IAM from "aws-cdk-lib/aws-iam";
import { RustFunction } from 'cargo-lambda-cdk';
import { Construct } from "constructs";

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

    const telemetryApiGateway = new ApiGateway.RestApi(
      this,
      "TelemetryApiGateway",
      {
        restApiName: "telemetry-api",
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
      path: telemetryQueue.queueName,
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

    new RustFunction(this, 'TelemetrySQSToS3RustFunction', {
      manifestPath: '../telemetry-sqs-to-s3/Cargo.toml',
    });
  }
}
