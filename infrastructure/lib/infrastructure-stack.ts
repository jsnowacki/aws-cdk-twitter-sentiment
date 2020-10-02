import * as cdk from '@aws-cdk/core';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as kinesisfirehose from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';


interface InfrastructureProps {
  baseStackName: string,
  twitterApiSecretName: string,
  twitterApiCallMinutes: number,
}


export class InfrastructureStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, infrastructureProps: InfrastructureProps, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for Twitts   
    const destinationS3Bucket = new s3.Bucket(this, 'DestinationBucket', {
      bucketName: infrastructureProps.baseStackName + '-raw',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Kinesis delivery
    const streamDeliveryRole = new iam.Role(this, 'DeliveryRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        "root": new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                's3:AbortMultipartUpload',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:ListBucketMultipartUploads',
                's3:PutObject',
              ],
              resources: [
                destinationS3Bucket.bucketArn,
                destinationS3Bucket.bucketArn + "/*"
              ],
            }),
            new iam.PolicyStatement({
              actions: ['glue:GetTableVersions'],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents',
              ],
              resources: ['*'],
            }),
          ]
        })
      }
    });


    const deliveryStreamName = infrastructureProps.baseStackName + '-stream-raw';
    const deliveryStream = new kinesisfirehose.CfnDeliveryStream(this, 'ExportToS3', {
      deliveryStreamName: deliveryStreamName,
      deliveryStreamType: 'DirectPut',
      extendedS3DestinationConfiguration: {
        roleArn: streamDeliveryRole.roleArn,
        bucketArn: destinationS3Bucket.bucketArn,
        prefix: `data/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`,
        errorOutputPrefix: `errors/!{firehose:error-output-type}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`,
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 64,
        },
        compressionFormat: "GZIP",
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: `KDF-${deliveryStreamName}`,
          logStreamName: 'S3Delivery'
        }
      }
    });

    // Lambda function
    const twitterLambdaFunction = new lambda.Function(this, 'twitterFunction', {
      functionName: `${this.stackName}-lambda`,
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset('../twitter', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_8.bundlingDockerImage,
          command: [
            'bash', '-c', `
            pip install -r requirements.txt -t /asset-output &&
            cp -au . /asset-output
            `,
          ],
        },
      }),
      memorySize: 128,
      timeout: cdk.Duration.minutes(15),
      environment: {
        'SECRET_NAME': infrastructureProps.twitterApiSecretName,
        'DELIVERY_STREAM_NAME': deliveryStream.deliveryStreamName ? deliveryStream.deliveryStreamName.toString() : '',
      }
    });

    const twitterSecretArn = cdk.Stack.of(this).formatArn({
      service: 'secretsmanager',
      resource: 'secret',
      sep: ':',
      resourceName: infrastructureProps.twitterApiSecretName + '*'
    });

    const rule = new events.Rule(this, 'Rule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(infrastructureProps.twitterApiCallMinutes))
    });

    rule.addTarget(new targets.LambdaFunction(twitterLambdaFunction))

    twitterLambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "secretsmanager:GetResourcePolicy",
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret",
        "secretsmanager:ListSecretVersionIds",
      ],
      resources: [
        twitterSecretArn
      ],
    }));

    twitterLambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "firehose:PutRecord",
        "firehose:PutRecordBatch",
      ],
      resources: [
        deliveryStream.attrArn
      ],
    }));

  }
}
