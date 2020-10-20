import * as cdk from '@aws-cdk/core';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import * as glue from '@aws-cdk/aws-glue';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as kinesisfirehose from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';

import {TWEETS_TABLE_COLUMNS, TWEETS_TABLE_PARTITION_KEYS} from "./glue-table-meta"

interface InfrastructureProps {
  baseStackName: string,
  twitterApiSecretName: string,
  twitterApiCallMinutes: number,
  glueDatabaseName: string,
  glueTableName: string,
}


export class InfrastructureStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, infrastructureProps: InfrastructureProps, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for Tweets
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

    // Glue

    const jobArtifactsS3Bucket = new s3.Bucket(this, 'jobArtifactsBucket', {
      bucketName: infrastructureProps.baseStackName + '-artifacts',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    new s3deploy.BucketDeployment(this, 'DeploySparkJobCode', {
      sources: [s3deploy.Source.asset('../spark_job', { exclude: ['**', '!*.py'] })],
      destinationBucket: jobArtifactsS3Bucket,
      retainOnDelete: false,
    });
    

    const glueDatabase = new glue.Database(this, infrastructureProps.glueDatabaseName, {
      databaseName: infrastructureProps.glueDatabaseName
    });

    const glueTable = new glue.Table(this, infrastructureProps.glueTableName, {
      database: glueDatabase,
      tableName: infrastructureProps.glueTableName,
      description: 'Simple Tweets',
      columns: TWEETS_TABLE_COLUMNS,
      partitionKeys: TWEETS_TABLE_PARTITION_KEYS,
      dataFormat: glue.DataFormat.PARQUET,
      storedAsSubDirectories: true,
      compressed: false,
    });

    const roleForGlueJob = new iam.Role(this, 'glueJobRule', {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      inlinePolicies: {
          "root": new iam.PolicyDocument({
              statements: [
                  new iam.PolicyStatement({
                      actions: [
                          "s3:GetObject",
                          "s3:ListBucket",
                          "s3:GetObjectVersion"
                      ],
                      resources: [
                        destinationS3Bucket.bucketArn,
                        destinationS3Bucket.bucketArn + "/*",
                      ]
                  }),
                  new iam.PolicyStatement({
                      actions: [
                        "glue:GetConnection",
                        "glue:GetDatabase",
                        "glue:GetTable",
                        "glue:GetPartition",
                        "glue:CreatePartition",
                        "glue:DeletePartition"
                      ],
                      resources: [glueTable.tableArn]
                  }),
                  new iam.PolicyStatement({
                    actions: [
                      "s3:GetObject",
                      "s3:PutObject",
                      "s3:ListBucket",
                      "s3:DeleteObject",
                      "s3:DeleteObjectVersion",
                      "s3:GetObjectVersion"
                    ],
                    resources: [
                      glueTable.bucket.bucketArn,
                      glueTable.bucket.bucketArn + "/*", 
                      jobArtifactsS3Bucket.bucketArn,
                      jobArtifactsS3Bucket.bucketArn + "/*",
                    ]
                })
              ]
          })
      },
      managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")
      ]
    });


    const glueJob = new glue.CfnJob(this, 'glueJob', {
      name: this.stackName,
          command: {
              name: "glueetl",
              scriptLocation: jobArtifactsS3Bucket.s3UrlForObject('job.py'),
          },
          defaultArguments: {
              "--job-bookmark-option": "job-bookmark-disable",
              "--job-language": "python",
              "--enable-continuous-cloudwatch-log": "true",
              "--enable-continuous-log-filter": "true",
              "--enable-metrics": "true",
              "--extra-py-files": jobArtifactsS3Bucket.s3UrlForObject('transform.py'),
              "--input_s3": destinationS3Bucket.s3UrlForObject(),
              "--glue_database": infrastructureProps.glueDatabaseName,
              "--glue_table": infrastructureProps.glueTableName,
          },
          executionProperty: {
              maxConcurrentRuns: 2
          },
          maxRetries: 0,
          maxCapacity: 2,
          glueVersion: "2.0",
          role: roleForGlueJob.roleArn
        });
     

    // Crawler
    const crawlerRole = new iam.Role(this, `twitter-crawler-role`, {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")
      ]
    });

    crawlerRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["s3:GetObject", "s3:PutObject"],
      resources: [`${glueTable.bucket.bucketArn}/*`]
    }));

    const crawlerName = `${glueDatabase.databaseName}-crawler`
    const crawler = new glue.CfnCrawler(this, `twitter-crawler`, {
      name: crawlerName,
      role: crawlerRole.roleArn,
      targets: {
        catalogTargets: [
          {
            databaseName: glueDatabase.databaseName,
            tables: [glueTable.tableName],
          }
        ]
      },
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "LOG"
      },
      configuration: "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"}},\"Grouping\":{\"TableGroupingPolicy\":\"CombineCompatibleSchemas\"}}"
    });

  }
}
