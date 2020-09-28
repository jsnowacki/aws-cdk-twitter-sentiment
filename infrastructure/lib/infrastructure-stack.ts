import * as cdk from '@aws-cdk/core';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';


interface InfrastructureProps {
  baseStackName: string,
  twitterApiSecretName: string,
  twitterApiCallMinutes: number,
}


export class InfrastructureStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, infrastructureProps: InfrastructureProps, props?: cdk.StackProps) {
    super(scope, id, props);

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
      }
    });

    const twitterSecretArn = cdk.Stack.of(this).formatArn({
      service: 'secretsmanager',
      resource: 'secret',
      sep: ':',
      resourceName: infrastructureProps.twitterApiSecretName + '*'
    });

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

    const rule = new events.Rule(this, 'Rule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(infrastructureProps.twitterApiCallMinutes))
    });

    rule.addTarget(new targets.LambdaFunction(twitterLambdaFunction))

  }
}
