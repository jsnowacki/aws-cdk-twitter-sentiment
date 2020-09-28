#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { InfrastructureStack } from '../lib/infrastructure-stack';

const baseStackName = 'aws-cdk-twitter-sentiment';

const app = new cdk.App();
new InfrastructureStack(app, baseStackName, {
    baseStackName,
    twitterApiSecretName: "TwitterAPIKeys",
    twitterApiCallMinutes: 1,
});
