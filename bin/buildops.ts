#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { BuildOpsStack } from "../lib/buildops-stack";

const app = new cdk.App();

new BuildOpsStack(app, "BuildOpsStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
