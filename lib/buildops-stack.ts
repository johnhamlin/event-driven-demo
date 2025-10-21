import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as eventsources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

export class BuildOpsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ============================================
    // DATABASE CONNECTION (Neon)
    // ============================================
    const DATABASE_URL = process.env.DATABASE_URL || "missing";

    // ============================================
    // 1. DynamoDB - Fast Read Projections
    // ============================================
    const workOrderTable = new dynamodb.Table(this, "WorkOrderProjections", {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const processedEventsTable = new dynamodb.Table(this, "ProcessedEvents", {
      partitionKey: { name: "eventId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ============================================
    // 2. SNS Topic - Event Distribution
    // ============================================
    const eventTopic = new sns.Topic(this, "DomainEvents", {
      displayName: "BuildOps Domain Events",
    });

    // ============================================
    // 3. SQS Queues with DLQs
    // ============================================
    const projectorDLQ = new sqs.Queue(this, "ProjectorDLQ", {
      retentionPeriod: cdk.Duration.days(14),
    });

    const projectorQueue = new sqs.Queue(this, "ProjectorQueue", {
      visibilityTimeout: cdk.Duration.seconds(30),
      deadLetterQueue: {
        queue: projectorDLQ,
        maxReceiveCount: 3,
      },
    });

    eventTopic.addSubscription(
      new subscriptions.SqsSubscription(projectorQueue),
    );

    // ============================================
    // 4. Lambda Functions (No VPC needed!)
    // ============================================

    // --- API Lambda (GraphQL) ---
    const apiLambda = new lambda.Function(this, "ApiLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/api"),
      timeout: cdk.Duration.seconds(10),
      environment: {
        DATABASE_URL,
        WORK_ORDER_TABLE: workOrderTable.tableName,
        NODE_OPTIONS: "--enable-source-maps",
      },
    });

    workOrderTable.grantReadData(apiLambda);

    const apiUrl = apiLambda.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      cors: {
        allowedOrigins: ["*"],
        allowedMethods: [lambda.HttpMethod.POST],
        allowedHeaders: ["content-type"],
      },
    });

    // --- Outbox Publisher Lambda ---
    const publisherLambda = new lambda.Function(this, "PublisherLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/publisher"),
      timeout: cdk.Duration.seconds(30),
      environment: {
        DATABASE_URL,
        SNS_TOPIC_ARN: eventTopic.topicArn,
      },
    });

    eventTopic.grantPublish(publisherLambda);

    const publisherRule = new events.Rule(this, "PublisherSchedule", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
    });
    publisherRule.addTarget(new targets.LambdaFunction(publisherLambda));

    // --- Projector Lambda ---
    const projectorLambda = new lambda.Function(this, "ProjectorLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/projector"),
      timeout: cdk.Duration.seconds(30),
      environment: {
        WORK_ORDER_TABLE: workOrderTable.tableName,
        PROCESSED_EVENTS_TABLE: processedEventsTable.tableName,
      },
    });

    workOrderTable.grantWriteData(projectorLambda);
    processedEventsTable.grantReadWriteData(projectorLambda);

    projectorLambda.addEventSource(
      new eventsources.SqsEventSource(projectorQueue, {
        batchSize: 10,
      }),
    );

    // ============================================
    // 5. Outputs
    // ============================================
    new cdk.CfnOutput(this, "ApiEndpoint", {
      value: apiUrl.url,
      description: "GraphQL API URL",
    });

    new cdk.CfnOutput(this, "WorkOrderTableName", {
      value: workOrderTable.tableName,
      description: "DynamoDB table for projections",
    });

    new cdk.CfnOutput(this, "ProcessedEventsTableName", {
      value: processedEventsTable.tableName,
      description: "DynamoDB table for idempotency",
    });
  }
}
