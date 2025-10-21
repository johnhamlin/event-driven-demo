import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as rds from "aws-cdk-lib/aws-rds";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as eventsources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

export class BuildOpsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ============================================
    // 1. VPC - Network isolation for RDS
    // ============================================
    const vpc = new ec2.Vpc(this, "BuildOpsVpc", {
      maxAzs: 2, // Multi-AZ for reliability (but still in free tier)
      natGateways: 0, // Save $$ - Lambdas will use VPC endpoints
      subnetConfiguration: [
        {
          name: "public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          name: "private-isolated",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
        },
      ],
    });

    // ============================================
    // 2. RDS Postgres - Source of Truth
    // ============================================
    const dbSecret = new secretsmanager.Secret(this, "DBSecret", {
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ username: "postgres" }),
        generateStringKey: "password",
        excludePunctuation: true,
        includeSpace: false,
      },
    });

    const dbSecurityGroup = new ec2.SecurityGroup(this, "DBSecurityGroup", {
      vpc,
      description: "Allow Lambda to access RDS",
      allowAllOutbound: true,
    });

    const db = new rds.DatabaseInstance(this, "BuildOpsDB", {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15,
      }),
      instanceType: ec2.InstanceType.of(
        ec2.InstanceClass.T3,
        ec2.InstanceSize.MICRO,
      ),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroups: [dbSecurityGroup],
      credentials: rds.Credentials.fromSecret(dbSecret),
      databaseName: "buildops",
      allocatedStorage: 20,
      maxAllocatedStorage: 20, // Prevent auto-scaling
      deletionProtection: false, // Allow easy teardown
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete on stack destroy
    });

    // ============================================
    // 3. DynamoDB - Fast Read Projections
    // ============================================
    const workOrderTable = new dynamodb.Table(this, "WorkOrderProjections", {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST, // Free tier friendly
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Table for idempotency tracking
    const processedEventsTable = new dynamodb.Table(this, "ProcessedEvents", {
      partitionKey: { name: "eventId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl", // Auto-cleanup old events after 7 days
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ============================================
    // 4. SNS Topic - Event Distribution
    // ============================================
    const eventTopic = new sns.Topic(this, "DomainEvents", {
      displayName: "BuildOps Domain Events",
    });

    // ============================================
    // 5. SQS Queues with DLQs
    // ============================================

    // DLQ for projector
    const projectorDLQ = new sqs.Queue(this, "ProjectorDLQ", {
      retentionPeriod: cdk.Duration.days(14),
    });

    // Main projector queue
    const projectorQueue = new sqs.Queue(this, "ProjectorQueue", {
      visibilityTimeout: cdk.Duration.seconds(30), // Match Lambda timeout
      deadLetterQueue: {
        queue: projectorDLQ,
        maxReceiveCount: 3, // After 3 failures, send to DLQ
      },
    });

    // Subscribe queue to SNS topic
    eventTopic.addSubscription(
      new subscriptions.SqsSubscription(projectorQueue),
    );

    // ============================================
    // 6. Lambda Functions
    // ============================================

    // Shared Lambda environment for DB access
    const lambdaSecurityGroup = new ec2.SecurityGroup(
      this,
      "LambdaSecurityGroup",
      {
        vpc,
        description: "Security group for Lambdas",
        allowAllOutbound: true,
      },
    );

    // Allow Lambda to connect to RDS
    dbSecurityGroup.addIngressRule(
      lambdaSecurityGroup,
      ec2.Port.tcp(5432),
      "Allow Lambda to access RDS",
    );

    // Shared environment variables
    const dbEnv = {
      DB_SECRET_ARN: dbSecret.secretArn,
      DB_HOST: db.dbInstanceEndpointAddress,
      DB_PORT: db.dbInstanceEndpointPort,
      DB_NAME: "buildops",
    };

    // --- API Lambda (GraphQL) ---
    const apiLambda = new lambda.Function(this, "ApiLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/api"), // We'll create this next
      timeout: cdk.Duration.seconds(10),
      environment: {
        ...dbEnv,
        NODE_OPTIONS: "--enable-source-maps",
      },
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroups: [lambdaSecurityGroup],
    });

    // Grant API access to read DB secret
    dbSecret.grantRead(apiLambda);

    // Create Function URL for API (simpler than API Gateway for demo)
    const apiUrl = apiLambda.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE, // No auth for demo
      cors: {
        allowedOrigins: ["*"],
        allowedMethods: [lambda.HttpMethod.POST],
      },
    });

    // --- Outbox Publisher Lambda ---
    const publisherLambda = new lambda.Function(this, "PublisherLambda", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/publisher"),
      timeout: cdk.Duration.seconds(30),
      environment: {
        ...dbEnv,
        SNS_TOPIC_ARN: eventTopic.topicArn,
      },
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroups: [lambdaSecurityGroup],
    });

    dbSecret.grantRead(publisherLambda);
    eventTopic.grantPublish(publisherLambda);

    // Schedule publisher to run every 1 minute
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

    // Grant DynamoDB permissions
    workOrderTable.grantWriteData(projectorLambda);
    processedEventsTable.grantReadWriteData(projectorLambda);

    // Trigger projector from SQS
    projectorLambda.addEventSource(
      new eventsources.SqsEventSource(projectorQueue, {
        batchSize: 10,
      }),
    );

    // ============================================
    // 7. Outputs
    // ============================================
    new cdk.CfnOutput(this, "ApiEndpoint", {
      value: apiUrl.url,
      description: "GraphQL API URL",
    });

    new cdk.CfnOutput(this, "DBSecretArn", {
      value: dbSecret.secretArn,
      description: "RDS credentials secret ARN",
    });

    new cdk.CfnOutput(this, "DBHost", {
      value: db.dbInstanceEndpointAddress,
      description: "RDS endpoint",
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
