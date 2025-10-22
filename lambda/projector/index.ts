// Projector Lambda: Transforms events into DynamoDB projections

import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import { EventEnvelope } from "../publisher";
import Logger from "../shared/logger";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const WORK_ORDER_TABLE = process.env.WORK_ORDER_TABLE!;
const PROCESSED_EVENTS_TABLE = process.env.PROCESSED_EVENTS_TABLE!;

interface WorkOrderData {
  id: string;
  org_id: string;
  customer_id: string;
  status: string;
  title: string;
  description: string | null;
  address: string | null;
  scheduled_at: string | null;
  created_at: string;
  updated_at: string;
}

interface WorkOrderProjection {
  pk: string;
  sk: string;
  entityType: string;
  workOrderId: string;
  orgId: string;
  customerId: string;
  status: string;
  title: string;
  description: string | null;
  address: string | null;
  scheduledAt: string | null;
  createdAt: string;
  updatedAt: string;
  projectedAt: string;
  sourceEventId: string;
  sourceEventType: string;
}

interface ProcessedEvent {
  eventId: string;
  eventType: string;
  processedAt: string;
  ttl: number;
}

async function processEvent(envelope: EventEnvelope, logger: Logger) {
  const result = await docClient.send(
    new GetCommand({
      TableName: PROCESSED_EVENTS_TABLE,
      Key: { eventId: envelope.id },
    }),
  );

  if (result.Item) {
    logger.info(`Event ${envelope.id} already processed, skipping`, {
      traceId: envelope.traceId,
      eventId: envelope.id,
      eventType: envelope.type,
      workOrderId: envelope.aggregateId,
    });
    return;
  }

  if (envelope.type === "WorkOrderCreated") {
    const data: WorkOrderData = envelope.data;
    const dbItem: WorkOrderProjection = {
      pk: `ORG#${data.org_id}`,
      sk: `STATUS#${data.status}#TS#${new Date(data.created_at).toISOString()}#WO#${data.id}`,
      address: data.address,
      createdAt: new Date(data.created_at).toISOString(),
      customerId: data.customer_id,
      description: data.description,
      entityType: envelope.aggregateType,
      orgId: data.org_id,
      projectedAt: new Date().toISOString(),
      scheduledAt: data.scheduled_at,
      sourceEventId: envelope.id,
      sourceEventType: envelope.type,
      status: data.status,
      title: data.title,
      updatedAt: new Date(data.updated_at).toISOString(),
      workOrderId: data.id,
    };

    await docClient.send(
      new PutCommand({
        TableName: WORK_ORDER_TABLE,
        Item: dbItem,
      }),
    );

    const processedEvent: ProcessedEvent = {
      eventId: envelope.id,
      eventType: envelope.type,
      processedAt: new Date().toISOString(),
      ttl: Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60,
    };

    await docClient.send(
      new PutCommand({
        TableName: PROCESSED_EVENTS_TABLE,
        Item: processedEvent,
      }),
    );
    logger.info(`Event ${envelope.id} successfully processed`, {
      traceId: envelope.traceId,
      eventId: envelope.id,
      eventType: envelope.type,
      workOrderId: envelope.aggregateId,
    });
  }
}

export const handler: AWSLambda.Handler = async (event: SQSEvent, context) => {
  const logger = new Logger("projector", context.awsRequestId);
  logger.info(`Received ${event.Records.length} messages from SQS`);

  for (const record of event.Records) {
    let traceId: string | undefined;
    try {
      const snsMessage = JSON.parse(record.body);
      const envelope: EventEnvelope = JSON.parse(snsMessage.Message);
      traceId = envelope.traceId;

      await processEvent(envelope, logger);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      logger.error(`Error processing event record`, {
        traceId,
        recordBody: record.body,
        error: errorMessage,
        stack: errorStack,
      });

      // Re-throw so SQS knows this message failed
      // After maxReceiveCount retries, it goes to DLQ
      throw error;
    }
  }

  return { statusCode: 200, message: "Processing complete" };
};
