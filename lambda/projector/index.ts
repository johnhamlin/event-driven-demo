// Projector Lambda: Transforms events into DynamoDB projections

import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const WORK_ORDER_TABLE = process.env.WORK_ORDER_TABLE!;
const PROCESSED_EVENTS_TABLE = process.env.PROCESSED_EVENTS_TABLE!;

interface EventEnvelope {
  id: string;
  type: string;
  version: number;
  occurredAt: string;
  aggregateId: string;
  aggregateType: string;
  data: any;
}

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

async function processEvent(envelope: EventEnvelope) {
  console.log(`Processing event: ${envelope.id} (${envelope.type})`);
  const result = await docClient.send(
    new GetCommand({
      TableName: PROCESSED_EVENTS_TABLE,
      Key: { eventId: envelope.id },
    }),
  );

  if (result.Item) {
    console.log(`Event ${envelope.id} already processed, skipping`);
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
  }
}

export const handler = async (event: SQSEvent) => {
  console.log(`Received ${event.Records.length} messages from SQS`);

  for (const record of event.Records) {
    try {
      const snsMessage = JSON.parse(record.body);
      const envelope: EventEnvelope = JSON.parse(snsMessage.Message);
      await processEvent(envelope);
    } catch (error) {
      console.error("Error processing record:", error);
      console.error("Record body:", record.body);
      // Re-throw so SQS knows this message failed
      // After maxReceiveCount retries, it goes to DLQ
      throw error;
    }
  }

  return { statusCode: 200, message: "Processing complete" };
};
