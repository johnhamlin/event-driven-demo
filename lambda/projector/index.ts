// lambda/projector/index.ts
// Projector Lambda: Transforms events into DynamoDB projections
// Your job: Implement idempotent event processing and DynamoDB writes

import { SQSEvent, SQSRecord } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
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

  // ============================================
  // TODO: Implement idempotent event processing
  // ============================================
  // Your implementation should:
  // 1. Check if this event was already processed (idempotency)
  // 2. Transform the event into a DynamoDB item
  // 3. Write the item to DynamoDB
  // 4. Mark the event as processed
  //
  // Idempotency requirements:
  // - Check processed_events table for envelope.id
  // - If found, log and return early (already processed)
  // - This prevents duplicate processing if publisher sends event twice
  //
  // DynamoDB key design for work orders:
  // pk: ORG#<orgId>
  // sk: STATUS#<status>#TS#<timestamp>#WO#<workOrderId>
  //
  // Why this design?
  // - Allows querying all work orders for an org
  // - Allows filtering by status (e.g., all OPEN orders)
  // - Orders are sorted by timestamp within each status
  //
  // The item should include:
  // - All work order fields
  // - Metadata (projectedAt, sourceEventId, etc.)
  // - Entity type for easier querying
  //
  // After writing to DynamoDB:
  // - Write to processed_events table
  // - Include eventId, eventType, processedAt timestamp
  // - Set TTL to 7 days from now (auto-cleanup old records)
  //
  // Questions to think about:
  // - What happens if DynamoDB write succeeds but processed_events write fails?
  // - On retry, will idempotency check catch this? (Yes, if you check first!)
  // - What if you need to update a work order's status? (Hint: delete old item, insert new)

  // Your code here
}

export const handler = async (event: SQSEvent) => {
  console.log(`Received ${event.Records.length} messages from SQS`);

  for (const record of event.Records) {
    try {
      // ============================================
      // TODO: Parse the SNS message from SQS
      // ============================================
      // SQS receives messages from SNS, so structure is:
      // - record.body is a JSON string (the SNS notification)
      // - That JSON has a "Message" field (our event envelope)
      // - Parse both layers to get the EventEnvelope

      // Your code here to extract envelope from record.body

      // Then process it
      // await processEvent(envelope);
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
