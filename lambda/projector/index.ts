// lambda/projector/index.ts
// Projector Lambda: Transforms events into DynamoDB projections
// Triggered by SQS queue (which subscribes to SNS topic)

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

// Event envelope interface (matches what publisher sends)
interface EventEnvelope {
  id: string;
  type: string;
  version: number;
  occurredAt: string;
  aggregateId: string;
  aggregateType: string;
  data: any;
}

// Process a single event
async function processEvent(envelope: EventEnvelope) {
  console.log(`Processing event: ${envelope.id} (${envelope.type})`);

  // ============================================
  // TODO #1: Check if event was already processed (idempotency)
  // ============================================
  // Query the processed_events table to see if we've seen this event ID before
  // If we have, log it and return early (don't process twice!)
  //
  // HINT: Use docClient.send(new GetCommand({...}))
  // Look for eventId = envelope.id

  // const checkResult = await docClient.send(
  //   new GetCommand({
  //     TableName: PROCESSED_EVENTS_TABLE,
  //     Key: { eventId: envelope.id },
  //   })
  // );

  // if (checkResult.Item) {
  //   console.log(`Event ${envelope.id} already processed, skipping`);
  //   return;
  // }

  // ============================================
  // TODO #2: Handle different event types
  // ============================================
  // Based on envelope.type, transform the event into a DynamoDB item
  // For now, just handle 'WorkOrderCreated'

  if (envelope.type === "WorkOrderCreated") {
    // ============================================
    // TODO #2a: Extract work order data from envelope
    // ============================================
    const workOrder = envelope.data;

    // ============================================
    // TODO #2b: Create a DynamoDB item with the key design
    // ============================================
    // Remember the key design from the architecture:
    // pk: ORG#<orgId>
    // sk: STATUS#<status>#TS#<timestamp>#WO#<workOrderId>
    //
    // This design allows you to query:
    // - All work orders for an org
    // - All OPEN work orders for an org (sorted by time)
    // - All CLOSED work orders for an org
    //
    // The item should contain all the work order fields plus metadata

    const item = {
      pk: `ORG#${workOrder.org_id}`,
      sk: `STATUS#${workOrder.status}#TS#${envelope.occurredAt}#WO#${workOrder.id}`,

      // Entity type for easier querying
      entityType: "WorkOrder",

      // Work order data
      workOrderId: workOrder.id,
      orgId: workOrder.org_id,
      customerId: workOrder.customer_id,
      status: workOrder.status,
      title: workOrder.title,
      description: workOrder.description,
      address: workOrder.address,
      scheduledAt: workOrder.scheduled_at,

      // Metadata
      createdAt: workOrder.created_at,
      updatedAt: workOrder.updated_at,
      projectedAt: new Date().toISOString(),

      // Original event reference (useful for debugging)
      sourceEventId: envelope.id,
      sourceEventType: envelope.type,
    };

    // ============================================
    // TODO #2c: Write the item to DynamoDB
    // ============================================
    // Use docClient.send(new PutCommand({...}))

    // await docClient.send(
    //   new PutCommand({
    //     TableName: WORK_ORDER_TABLE,
    //     Item: item,
    //   })
    // );

    console.log(`Projected work order ${workOrder.id} to DynamoDB`);
  } else {
    console.log(`Unknown event type: ${envelope.type}, skipping`);
  }

  // ============================================
  // TODO #3: Mark event as processed
  // ============================================
  // Write to processed_events table to record that we've handled this event
  // Include a TTL so old records get auto-deleted after 7 days
  //
  // TTL value should be a Unix timestamp (seconds since epoch)
  // 7 days from now = Math.floor(Date.now() / 1000) + (7 * 24 * 60 * 60)

  const ttl = Math.floor(Date.now() / 1000) + 7 * 24 * 60 * 60;

  // await docClient.send(
  //   new PutCommand({
  //     TableName: PROCESSED_EVENTS_TABLE,
  //     Item: {
  //       eventId: envelope.id,
  //       eventType: envelope.type,
  //       processedAt: new Date().toISOString(),
  //       ttl: ttl,
  //     },
  //   })
  // );

  console.log(`Marked event ${envelope.id} as processed`);
}

// Lambda handler
export const handler = async (event: SQSEvent) => {
  console.log(`Received ${event.Records.length} messages from SQS`);

  // Process each SQS record
  for (const record of event.Records) {
    try {
      // ============================================
      // TODO #4: Parse the SNS message from SQS
      // ============================================
      // SQS receives messages from SNS, so the structure is:
      // record.body is a JSON string containing an SNS message
      // The SNS message has a 'Message' field with our event envelope
      //
      // You need to:
      // 1. Parse record.body as JSON
      // 2. Extract the Message field
      // 3. Parse the Message as JSON to get the event envelope

      const snsMessage = JSON.parse(record.body);
      const envelope: EventEnvelope = JSON.parse(snsMessage.Message);

      await processEvent(envelope);
    } catch (error) {
      console.error("Error processing record:", error);
      console.error("Record body:", record.body);
      // Throw error so SQS knows this message failed
      // After 3 failures, it'll go to the DLQ
      throw error;
    }
  }

  return { statusCode: 200, message: "Processing complete" };
};

// ============================================
// LEARNING NOTES:
// ============================================
// 1. Why check for duplicate processing (idempotency)?
//    - The publisher might send the same event twice (if it crashes)
//    - SQS might deliver the same message twice (at-least-once delivery)
//    - Network retries could cause duplicates
//    - Without this check, you'd create duplicate items in DynamoDB!
//
// 2. What happens if the Lambda crashes after writing to DynamoDB but before marking as processed?
//    - On retry, the idempotency check will fail... wait, no!
//    - The idempotency check happens FIRST, so we'd process it again
//    - This is where you have a choice:
//      a) Check processed_events first (prevents duplicate processing)
//      b) Use DynamoDB conditional writes (prevents duplicate items)
//    - We're doing (a) - simpler and works for most cases
//
// 3. Why use a TTL for processed_events?
//    - Without TTL, this table would grow forever
//    - After 7 days, we assume duplicates are unlikely
//    - This saves storage costs and keeps queries fast
//
// 4. What about the DynamoDB key design?
//    - pk = ORG#<orgId> - partitions data by organization
//    - sk = STATUS#OPEN#TS#<time>#WO#<id> - allows range queries
//    - This lets you efficiently query: "All OPEN work orders for org X, newest first"
//    - To query closed work orders, you'd use sk begins_with "STATUS#CLOSED"
//
// 5. What if you need to update a work order's status?
//    - You'd handle a 'WorkOrderStatusChanged' event
//    - Delete the old item (with old status in sk)
//    - Insert a new item (with new status in sk)
//    - This is the tradeoff of denormalization - updates are more complex
//
// 6. Why not just query Postgres directly?
//    - This DynamoDB view is optimized for specific query patterns
//    - It's faster (single-digit millisecond latency)
//    - It scales better (no connection pooling issues)
//    - It's cheaper at scale (no complex JOIN operations)
// ============================================
