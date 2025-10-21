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

async function processEvent(envelope: EventEnvelope) {
  console.log(`Processing event: ${envelope.id} (${envelope.type})`);

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

  throw new Error("TODO: Implement event processing logic");
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

      throw new Error("TODO: Implement SQS message parsing");
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
