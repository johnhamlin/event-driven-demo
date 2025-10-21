// lambda/publisher/index.ts
// Outbox Publisher: Reads unpublished events and publishes to SNS
// Your job: Implement the polling and publishing logic

import { Client } from "pg";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const snsClient = new SNSClient({});

async function createDBClient() {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
  });
  await client.connect();
  return client;
}

export const handler = async () => {
  const client = await createDBClient();

  try {
    // ============================================
    // TODO: Implement the outbox publisher
    // ============================================
    // Your implementation should:
    // 1. Query the outbox table for unpublished events
    // 2. For each event, create an envelope and publish to SNS
    // 3. Mark each successfully published event as published
    //
    // Query requirements:
    // - Find events where published_at IS NULL
    // - Order by occurred_at (chronological)
    // - Limit to a reasonable batch size (e.g., 10)
    //
    // Event envelope structure:
    // {
    //   id: <event UUID>,
    //   type: <event_type from database>,
    //   version: <version number>,
    //   occurredAt: <timestamp in ISO format>,
    //   aggregateId: <aggregate_id>,
    //   aggregateType: <aggregate_type>,
    //   data: <payload from database>
    // }
    //
    // Publishing requirements:
    // - Publish to SNS using process.env.SNS_TOPIC_ARN
    // - Message should be JSON.stringify of the envelope
    // - Add a MessageAttribute for eventType (helps with filtering)
    // - After successful publish, update outbox.published_at to NOW()
    //
    // Error handling:
    // - If one event fails, continue with others (don't fail entire batch)
    // - Log errors but don't throw (we'll retry on next cron run)
    //
    // Questions to think about:
    // - What happens if SNS publish succeeds but the DB update fails?
    // - Is that okay? (Hint: think about idempotency on the consumer side)

    // Your code here

    return {
      statusCode: 200,
      message: "TODO: Implement publisher logic",
    };
  } catch (error) {
    console.error("Error in publisher:", error);
    throw error;
  } finally {
    await client.end();
  }
};
