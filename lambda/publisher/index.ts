// lambda/publisher/index.ts
// Outbox Publisher: Reads unpublished events and publishes to SNS
// Runs every 1 minute via EventBridge cron

import { Client } from "pg";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const snsClient = new SNSClient({});

// Helper: Get DB credentials
async function getDBCredentials() {
  const client = new SecretsManagerClient({});
  const response = await client.send(
    new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }),
  );
  return JSON.parse(response.SecretString!);
}

// Helper: Create PostgreSQL client
async function createDBClient() {
  const credentials = await getDBCredentials();
  const client = new Client({
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || "5432"),
    database: process.env.DB_NAME,
    user: credentials.username,
    password: credentials.password,
  });
  await client.connect();
  return client;
}

export const handler = async () => {
  const client = await createDBClient();

  try {
    // ============================================
    // TODO #1: Query for unpublished events
    // ============================================
    // Select events from the outbox table where published_at IS NULL
    // Order by occurred_at to publish events in chronological order
    // HINT: You might want to LIMIT this to prevent processing too many at once

    const result = await client.query(`
      SELECT * FROM outbox 
      WHERE published_at IS NULL 
      ORDER BY occurred_at ASC 
      LIMIT 10
    `);

    const unpublishedEvents = result.rows;

    if (unpublishedEvents.length === 0) {
      console.log("No unpublished events found");
      return { statusCode: 200, message: "No events to publish" };
    }

    console.log(`Found ${unpublishedEvents.length} unpublished events`);

    // ============================================
    // TODO #2: Publish each event to SNS
    // ============================================
    // For each event, you need to:
    // 1. Create an event envelope (wrapper with metadata)
    // 2. Publish it to SNS
    // 3. Mark it as published in the database
    //
    // The envelope should look like:
    // {
    //   id: <event id>,
    //   type: <event_type>,
    //   version: <version>,
    //   occurredAt: <occurred_at>,
    //   aggregateId: <aggregate_id>,
    //   aggregateType: <aggregate_type>,
    //   data: <payload>
    // }

    for (const event of unpublishedEvents) {
      try {
        // ============================================
        // TODO #2a: Create event envelope
        // ============================================
        const envelope = {
          id: event.id,
          type: event.event_type,
          version: event.version,
          occurredAt: event.occurred_at.toISOString(),
          aggregateId: event.aggregate_id,
          aggregateType: event.aggregate_type,
          data: event.payload,
        };

        // ============================================
        // TODO #2b: Publish to SNS
        // ============================================
        // Use the SNS client to publish the envelope
        // HINT: The message should be JSON.stringify(envelope)
        // HINT: Use process.env.SNS_TOPIC_ARN for the TopicArn

        // await snsClient.send(
        //   new PublishCommand({
        //     TopicArn: process.env.SNS_TOPIC_ARN,
        //     Message: JSON.stringify(envelope),
        //     MessageAttributes: {
        //       eventType: {
        //         DataType: 'String',
        //         StringValue: event.event_type,
        //       },
        //     },
        //   })
        // );

        console.log(`Published event ${event.id} to SNS`);

        // ============================================
        // TODO #2c: Mark event as published
        // ============================================
        // Update the outbox table to set published_at = NOW()
        // This prevents the event from being published again

        // await client.query(
        //   'UPDATE outbox SET published_at = NOW() WHERE id = $1',
        //   [event.id]
        // );

        console.log(`Marked event ${event.id} as published`);
      } catch (error) {
        // If publishing fails, we'll try again on the next cron run
        console.error(`Error publishing event ${event.id}:`, error);
        // Don't throw - we want to continue processing other events
      }
    }

    return {
      statusCode: 200,
      message: `Published ${unpublishedEvents.length} events`,
    };
  } catch (error) {
    console.error("Error in publisher:", error);
    throw error;
  } finally {
    await client.end();
  }
};

// ============================================
// LEARNING NOTES:
// ============================================
// 1. Why poll instead of CDC (Change Data Capture)?
//    - Simpler to implement and understand
//    - No need for database plugins or special setup
//    - Good enough for most use cases (eventual consistency is okay!)
//    - In production, you might use something like Debezium for CDC
//
// 2. What if the Lambda crashes after publishing to SNS but before marking as published?
//    - The event will be published AGAIN on the next cron run
//    - This is okay! The consumers are responsible for idempotency
//    - This is called "at-least-once delivery" (vs. "exactly-once")
//
// 3. Why not mark ALL events as published in one transaction?
//    - We publish them one at a time to handle partial failures
//    - If event #3 fails, events #1 and #2 are still marked as published
//    - This is a tradeoff: more database round-trips but better fault tolerance
//
// 4. What about ordering?
//    - We ORDER BY occurred_at to maintain chronological order
//    - But SNS doesn't guarantee order, so consumers must handle out-of-order events
//    - This is a common challenge in distributed systems!
// ============================================
