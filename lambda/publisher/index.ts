// lambda/publisher/index.ts
// Outbox Publisher: Reads unpublished events and publishes to SNS
// Your job: Implement the polling and publishing logic

import { Client } from "pg";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { type } from "node:os";

interface OutboxEvent {
  id: string;
  event_type: string;
  aggregate_id: string;
  aggregate_type: string;
  payload: any; // This is JSONB, so it's already parsed
  occurred_at: Date;
  published_at: Date | null;
  version: number;
}

interface EventEnvelope {
  id: string;
  type: string;
  version: number;
  occurredAt: string;
  aggregateId: string;
  aggregateType: string;
  data: any;
}

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

    const res = await client.query<OutboxEvent>(`
      SELECT * from outbox WHERE published_at IS NULL
        ORDER BY occurred_at ASC
        LIMIT 10
    `);

    if (!res.rows.length) {
      return {
        statusCode: 204,
      };
    }

    console.log(`Found ${res.rows.length} unpublished events`);

    for (const row of res.rows) {
      try {
        const eventEnvelope: EventEnvelope = {
          aggregateId: row.aggregate_id,
          aggregateType: row.aggregate_type,
          data: row.payload,
          id: row.id,
          occurredAt: new Date(row.occurred_at).toISOString(),
          type: row.event_type,
          version: row.version,
        };

        await snsClient.send(
          new PublishCommand({
            TopicArn: process.env.SNS_TOPIC_ARN,
            Message: JSON.stringify(eventEnvelope),
            MessageAttributes: {
              eventType: {
                DataType: "String",
                StringValue: eventEnvelope.type,
              },
            },
          }),
        );

        await client.query(
          `UPDATE outbox SET published_at = NOW() WHERE id = $1`,
          [row.id],
        );
        console.log(`Successfully published event ${row.id}`);
      } catch (error) {
        console.error(`Failed to publish event ${row.id}: `, error);
      }
    }

    return {
      statusCode: 200,
      message: `Processed ${res.rows.length} events`,
    };
  } catch (error) {
    console.error("Error in publisher:", error);
    throw error;
  } finally {
    await client.end();
  }
};
