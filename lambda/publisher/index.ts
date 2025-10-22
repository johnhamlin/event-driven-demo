// Outbox Publisher: Reads unpublished events and publishes to SNS

import { Client } from "pg";
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";

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
