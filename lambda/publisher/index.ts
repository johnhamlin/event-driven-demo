// Outbox Publisher: Reads unpublished events and publishes to SNS

import { Client } from "pg";
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";
import { OutboxEvent } from "../api";
import crypto from "crypto";
import Logger from "../shared/logger";

export interface EventEnvelope {
  id: string;
  type: string;
  version: number;
  occurredAt: string;
  aggregateId: string;
  aggregateType: string;
  data: any;
  traceId: crypto.UUID;
}

const snsClient = new SNSClient({});

async function createDBClient() {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
  });
  await client.connect();
  return client;
}

export const handler: AWSLambda.Handler = async (_, context) => {
  const client = await createDBClient();
  const logger = new Logger("publisher", context.awsRequestId);

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

    logger.info(`Found ${res.rows.length} unpublished events`);

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
          traceId: row.trace_id,
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
        logger.info(`Successfully published event ${row.id}`, {
          traceId: row.trace_id,
          eventType: row.event_type,
          workOrderId: row.aggregate_id,
        });
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;

        logger.error(`Failed to publish event ${row.id}: `, {
          traceId: row.trace_id,
          eventType: row.event_type,
          workOrderId: row.aggregate_id,
          error: errorMessage,
          stack: errorStack,
        });
      }
    }

    return {
      statusCode: 200,
      message: `Processed ${res.rows.length} events`,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const errorStack = error instanceof Error ? error.stack : undefined;
    logger.error("Error in publisher", {
      error: errorMessage,
      stack: errorStack,
    });
    throw error;
  } finally {
    await client.end();
  }
};
