// API Lambda: Apollo GraphQL with Function URL

import { ApolloServer } from "@apollo/server";
import {
  startServerAndCreateLambdaHandler,
  handlers,
} from "@as-integrations/aws-lambda";
import { Client } from "pg";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import Logger from "../shared/logger";
import crypto from "crypto";

interface ApolloContext {
  requestId: string;
}

interface CreateWorkOrderArgs {
  orgId: string;
  customerId: string;
  title: string;
  description?: string;
  address?: string;
  scheduledAt?: string;
}

interface WorkOrder {
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

export interface OutboxEvent {
  id: string;
  event_type: string;
  aggregate_id: string;
  aggregate_type: string;
  payload: any; // This is JSONB, so it's already parsed
  occurred_at: Date;
  published_at: Date | null;
  version: number;
  trace_id: crypto.UUID;
}

const typeDefs = `#graphql
  type Customer {
    id: ID!
    name: String!
    email: String
    phone: String
  }

  type WorkOrder {
    id: ID!
    orgId: String!
    customerId: ID!
    status: String!
    title: String!
    description: String
    address: String
    scheduledAt: String
    createdAt: String!
  }

  type Query {
    healthCheck: String!
    listCustomers: [Customer!]!
    workOrdersForOrg(orgId: String!): [WorkOrder!]!
  }

  type Mutation {
    createWorkOrder(
      orgId: String!
      customerId: ID!
      title: String!
      description: String
      address: String
      scheduledAt: String
    ): WorkOrder!
  }
`;

async function createDBClient() {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
  });
  await client.connect();
  return client;
}

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const resolvers = {
  Query: {
    healthCheck: () => "API is healthy!",

    listCustomers: async () => {
      const client = await createDBClient();
      const result = await client.query("SELECT * FROM customers");
      await client.end();
      return result.rows;
    },

    workOrdersForOrg: async (_: any, args: { orgId: string }) => {
      const result = await docClient.send(
        new QueryCommand({
          TableName: process.env.WORK_ORDER_TABLE,
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: {
            ":pk": `ORG#${args.orgId}`,
          },
        }),
      );

      return (
        result.Items?.map((item) => ({
          id: item.workOrderId,
          orgId: item.orgId,
          customerId: item.customerId,
          status: item.status,
          title: item.title,
          description: item.description,
          address: item.address,
          scheduledAt: item.scheduledAt,
          createdAt: item.createdAt,
        })) || []
      );
    },
  },

  Mutation: {
    createWorkOrder: async (
      _: any,
      args: CreateWorkOrderArgs,
      context: ApolloContext,
    ) => {
      const traceId = crypto.randomUUID();
      const logger = new Logger("api", context.requestId);
      logger.info("Creating work order", {
        traceId,
        orgId: args.orgId,
        customerId: args.customerId,
      });
      const client = await createDBClient();

      try {
        await client.query("BEGIN");
        const res = await client.query<WorkOrder>(
          `INSERT INTO work_orders (org_id, customer_id, title, description, address, scheduled_at, trace_id)
           VALUES($1, $2, $3, $4, $5, $6, $7) 
           RETURNING *`,
          [
            args.orgId,
            args.customerId,
            args.title,
            args.description,
            args.address,
            args.scheduledAt,
            traceId,
          ],
        );
        const createdWorkOrder = res.rows[0];

        const eventType = "WorkOrderCreated";
        await client.query<OutboxEvent>(
          `INSERT INTO outbox (event_type, aggregate_id, aggregate_type, payload, trace_id)
            VALUES ($1, $2, $3, $4, $5)`,
          [
            eventType,
            createdWorkOrder.id, // aggregate_id
            "WorkOrderCreated", // aggregate_type
            JSON.stringify(createdWorkOrder),
            traceId,
          ],
        );
        await client.query("COMMIT");

        logger.info("Work order successfully created", {
          workOrderId: createdWorkOrder.id,
          traceId,
          orgId: args.orgId,
          customerId: args.customerId,
          eventType,
        });
        return {
          ...createdWorkOrder,
          createdAt: new Date(createdWorkOrder.created_at).toISOString(),
          updatedAt: new Date(createdWorkOrder.updated_at).toISOString(),
          orgId: createdWorkOrder.org_id,
          customerId: createdWorkOrder.customer_id,
        };
      } catch (creationError) {
        const errorMessage =
          creationError instanceof Error
            ? creationError.message
            : String(creationError);
        const errorStack =
          creationError instanceof Error ? creationError.stack : undefined;

        logger.error(`Error creating work order`, {
          traceId,
          orgId: args.orgId,
          customerId: args.customerId,
          error: errorMessage,
          stack: errorStack,
        });
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          const errorMessage =
            rollbackError instanceof Error
              ? rollbackError.message
              : String(rollbackError);
          const errorStack =
            rollbackError instanceof Error ? rollbackError.stack : undefined;

          logger.error(`Error rolling back transaction`, {
            traceId,
            orgId: args.orgId,
            customerId: args.customerId,
            error: errorMessage,
            stack: errorStack,
          });
        }
        throw creationError;
      } finally {
        await client.end();
      }
    },
  },
};

const server = new ApolloServer<ApolloContext>({
  typeDefs,
  resolvers,
});

export const handler = startServerAndCreateLambdaHandler(
  server,
  handlers.createAPIGatewayProxyEventV2RequestHandler(),
  {
    context: async ({ context }) => ({
      requestId: context.awsRequestId,
    }),
  },
);
