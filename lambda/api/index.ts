// lambda/api/index.ts
// API Lambda: Apollo GraphQL with Function URL
// This is your BFF layer - handles mutations and queries

import { ApolloServer } from "@apollo/server";
import {
  startServerAndCreateLambdaHandler,
  handlers,
} from "@as-integrations/aws-lambda";
import { Client } from "pg";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

// GraphQL Schema
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
    # TODO: Add query to fetch work orders from DynamoDB
    # For now, just return a placeholder
    healthCheck: String!
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

// Helper: Get DB credentials from Secrets Manager
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

// Resolvers
const resolvers = {
  Query: {
    healthCheck: () => "API is healthy!",
  },
  Mutation: {
    createWorkOrder: async (_: any, args: any) => {
      const client = await createDBClient();

      try {
        // ============================================
        // TODO #1: Begin a database transaction
        // ============================================
        // Hint: Use client.query('BEGIN')

        // ============================================
        // TODO #2: Insert the work order into work_orders table
        // ============================================
        // Use INSERT INTO work_orders (...) VALUES (...) RETURNING *
        // Remember to use parameterized queries ($1, $2, etc.) to prevent SQL injection
        // Store the result so you can get the generated ID

        const workOrderResult = null; // Replace with your INSERT query
        const workOrder = null; // Extract the row from workOrderResult

        // ============================================
        // TODO #3: Insert event into outbox table
        // ============================================
        // This is the KEY to the transactional outbox pattern!
        //
        // You need to create an event record that will be published later.
        // The event should contain:
        // - event_type: 'WorkOrderCreated'
        // - aggregate_id: the work order ID you just created
        // - aggregate_type: 'WorkOrder'
        // - payload: JSON object with all the work order data
        // - occurred_at: NOW() (database function)
        // - published_at: NULL (it hasn't been published yet)
        //
        // Why is this important? Because this INSERT happens in the SAME
        // transaction as the work order INSERT. Either both succeed or both fail.
        // This guarantees we'll never lose an event!

        // await client.query(
        //   `INSERT INTO outbox (...) VALUES (...)`,
        //   [...params]
        // );

        // ============================================
        // TODO #4: Commit the transaction
        // ============================================
        // await client.query('COMMIT')

        console.log("Work order created with transactional outbox:", workOrder);

        return workOrder; // Return the created work order
      } catch (error) {
        // ============================================
        // TODO #5: Rollback on error
        // ============================================
        // If anything fails, rollback the transaction
        // await client.query('ROLLBACK')

        console.error("Error creating work order:", error);
        throw error;
      } finally {
        await client.end();
      }
    },
  },
};

// Create Apollo Server
const server = new ApolloServer({
  typeDefs,
  resolvers,
});

// Export Lambda handler
export const handler = startServerAndCreateLambdaHandler(
  server,
  handlers.createAPIGatewayProxyEventV2RequestHandler(),
);

// ============================================
// LEARNING NOTES:
// ============================================
// 1. Why use a transaction?
//    - Atomicity: Both work_order AND outbox inserts succeed or both fail
//    - This prevents "lost events" where data is written but event never fires
//
// 2. Why write to outbox instead of publishing to SNS directly?
//    - What if SNS is down? You'd write the work order but fail to notify anyone
//    - What if the Lambda times out after INSERT but before SNS publish?
//    - The outbox pattern solves this by making event publishing a separate concern
//
// 3. What happens next?
//    - The publisher Lambda will poll this outbox table
//    - Find unpublished events (published_at IS NULL)
//    - Publish them to SNS
//    - Mark them as published
//
// This is called "eventual consistency" - the event will EVENTUALLY be published,
// but it might not be immediate. That's okay! The important thing is reliability.
// ============================================
