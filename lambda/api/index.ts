// lambda/api/index.ts
// API Lambda: Apollo GraphQL with Function URL
// Your job: Implement the createWorkOrder mutation with the transactional outbox pattern

import { ApolloServer } from "@apollo/server";
import {
  startServerAndCreateLambdaHandler,
  handlers,
} from "@as-integrations/aws-lambda";
import { Client } from "pg";

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

const resolvers = {
  Query: {
    healthCheck: () => "API is healthy!",

    listCustomers: async () => {
      const client = await createDBClient();
      const result = await client.query("SELECT * FROM customers");
      await client.end();
      return result.rows;
    },
  },

  Mutation: {
    createWorkOrder: async (_: any, args: CreateWorkOrderArgs) => {
      const client = await createDBClient();

      try {
        // ============================================
        // TODO: Implement the transactional outbox pattern
        // ============================================
        // Your implementation should:
        // 1. Start a database transaction
        // 2. Insert the work order into the work_orders table
        // 3. Insert an event into the outbox table (same transaction!)
        // 4. Commit the transaction
        // 5. Return the created work order
        //
        // Why is this pattern important?
        // - Both inserts succeed or both fail (atomicity)
        // - Events can't be lost even if publishing fails later
        // - The outbox publisher will handle getting events to SNS
        //
        // Requirements for the outbox event:
        // - event_type: 'WorkOrderCreated'
        // - aggregate_type: 'WorkOrder'
        // - aggregate_id: the work order's ID
        // - payload: JSONB containing all work order data
        // - published_at: should be NULL (not published yet)
        //
        // Error handling:
        // - If anything fails, rollback the transaction
        // - Make sure to close the database connection in finally block

        await client.query("BEGIN");
        const res = await client.query<WorkOrder>(
          `INSERT INTO work_orders (org_id, customer_id, title, description, address, scheduled_at)
           VALUES($1, $2, $3, $4, $5, $6) 
           RETURNING *`,
          [
            args.orgId,
            args.customerId,
            args.title,
            args.description,
            args.address,
            args.scheduledAt,
          ],
        );
        const createdWorkOrder = res.rows[0];

        await client.query<WorkOrder>(
          `INSERT INTO outbox (event_type, aggregate_id, aggregate_type, payload)
            VALUES ($1, $2, $3, $4)`,
          [
            "WorkOrderCreated",
            createdWorkOrder.id,
            "WorkOrder",
            JSON.stringify(createdWorkOrder),
          ],
        );
        await client.query("COMMIT");

        return {
          ...createdWorkOrder,
          createdAt: new Date(createdWorkOrder.created_at).toISOString(),
          updatedAt: new Date(createdWorkOrder.updated_at).toISOString(),
          orgId: createdWorkOrder.org_id,
          customerId: createdWorkOrder.customer_id,
        };
      } catch (creationError) {
        console.error("Error creating work order:", creationError);
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          console.error("Error rolling back transaction:", rollbackError);
        }
        throw creationError;
      } finally {
        await client.end();
      }
    },
  },
};

const server = new ApolloServer({
  typeDefs,
  resolvers,
});

export const handler = startServerAndCreateLambdaHandler(
  server,
  handlers.createAPIGatewayProxyEventV2RequestHandler(),
);
