# Event-Driven Work Order System

Built this to explore event-driven patterns for a BuildOps interview. Implements transactional outbox, idempotent processing, and distributed tracing across a serverless stack.

## Live Demo
**[See It In Action →](https://johnhamlin.github.io/event-driven-demo/)**

This repo includes a simple React UI on GitHub Pages. You can create work orders in Neon DB through the API lambda and display them from DynamoDB. Good enough to show the flow works end-to-end.


## Architecture

```
GraphQL API (Lambda)
→ Postgres (transactional outbox)
→ Publisher Lambda (EventBridge cron)
→ SNS
→ SQS
→ Projector Lambda
→ DynamoDB (read projections)
```

**Key patterns implemented:**
- **Transactional outbox** - atomic writes to domain data + event intent
- **At-least-once delivery** - with idempotent consumers
- **CQRS-lite** - write to Postgres, read from DynamoDB projections
- **Distributed tracing** - trace IDs flow through all services with structured logging

## Stack

- **API**: Apollo GraphQL on Lambda (Function URL)
- **Database**: Postgres (Neon) for writes, DynamoDB for reads
- **Events**: SNS/SQS for pub/sub
- **Infrastructure**: AWS CDK with TypeScript
- **Observability**: Custom structured logging, queryable in CloudWatch Insights

## Setup

1. **Install dependencies**
```bash
npm install
```

2. **Configure database**
```bash
cp .env.template .env
# Add your Postgres connection string
```

3. **Initialize database**
   Run `init.sql` against your Postgres instance (creates tables + seed data)

4. **Deploy to AWS**
```bash
cdk deploy
```

The API URL will be in the stack outputs.

## Design Decisions

**Why outbox pattern?** Guarantees we never lose events. If SNS is down or Lambda times out, events stay in outbox and get retried. Tradeoff: ~1min publishing latency.

**Why Postgres + DynamoDB?** Postgres gives us transactions for the write model. DynamoDB gives us fast queries optimized for specific access patterns (e.g., "all open work orders for org X"). Classic CQRS thinking.

**Why polling over CDC?** Simpler. EventBridge triggers Publisher every minute. For higher throughput, I'd switch to Debezium CDC with Kafka, but that's operational complexity this demo doesn't need.

**DynamoDB key design:** `pk: ORG#<orgId>`, `sk: STATUS#<status>#TS#<timestamp>#WO#<id>`. Single-partition queries for "all work orders by status for this org." Downside: status updates require delete + insert since status is in the sort key.

## Observability

Generated trace IDs at request entry point, propagated through event payloads. Every service logs structured JSON with standard fields (timestamp, level, service, requestId) plus contextual fields (traceId, workOrderId, etc.).

CloudWatch Insights query to follow a trace:
```
fields @timestamp, service, message, traceId, workOrderId
| filter traceId = "your-trace-id"
| sort @timestamp asc
```

## What I'd Add for Production

- **Monitoring**: CloudWatch alarms on DLQ depth, Lambda errors, processing latency
- **Testing**: Contract tests for event schemas, integration tests for the full flow
- **Error handling**: Exponential backoff, circuit breakers, better DLQ processing
- **Schema evolution**: Event versioning strategy, backward compatibility
- **Security**: API authentication, least-privilege IAM, encryption at rest
- **Performance**: Connection pooling (RDS Proxy), DynamoDB on-demand scaling

---

Built in a couple of evenings to prep for a job interview. Focused on event-driven architecture patterns. Let Claude Code whip up the demo site.
