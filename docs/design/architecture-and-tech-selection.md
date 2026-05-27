# Paimon REST Catalog Architecture And Tech Selection

## Goal

Build a production-oriented REST Catalog Server compatible with Apache Paimon REST Catalog protocol. Flink, Spark, and Java clients should be able to use it through Paimon's official REST Catalog client.

The service is not a data proxy. It only handles catalog metadata operations, authentication, authorization, auditing, and operational governance. Table data, schema files, snapshots, manifests, and data files remain in the Paimon warehouse.

## Scope

### In Scope

- Implement Paimon official REST Catalog API shape under `/v1/**`.
- Use Paimon official `JdbcCatalog` as the backend catalog implementation.
- Use MySQL as the JDBC catalog metadata store and service governance store.
- Deploy as stateless Spring Boot service on Kubernetes with two or more pods.
- Provide authentication, authorization, audit logging, metrics, health checks, and deployment manifests.
- Add optional admin APIs under `/admin/**` for operational management.

### Out Of Scope For MVP

- Proxying table data reads or writes through this service.
- Storing Paimon table schema as the source of truth in MySQL.
- Reimplementing Paimon catalog metadata rules manually.
- Replacing Paimon snapshot, schema, manifest, or compaction logic.
- Full UI console.

## Package Name

Use:

```text
com.paimon.restcatalog
```

Do not use `org.apache.paimon.*`, because that would imply official Apache Paimon ownership.

## High-Level Architecture

```text
Flink / Spark / Java Client / Internal Backend
        |
        | Paimon REST Catalog API
        v
Kubernetes Service / Ingress
        |
        v
+-------------------------------+
| paimon-rest-catalog            |
| Spring Boot                    |
|                               |
| - REST API controllers         |
| - auth and permission checks   |
| - audit logging                |
| - request/response mapping     |
| - Paimon Catalog delegation    |
+-------------------------------+
        |
        | Paimon Catalog API
        v
Apache Paimon JdbcCatalog
        |
        v
MySQL / RDS

Paimon warehouse:
OSS / S3 / HDFS
  - schema/
  - snapshot/
  - manifest/
  - data/
  - index/
```

## Core Principle

The REST service must call Paimon official Catalog APIs. It must not directly mutate Paimon catalog tables or warehouse files.

Correct:

```java
catalog.createTable(identifier, schema, false);
catalog.dropTable(identifier, false);
catalog.alterTable(identifier, schemaChanges, false);
```

Incorrect:

```sql
DELETE FROM paimon_tables WHERE table_name = 't1';
```

## Technology Selection

| Area | Choice | Reason |
| --- | --- | --- |
| Language | Java 17 | Good Spring Boot 3 support and long-term runtime baseline. |
| Framework | Spring Boot 3.x | REST, security, configuration, actuator, and K8s support are mature. |
| Catalog backend | Apache Paimon `JdbcCatalog` | Official implementation; avoids reimplementing Paimon metadata semantics. |
| Catalog DB | MySQL 8.x / RDS | Stores JDBC catalog metadata, locks, audit, permissions. |
| Connection pool | HikariCP | Default and reliable Spring Boot pool. |
| DB migration | Flyway | Versioned governance schema migrations. |
| Auth | Spring Security + Bearer/JWT | Fits REST clients and internal service-to-service calls. |
| Metrics | Micrometer + Prometheus | Works well in K8s. |
| Logging | Logback JSON logs | Easier log collection and trace correlation. |
| API docs | OpenAPI / springdoc | Documents admin APIs; official Paimon REST API follows Paimon OpenAPI. |
| Deployment | Kubernetes Deployment | Stateless service with horizontal replicas. |

## Runtime Topology

```text
replicas: 2

Pod A                         Pod B
  |                             |
  | JdbcCatalog                 | JdbcCatalog
  | HikariCP                    | HikariCP
  +-------------+---------------+
                |
                v
             MySQL / RDS
                |
                v
          Paimon warehouse
```

The pods are stateless. Shared state lives in MySQL and the warehouse.

## MySQL Responsibilities

### Paimon JdbcCatalog Tables

Owned by Apache Paimon. The service must not manually update these tables.

```text
paimon_tables
paimon_database_properties
paimon_distributed_lock
```

### Service Governance Tables

Owned by this service and managed with Flyway.

```text
catalog_audit_log
catalog_access_token
catalog_permission
catalog_tenant
catalog_operation_guard
```

The governance tables can store audit records, permissions, token metadata, tenant mappings, and delete-protection policies. They must not become the source of truth for Paimon table schema.

## Paimon Metadata Responsibilities

| Metadata | Source Of Truth |
| --- | --- |
| Database exists | JdbcCatalog / MySQL |
| Table exists | JdbcCatalog / MySQL |
| Table location | JdbcCatalog + warehouse convention |
| Table schema versions | Paimon table directory `schema/schema-N` |
| Snapshots | Paimon table directory `snapshot/` |
| Manifests | Paimon table directory `manifest/` |
| Data files | Paimon table directory `data/` or external paths |
| Operation audit | Service governance DB |

## Project Structure

```text
paimon-rest-catalog
├── pom.xml
├── docs
│   └── design
│       └── architecture-and-tech-selection.md
├── src/main/java/com/paimon/restcatalog
│   ├── PaimonRestCatalogApplication.java
│   ├── config
│   │   ├── PaimonCatalogConfig.java
│   │   ├── SecurityConfig.java
│   │   ├── WebConfig.java
│   │   └── ActuatorConfig.java
│   ├── rest
│   │   ├── ConfigController.java
│   │   ├── DatabaseController.java
│   │   ├── TableController.java
│   │   ├── SnapshotController.java
│   │   ├── PartitionController.java
│   │   ├── BranchController.java
│   │   └── FunctionController.java
│   ├── service
│   │   ├── CatalogService.java
│   │   ├── DatabaseService.java
│   │   ├── TableService.java
│   │   ├── SnapshotService.java
│   │   ├── PartitionService.java
│   │   └── PermissionService.java
│   ├── paimon
│   │   ├── CatalogProvider.java
│   │   ├── CatalogOptionsFactory.java
│   │   ├── PaimonRequestMapper.java
│   │   └── PaimonResponseMapper.java
│   ├── security
│   │   ├── JwtAuthenticationFilter.java
│   │   ├── TokenService.java
│   │   ├── PrincipalContext.java
│   │   └── PermissionEvaluator.java
│   ├── audit
│   │   ├── AuditAspect.java
│   │   ├── AuditService.java
│   │   ├── AuditLogEntity.java
│   │   └── AuditLogRepository.java
│   ├── admin
│   │   ├── AdminTableController.java
│   │   ├── AdminAuditController.java
│   │   ├── AdminTokenController.java
│   │   └── AdminPermissionController.java
│   └── common
│       ├── ApiException.java
│       ├── ErrorHandler.java
│       ├── RequestIdFilter.java
│       └── PageTokenCodec.java
├── src/main/resources
│   ├── application.yml
│   └── db/migration
│       ├── V1__create_audit_log.sql
│       ├── V2__create_access_token.sql
│       ├── V3__create_permission.sql
│       ├── V4__create_tenant.sql
│       └── V5__create_operation_guard.sql
└── deploy
    ├── deployment.yaml
    ├── service.yaml
    ├── configmap.yaml
    ├── secret.yaml
    └── servicemonitor.yaml
```

## Component Responsibilities

### `rest`

Implements HTTP endpoints matching Paimon REST Catalog OpenAPI. Controllers should be thin: validate path/query/body, call services, and return response DTOs.

### `service`

Owns application use cases: list databases, create table, alter table, drop table, list snapshots, check permissions, and write audit outcomes.

### `paimon`

Creates and exposes Paimon `Catalog` instances. Encapsulates Paimon-specific request/response conversion so controllers do not depend deeply on Paimon internals.

### `security`

Authenticates Bearer/JWT tokens and builds a request principal containing user, tenant, roles, and allowed operations.

### `audit`

Records user, operation, target, request id, status, duration, pod name, and error message.

### `admin`

Provides non-Paimon management APIs for operators, such as audit query, token management, permissions, and table deletion protection.

### `common`

Shared error handling, request id propagation, page token handling, and API exceptions.

## API Design

### Official Paimon REST API

Use `/v1/**` for Paimon-compatible endpoints.

MVP endpoints:

```text
GET    /v1/config
GET    /v1/{prefix}/databases
POST   /v1/{prefix}/databases
GET    /v1/{prefix}/databases/{database}
POST   /v1/{prefix}/databases/{database}
DELETE /v1/{prefix}/databases/{database}
GET    /v1/{prefix}/databases/{database}/tables
POST   /v1/{prefix}/databases/{database}/tables
GET    /v1/{prefix}/databases/{database}/tables/{table}
POST   /v1/{prefix}/databases/{database}/tables/{table}
DELETE /v1/{prefix}/databases/{database}/tables/{table}
POST   /v1/{prefix}/databases/{database}/tables/{table}/rename
```

Second-stage endpoints:

```text
GET    /v1/{prefix}/databases/{database}/tables/{table}/snapshots
GET    /v1/{prefix}/databases/{database}/tables/{table}/partitions
GET    /v1/{prefix}/databases/{database}/tables/{table}/branches
POST   /v1/{prefix}/databases/{database}/tables/{table}/commit
POST   /v1/{prefix}/databases/{database}/tables/{table}/rollback
```

### Admin API

Use `/admin/**` for service governance APIs. These are not part of Paimon REST Catalog protocol.

```text
GET    /admin/audit-logs
GET    /admin/permissions
POST   /admin/permissions
GET    /admin/tokens
POST   /admin/tokens
POST   /admin/tables/{database}/{table}/lock-delete
DELETE /admin/tables/{database}/{table}/lock-delete
```

## Key Flows

### Create Table

```text
POST /v1/{prefix}/databases/{database}/tables
  -> authenticate token
  -> authorize CREATE_TABLE
  -> map request to Paimon Schema
  -> catalog.createTable(identifier, schema, ignoreIfExists)
  -> JdbcCatalog writes schema-0 to warehouse
  -> JdbcCatalog inserts table registration into MySQL
  -> write audit log
  -> return response
```

### Drop Table

```text
DELETE /v1/{prefix}/databases/{database}/tables/{table}
  -> authenticate token
  -> authorize DROP_TABLE
  -> check delete guard
  -> catalog.dropTable(identifier, ignoreIfNotExists)
  -> JdbcCatalog deletes MySQL table registration
  -> JdbcCatalog deletes warehouse table directory
  -> write audit log
  -> return response
```

If warehouse deletion fails after MySQL deletion, files may remain as orphan files. The service should record failure details and expose cleanup procedures.

### Alter Table

```text
POST /v1/{prefix}/databases/{database}/tables/{table}
  -> authenticate token
  -> authorize ALTER_TABLE
  -> map request to List<SchemaChange>
  -> catalog.alterTable(identifier, schemaChanges, ignoreIfNotExists)
  -> JdbcCatalog verifies table exists in MySQL
  -> JdbcCatalogLock serializes concurrent alter operations
  -> SchemaManager writes new schema-N file to warehouse
  -> write audit log
  -> return response
```

MySQL does not store the alter SQL or schema versions unless the audit module records the request body.

### Get Table Schema

```text
GET /v1/{prefix}/databases/{database}/tables/{table}
  -> authenticate token
  -> authorize GET_TABLE
  -> JdbcCatalog checks table registration in MySQL
  -> Paimon reads latest schema from warehouse schema/schema-N
  -> return table metadata response
```

## Configuration

Example `application.yml` shape:

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:mysql://mysql:3306/paimon_catalog
    username: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 2
  flyway:
    enabled: true

paimon:
  rest:
    prefix: prod
  catalog:
    type: jdbc
    warehouse: s3://bucket/paimon-warehouse
    uri: jdbc:mysql://mysql:3306/paimon_catalog
    lock-enabled: true
  auth:
    issuer: paimon-rest-catalog
    jwt-secret: ${JWT_SECRET}

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

Exact Paimon option keys must be verified against the chosen Paimon version during implementation.

## Kubernetes Design

Deployment:

```text
replicas: 2
readinessProbe: /actuator/health/readiness
livenessProbe: /actuator/health/liveness
rollingUpdate: maxUnavailable=0, maxSurge=1
```

Configuration:

```text
ConfigMap: warehouse, prefix, log level, non-secret catalog options
Secret: MySQL password, JWT secret, object storage credentials
```

Operational requirements:

- Pods must be stateless.
- Multiple pods must connect to the same MySQL and warehouse.
- Do not use local disk for catalog state.
- Enable graceful shutdown.
- Use request id in logs and audit rows.

## Caching Strategy

MVP:

- Cache Paimon `Catalog` instance per configured prefix or tenant.
- Cache parsed tokens with short TTL.
- Do not cache table schema, listTables, or table existence initially.

Later:

- Add short TTL read cache for list operations if metrics prove it is needed.
- Add explicit cache invalidation after create/drop/alter/rename.
- Consider Redis only if multiple pods need coordinated cache invalidation.

## Error Handling

Map internal exceptions to Paimon-compatible REST error responses.

Examples:

| Condition | HTTP Status |
| --- | --- |
| Invalid request | 400 |
| Authentication failed | 401 |
| Permission denied | 403 |
| Database or table not found | 404 |
| Already exists | 409 |
| Unsupported operation | 501 |
| Unexpected failure | 500 |

All error responses should include request id and a stable error code.

## Observability

Metrics:

```text
http.server.requests
catalog.operation.duration
catalog.operation.count
catalog.operation.failures
catalog.audit.write.failures
paimon.jdbc.pool.active
paimon.jdbc.pool.pending
```

Logs:

- JSON format.
- Include request id, operator, tenant, operation, database, table, status, duration.
- Do not log secrets or full credentials.

Audit:

```text
id
request_id
operator
tenant
operation
database_name
table_name
request_body
status
error_code
error_message
duration_ms
pod_name
created_at
```

## Testing Strategy

Unit tests:

- Request/response mapping.
- Permission evaluation.
- Error mapping.
- Audit record generation.

Integration tests:

- Spring Boot controller tests.
- Paimon `JdbcCatalog` against Testcontainers MySQL.
- Local filesystem warehouse for fast tests.
- Create/list/get/alter/drop table round trip.

Compatibility tests:

- Use Paimon `RESTCatalog` client to call the server.
- Verify endpoint shape against Paimon OpenAPI.
- Run selected Flink/Spark catalog operations if dependencies are available.

K8s tests:

- Two service instances against the same MySQL.
- Concurrent alter table requests.
- Restart one pod during catalog operations.

## Risks And Mitigations

| Risk | Mitigation |
| --- | --- |
| Paimon REST API changes across versions | Pin Paimon version and add compatibility tests. |
| MySQL metadata and warehouse files become inconsistent after failure | Use Paimon APIs only, record failures, provide orphan cleanup procedure. |
| Concurrent alter conflicts | Enable JdbcCatalog lock and test multi-pod concurrency. |
| Over-caching stale metadata | Avoid table metadata cache in MVP. |
| Client/server Paimon version mismatch | Publish supported client version matrix. |
| Direct MySQL mutation by operators | Restrict DB access and document that Paimon catalog tables are not manually edited. |

## Delivery Phases

### Phase 1: MVP Protocol Server

- Spring Boot skeleton.
- `JdbcCatalog` creation.
- `/v1/config`.
- Database CRUD.
- Table create/list/get/drop/alter/rename.
- JWT auth.
- Audit logging.
- MySQL Flyway migrations for service tables.
- Dockerfile and K8s manifests.

### Phase 2: Read Metadata Coverage

- Snapshots.
- Partitions.
- Branches.
- Functions if needed by clients.
- Metrics and dashboards.
- Compatibility tests using Paimon REST client.

### Phase 3: Production Governance

- Multi-tenant prefix-to-catalog mapping.
- Delete protection.
- Token management APIs.
- Permission admin APIs.
- Cache tuning.
- Orphan cleanup integration.

## Design Decisions

1. The server is official REST Catalog protocol first, admin API second.
2. The backend catalog is Paimon official `JdbcCatalog`.
3. MySQL stores catalog registration, locks, audit, permissions, and governance data.
4. Paimon table schema remains in the warehouse table directory.
5. Kubernetes pods are stateless and horizontally scalable.
6. The MVP avoids table metadata caching to prevent stale reads across pods.
