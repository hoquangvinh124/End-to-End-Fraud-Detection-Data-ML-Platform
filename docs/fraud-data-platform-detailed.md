# Fraud Data Platform Detailed Architecture Guide

## 1. Purpose of This Document

This document expands the summary architecture into a detailed, system-level design for the fraud detection data platform.

It exists to answer the questions that the short architecture summary cannot answer clearly enough:

- What exactly is the source data?
- How can a flat historical fraud dataset become a realistic multi-source financial architecture?
- Where does CDC really happen?
- What do Bronze, Silver, and Gold mean in this project?
- What should Flink do, and what should Spark do?
- How should Feast interact with Gold and Redis?
- How do we avoid label leakage in a fraud project?
- How do we justify the 100GB scale target without making the design look fake?

This guide is intentionally long and explicit so it can serve as the architectural rationale for both implementation and presentation.

## 2. Main Problems in the Original Draft

The original architecture was directionally correct in terms of components, but several contracts were still unclear.

### 2.1 Source data was underspecified

The old draft said Kafka received data from a "CDC simulator Python producer". That phrasing mixed two different ingestion models:

- a true CDC model from OLTP tables through Debezium
- a synthetic event producer that writes directly to Kafka

Those are not the same thing. If the project claims CDC, it should clearly say what database tables exist and how row changes become Kafka events.

### 2.2 Bronze, Silver, and Gold were named but not defined

MinIO and a lakehouse layer were present, but the architecture did not explain:

- what lands in Bronze
- what transformations happen in Silver
- what Gold is actually used for

Without these definitions, the data lakehouse remains a storage box instead of a set of explicit contracts.

### 2.3 Feature store boundaries were blurry

The original document listed Feast with PostgreSQL offline store and Redis online store, but it did not explain:

- where historical features are built
- whether Gold feeds the feature store
- which system is the real source of truth for offline training features
- how online features are refreshed

### 2.4 Flink and Spark responsibilities overlapped conceptually

Flink was described as generic window aggregation, while Spark was described as generic batch feature engineering. That makes it unclear which engine owns:

- real-time stateful feature computation
- backfill and recomputation
- training dataset assembly
- alerting and rule evaluation

### 2.5 Fraud label timing was not modeled explicitly

In financial fraud, labels do not usually exist at the moment of transaction authorization. Many labels arrive later through chargebacks, analyst review, customer claims, or downstream investigations.

If that delay is not modeled, then the architecture risks accidental leakage and an unrealistic online path.

### 2.6 The 100GB claim needed operational meaning

Saying the platform will process 100GB is easy. The harder part is explaining whether the scale target tests:

- CDC throughput
- Kafka durability and lag
- Flink state growth
- Bronze Delta table performance
- batch recomputation time
- feature freshness under load

The improved architecture should explain that scale is not only about row count but also about behavior, cardinality, event disorder, and late labels.

## 3. Design Principles

This architecture is driven by the following principles.

### 3.1 Be honest about the real starting point

The project begins from a historical flat fraud dataset. That is fine.

The architecture should not pretend that a full banking core system already exists. Instead, it should explicitly say:

- the flat dataset is the **seed dataset**
- the project **normalizes** and **replays** it into simulated operational systems
- those systems are then treated as the realistic source systems for CDC and downstream processing

This is more defensible than claiming a fictional source landscape without explaining how it is produced.

### 3.2 Separate online and offline semantics

The online system should only use information that is available at prediction time.

The offline system can use delayed labels, point-in-time feature joins, and full historical context.

This separation is crucial in fraud detection because otherwise offline evaluation becomes unrealistically optimistic.

### 3.3 Treat each layer as a contract, not as a bucket

Bronze, Silver, Gold, Feast, Redis, and MLflow are useful only if each one has a clear purpose.

Each layer in this design therefore answers three questions:

- what enters the layer
- what transformations are allowed there
- what leaves the layer and who consumes it

### 3.4 Time is a first-class concern

Fraud systems are time-sensitive.

The architecture must explicitly consider:

- event time versus processing time
- late and out-of-order events
- delayed labels
- point-in-time correctness
- rolling windows and state TTLs

### 3.5 Keep the full stack because the rubric requires it, but assign minimal clear responsibilities

The architecture is intentionally broad because the project requirements ask for it.

That is acceptable as long as each component has one crisp job rather than overlapping with every other component.

## 4. End-to-End System Narrative

The platform works as follows.

1. A historical fraud dataset is used as the seed source.
2. Bootstrap logic normalizes that dataset into simulated OLTP source tables in PostgreSQL.
3. Replay and generation jobs continue creating realistic transaction traffic, dimension updates, and delayed fraud outcomes.
4. Debezium captures source table changes and publishes them into Kafka.
5. Kafka becomes the transport layer for raw CDC, replay streams, feature updates, and alerts.
6. Spark Structured Streaming consumes Kafka CDC topics and lightly unwraps Debezium events into flat Bronze rows plus `_`-prefixed CDC metadata.
7. Bronze lands in MinIO-backed Delta tables on a `5 minute` micro-batch cadence with checkpointed restartability.
8. Flink consumes Kafka streams and downstream curated data to compute near-real-time features and risk signals.
9. Spark also performs backfill, recomputation, historical feature engineering, and training dataset construction.
10. Feast exposes curated features for both offline retrieval and online serving.
11. Redis stores low-latency online features.
12. MLflow tracks experiments and registered models.
13. KServe and Triton serve the model using online features from Redis.
14. Prometheus, Loki, Jaeger, OTel Collector, and Grafana monitor the platform end to end.

## 5. Source Data Strategy

## 5.1 Real starting data

The current seed dataset contains transaction-level facts such as:

- `TRANSACTION_ID`
- `TX_DATETIME`
- `CUSTOMER_ID`
- `TERMINAL_ID`
- `TX_AMOUNT`
- `TX_TIME_SECONDS`
- `TX_TIME_DAYS`
- `TX_FRAUD`
- `TX_FRAUD_SCENARIO`

That is a strong starting point because it already includes:

- event timestamp
- customer identity
- terminal identity
- transaction amount
- delayed fraud outcome labels for offline study

What it does not contain natively is a realistic multi-table operational model. That is where normalization and simulation come in.

## 5.2 Converting the flat dataset into multiple source systems

The architecture should explicitly say that the seed dataset is transformed into simulated financial source systems.

Recommended logical source tables are the following.

| Source table | Role in the system | Derived from seed data | Change pattern |
| --- | --- | --- | --- |
| `customers` | Customer master and risk profile | Distinct `CUSTOMER_ID` plus synthetic enrichment | Slow updates |
| `terminals` | Terminal or merchant endpoint master | Distinct `TERMINAL_ID` plus synthetic enrichment | Slow updates |
| `accounts` or `cards` | Payment instrument and ownership relation | Synthetic table linked to customers | Slow updates |
| `transactions` | Authorization or payment events | Directly from transaction rows | High-volume inserts |
| `fraud_cases` or `chargebacks` | Delayed outcome labels | Derived from `TX_FRAUD` and `TX_FRAUD_SCENARIO` with delay simulation | Delayed inserts or updates |

This design is important because it makes the project look like a real financial platform rather than a notebook that happens to use Kafka.

## 5.3 Recommended enrichment categories

If more realism is needed, enrichment should be split into three categories.

### Derived from raw transaction facts

- hour of day
- day of week
- weekend flag
- night flag
- transaction gap features
- customer or terminal velocity counters

### Synthetic reference data

- customer segment
- region or country
- card product type
- terminal region
- merchant category
- channel type such as POS, ATM, e-commerce

### Delayed outcome data

- confirmed fraud result
- chargeback reason
- analyst review result
- review timestamp
- case severity

The architecture should call these out separately so it is obvious which fields are original, derived, or synthetic.

## 6. OLTP Simulation Design

## 6.1 Why an OLTP simulation is needed

CDC needs a source database. A custom producer that writes straight to Kafka can generate events, but it does not represent OLTP row-change capture.

If the architecture claims CDC, then a realistic source mutation layer is necessary.

## 6.2 Two operational phases

### Phase A: Bootstrap

The seed dataset is normalized into source tables.

Example bootstrap responsibilities:

- create customers from unique customer IDs
- create terminals from unique terminal IDs
- create account or card assignments for each customer
- load transactions into the transaction source table in chronological order
- convert fraud labels into delayed case records rather than immediate online labels

### Phase B: Streaming simulation

After bootstrap, generator or replay jobs continue mutating source tables.

These jobs simulate:

- new transactions arriving over time
- slowly changing customer or terminal attributes
- new fraud case confirmations arriving after the transaction time
- scale amplification to stress Kafka, Flink, and Delta table writes

## 6.3 What the simulator should actually vary

To make the 100GB objective meaningful, the simulator should vary more than row count.

It should vary:

- event rate by hour and day
- customer activity distribution
- terminal hot spots
- fraud campaigns or bursts
- late and out-of-order events
- label delay distributions
- dimension updates such as terminal status changes or customer risk segment changes

That creates a much stronger data engineering story than saying "we duplicated rows until Kafka became busy".

## 7. CDC Architecture with Debezium

## 7.1 Role of Debezium

Debezium captures table-level changes from the simulated OLTP database and emits them into Kafka.

This gives the platform a credible CDC layer with:

- snapshots
- inserts
- updates
- deletes
- source metadata such as table, operation, and event timestamp

## 7.2 Snapshot plus streaming model

The architecture should clearly state the following lifecycle.

### Snapshot stage

- Debezium reads initial state from source tables.
- Kafka receives a historical baseline.
- Bronze can be initialized from the same CDC stream rather than from side-loading files.

### Streaming stage

- replay or generation jobs mutate the OLTP tables
- Debezium emits incremental change records
- Kafka consumers see realistic source-table changes in near real time

## 7.3 CDC event semantics

Debezium emits row-change events with concepts like:

- `before`
- `after`
- operation type such as create, update, delete, snapshot read
- source metadata
- event timestamp

This matters because Silver processing must flatten and normalize those envelopes before downstream analytics or feature logic can use them safely.

## 7.4 Topic catalog

The short architecture should not stop at saying "Kafka topics". A more concrete topic model is recommended.

| Topic | Key | Payload type | Retention intent | Primary consumers |
| --- | --- | --- | --- | --- |
| `cdc.customers` | `customer_id` | Debezium CDC | compacted or long retention | Silver ETL, governance |
| `cdc.terminals` | `terminal_id` | Debezium CDC | compacted or long retention | Silver ETL, governance |
| `cdc.accounts` | `account_id` or `card_id` | Debezium CDC | compacted or long retention | Silver ETL |
| `cdc.transactions` | `transaction_id` | Debezium CDC | append-heavy | Flink, Bronze ingest |
| `cdc.chargebacks` | `case_id` or `transaction_id` | Debezium CDC | append-heavy | Silver labels, training |
| `silver.transactions_clean` | `transaction_id` | canonical transaction event | append-heavy | Gold builders, Trino |
| `feature_updates.online` | entity key | feature delta | short or medium retention | Feast push or Redis loader |
| `fraud_alerts` | alert key | rule or model alert | medium retention | monitoring, ops |
| `dlq.invalid_events` | source key | bad or malformed record | long retention | data quality remediation |

## 7.5 CDC versus business events

This distinction should be documented explicitly.

- CDC events say that a row changed.
- business events say that a business action happened.

If the project does not implement an outbox pattern, that is acceptable. In that case the architecture should say:

"The raw ingestion layer uses table-level CDC events. Silver processing transforms those events into canonical domain tables and domain-aligned analytical events."

That wording is precise and defensible.

## 8. Kafka Design Considerations

Kafka is more than transport here. It is the system boundary between operational data mutation and analytical or streaming consumers.

Important design points worth documenting:

- partition transactions by a stable key such as `customer_id` or `transaction_id`, depending on consumer needs
- keep dimension topics compacted when appropriate
- separate raw CDC topics from curated downstream topics
- use DLQ topics for schema or data quality failures
- track consumer lag and backlog as first-class operational metrics

For a fraud platform, Kafka is also where you can demonstrate the scale story:

- high write throughput
- multiple topic classes
- different retention strategies
- multiple consumer groups such as Flink, Bronze ingest, governance, and replay monitors

## 9. Bronze, Silver, and Gold Lakehouse Design

## 9.1 Why Delta on MinIO is appropriate for the current Bronze landing

Delta gives the current Bronze landing path:

- ACID table semantics
- append-oriented streaming sink behavior
- `_delta_log` transaction history
- schema evolution
- straightforward Spark Structured Streaming integration
- reproducible checkpointed writes on object storage

MinIO provides an S3-compatible local data lake, which fits the current Bronze sink design and keeps the local stack close to cloud object-storage patterns.

## 9.2 Bronze contract

Bronze should be the immutable audit trail and the first queryable CDC landing layer.

### Bronze contains

- flat business columns lightly unwrapped from the Debezium `after` or `before` payload
- `_`-prefixed CDC metadata such as `_op`, `_source_table`, `_source_ts_ms`, `_cdc_ts_ms`, `_snapshot`, `_lsn`, `_deleted`, and `_ingested_at`
- source identifiers and event timestamps kept close to the originating transaction schema

### Bronze does not contain

- the full nested Debezium envelope as the primary query surface
- heavy business transformation
- label maturity decisions
- aggressive deduplication logic that could remove the ability to reprocess

### Bronze purpose

- queryable CDC landing table
- audit trail
- replay anchor
- root cause analysis
- lineage anchor

## 9.3 Silver contract

Silver is the operationally usable canonical layer.

Recommended Silver responsibilities:

- enforce canonical business semantics over lightly unwrapped Bronze rows
- cast and standardize types
- normalize timestamps
- handle invalid rows and route bad records to quarantine
- deduplicate repeated transaction events
- apply time-aware joins to customer and terminal dimensions
- create canonical transaction events with consistent column names and semantics

Given the current seed dataset, Silver is also the right place to fix data typing problems such as:

- `TX_TIME_SECONDS` stored as object
- `TX_TIME_DAYS` stored as object

Those should become numeric types or be replaced by more canonical timestamp-derived fields.

## 9.4 Gold contract

Gold exists to serve specific consumption patterns.

Recommended Gold tables include:

| Gold table | Purpose |
| --- | --- |
| `gold.customer_window_features` | customer velocity and rolling statistics |
| `gold.terminal_window_features` | terminal activity and risk-related aggregates |
| `gold.training_dataset` | point-in-time aligned dataset for model training |
| `gold.fraud_monitoring_mart` | dashboards, fraud rate trends, operations reporting |
| `gold.feature_export_snapshot` | curated export set for Feast or materialization jobs |

Gold should not be described as generic "processed data". Each Gold table should correspond to a consumer or use case.

## 9.5 Partitioning and lifecycle guidance

For this project, a practical design is:

- partition Bronze and Silver by event date
- partition Gold marts by feature date or transaction date
- compact small files regularly
- maintain retention policies for raw replay data separately from curated Gold tables

## 10. Flink Responsibilities

## 10.1 What Flink should own

Flink should own **stateful online feature computation** and **stream-time fraud signals**.

That includes:

- event-time processing
- watermarks
- keyed state by `customer_id`, `terminal_id`, and optionally `account_id`
- rolling windows
- state TTL
- near-real-time output generation

## 10.2 Flink inputs

Primary inputs include:

- transaction CDC stream
- customer dimension updates
- terminal dimension updates
- optionally delayed label stream if certain risk aggregates depend on confirmed historical fraud outcomes

## 10.3 Flink outputs

Flink should produce outputs with explicit destinations.

Recommended outputs:

- enriched Silver transaction stream
- Gold customer rolling feature table updates
- Gold terminal rolling feature table updates
- `feature_updates.online` Kafka topic for Redis refresh
- `fraud_alerts` Kafka topic for rule-based suspicious patterns

## 10.4 Event-time semantics

Fraud systems should not rely only on processing time.

Flink should explicitly use:

- transaction event timestamps
- watermarking
- handling for late records
- rules for out-of-order arrival

This is especially important when replaying or amplifying historical data.

## 10.5 Candidate streaming features

The current notebook already suggests a good initial feature family.

Customer features:

- `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D`
- `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D`
- `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D`
- `CUSTOMER_AVG_AMOUNT_WINDOW_1D`
- `CUSTOMER_AVG_AMOUNT_WINDOW_7D`
- `CUSTOMER_AVG_AMOUNT_WINDOW_30D`

Terminal features:

- `TERMINAL_NB_TX_1DAY_WINDOW`
- `TERMINAL_NB_TX_7DAY_WINDOW`
- `TERMINAL_NB_TX_30DAY_WINDOW`
- `TERMINAL_RISK_1DAY_WINDOW`
- `TERMINAL_RISK_7DAY_WINDOW`
- `TERMINAL_RISK_30DAY_WINDOW`

Request-time features:

- `TX_AMOUNT`
- `IS_WEEKEND`
- `IS_NIGHT`

## 10.6 Important warning on terminal risk features

Terminal risk features are powerful but dangerous.

If they are based on fraud labels, they must only use labels that were confirmed **before** the transaction being scored.

That means:

- delayed label timing must be modeled explicitly
- point-in-time correctness must be enforced
- no current or future labels can leak into the feature state

If that is too complex in the first version, the safer online starting point is:

- terminal transaction count
- terminal average amount
- terminal velocity
- terminal anomaly score without direct confirmed fraud labels

Then confirmed-fraud-based risk can be added later once delayed-label semantics are implemented properly.

## 11. Spark Responsibilities

Spark should own the Bronze landing path and other historical or batch-heavy work.

Recommended Spark responsibilities:

- micro-batch ingestion from Kafka CDC topics into Bronze Delta tables on MinIO
- light unwrap of Debezium envelopes into flat business columns plus `_`-prefixed metadata
- checkpointed restartable Bronze landing on the default `5 minute` cadence
- historical backfill of feature tables
- full recomputation of Gold features from Silver
- point-in-time dataset construction for training
- large-scale joins with delayed labels
- periodic compaction or maintenance jobs if needed

Spark should not be the primary owner of low-latency online state. That remains Flink's job in this design, even though Spark owns the near-offline Bronze landing path.

## 12. Feature Store Design with Feast

## 12.1 Role of Feast in this architecture

Feast provides the contract between feature engineering and model consumption.

The architecture should position Feast as the layer that:

- exposes entities and feature views
- supports historical retrieval for offline training
- manages online feature serving through Redis

## 12.2 Source of truth for historical features

The cleanest conceptual design is:

- Gold feature tables are the system of record for curated historical features
- Feast definitions reference those curated sources for historical retrieval
- Redis stores only the subset needed for low-latency serving

This is better than treating Redis or raw CDC as the historical feature source.

## 12.3 Feast entities

Recommended initial entities:

- `customer_id`
- `terminal_id`
- `account_id` or `card_id` if added

These entities align directly with the fraud problem and with the current dataset structure.

## 12.4 Offline and online split

### Offline path

- Gold feature tables
- historical feature retrieval
- point-in-time dataset generation
- batch scoring or evaluation

### Online path

- Redis lookup
- low-latency response
- only request-safe features
- fresh feature updates from streaming or materialization jobs

## 12.5 Recommended materialization model

Two paths should coexist.

### Batch materialization

- Gold feature tables are materialized into Redis at scheduled intervals.
- Useful for slower-moving aggregates or daily refreshes.

### Streaming refresh

- Flink emits feature updates into a Kafka topic or a push pipeline.
- A small service or connector updates Redis for low-latency freshness.

This hybrid model matches the fraud use case well.

## 12.6 Example feature ownership

| Entity | Feature family | Typical freshness | Online use |
| --- | --- | --- | --- |
| `customer_id` | count and average amount windows | minutes to near real time | yes |
| `terminal_id` | transaction count and safe terminal behavior features | minutes to near real time | yes |
| `customer_id` | historical segment features | daily or slower | yes |
| `transaction_id` | delayed fraud label | not request-time safe | no |

## 13. Label Lifecycle and Leakage Prevention

This is one of the most important sections in the whole architecture.

## 13.1 Why fraud labels are delayed

At online scoring time, a system usually does not know whether the transaction is confirmed fraud.

Labels often arrive later through:

- chargebacks
- customer disputes
- analyst review
- downstream investigations

## 13.2 How to represent labels in this project

The architecture should treat:

- `TX_FRAUD` as an **offline ground truth label**
- `TX_FRAUD_SCENARIO` as a **scenario or analysis label**, not an online feature

During source simulation, those labels should be turned into delayed case events instead of appearing in the transaction payload used by serving.

## 13.3 Maturity windows

Training datasets should use a maturity rule such as:

- use only transactions whose labels are considered final after `T+7`
- or use a more conservative `T+30` depending on the scenario being simulated

This avoids training on labels that would not have been known yet.

## 13.4 Point-in-time correctness

For every training row, features must be built using only information that existed before the transaction time.

That applies to:

- customer velocity features
- terminal activity features
- risk features based on confirmed fraud history
- dimension joins such as customer segment or terminal status

## 13.5 Specific leakage risks in this project

The main leakage risks are:

- using `TX_FRAUD` in online-serving-like features
- computing terminal risk with future labels
- joining dimensions using the latest value instead of the value valid at transaction time
- building training data from already-precomputed features that accidentally used the full dataset window

The detailed architecture should acknowledge those risks explicitly because that shows maturity in a fraud use case.

## 14. Training, Evaluation, and Model Registry

## 14.1 Training data path

Recommended training path:

1. Bronze and Silver are built from CDC and replay streams.
2. Gold feature tables are recomputed or maintained.
3. Labels are joined only after maturity rules are satisfied.
4. Historical feature retrieval creates point-in-time correct training datasets.
5. Model training happens on time-based splits.
6. Metrics and artifacts are logged into MLflow.
7. Promoted models are exported to ONNX and prepared for Triton.

## 14.2 Evaluation style

Time-based validation is the correct choice here.

Recommended metrics for fraud:

- PR-AUC
- ROC-AUC
- Precision at top K
- recall at operational thresholds
- calibration review if needed

Your notebook already uses time-based splits and top-K style evaluation, which fits the fraud context well.

## 14.3 MLflow role

MLflow should track:

- experiment parameters
- training metrics
- feature versions
- model artifacts
- promotion status

The architecture should mention that model lineage matters because the platform has multiple feature-generation paths.

## 15. Serving Path: Target State Versus Current Repo State

## 15.1 Target serving contract

The target architecture should behave like this.

The request contains:

- transaction identifiers
- entity identifiers such as `customer_id` and `terminal_id`
- request-time attributes like amount and timestamp
- possibly channel or merchant context if enriched later

Then:

1. Traefik receives the request at the GKE edge.
2. Traefik routes the request to the KServe InferenceService.
3. Transformer fetches online features from Redis via Feast.
4. Request-time features are combined with online state.
5. Triton runs the model.
6. Prediction event is emitted for monitoring and feedback.

## 15.2 Current repository status

The current API still accepts a full precomputed feature vector.

That is acceptable as a temporary implementation state, but the architecture should explicitly call it out as transitional rather than as the final serving contract.
FastAPI remains the current local prototype API, not the target GKE entrypoint.

## 15.3 Why this transition matters

If the architecture does not say this clearly, a reviewer may reasonably ask:

"Why do you need Feast and Redis if the client already sends all model features?"

The improved architecture answers that by distinguishing:

- current prototype serving path
- target production-like serving path
- Traefik edge routing versus in-cluster inference serving

## 16. Data Quality, Governance, and Security

## 16.1 Data quality controls

The platform should define quality checks at each layer.

### Bronze

- ingestion completeness
- schema presence
- topic-to-table traceability

### Silver

- type validation
- null checks
- duplicate detection
- timestamp sanity checks
- invalid-event quarantine

### Gold

- feature freshness
- aggregate sanity bounds
- label coverage
- training-data completeness

## 16.2 Governance and metadata

Governance should stay lightweight in this version:

- document dataset and feature ownership in the repo
- keep lineage visible through source tables, dbt models, and Feast definitions
- treat schema and feature contracts as code, not as a separate platform requirement

This keeps metadata honest without adding a dedicated catalog service before the core pipeline is stable.

## 16.3 PII and sensitive financial data

Even in a simulated academic system, it is worth documenting these controls:

- mask or tokenize customer-identifying attributes
- separate analytical IDs from raw identifiers where possible
- audit raw-to-curated lineage
- keep full raw CDC payload retention limited to Kafka or restricted archival surfaces where necessary

This makes the design look more like a real financial platform.

## 17. Observability for the Data Platform

The current observability stack already covers API monitoring. The detailed design should extend that to the full platform.

Recommended platform metrics and signals:

- Kafka topic throughput
- Kafka consumer lag
- Debezium connector health
- Flink checkpoint success and duration
- Flink watermark lag
- late-event rate
- Bronze Delta write latency
- small-file growth and compaction backlog
- Spark job duration
- feature freshness in Redis
- training-serving skew indicators
- label delay distribution

These signals make the observability section much stronger than only exposing `/metrics` from the API.

## 18. 100GB Scale Strategy

## 18.1 What 100GB should mean here

The scale target should be explained as a test of platform behavior, not just storage size.

It should validate:

- Kafka ingestion under replay pressure
- Flink state growth and checkpoint stability
- Bronze Delta write behavior
- Spark recomputation time
- feature materialization freshness
- Trino query behavior over curated tables

## 18.2 How to reach the scale target credibly

Practical methods include:

- replay the seed dataset repeatedly with shifted timestamps
- increase customer and terminal cardinality synthetically
- vary traffic volume by hour and day
- simulate fraud bursts and terminal hot spots
- inject out-of-order arrivals and delayed labels

This creates a much more meaningful 100GB environment than straightforward duplication.

## 18.3 What not to do

Avoid describing the scale plan as only:

- "run Kafka for a few hours"
- "duplicate rows until the folder is large"

Those may produce data volume, but they do not demonstrate the architectural challenges that streaming fraud platforms actually face.

## 19. Component-by-Component Responsibility Summary

This section is useful when presenting or defending the architecture.

| Component | Primary responsibility |
| --- | --- |
| PostgreSQL source layer | simulated OLTP source systems |
| Debezium | row-level CDC from OLTP to Kafka |
| Kafka | transport boundary and event distribution |
| Flink | real-time stateful feature computation and fraud signals |
| Spark | Bronze landing, backfill, recomputation, point-in-time dataset assembly |
| MinIO | object storage for the lakehouse |
| Delta Lake | Bronze table format and current contract layer |
| Trino | SQL access to curated lakehouse data |
| Airflow | orchestration of data and model workflows |
| Feast | feature contract across offline and online contexts |
| Redis | low-latency online feature serving |
| MLflow | experiment tracking and model registry |
| Traefik | GKE ingress / edge routing for public traffic |
| KServe | inference routing and serving abstraction |
| Triton | low-latency model inference runtime |
| Knative Eventing | prediction event fan-out |
| Prometheus, Loki, Jaeger, OTel, Grafana | observability |

## 20. Recommended Improvements to Keep in Mind

These are the main architecture improvements worth preserving in the docs and later implementation.

### 20.1 Make the source story explicit

Do not say only "CDC simulator".

Say:

- seed dataset bootstraps a simulated OLTP layer
- Debezium captures true row-level CDC from that OLTP layer
- replay or generators continue producing live mutations

### 20.2 Treat Gold as the curated historical feature source

Even if local implementation details evolve, the architecture should make Gold the conceptual source of truth for historical features.

### 20.3 Document delayed labels everywhere

This is one of the strongest domain-specific differentiators for a fraud project.

### 20.4 Clarify that Flink owns low-latency state, Spark owns historical recomputation

This removes most ambiguity in the pipeline design.

### 20.5 Keep the target serving path separate from the current prototype API

This protects the architecture from criticism while still allowing the repository to evolve incrementally.

## 21. Risks and Tradeoffs

No architecture is free of tradeoffs. This one also has some.

### 21.1 High stack complexity

Because the rubric requires many subsystems, the architecture can look large for a solo project.

The best mitigation is not removing components, but giving each one a minimal clear job.

### 21.2 Synthetic multi-source realism

The sources are simulated, not real bank systems.

That is acceptable as long as the docs are explicit about:

- what is original data
- what is synthetic enrichment
- what is operational simulation

### 21.3 Label-based terminal risk is easy to get wrong

If delayed-label timing is not implemented carefully, leakage can invalidate the evaluation story.

### 21.4 Governance tooling can become decorative

If governance tooling is listed without a concrete operating model, it may look like architecture inflation. Keep governance lightweight until the repo has a real operational need for more structure.

## 22. Recommended Implementation Order

Even though this document is architectural, a practical order helps anchor the design.

### Phase 1: Source simulation and CDC foundation

This is the first thing to do.

- normalize seed dataset into source tables
- stand up PostgreSQL source schemas
- connect Debezium to Kafka
- validate snapshot plus streaming CDC

### Phase 2: Bronze and Silver correctness

- land lightly unwrapped CDC into Bronze Delta tables with Spark Structured Streaming
- create Silver canonical transaction and dimension tables
- add data quality, checkpointing, and quarantine logic

### Phase 3: Feature serving alignment

- compute customer and terminal rolling features
- emit feature update topics
- persist enriched outputs into Gold
- materialize online features into Redis
- expose KServe behind Traefik as the target GKE path
- keep the current FastAPI prototype separate from the target GKE serving path

## 23. Final Positioning Statement

If this architecture needs to be summarized in one paragraph during review, the strongest version is this:

This project starts from a historical flat fraud dataset but upgrades it into a realistic financial data platform by normalizing it into simulated OLTP source systems, capturing true row-level CDC with Debezium, landing lightly unwrapped Kafka CDC into Delta Bronze tables on MinIO through Spark Structured Streaming, and then exposing curated features through Flink, Spark, and Feast for both offline training and online inference. The architecture explicitly models delayed fraud labels, Bronze-Silver-Gold contracts, point-in-time correctness, and the distinction between the current prototype API and the target Traefik → KServe → Triton serving path.

That statement is concise, honest, and technically defensible.
