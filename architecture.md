# Feast Flink DSL Implementation - Architecture Diagrams

This document contains comprehensive Mermaid diagrams that capture the complete architecture, phases, and component interconnections from the Feast Flink DSL Implementation plan.

## Complete System Architecture

```mermaid
graph TB
    subgraph "Feature Definition Layer"
        FD[Ibis Feature Definitions<br/>Single Source of Truth]
        BC[Bootstrap Config<br/>Large Windows 60+ days]
        FD --> BC
    end

    subgraph "Data Sources"
        KAFKA[Kafka Streams<br/>Real-time Events]
        BQ_RAW[BigQuery<br/>Raw Events]
        BOOTSTRAP[Bootstrap Tables<br/>Pre-aggregated 60d]
    end

    subgraph "Feast Core Components"
        IDS[IbisDataSource<br/>cloudpickle serialization]
        IFOS[IbisFlinkOfflineStore<br/>OfflineStore SPI]
        FV[FeatureView<br/>Registry]
        FS[FeatureStore API<br/>get_historical_features<br/>get_online_features]
    end

    subgraph "Compilation Layer"
        IC[Ibis Compiler<br/>Multi-dialect]
        FLINK_SQL[Flink SQL<br/>Streaming + Batch]
        BQ_SQL[BigQuery SQL<br/>Historical]
        DUCK_SQL[DuckDB SQL<br/>Testing]
    end

    subgraph "Execution Engines"
        subgraph "Path 1: Real-time"
            FLINK_STREAM[Flink Streaming<br/>Sub-second latency<br/>7-day state]
        end

        subgraph "Path 2: Batch"
            FCE[FlinkComputeEngine<br/>Materialization]
            FLINK_BATCH[Flink Batch Jobs<br/>Bootstrap + Rollups]
        end

        subgraph "Path 3: Historical"
            BQ_ENGINE[BigQuery Engine<br/>10-50% faster<br/>Columnar storage]
        end
    end

    subgraph "Storage Layer"
        BIGTABLE[Bigtable<br/>Online Store<br/>Low-latency serving]
        BQ_MAT[BigQuery<br/>Materialized Features<br/>batch_source]
        BQ_HIST[BigQuery<br/>Historical Training Data]
    end

    subgraph "Orchestration"
        AIRFLOW[Airflow DAGs<br/>Auto-generated]
        ROLLUP[Daily Rollup Jobs]
        MONITOR[Monitoring<br/>5-min checks]
        CONSIST[Consistency Checks<br/>Hourly]
        FRESH[Freshness SLI/SLO<br/>P99 < 200ms]
    end

    subgraph "Monitoring & Discovery"
        CATALOG[Feature Catalog<br/>Web UI + CLI]
        DOCS[Auto-generated Docs<br/>Markdown + HTML]
        QUALITY[Quality Metrics<br/>Drift Detection]
    end

    %% Feature Definition Flow
    FD --> IDS
    IDS --> FV
    FV --> IFOS
    IFOS --> FS

    %% Compilation Paths
    FD --> IC
    IC --> FLINK_SQL
    IC --> BQ_SQL
    IC --> DUCK_SQL

    %% Path 1: Streaming
    KAFKA --> FLINK_STREAM
    BOOTSTRAP --> FLINK_STREAM
    FLINK_SQL --> FLINK_STREAM
    FLINK_STREAM --> BIGTABLE

    %% Path 2: Batch Materialization
    BQ_RAW --> FCE
    FLINK_SQL --> FLINK_BATCH
    FCE --> FLINK_BATCH
    FLINK_BATCH --> BIGTABLE
    FLINK_BATCH --> BQ_MAT
    FLINK_BATCH --> BOOTSTRAP

    %% Path 3: Historical Retrieval
    BQ_RAW --> BQ_ENGINE
    BQ_SQL --> BQ_ENGINE
    BQ_ENGINE --> BQ_HIST
    BQ_HIST --> FS

    %% Alternative Historical Path
    BQ_MAT --> FS

    %% Orchestration Connections
    AIRFLOW --> ROLLUP
    AIRFLOW --> MONITOR
    AIRFLOW --> CONSIST
    ROLLUP --> BOOTSTRAP
    MONITOR --> FLINK_STREAM
    CONSIST --> BIGTABLE
    FRESH --> MONITOR

    %% Discovery & Monitoring
    FV --> CATALOG
    FV --> DOCS
    FLINK_STREAM --> QUALITY
    QUALITY --> FRESH

    %% User Interactions
    FS --> BIGTABLE
    FS --> BQ_HIST

    classDef definition fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef compilation fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef execution fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef storage fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef orchestration fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    classDef monitoring fill:#fce4ec,stroke:#880e4f,stroke-width:2px

    class FD,BC,IDS definition
    class IC,FLINK_SQL,BQ_SQL,DUCK_SQL compilation
    class FLINK_STREAM,FCE,FLINK_BATCH,BQ_ENGINE execution
    class BIGTABLE,BQ_MAT,BQ_HIST,BOOTSTRAP storage
    class AIRFLOW,ROLLUP,MONITOR,CONSIST,FRESH orchestration
    class CATALOG,DOCS,QUALITY monitoring
```

## Implementation Phase Dependencies

```mermaid
graph LR
    P0[Phase 0<br/>Connector Validation<br/>6-8 weeks<br/>🟢 Low Risk]
    P1[Phase 1<br/>Ibis Validation<br/>2 weeks<br/>🟢 Low Risk]
    P2[Phase 2<br/>IbisFlinkOfflineStore<br/>4 weeks<br/>🟢 Low Risk]
    P3[Phase 3<br/>Multi-Backend Testing<br/>2 weeks<br/>🟢 Low Risk<br/>OPTIONAL]
    P4[Phase 4<br/>Feature Discovery<br/>+ Freshness SLI/SLO<br/>2-3 weeks<br/>🟢 Low Risk]
    P5[Phase 5<br/>Performance Validation<br/>3-4 weeks<br/>🟡 Medium Risk]
    P6[Phase 6<br/>Pilot Production<br/>4-6 weeks<br/>🟡 Medium Risk]
    P7[Phase 7<br/>Bootstrap Pattern<br/>4-6 weeks<br/>🟡 Medium Risk]
    P8[Phase 8<br/>Airflow Orchestration<br/>2-3 weeks<br/>🟢 Low Risk]
    P9[Phase 9<br/>Full Scale-Out<br/>6-10 weeks<br/>🔴 High Risk]

    P0 --> P1
    P1 --> P2
    P2 --> P3
    P2 --> P4
    P2 --> P5
    P3 --> P6
    P4 --> P6
    P5 --> P6
    P6 --> P7
    P7 --> P8
    P8 --> P9

    style P0 fill:#c8e6c9,stroke:#2e7d32
    style P1 fill:#c8e6c9,stroke:#2e7d32
    style P2 fill:#c8e6c9,stroke:#2e7d32
    style P3 fill:#c8e6c9,stroke:#2e7d32,stroke-dasharray: 5 5
    style P4 fill:#c8e6c9,stroke:#2e7d32
    style P5 fill:#fff9c4,stroke:#f57f17
    style P6 fill:#fff9c4,stroke:#f57f17
    style P7 fill:#fff9c4,stroke:#f57f17
    style P8 fill:#c8e6c9,stroke:#2e7d32
    style P9 fill:#ffccbc,stroke:#bf360c
```

**Legend:**
- 🟢 Green (Low Risk): Foundational work, no production impact
- 🟡 Yellow (Medium Risk): Some risk but mitigated
- 🔴 Red (High Risk): Significant production deployment
- Dashed border: Optional phase (can be skipped)

## Three Execution Paths Detail

```mermaid
graph TB
    subgraph "Single Ibis Expression"
        IBIS[Ibis Feature Definition<br/>COUNT, SUM, AVG, Windows]
    end

    subgraph "Path 1: Real-time Streaming"
        P1_COMPILE[Compile to<br/>Flink SQL]
        P1_EXEC[Flink Streaming Job<br/>Sub-second latency]
        P1_STATE[7-day window state]
        P1_BOOT[Join with Bootstrap<br/>for 60+ day windows]
        P1_STORE[Write to Bigtable<br/>Online Store]

        P1_COMPILE --> P1_EXEC
        P1_EXEC --> P1_STATE
        P1_EXEC --> P1_BOOT
        P1_EXEC --> P1_STORE
    end

    subgraph "Path 2: Batch Materialization"
        P2_COMPILE[Compile to<br/>Flink SQL]
        P2_EXEC[Flink Batch Jobs<br/>High throughput]
        P2_BOOT_GEN[Generate Bootstrap<br/>60-day aggregates]
        P2_ROLLUP[Daily Rollup Jobs]
        P2_STORE[Write to Bigtable<br/>+ BigQuery]

        P2_COMPILE --> P2_EXEC
        P2_EXEC --> P2_BOOT_GEN
        P2_EXEC --> P2_ROLLUP
        P2_EXEC --> P2_STORE
    end

    subgraph "Path 3: Historical Retrieval OPTIONAL"
        P3_COMPILE[Compile to<br/>BigQuery SQL]
        P3_EXEC[BigQuery Engine<br/>10-50% faster]
        P3_COLUMNAR[Columnar Storage<br/>Optimization]
        P3_RESULT[Training Data<br/>with PIT joins]

        P3_COMPILE --> P3_EXEC
        P3_EXEC --> P3_COLUMNAR
        P3_EXEC --> P3_RESULT
    end

    subgraph "Alternative Historical Path"
        ALT[Read Pre-materialized<br/>from batch_source]
        ALT --> P3_RESULT
    end

    IBIS --> P1_COMPILE
    IBIS --> P2_COMPILE
    IBIS --> P3_COMPILE

    KAFKA[Kafka Stream] --> P1_EXEC
    BQ_RAW[BigQuery Raw Events] --> P2_EXEC
    BQ_RAW2[BigQuery Raw Events] --> P3_EXEC
    P2_BOOT_GEN --> P1_BOOT

    classDef path1 fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    classDef path2 fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px
    classDef path3 fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    classDef source fill:#fff3e0,stroke:#e65100,stroke-width:2px

    class P1_COMPILE,P1_EXEC,P1_STATE,P1_BOOT,P1_STORE path1
    class P2_COMPILE,P2_EXEC,P2_BOOT_GEN,P2_ROLLUP,P2_STORE path2
    class P3_COMPILE,P3_EXEC,P3_COLUMNAR,P3_RESULT,ALT path3
    class IBIS,KAFKA,BQ_RAW,BQ_RAW2 source
```

**Key Points:**
- **Path 1**: Real-time serving with sub-second latency (Kafka → Flink → Bigtable)
- **Path 2**: Batch materialization and bootstrap generation (BigQuery → Flink → Bigtable)
- **Path 3**: Historical retrieval for training data (BigQuery → BigQuery Engine → Training Data)
- All three paths share the same Ibis feature definition (single source of truth)

## Bootstrap Pattern for Large Windows

```mermaid
graph TB
    subgraph "Without Bootstrap - State Explosion"
        WO_STATE[60-day Window State<br/>❌ 8.6x larger state<br/>❌ Slower recovery<br/>❌ Higher memory]
    end

    subgraph "With Bootstrap Pattern"
        BOOT_TABLE[Bootstrap Table<br/>Pre-computed 53 days<br/>Updated Daily]
        STREAM_STATE[7-day Stream State<br/>✅ 90% smaller<br/>✅ Fast recovery<br/>✅ Low memory]
        COMBINE[Combine Aggregates<br/>bootstrap[53d] + stream[7d]<br/>= total[60d]]

        BOOT_TABLE --> COMBINE
        STREAM_STATE --> COMBINE
    end

    subgraph "Bootstrap Lifecycle"
        DAILY[Daily Airflow DAG]
        FLINK_JOB[Flink Batch Job<br/>Compute 53-day rollup]
        WRITE_BQ[Write to BigQuery<br/>Bootstrap table]
        STREAM_READ[Streaming Job Reads<br/>Latest bootstrap]

        DAILY --> FLINK_JOB
        FLINK_JOB --> WRITE_BQ
        WRITE_BQ --> BOOT_TABLE
        BOOT_TABLE --> STREAM_READ
        STREAM_READ --> COMBINE
    end

    RAW[Raw Events<br/>BigQuery] --> FLINK_JOB

    style WO_STATE fill:#ffcdd2,stroke:#c62828,stroke-width:2px
    style BOOT_TABLE fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style STREAM_STATE fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style COMBINE fill:#fff9c4,stroke:#f57f17,stroke-width:2px
```

**Benefits:**
- 90% reduction in streaming state size
- Faster job recovery and restarts
- Lower memory consumption
- Efficient handling of 60+ day windows

## Feature Discovery & Monitoring System

```mermaid
graph TB
    subgraph "Feature Metadata"
        IBIS_DEF[Ibis Feature Definition]
        METADATA[Enhanced Metadata<br/>Schema, Types, Windows<br/>Dependencies, Owner]
    end

    subgraph "Documentation Generation"
        MD_GEN[Markdown Generator]
        HTML_GEN[HTML Generator]
        MD_DOCS[Markdown Docs<br/>Git-versioned]
        WEB_CATALOG[Web Catalog<br/>Searchable UI]
    end

    subgraph "Quality Monitoring"
        INLINE_METRICS[Inline Quality Checks<br/>Computed with features]
        AIRFLOW_MON[Airflow Monitoring<br/>Existing infrastructure]
        DRIFT_DETECT[Schema Drift Detection]
    end

    subgraph "Freshness SLI/SLO"
        FRESH_METRICS[Per-feature Freshness<br/>P50, P90, P99]
        SLO_TARGET[SLO Targets<br/>P99 < 200ms<br/>Goal: 150ms]
        ALERT[Violation Alerts<br/>Budget tracking]
        IMPACT[Staleness Impact<br/>Model performance]
    end

    subgraph "Discovery API"
        CLI[CLI Commands<br/>feast features list<br/>feast features search]
        REST[REST API<br/>Feature search<br/>Lineage queries]
    end

    IBIS_DEF --> METADATA
    METADATA --> MD_GEN
    METADATA --> HTML_GEN
    METADATA --> CLI
    METADATA --> REST

    MD_GEN --> MD_DOCS
    HTML_GEN --> WEB_CATALOG

    IBIS_DEF --> INLINE_METRICS
    INLINE_METRICS --> AIRFLOW_MON
    INLINE_METRICS --> DRIFT_DETECT
    INLINE_METRICS --> FRESH_METRICS

    FRESH_METRICS --> SLO_TARGET
    SLO_TARGET --> ALERT
    ALERT --> IMPACT

    classDef metadata fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef docs fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef quality fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef freshness fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef api fill:#fce4ec,stroke:#880e4f,stroke-width:2px

    class IBIS_DEF,METADATA metadata
    class MD_GEN,HTML_GEN,MD_DOCS,WEB_CATALOG docs
    class INLINE_METRICS,AIRFLOW_MON,DRIFT_DETECT quality
    class FRESH_METRICS,SLO_TARGET,ALERT,IMPACT freshness
    class CLI,REST api
```

**Features:**
- Auto-generated documentation from Ibis definitions
- Searchable feature catalog with web UI
- Inline quality metrics (computed with features)
- Freshness SLI/SLO tracking (P99 < 200ms target)
- Schema drift detection
- CLI and REST API for discovery

## Key Component Relationships

```mermaid
graph TB
    subgraph "Feast Registry Layer"
        REG[Feast Registry<br/>Feature definitions<br/>Metadata store]
        FV[FeatureView<br/>name, entities<br/>stream_source<br/>batch_source<br/>flink_dsl_definition]
        IDS[IbisDataSource<br/>Ibis expression<br/>cloudpickle]
    end

    subgraph "Offline Store SPI"
        IFOS[IbisFlinkOfflineStore<br/>OfflineStore subclass]
        GHF[get_historical_features<br/>Training data retrieval]
        COMPILE[compile_to_continuous_insert<br/>Streaming SQL generation]
    end

    subgraph "Batch Engine Integration"
        FCE[FlinkComputeEngine<br/>FlinkProvider]
        MAT[materialize<br/>Batch writes]
        BOOT_GEN[generate_bootstrap<br/>Large window support]
    end

    subgraph "Testing Strategy"
        DUCK[DuckDB Unit Tests<br/>Fast, local<br/>Milliseconds]
        BQ_TEST[BigQuery Tests<br/>Historical retrieval<br/>Dialect validation]
        FLINK_TEST[Flink Integration Tests<br/>Batch + Streaming<br/>End-to-end]
    end

    IDS --> FV
    FV --> REG
    FV --> IFOS

    IFOS --> GHF
    IFOS --> COMPILE

    COMPILE --> FCE
    FCE --> MAT
    FCE --> BOOT_GEN

    IDS --> DUCK
    IDS --> BQ_TEST
    IDS --> FLINK_TEST

    classDef registry fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef offline fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef batch fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef test fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px

    class REG,FV,IDS registry
    class IFOS,GHF,COMPILE offline
    class FCE,MAT,BOOT_GEN batch
    class DUCK,BQ_TEST,FLINK_TEST test
```

**Integration Points:**
- **IbisDataSource**: Wraps Ibis expressions with cloudpickle serialization for Feast registry
- **IbisFlinkOfflineStore**: Standard OfflineStore SPI implementation (~800 lines)
- **FlinkComputeEngine**: Handles materialization and bootstrap generation
- **Multi-tier Testing**: DuckDB (fast unit tests) → BigQuery (dialect validation) → Flink (end-to-end)

## Data Flow: Historical Retrieval

```mermaid
sequenceDiagram
    participant User
    participant FS as FeatureStore
    participant IFOS as IbisFlinkOfflineStore
    participant IC as Ibis Compiler
    participant BQ as BigQuery
    participant Feast as Feast PIT Join

    User->>FS: get_historical_features(entity_df, features)
    FS->>IFOS: Route to offline store

    alt Ibis On-Demand Approach (Recommended)
        IFOS->>IC: Compile Ibis expression
        IC->>IC: Generate BigQuery SQL
        IFOS->>BQ: Execute aggregation on raw events
        BQ-->>IFOS: Aggregated results
    else Pre-Materialized Approach
        IFOS->>BQ: Read from batch_source
        BQ-->>IFOS: Pre-computed features
    end

    IFOS->>Feast: Apply PIT join
    Feast->>Feast: Join with entity_df timestamps
    Feast-->>FS: Training data
    FS-->>User: DataFrame with features
```

## Data Flow: Real-time Serving

```mermaid
sequenceDiagram
    participant User
    participant FS as FeatureStore
    participant BT as Bigtable
    participant Flink as Flink Streaming
    participant Kafka
    participant Boot as Bootstrap Table

    Note over Flink,Kafka: Continuous processing
    Kafka->>Flink: Real-time events
    Boot->>Flink: Pre-computed 53-day aggregates
    Flink->>Flink: Compute 7-day window + join bootstrap
    Flink->>BT: Write to online store

    Note over User,BT: Serving path
    User->>FS: get_online_features(entity_ids)
    FS->>BT: Lookup by entity key
    BT-->>FS: Feature values (sub-second latency)
    FS-->>User: Feature response
```



## Timeline Overview

```mermaid
gantt
    title Feast Flink DSL Implementation Timeline (6-8 Months)
    dateFormat YYYY-MM-DD
    section Phase 0
    Connector Validation           :p0, 2024-01-01, 56d
    section Phase 1
    Ibis Validation               :p1, after p0, 14d
    section Phase 2
    IbisFlinkOfflineStore         :p2, after p1, 28d
    section Phase 3-5
    Multi-Backend Testing         :p3, after p2, 14d
    Feature Discovery             :p4, after p2, 21d
    Performance Validation        :p5, after p2, 28d
    section Phase 6
    Pilot Production              :p6, after p5, 35d
    section Phase 7-8
    Bootstrap Pattern             :p7, after p6, 35d
    Airflow Orchestration         :p8, after p7, 21d
    section Phase 9
    Full Scale-Out                :p9, after p8, 56d
```

## Summary

These diagrams capture the complete Feast Flink DSL implementation architecture:

1. **System Architecture**: All components and their interactions
2. **Phase Dependencies**: 9 phases over 6-8 months with risk assessment
3. **Execution Paths**: Three paths from single Ibis definition (streaming, batch, historical)
4. **Bootstrap Pattern**: 90% state reduction for large windows
5. **Feature Discovery**: Auto-documentation, quality monitoring, freshness SLI/SLO
6. **Component Relationships**: Feast integration via standard SPI patterns
7. **Data Flows**: Sequence diagrams for historical retrieval and real-time serving
8. **Ibis vs Custom DSL**: 78% code reduction, 33-43% faster timeline

**Key Innovation**: Leverage mature Ibis library instead of building custom DSL, reducing complexity and timeline while gaining multi-backend support.

