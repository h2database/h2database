# H2 Database - Architecture Overview

This document provides an overview of the H2 Database Engine architecture to help new developers understand the codebase structure and design.

## Table of Contents

- [Introduction](#introduction)
- [High-Level Architecture](#high-level-architecture)
- [Core Components](#core-components)
- [Package Organization](#package-organization)
- [Key Subsystems](#key-subsystems)
- [Data Flow](#data-flow)
- [Storage Layer](#storage-layer)
- [Design Patterns](#design-patterns)
- [Extending H2](#extending-h2)

## Introduction

H2 is a pure Java SQL database engine that can run in embedded mode (in-process) or as a standalone server. It implements:

- **ANSI SQL compliance**: SQL-89 compliant with many SQL:2003 and later features
- **JDBC API**: Standard Java database connectivity
- **Multiple storage modes**: In-memory, file-based, and MVStore
- **Transaction support**: ACID compliance with MVCC (Multi-Version Concurrency Control)
- **Multiple database modes**: PostgreSQL, MySQL, Oracle, SQL Server compatibility modes

### Design Philosophy

- **Pure Java**: No native dependencies (except optional features)
- **Small footprint**: ~2.5 MB JAR file
- **Fast performance**: Optimized for speed with minimal overhead
- **Embedded-first**: Designed primarily for embedded use, with server mode as an option
- **Standards compliant**: Follows JDBC and SQL standards closely

## High-Level Architecture

H2's architecture follows a layered design, from top to bottom:

```
┌─────────────────────────────────────────┐
│        JDBC Driver (API Layer)          │  org.h2.jdbc, org.h2.jdbcx
├─────────────────────────────────────────┤
│    Connection/Session Management        │  org.h2.engine
├─────────────────────────────────────────┤
│           SQL Parser                    │  org.h2.command.Parser
├─────────────────────────────────────────┤
│    Command Execution & Planning         │  org.h2.command.*
├─────────────────────────────────────────┤
│   Schema Objects (Tables/Indexes)       │  org.h2.table, org.h2.index
├─────────────────────────────────────────┤
│      Expression Evaluation              │  org.h2.expression.*
├─────────────────────────────────────────┤
│   Transaction & Concurrency Control     │  org.h2.engine, org.h2.mvstore.tx
├─────────────────────────────────────────┤
│       Storage Engine (MVStore)          │  org.h2.mvstore.*
├─────────────────────────────────────────┤
│      Filesystem Abstraction             │  org.h2.store.fs
└─────────────────────────────────────────┘
```

## Core Components

### 1. JDBC Driver (`org.h2.jdbc`, `org.h2.jdbcx`)

**Purpose**: Implements the standard Java JDBC API.

**Key Classes**:
- `JdbcConnection` - JDBC connection implementation
- `JdbcStatement` - JDBC statement implementation
- `JdbcPreparedStatement` - Prepared statement implementation
- `JdbcResultSet` - Result set implementation
- `JdbcDatabaseMetaData` - Database metadata
- `Driver` - JDBC driver registration

**Responsibility**: Provides the external API that applications use to interact with H2.

### 2. Connection/Session Management (`org.h2.engine`)

**Purpose**: Manages database instances and user sessions.

**Key Classes**:
- `Database` - Root class representing a database instance
- `Session` - Represents a user session (embedded mode)
- `SessionRemote` - Represents a remote session (client-server mode)
- `SessionInterface` - Common interface for both session types
- `User` - User account information
- `Mode` - Database compatibility mode (PostgreSQL, MySQL, etc.)

**Responsibility**: 
- Database lifecycle (open, close, checkpoint)
- Session management (authentication, transaction boundaries)
- Global database settings and metadata

### 3. SQL Parser (`org.h2.command.Parser`)

**Purpose**: Parses SQL statements into executable command objects.

**Design**: Recursive descent parser that directly builds command objects.

**Key Features**:
- Single-pass parsing (no AST intermediate representation)
- Generates Command objects directly
- Supports SQL extensions and compatibility modes

**Flow**:
```
SQL String → Parser → Command Object → Optimize → Execute
```

### 4. Command Execution (`org.h2.command.*`)

**Purpose**: Represents and executes SQL commands.

**Package Structure**:
- `org.h2.command` - Base command classes
- `org.h2.command.ddl` - Data Definition Language (CREATE, ALTER, DROP)
- `org.h2.command.dml` - Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)
- `org.h2.command.query` - Query operations and planning

**Key Classes**:
- `Command` - Base class for all commands
- `Query` - SELECT statement execution
- `Select` - Main SELECT implementation
- `Insert`, `Update`, `Delete` - DML operations
- `Prepared` - Prepared statement representation

### 5. Schema Objects (`org.h2.schema`, `org.h2.table`, `org.h2.index`)

**Purpose**: Represents database objects (tables, indexes, constraints).

**Key Classes**:

*Tables* (`org.h2.table`):
- `Table` - Base table interface
- `TableBase` - Base implementation
- `RegularTable` - Standard persistent table
- `TableView` - View implementation
- `TableFilter` - Used in query execution for filtering

*Indexes* (`org.h2.index`):
- `Index` - Base index interface
- `PageBtreeIndex` - B-tree based index (legacy)
- `MVPrimaryIndex` - MVStore primary index
- `MVSecondaryIndex` - MVStore secondary index

*Constraints* (`org.h2.constraint`):
- `Constraint` - Base constraint class
- `ConstraintUnique` - Unique/primary key constraints
- `ConstraintReferential` - Foreign key constraints

**Important**: In H2, indexes are stored as special tables internally.

### 6. Expression Evaluation (`org.h2.expression.*`)

**Purpose**: Represents and evaluates SQL expressions.

**Package Structure**:
- `org.h2.expression` - Base expression classes
- `org.h2.expression.condition` - Boolean conditions (WHERE, HAVING)
- `org.h2.expression.aggregate` - Aggregate functions (SUM, COUNT, AVG)
- `org.h2.expression.function` - Built-in functions
- `org.h2.expression.analysis` - Window functions

**Key Classes**:
- `Expression` - Base class for all expressions
- `ValueExpression` - Constant value
- `ExpressionColumn` - Column reference
- `Operation` - Binary operations (+, -, *, /)
- `Aggregate` - Aggregate functions
- `Function` - Built-in functions

### 7. Value System (`org.h2.value`)

**Purpose**: Represents all SQL data types and values.

**Key Classes**:
- `Value` - Base class for all values
- `ValueInt`, `ValueLong`, `ValueDecimal` - Numeric types
- `ValueString`, `ValueStringFixed` - String types
- `ValueDate`, `ValueTime`, `ValueTimestamp` - Temporal types
- `ValueNull` - NULL value
- `TypeInfo` - Type metadata and information

**Design**: Immutable value objects with type coercion support.

### 8. Storage Engine - MVStore (`org.h2.mvstore.*`)

**Purpose**: Multi-version storage engine with MVCC support.

**Package Structure**:
- `org.h2.mvstore` - Core MVStore classes
- `org.h2.mvstore.db` - Database integration
- `org.h2.mvstore.tx` - Transaction support
- `org.h2.mvstore.cache` - Caching layer
- `org.h2.mvstore.type` - Data type serialization

**Key Classes**:
- `MVStore` - Main store class
- `MVMap` - Persistent map implementation
- `TransactionStore` - Transaction management
- `MVPrimaryIndex`, `MVSecondaryIndex` - Index implementations

**Features**:
- Copy-on-write B-tree structure
- MVCC (Multi-Version Concurrency Control)
- Background compression and compaction
- Crash recovery

### 9. Server Components (`org.h2.server`)

**Purpose**: Provides server modes (TCP, Web Console, PG protocol).

**Key Classes**:
- `TcpServer` - TCP/IP server for remote connections
- `WebServer` - HTTP server for web console
- `PgServer` - PostgreSQL protocol compatibility server

## Package Organization

### Main Source (`src/main/org/h2/`)

```
org.h2/
├── api/              - Public APIs and interfaces
├── bnf/              - BNF (grammar) related classes
├── command/          - SQL command execution
│   ├── ddl/          - DDL commands (CREATE, ALTER, DROP)
│   ├── dml/          - DML commands (SELECT, INSERT, UPDATE, DELETE)
│   └── query/        - Query processing and optimization
├── compress/         - Compression utilities
├── constraint/       - Constraint implementations
├── engine/           - Core database engine
├── expression/       - SQL expression evaluation
│   ├── aggregate/    - Aggregate functions
│   ├── analysis/     - Window functions
│   ├── condition/    - Boolean conditions
│   └── function/     - Built-in functions
├── fulltext/         - Full-text search
├── index/            - Index implementations
├── jdbc/             - JDBC driver implementation
│   └── meta/         - Database metadata
├── jdbcx/            - JDBC extensions (DataSource, XA)
├── message/          - Error messages and localization
├── mode/             - Database compatibility modes
├── mvstore/          - MVStore storage engine
│   ├── cache/        - Cache implementation
│   ├── db/           - Database integration
│   ├── rtree/        - R-tree for spatial data
│   ├── tx/           - Transaction support
│   └── type/         - Type serialization
├── result/           - Result set implementations
├── schema/           - Schema objects
├── security/         - Security and authentication
├── server/           - Server implementations
│   ├── pg/           - PostgreSQL protocol
│   └── web/          - Web console
├── store/            - Storage layer (legacy)
│   └── fs/           - Filesystem abstraction
├── table/            - Table implementations
├── tools/            - H2 tools (Console, Shell, Backup, etc.)
├── util/             - Utility classes
└── value/            - Value and data type implementations
    └── lob/          - Large object (BLOB/CLOB) handling
```

### Test Source (`src/test/org/h2/test/`)

```
org.h2.test/
├── TestAll.java      - Main test suite
├── TestBase.java     - Base class for all tests
├── db/               - Database functionality tests
├── jdbc/             - JDBC driver tests
├── mvcc/             - MVCC tests
├── server/           - Server tests
├── store/            - Storage tests
├── unit/             - Unit tests
└── scripts/          - SQL script tests
```

## Key Subsystems

### Query Execution Pipeline

1. **Parse**: SQL string → `Command` object
2. **Prepare**: Resolve names, validate types
3. **Optimize**: Cost-based optimization, index selection
4. **Execute**: Run the optimized plan
5. **Return**: Results to JDBC layer

**Example Flow for SELECT**:
```
SQL: "SELECT * FROM users WHERE age > 18"
  ↓
Parser creates Select command
  ↓
Select.prepare() resolves "users" table, "age" column
  ↓
Select.optimize() chooses index on "age" if available
  ↓
Select.query() executes using TableFilter
  ↓
Returns ResultInterface → JdbcResultSet
```

### Transaction Management

**Isolation Levels**:
- READ_UNCOMMITTED
- READ_COMMITTED (default)
- REPEATABLE_READ
- SERIALIZABLE

**MVCC Implementation**:
- Each row has a version
- Readers don't block writers
- Writers don't block readers
- Garbage collection removes old versions

**Transaction Flow**:
```
BEGIN TRANSACTION
  ↓
Session.begin()
  ↓
Execute commands (insert/update/delete)
  ↓
MVStore creates new versions
  ↓
COMMIT → TransactionStore.commit()
  or
ROLLBACK → TransactionStore.rollback()
```

### Index Selection

H2 uses cost-based optimization to select indexes:

1. **Parse** WHERE clause into conditions
2. **Find** applicable indexes for each condition
3. **Calculate** estimated cost for each index
4. **Choose** index with lowest cost
5. **Use** index scan or table scan

**Cost Factors**:
- Index selectivity (how many rows match)
- Index coverage (does it cover all needed columns)
- I/O cost (estimated disk reads)

## Storage Layer

### MVStore Architecture

**Persistent B-tree Map**:
- Keys and values stored in B-tree structure
- Copy-on-write: updates create new versions
- Old versions kept for MVCC
- Background compaction removes old data

**File Format**:
```
[Header]
[Chunk 1: Pages with version 1]
[Chunk 2: Pages with version 2]
...
[Chunk N: Latest pages]
```

**Page Types**:
- **Leaf pages**: Store actual key-value pairs
- **Node pages**: Store keys and pointers to child pages
- **Meta page**: Root and metadata

### Filesystem Abstraction

`org.h2.store.fs` provides unified access to:
- Regular files (`file:`)
- In-memory databases (`mem:`)
- Encrypted files (`encrypt:`)
- Compressed files (`compress:`)
- NIO files (`nio:`)
- Zip files (`zip:`)

**Usage**:
```java
FileUtils.newOutputStream("encrypt:aes:~/test.db", true);
```

## Design Patterns

### 1. Strategy Pattern
- **Where**: Database compatibility modes
- **How**: Different SQL dialects implemented as Mode strategies

### 2. Template Method
- **Where**: Table and Index base classes
- **How**: Common logic in base, specifics in subclasses

### 3. Factory Pattern
- **Where**: Value creation, Expression parsing
- **How**: Factory methods create appropriate types

### 4. Command Pattern
- **Where**: SQL command execution
- **How**: Each SQL statement is a Command object

### 5. Visitor Pattern
- **Where**: Expression tree traversal
- **How**: ExpressionVisitor walks expression trees

### 6. Singleton (per Database)
- **Where**: Database instance
- **How**: One Database object per database

## Extending H2

### Adding a New Function

1. **Create function class** in `org.h2.expression.function`
2. **Extend `Function`** or specific function type
3. **Implement `getValue()`** method
4. **Register** in `Function.getFunction()`
5. **Add tests** in `src/test/org/h2/test/db/`

Example:
```java
public class MyFunction extends Function {
    @Override
    public Value getValue(Session session) {
        Value v = args[0].getValue(session);
        // Your logic here
        return ValueString.get(result);
    }
}
```

### Adding a New Data Type

1. **Create Value subclass** in `org.h2.value`
2. **Implement required methods** (compare, convert, etc.)
3. **Add type constant** to `Value` class
4. **Update TypeInfo** handling
5. **Add tests**

### Adding a New Command

1. **Create command class** in `org.h2.command.ddl` or `org.h2.command.dml`
2. **Extend `Prepared`** or appropriate base
3. **Implement `update()` or `query()` method**
4. **Add parsing** in `Parser` class
5. **Add tests**

### Adding a Compatibility Mode

1. **Create Mode configuration** in `org.h2.mode`
2. **Define differences** (functions, keywords, behavior)
3. **Register mode** in Mode registry
4. **Add tests**

## Key Architectural Decisions

### Why No Intermediate Representation?

H2 directly creates executable Command objects from SQL, skipping an IR step. This:
- Reduces memory overhead
- Speeds up parsing
- Simplifies the codebase
- Trade-off: Less flexible for advanced optimizations

### Why Indexes as Tables?

Storing indexes as special tables:
- Simplifies storage layer (one abstraction)
- Indexes use same B-tree structure as tables
- Simplifies transaction handling

### Why MVStore?

Replaced older PageStore with MVStore for:
- Better MVCC support
- Improved crash recovery
- Simpler implementation
- Better performance

## Performance Considerations

### Caching
- **Row cache**: Recently accessed rows
- **Query cache**: Compiled query plans
- **MVStore cache**: Hot pages in memory

### Optimization
- Cost-based index selection
- Subquery optimization
- Join reordering
- Constant folding

### Concurrency
- MVCC allows readers and writers to work concurrently
- Lock-free reads in most cases
- Fine-grained locking for schema changes

## Further Reading

- **Source Code**: Well-commented, start with `org.h2.engine.Database`
- **Tests**: `src/test/org/h2/test/` shows usage patterns
- **Documentation**: `src/docsrc/html/architecture.html` (detailed layer descriptions)
- **Javadoc**: Run `./build.sh javadoc` to generate API docs

## Summary

H2's architecture is:
- **Layered**: Clear separation of concerns
- **Modular**: Well-organized packages
- **Extensible**: Easy to add functions, types, commands
- **Efficient**: Minimal overhead, optimized for embedded use

Understanding these core concepts will help you navigate and contribute to the H2 codebase effectively.
