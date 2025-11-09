# Database Migrations Guide

This guide covers database migrations using Linkgress ORM's `DbSchemaManager`, including automatic migrations, schema creation/deletion, and integration with your development workflow.

## Table of Contents

- [Overview](#overview)
- [DbSchemaManager](#dbschemamanager)
- [Automatic Migrations](#automatic-migrations)
  - [How It Works](#how-it-works)
  - [Running Migrations](#running-migrations)
  - [Interactive Confirmations](#interactive-confirmations)
- [Schema Creation and Deletion](#schema-creation-and-deletion)
  - [ensureCreated](#ensurecreated)
  - [ensureDeleted](#ensuredeleted)
- [Post-Migration Hooks](#post-migration-hooks)
- [NPM Script Integration](#npm-script-integration)
- [Migration Operations](#migration-operations)
- [Future: Planned Migrations with Journal](#future-planned-migrations-with-journal)

## Overview

Linkgress ORM provides a powerful schema management system through the `DbSchemaManager` class. Currently, the ORM supports **automatic migrations** that analyze your entity models and synchronize them with your database schema.

**Current Features:**
- Automatic migrations based on model comparison
- Interactive confirmations for destructive operations
- Schema creation from scratch
- Complete schema deletion
- Post-migration hooks for custom SQL

**Planned Features:**
- Migration journal/history tracking
- Explicit migration files
- Rollback support
- Migration versioning

## DbSchemaManager

The `DbSchemaManager` is the core class responsible for all schema operations. You access it through your `DbContext`:

```typescript
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from './database';

const client = new PgClient({
  host: 'localhost',
  port: 5432,
  database: 'myapp',
  user: 'postgres',
  password: 'postgres',
});

const db = new AppDatabase(client);

// Get the schema manager
const schemaManager = db.getSchemaManager();
```

## Automatic Migrations

Automatic migrations analyze your entity model definitions and compare them with your actual database schema, then generate and apply the necessary changes.

### How It Works

The migration process follows these steps:

1. **Schema Analysis** - Reads your current database schema
2. **Model Comparison** - Compares database schema with entity models
3. **Operation Generation** - Creates a list of migration operations
4. **User Review** - Displays all planned operations
5. **Confirmation** - Prompts for confirmation on destructive operations (drops)
6. **Execution** - Applies changes sequentially
7. **Post-Migration** - Executes optional post-migration hooks

### Running Migrations

```typescript
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from './database';

async function migrate() {
  const client = new PgClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'myapp',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
  });

  const db = new AppDatabase(client);

  try {
    const schemaManager = db.getSchemaManager();

    // Run automatic migration
    await schemaManager.migrate();

    console.log('✓ Migration completed successfully');
  } catch (error) {
    console.error('Migration failed:', error);
    throw error;
  } finally {
    await db.dispose();
  }
}

migrate().catch(console.error);
```

### Interactive Confirmations

When the migration includes destructive operations (dropping tables or columns), you'll be prompted for confirmation:

```
Pending migrations:
  1. Add column 'users.phone_number' (VARCHAR(20))
  2. Drop column 'users.old_field'
  3. Create index 'ix_users_email'

⚠️  Warning: This migration includes destructive operations that will result in data loss:
  - Drop column 'users.old_field'

Do you want to continue? (y/N): _
```

**Non-Interactive Mode:**
In non-TTY environments (CI/CD pipelines), the migration will default to the safe option (abort if destructive operations are detected).

### Migration Operations

The automatic migrator can detect and handle the following changes:

**Schema Level:**
- Create PostgreSQL schemas
- Create ENUM types
- Create sequences

**Table Level:**
- Create new tables
- Drop existing tables

**Column Level:**
- Add new columns
- Drop columns
- Alter column types
- Change column nullability
- Modify default values

**Index Level:**
- Create indexes
- Drop indexes

**Constraint Level:**
- Create foreign keys
- Drop foreign keys
- Modify foreign key actions (CASCADE, SET NULL, etc.)

## Schema Creation and Deletion

For development and testing, you can create or drop entire schemas programmatically.

### ensureCreated()

Creates all database objects from your entity models. This is useful for:
- Initial development setup
- Automated testing (create fresh schema for each test)
- Seeding new environments

```typescript
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from './database';

async function setupDatabase() {
  const client = new PgClient({
    host: 'localhost',
    port: 5432,
    database: 'myapp',
    user: 'postgres',
    password: 'postgres',
  });

  const db = new AppDatabase(client);

  try {
    const schemaManager = db.getSchemaManager();

    // Create all schemas, tables, indexes, and constraints
    await schemaManager.ensureCreated();

    console.log('✓ Database schema created successfully');
  } catch (error) {
    console.error('Schema creation failed:', error);
    throw error;
  } finally {
    await db.dispose();
  }
}

setupDatabase().catch(console.error);
```

**Creation Order:**
1. PostgreSQL schemas (e.g., `CREATE SCHEMA auth`)
2. ENUM types
3. Sequences
4. Tables with columns and constraints
5. Foreign keys
6. Indexes
7. Post-migration hooks (if configured)

### ensureDeleted()

Drops all database objects managed by the ORM. Use with caution - this is a destructive operation!

```typescript
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from './database';

async function teardownDatabase() {
  const client = new PgClient({
    host: 'localhost',
    port: 5432,
    database: 'myapp_test',
    user: 'postgres',
    password: 'postgres',
  });

  const db = new AppDatabase(client);

  try {
    const schemaManager = db.getSchemaManager();

    // Drop all tables, sequences, enums, and schemas
    await schemaManager.ensureDeleted();

    console.log('✓ Database schema deleted successfully');
  } catch (error) {
    console.error('Schema deletion failed:', error);
    throw error;
  } finally {
    await db.dispose();
  }
}

teardownDatabase().catch(console.error);
```

**Deletion Order (reverse of creation):**
1. Foreign keys
2. Indexes
3. Tables (with CASCADE)
4. Sequences
5. ENUM types
6. PostgreSQL schemas

**Use Cases:**
- Test cleanup (tear down test databases)
- Development reset (start fresh)
- CI/CD pipeline cleanup

## Post-Migration Hooks

You can execute custom SQL after migrations complete by overriding the `onMigrationComplete` method in your `DbContext`. This is useful for:

- Creating custom PostgreSQL functions
- Creating database views
- Setting up triggers
- Creating specialized indexes (GIN, GiST, etc.)
- Executing database-specific initialization

### Example: Custom Functions and Views

```typescript
import { DbContext, DbEntityTable, DbModelConfig, DatabaseClient } from 'linkgress-orm';
import { User } from './entities/user';
import { Post } from './entities/post';

export class AppDatabase extends DbContext {
  get users(): DbEntityTable<User> {
    return this.table(User);
  }

  get posts(): DbEntityTable<Post> {
    return this.table(Post);
  }

  protected override setupModel(model: DbModelConfig): void {
    // ... your entity configurations
  }

  /**
   * This hook is called automatically after ensureCreated() completes
   * and after automatic migrations finish
   */
  protected override async onMigrationComplete(client: DatabaseClient): Promise<void> {
    // Create a custom PostgreSQL function
    await client.query(`
      CREATE OR REPLACE FUNCTION get_user_by_username(p_username VARCHAR)
      RETURNS TABLE (
        id INTEGER,
        username VARCHAR,
        email TEXT
      ) AS $$
      BEGIN
        RETURN QUERY
        SELECT u.id, u.username, u.email
        FROM users u
        WHERE u.username = p_username;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Create a materialized view
    await client.query(`
      CREATE MATERIALIZED VIEW IF NOT EXISTS user_post_stats AS
      SELECT
        u.id,
        u.username,
        COUNT(p.id) as post_count,
        MAX(p.published_at) as last_post_date
      FROM users u
      LEFT JOIN posts p ON p.user_id = u.id
      GROUP BY u.id, u.username;
    `);

    // Create a full-text search index
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_posts_content_fts
      ON posts USING gin(to_tsvector('english', content));
    `);

    // Create a trigger
    await client.query(`
      CREATE OR REPLACE FUNCTION update_modified_timestamp()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await client.query(`
      DROP TRIGGER IF EXISTS set_updated_at ON users;
      CREATE TRIGGER set_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION update_modified_timestamp();
    `);

    console.log('✓ Post-migration hooks executed successfully');
  }
}
```

**Important Notes:**
- Use `CREATE OR REPLACE` for functions/views to make the hook idempotent
- Use `IF NOT EXISTS` for indexes and triggers
- The hook receives a `DatabaseClient` instance for executing raw SQL
- Errors in the hook will cause the entire migration to fail
- The hook is called AFTER all schema objects are created

## NPM Script Integration

Integrate migrations into your development workflow using NPM scripts.

### package.json

```json
{
  "name": "my-app",
  "version": "1.0.0",
  "scripts": {
    "db:migrate": "ts-node scripts/migrate.ts",
    "db:create": "ts-node scripts/create-schema.ts",
    "db:drop": "ts-node scripts/drop-schema.ts",
    "db:reset": "npm run db:drop && npm run db:create",
    "db:seed": "npm run db:create && ts-node scripts/seed.ts"
  }
}
```

### scripts/migrate.ts

```typescript
import 'dotenv/config';
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from '../src/database';

async function migrate() {
  console.log('Running database migration...\n');

  const client = new PgClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'myapp',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
  });

  const db = new AppDatabase(client, {
    logQueries: false, // Disable query logging for cleaner output
  });

  try {
    const schemaManager = db.getSchemaManager();
    await schemaManager.migrate();

    console.log('\n✓ Migration completed successfully');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await db.dispose();
  }
}

migrate();
```

### scripts/create-schema.ts

```typescript
import 'dotenv/config';
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from '../src/database';

async function createSchema() {
  console.log('Creating database schema...\n');

  const client = new PgClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'myapp',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
  });

  const db = new AppDatabase(client);

  try {
    await db.getSchemaManager().ensureCreated();

    console.log('✓ Schema created successfully');
    process.exit(0);
  } catch (error) {
    console.error('❌ Schema creation failed:', error);
    process.exit(1);
  } finally {
    await db.dispose();
  }
}

createSchema();
```

### scripts/drop-schema.ts

```typescript
import 'dotenv/config';
import { PgClient } from 'linkgress-orm';
import { AppDatabase } from '../src/database';

async function dropSchema() {
  // Safety check: prevent accidental production drops
  if (process.env.NODE_ENV === 'production') {
    console.error('❌ Cannot drop schema in production environment');
    process.exit(1);
  }

  console.log('⚠️  WARNING: This will drop all database objects!');
  console.log(`Database: ${process.env.DB_NAME || 'myapp'}\n`);

  const client = new PgClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'myapp',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
  });

  const db = new AppDatabase(client);

  try {
    await db.getSchemaManager().ensureDeleted();

    console.log('✓ Schema dropped successfully');
    process.exit(0);
  } catch (error) {
    console.error('❌ Schema deletion failed:', error);
    process.exit(1);
  } finally {
    await db.dispose();
  }
}

dropSchema();
```

### Usage Examples

```bash
# Run automatic migration
npm run db:migrate

# Create schema from scratch
npm run db:create

# Drop entire schema (development only)
npm run db:drop

# Reset database (drop + create)
npm run db:reset

# Create schema and seed data
npm run db:seed
```

### CI/CD Integration

```yaml
# Example GitHub Actions workflow
name: Database Migration

on:
  push:
    branches: [main, develop]

jobs:
  migrate:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'

      - name: Install dependencies
        run: npm ci

      - name: Run migrations
        env:
          DB_HOST: localhost
          DB_PORT: 5432
          DB_NAME: testdb
          DB_USER: postgres
          DB_PASSWORD: postgres
        run: npm run db:migrate
```

## Migration Operations

### Supported Operations

The `DbSchemaManager` analyzes and generates the following migration operations:

#### Schema Operations
```typescript
{ type: 'create_schema', schemaName: 'auth' }
```

#### ENUM Operations
```typescript
{
  type: 'create_enum',
  enumName: 'order_status',
  values: ['pending', 'processing', 'completed']
}
```

#### Table Operations
```typescript
{ type: 'create_table', tableName: 'users', schema: TableSchema }
{ type: 'drop_table', tableName: 'old_users' }
```

#### Column Operations
```typescript
{
  type: 'add_column',
  tableName: 'users',
  columnName: 'phone_number',
  config: ColumnConfig
}
{
  type: 'drop_column',
  tableName: 'users',
  columnName: 'deprecated_field'
}
{
  type: 'alter_column',
  tableName: 'users',
  columnName: 'age',
  from: DbColumnInfo,
  to: ColumnConfig
}
```

#### Index Operations
```typescript
{
  type: 'create_index',
  tableName: 'users',
  indexName: 'ix_users_email',
  columns: ['email']
}
{
  type: 'drop_index',
  tableName: 'users',
  indexName: 'ix_users_old_field'
}
```

#### Foreign Key Operations
```typescript
{
  type: 'create_foreign_key',
  tableName: 'posts',
  constraint: ForeignKeyConstraint
}
{
  type: 'drop_foreign_key',
  tableName: 'posts',
  constraintName: 'FK_posts_users'
}
```

### Operation Analysis

Before executing, you can analyze what changes are needed:

```typescript
const schemaManager = db.getSchemaManager();

// Get list of pending operations without executing
const operations = await schemaManager.analyze();

console.log('Pending operations:');
operations.forEach((op, index) => {
  console.log(`  ${index + 1}. ${op.type} - ${JSON.stringify(op)}`);
});

// Decide whether to proceed
if (operations.length === 0) {
  console.log('Schema is up to date');
} else {
  await schemaManager.migrate();
}
```

## Future: Planned Migrations with Journal

**Status:** Planned feature (not yet implemented)

Future versions of Linkgress ORM will support explicit migration files with journal/history tracking, similar to other popular ORMs.

### Planned Features

**Migration Journal:**
- Track migration history in a database table
- Record applied migrations with timestamps
- Support for up/down migrations
- Rollback capabilities

**Explicit Migration Files:**
```typescript
// migrations/001_create_users_table.ts
export class CreateUsersTable implements Migration {
  async up(schema: SchemaBuilder): Promise<void> {
    await schema.createTable('users', table => {
      table.integer('id').primaryKey();
      table.varchar('username', 100).notNull();
      table.text('email').notNull();
    });
  }

  async down(schema: SchemaBuilder): Promise<void> {
    await schema.dropTable('users');
  }
}
```

**Migration CLI:**
```bash
# Generate new migration
linkgress migration:create add_users_table

# Run pending migrations
linkgress migration:up

# Rollback last migration
linkgress migration:down

# View migration status
linkgress migration:status
```

**Migration History Table:**
```sql
CREATE TABLE __linkgress_migrations (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  batch INTEGER NOT NULL,
  applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Current Workaround

Until planned migrations are implemented, you can version your automatic migrations using these strategies:

**1. Database Branching:**
```bash
# Create separate databases for different branches
myapp_main
myapp_develop
myapp_feature_x
```

**2. Manual Tracking:**
```typescript
// Track schema version in your database
CREATE TABLE schema_version (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMP DEFAULT NOW()
);
```

**3. Backup Before Migration:**
```bash
# Always backup before running migrations
pg_dump myapp > backup_$(date +%Y%m%d_%H%M%S).sql
npm run db:migrate
```

## Best Practices

### Development Workflow

1. **Make model changes** in your entity classes
2. **Run migration** to sync database: `npm run db:migrate`
3. **Review changes** carefully before confirming destructive operations
4. **Test thoroughly** after migration completes
5. **Commit** model changes and any related code together

### Testing

```typescript
// test/setup.ts
beforeAll(async () => {
  const db = new AppDatabase(testClient);
  await db.getSchemaManager().ensureCreated();
});

afterAll(async () => {
  const db = new AppDatabase(testClient);
  await db.getSchemaManager().ensureDeleted();
  await testClient.end();
});
```

### Safety Guidelines

1. **Always backup production** before migrations
2. **Test migrations** on staging environment first
3. **Avoid dropping columns** with data - consider soft deletes
4. **Be cautious with type changes** that may cause data loss
5. **Use post-migration hooks** for data migrations
6. **Set NODE_ENV** to prevent accidental production drops

### Performance Tips

1. **Create indexes** for foreign keys and frequently queried columns
2. **Use post-migration hooks** for creating specialized indexes
3. **Run migrations** during low-traffic periods
4. **Monitor query performance** after schema changes
5. **Consider table partitioning** for large tables (via hooks)

## See Also

- [Schema Configuration Guide](./schema-configuration.md) - Entity and relationship setup
- [Database Clients](../database-clients.md) - Connecting to PostgreSQL
- [Getting Started](../getting-started.md) - Quick start guide
