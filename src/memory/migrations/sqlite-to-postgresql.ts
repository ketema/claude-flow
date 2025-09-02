/**
 * SQLite to PostgreSQL migration utilities for Claude Flow memory system
 */

import { Pool } from 'pg';
import type { ILogger } from '../../core/logger.js';
import type { MemoryEntry } from '../../utils/types.js';
import { MemoryError } from '../../utils/errors.js';

// Dynamic imports for SQLite
let createDatabase: any;
let isSQLiteAvailable: any;

export interface MigrationStats {
  totalEntries: number;
  migratedEntries: number;
  skippedEntries: number;
  errors: number;
  duration: number;
}

export class SQLiteToPostgreSQLMigrator {
  private sqliteDb?: any;
  private pgPool?: Pool;
  private sqliteLoaded: boolean = false;

  constructor(
    private sqlitePath: string,
    private postgresConnectionString: string,
    private logger: ILogger,
    private batchSize: number = 1000
  ) {}

  async initialize(): Promise<void> {
    this.logger.info('Initializing SQLite to PostgreSQL migrator');

    try {
      // Load SQLite wrapper if not loaded
      if (!this.sqliteLoaded) {
        const module = await import('../sqlite-wrapper.js');
        createDatabase = module.createDatabase;
        isSQLiteAvailable = module.isSQLiteAvailable;
        this.sqliteLoaded = true;
      }

      // Check if SQLite is available
      const sqliteAvailable = await isSQLiteAvailable();
      if (!sqliteAvailable) {
        throw new Error('SQLite module not available');
      }

      // Initialize SQLite connection
      this.sqliteDb = await createDatabase(this.sqlitePath);

      // Initialize PostgreSQL connection
      this.pgPool = new Pool({
        connectionString: this.postgresConnectionString,
        max: 20,
      });

      // Test PostgreSQL connection
      const client = await this.pgPool.connect();
      try {
        await client.query('SELECT 1');
      } finally {
        client.release();
      }

      this.logger.info('SQLite to PostgreSQL migrator initialized');
    } catch (error) {
      throw new MemoryError('Failed to initialize migrator', { error });
    }
  }

  async shutdown(): Promise<void> {
    this.logger.info('Shutting down migrator');

    if (this.sqliteDb) {
      this.sqliteDb.close();
      delete this.sqliteDb;
    }

    if (this.pgPool) {
      await this.pgPool.end();
      delete this.pgPool;
    }
  }

  async migrate(dryRun: boolean = false): Promise<MigrationStats> {
    if (!this.sqliteDb || !this.pgPool) {
      throw new MemoryError('Migrator not initialized');
    }

    const startTime = Date.now();
    const stats: MigrationStats = {
      totalEntries: 0,
      migratedEntries: 0,
      skippedEntries: 0,
      errors: 0,
      duration: 0,
    };

    this.logger.info(`Starting migration (dry run: ${dryRun})`);

    try {
      // Get total count
      const countResult = this.sqliteDb
        .prepare('SELECT COUNT(*) as count FROM memory_entries')
        .get();
      stats.totalEntries = countResult.count;

      this.logger.info(`Found ${stats.totalEntries} entries to migrate`);

      if (stats.totalEntries === 0) {
        stats.duration = Date.now() - startTime;
        return stats;
      }

      // Create PostgreSQL tables if not dry run
      if (!dryRun) {
        await this.createPostgreSQLTables();
      }

      // Migrate in batches
      let offset = 0;
      while (offset < stats.totalEntries) {
        const batch = this.sqliteDb
          .prepare('SELECT * FROM memory_entries ORDER BY timestamp LIMIT ? OFFSET ?')
          .all(this.batchSize, offset);

        if (batch.length === 0) {
          break;
        }

        const batchStats = await this.migrateBatch(batch, dryRun);
        stats.migratedEntries += batchStats.migratedEntries;
        stats.skippedEntries += batchStats.skippedEntries;
        stats.errors += batchStats.errors;

        offset += this.batchSize;

        this.logger.info(`Migrated ${stats.migratedEntries}/${stats.totalEntries} entries`);
      }

      stats.duration = Date.now() - startTime;
      this.logger.info(`Migration completed in ${stats.duration}ms`, stats);

      return stats;
    } catch (error) {
      this.logger.error('Migration failed', error);
      throw new MemoryError('Migration failed', { error });
    }
  }

  async verifyMigration(): Promise<{
    sqliteCount: number;
    postgresCount: number;
    match: boolean;
    sampleVerification: boolean;
  }> {
    if (!this.sqliteDb || !this.pgPool) {
      throw new MemoryError('Migrator not initialized');
    }

    // Get SQLite count
    const sqliteResult = this.sqliteDb
      .prepare('SELECT COUNT(*) as count FROM memory_entries')
      .get();
    const sqliteCount = sqliteResult.count;

    // Get PostgreSQL count
    const pgResult = await this.pgPool.query(
      'SELECT COUNT(*) as count FROM claude_flow_graph.memory_entries'
    );
    const postgresCount = parseInt(pgResult.rows[0].count);

    // Verify sample entries
    const sampleVerification = await this.verifySampleEntries();

    return {
      sqliteCount,
      postgresCount,
      match: sqliteCount === postgresCount,
      sampleVerification,
    };
  }

  private async migrateBatch(
    batch: any[],
    dryRun: boolean
  ): Promise<Omit<MigrationStats, 'totalEntries' | 'duration'>> {
    const stats = {
      migratedEntries: 0,
      skippedEntries: 0,
      errors: 0,
    };

    if (dryRun) {
      // In dry run, just validate the data
      for (const row of batch) {
        try {
          this.validateEntry(row);
          stats.migratedEntries++;
        } catch (error) {
          this.logger.warn(`Would skip entry ${row.id}: ${error}`);
          stats.skippedEntries++;
        }
      }
      return stats;
    }

    // Convert SQLite entries to PostgreSQL format
    const pgEntries = batch
      .map((row) => {
        try {
          return this.convertEntry(row);
        } catch (error) {
          this.logger.warn(`Skipping entry ${row.id}: ${error}`);
          stats.skippedEntries++;
          return null;
        }
      })
      .filter((entry): entry is MemoryEntry => entry !== null);

    // Insert batch into PostgreSQL
    if (pgEntries.length > 0) {
      try {
        await this.insertBatch(pgEntries);
        stats.migratedEntries = pgEntries.length;
      } catch (error) {
        this.logger.error('Failed to insert batch', error);
        stats.errors = pgEntries.length;
      }
    }

    return stats;
  }

  private async createPostgreSQLTables(): Promise<void> {
    if (!this.pgPool) {
      throw new MemoryError('PostgreSQL connection not available');
    }

    const sql = `
      -- Create schema if it doesn't exist
      CREATE SCHEMA IF NOT EXISTS claude_flow_graph;
      
      -- Create memory_entries table
      CREATE TABLE IF NOT EXISTS claude_flow_graph.memory_entries (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        type TEXT NOT NULL,
        content TEXT NOT NULL,
        context JSONB NOT NULL DEFAULT '{}',
        timestamp TIMESTAMPTZ NOT NULL,
        tags JSONB NOT NULL DEFAULT '[]',
        version INTEGER NOT NULL DEFAULT 1,
        parent_id TEXT,
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        
        -- Foreign key constraint for hierarchical entries
        CONSTRAINT fk_memory_entries_parent 
          FOREIGN KEY (parent_id) 
          REFERENCES claude_flow_graph.memory_entries(id) 
          ON DELETE SET NULL
      );
      
      -- Create indexes
      CREATE INDEX IF NOT EXISTS idx_memory_entries_agent_id ON claude_flow_graph.memory_entries(agent_id);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_session_id ON claude_flow_graph.memory_entries(session_id);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_type ON claude_flow_graph.memory_entries(type);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_timestamp ON claude_flow_graph.memory_entries(timestamp);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_parent_id ON claude_flow_graph.memory_entries(parent_id);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_tags ON claude_flow_graph.memory_entries USING GIN(tags);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_context ON claude_flow_graph.memory_entries USING GIN(context);
      CREATE INDEX IF NOT EXISTS idx_memory_entries_metadata ON claude_flow_graph.memory_entries USING GIN(metadata);
    `;

    await this.pgPool.query(sql);
    this.logger.info('PostgreSQL tables created');
  }

  private validateEntry(row: any): void {
    if (!row.id || !row.agent_id || !row.session_id || !row.type || !row.content) {
      throw new Error('Missing required fields');
    }

    if (!row.timestamp) {
      throw new Error('Missing timestamp');
    }
  }

  private convertEntry(row: any): MemoryEntry {
    this.validateEntry(row);

    return {
      id: row.id,
      agentId: row.agent_id,
      sessionId: row.session_id,
      type: row.type,
      content: row.content,
      context: typeof row.context === 'string' ? JSON.parse(row.context) : row.context,
      timestamp: new Date(row.timestamp),
      tags: typeof row.tags === 'string' ? JSON.parse(row.tags) : row.tags,
      version: row.version || 1,
      ...(row.parent_id && { parentId: row.parent_id }),
      ...(row.metadata && {
        metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
      }),
    };
  }

  private async insertBatch(entries: MemoryEntry[]): Promise<void> {
    if (!this.pgPool || entries.length === 0) {
      return;
    }

    const values: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    for (const entry of entries) {
      values.push(
        `($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`
      );

      params.push(
        entry.id,
        entry.agentId,
        entry.sessionId,
        entry.type,
        entry.content,
        JSON.stringify(entry.context),
        entry.timestamp.toISOString(),
        JSON.stringify(entry.tags),
        entry.version,
        entry.parentId || null,
        entry.metadata ? JSON.stringify(entry.metadata) : null
      );
    }

    const sql = `
      INSERT INTO claude_flow_graph.memory_entries (
        id, agent_id, session_id, type, content, context, 
        timestamp, tags, version, parent_id, metadata
      ) VALUES ${values.join(', ')}
      ON CONFLICT (id) DO UPDATE SET
        agent_id = EXCLUDED.agent_id,
        session_id = EXCLUDED.session_id,
        type = EXCLUDED.type,
        content = EXCLUDED.content,
        context = EXCLUDED.context,
        timestamp = EXCLUDED.timestamp,
        tags = EXCLUDED.tags,
        version = EXCLUDED.version,
        parent_id = EXCLUDED.parent_id,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
    `;

    await this.pgPool.query(sql, params);
  }

  private async verifySampleEntries(): Promise<boolean> {
    if (!this.sqliteDb || !this.pgPool) {
      return false;
    }

    try {
      // Get a few sample entries from SQLite
      const sampleEntries = this.sqliteDb
        .prepare('SELECT * FROM memory_entries ORDER BY timestamp LIMIT 5')
        .all();

      if (sampleEntries.length === 0) {
        return true; // No entries to verify
      }

      // Check if they exist in PostgreSQL with matching content
      for (const entry of sampleEntries) {
        const pgResult = await this.pgPool.query(
          'SELECT * FROM claude_flow_graph.memory_entries WHERE id = $1',
          [entry.id]
        );

        if (pgResult.rows.length === 0) {
          this.logger.error(`Entry ${entry.id} not found in PostgreSQL`);
          return false;
        }

        const pgEntry = pgResult.rows[0];
        if (
          pgEntry.agent_id !== entry.agent_id ||
          pgEntry.session_id !== entry.session_id ||
          pgEntry.content !== entry.content
        ) {
          this.logger.error(`Entry ${entry.id} content mismatch`);
          return false;
        }
      }

      return true;
    } catch (error) {
      this.logger.error('Sample verification failed', error);
      return false;
    }
  }
}

/**
 * Utility function to create and run a migration
 */
export async function migrateClaudeFlowData(
  sqlitePath: string,
  postgresConnectionString: string,
  logger: ILogger,
  options: {
    dryRun?: boolean;
    batchSize?: number;
    verify?: boolean;
  } = {}
): Promise<MigrationStats> {
  const migrator = new SQLiteToPostgreSQLMigrator(
    sqlitePath,
    postgresConnectionString,
    logger,
    options.batchSize
  );

  try {
    await migrator.initialize();

    const stats = await migrator.migrate(options.dryRun || false);

    if (options.verify && !options.dryRun) {
      const verification = await migrator.verifyMigration();
      logger.info('Migration verification', verification);

      if (!verification.match || !verification.sampleVerification) {
        throw new MemoryError('Migration verification failed');
      }
    }

    return stats;
  } finally {
    await migrator.shutdown();
  }
}