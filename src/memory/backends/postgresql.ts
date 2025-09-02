/**
 * PostgreSQL backend implementation for memory storage
 */

import { Pool, PoolClient } from 'pg';
import type { IMemoryBackend } from './base.js';
import type { MemoryEntry, MemoryQuery } from '../../utils/types.js';
import type { ILogger } from '../../core/logger.js';
import { MemoryBackendError } from '../../utils/errors.js';

/**
 * PostgreSQL-based memory backend
 */
export class PostgreSQLBackend implements IMemoryBackend {
  private pool?: Pool;
  private initialized: boolean = false;

  constructor(
    private connectionString: string,
    private logger: ILogger,
    private poolConfig: {
      max?: number;
      idleTimeoutMillis?: number;
      connectionTimeoutMillis?: number;
    } = {}
  ) {}

  async initialize(): Promise<void> {
    this.logger.info('Initializing PostgreSQL backend', { 
      connectionString: this.connectionString.replace(/:[^:@]+@/, ':****@') // Mask password
    });

    try {
      // Create connection pool
      this.pool = new Pool({
        connectionString: this.connectionString,
        max: this.poolConfig.max || 20,
        idleTimeoutMillis: this.poolConfig.idleTimeoutMillis || 30000,
        connectionTimeoutMillis: this.poolConfig.connectionTimeoutMillis || 2000,
      });

      // Test connection
      const client = await this.pool.connect();
      try {
        await client.query('SELECT 1');
      } finally {
        client.release();
      }

      // Create tables and indexes
      await this.createTables();
      await this.createIndexes();

      this.initialized = true;
      this.logger.info('PostgreSQL backend initialized');
    } catch (error) {
      throw new MemoryBackendError('Failed to initialize PostgreSQL backend', { error });
    }
  }

  async shutdown(): Promise<void> {
    this.logger.info('Shutting down PostgreSQL backend');

    if (this.pool) {
      await this.pool.end();
      delete this.pool;
    }
    this.initialized = false;
  }

  async store(entry: MemoryEntry): Promise<void> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const sql = `
      INSERT INTO claude_flow_graph.memory_entries (
        id, agent_id, session_id, type, content, 
        context, timestamp, tags, version, parent_id, metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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

    const params = [
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
      entry.metadata ? JSON.stringify(entry.metadata) : null,
    ];

    try {
      await this.pool.query(sql, params);
    } catch (error) {
      throw new MemoryBackendError('Failed to store entry', { error });
    }
  }

  async retrieve(id: string): Promise<MemoryEntry | undefined> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const sql = 'SELECT * FROM claude_flow_graph.memory_entries WHERE id = $1';

    try {
      const result = await this.pool.query(sql, [id]);
      
      if (result.rows.length === 0) {
        return undefined;
      }

      return this.rowToEntry(result.rows[0]);
    } catch (error) {
      throw new MemoryBackendError('Failed to retrieve entry', { error });
    }
  }

  async update(id: string, entry: MemoryEntry): Promise<void> {
    // PostgreSQL UPSERT handles updates
    await this.store(entry);
  }

  async delete(id: string): Promise<void> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const sql = 'DELETE FROM claude_flow_graph.memory_entries WHERE id = $1';

    try {
      await this.pool.query(sql, [id]);
    } catch (error) {
      throw new MemoryBackendError('Failed to delete entry', { error });
    }
  }

  async query(query: MemoryQuery): Promise<MemoryEntry[]> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIndex = 1;

    if (query.agentId) {
      conditions.push(`agent_id = $${paramIndex++}`);
      params.push(query.agentId);
    }

    if (query.sessionId) {
      conditions.push(`session_id = $${paramIndex++}`);
      params.push(query.sessionId);
    }

    if (query.type) {
      conditions.push(`type = $${paramIndex++}`);
      params.push(query.type);
    }

    if (query.startTime) {
      conditions.push(`timestamp >= $${paramIndex++}`);
      params.push(query.startTime.toISOString());
    }

    if (query.endTime) {
      conditions.push(`timestamp <= $${paramIndex++}`);
      params.push(query.endTime.toISOString());
    }

    if (query.search) {
      conditions.push(`(content ILIKE $${paramIndex} OR tags::text ILIKE $${paramIndex + 1})`);
      params.push(`%${query.search}%`, `%${query.search}%`);
      paramIndex += 2;
    }

    if (query.tags && query.tags.length > 0) {
      const tagConditions = query.tags.map(() => `tags::jsonb ? $${paramIndex++}`);
      conditions.push(`(${tagConditions.join(' OR ')})`);
      query.tags.forEach((tag: string) => params.push(tag));
    }

    if (query.namespace) {
      conditions.push(`metadata->>'namespace' = $${paramIndex++}`);
      params.push(query.namespace);
    }

    let sql = 'SELECT * FROM claude_flow_graph.memory_entries';
    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    sql += ' ORDER BY timestamp DESC';

    if (query.limit) {
      sql += ` LIMIT $${paramIndex++}`;
      params.push(query.limit);
    }

    if (query.offset) {
      sql += ` OFFSET $${paramIndex++}`;
      params.push(query.offset);
    }

    try {
      const result = await this.pool.query(sql, params);
      return result.rows.map(row => this.rowToEntry(row));
    } catch (error) {
      throw new MemoryBackendError('Failed to query entries', { error });
    }
  }

  async getAllEntries(): Promise<MemoryEntry[]> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const sql = 'SELECT * FROM claude_flow_graph.memory_entries ORDER BY timestamp DESC';

    try {
      const result = await this.pool.query(sql);
      return result.rows.map(row => this.rowToEntry(row));
    } catch (error) {
      throw new MemoryBackendError('Failed to get all entries', { error });
    }
  }

  async getHealthStatus(): Promise<{
    healthy: boolean;
    error?: string;
    metrics?: Record<string, number>;
  }> {
    if (!this.pool) {
      return {
        healthy: false,
        error: 'Database not initialized',
      };
    }

    try {
      // Check database connectivity
      await this.pool.query('SELECT 1');

      // Get metrics
      const countResult = await this.pool.query(
        'SELECT COUNT(*) as count FROM claude_flow_graph.memory_entries'
      );
      const entryCount = parseInt(countResult.rows[0].count);

      const sizeResult = await this.pool.query(`
        SELECT pg_total_relation_size('claude_flow_graph.memory_entries') as size
      `);
      const tableSize = parseInt(sizeResult.rows[0].size);

      const poolMetrics = {
        totalConnections: this.pool.totalCount,
        idleConnections: this.pool.idleCount,
        waitingCount: this.pool.waitingCount,
      };

      return {
        healthy: true,
        metrics: {
          entryCount,
          tableSizeBytes: tableSize,
          ...poolMetrics,
        },
      };
    } catch (error) {
      return {
        healthy: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  async performMaintenance(): Promise<void> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    try {
      // Analyze table for query optimization
      await this.pool.query('ANALYZE claude_flow_graph.memory_entries');
      
      // Vacuum table to reclaim space (non-blocking)
      await this.pool.query('VACUUM (ANALYZE) claude_flow_graph.memory_entries');
      
      this.logger.info('Database maintenance completed');
    } catch (error) {
      this.logger.error('Database maintenance failed', error);
      throw new MemoryBackendError('Failed to perform maintenance', { error });
    }
  }

  private async createTables(): Promise<void> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
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
      
      -- Create swarm_metadata table for coordination
      CREATE TABLE IF NOT EXISTS claude_flow_graph.swarm_metadata (
        id TEXT PRIMARY KEY,
        swarm_id TEXT NOT NULL,
        agent_id TEXT,
        metadata_type TEXT NOT NULL,
        data JSONB NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      
      -- Create worktree_sessions table for session management
      CREATE TABLE IF NOT EXISTS claude_flow_graph.worktree_sessions (
        id TEXT PRIMARY KEY,
        session_name TEXT NOT NULL,
        workspace_path TEXT,
        agent_assignments JSONB NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_activity TIMESTAMPTZ DEFAULT NOW()
      );
    `;

    await this.pool.query(sql);
  }

  private async createIndexes(): Promise<void> {
    if (!this.pool) {
      throw new MemoryBackendError('Database not initialized');
    }

    const indexes = [
      // Memory entries indexes
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_agent_id ON claude_flow_graph.memory_entries(agent_id)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_session_id ON claude_flow_graph.memory_entries(session_id)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_type ON claude_flow_graph.memory_entries(type)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_timestamp ON claude_flow_graph.memory_entries(timestamp)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_parent_id ON claude_flow_graph.memory_entries(parent_id)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_tags ON claude_flow_graph.memory_entries USING GIN(tags)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_context ON claude_flow_graph.memory_entries USING GIN(context)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_metadata ON claude_flow_graph.memory_entries USING GIN(metadata)',
      'CREATE INDEX IF NOT EXISTS idx_memory_entries_content_search ON claude_flow_graph.memory_entries USING GIN(to_tsvector(\'english\', content))',
      
      // Swarm metadata indexes
      'CREATE INDEX IF NOT EXISTS idx_swarm_metadata_swarm_id ON claude_flow_graph.swarm_metadata(swarm_id)',
      'CREATE INDEX IF NOT EXISTS idx_swarm_metadata_agent_id ON claude_flow_graph.swarm_metadata(agent_id)',
      'CREATE INDEX IF NOT EXISTS idx_swarm_metadata_type ON claude_flow_graph.swarm_metadata(metadata_type)',
      'CREATE INDEX IF NOT EXISTS idx_swarm_metadata_data ON claude_flow_graph.swarm_metadata USING GIN(data)',
      
      // Worktree sessions indexes
      'CREATE INDEX IF NOT EXISTS idx_worktree_sessions_name ON claude_flow_graph.worktree_sessions(session_name)',
      'CREATE INDEX IF NOT EXISTS idx_worktree_sessions_status ON claude_flow_graph.worktree_sessions(status)',
      'CREATE INDEX IF NOT EXISTS idx_worktree_sessions_activity ON claude_flow_graph.worktree_sessions(last_activity)',
    ];

    for (const sql of indexes) {
      await this.pool.query(sql);
    }
  }

  private rowToEntry(row: any): MemoryEntry {
    const entry: MemoryEntry = {
      id: row.id,
      agentId: row.agent_id,
      sessionId: row.session_id,
      type: row.type,
      content: row.content,
      context: typeof row.context === 'string' ? JSON.parse(row.context) : row.context,
      timestamp: new Date(row.timestamp),
      tags: typeof row.tags === 'string' ? JSON.parse(row.tags) : row.tags,
      version: row.version,
    };

    if (row.parent_id) {
      entry.parentId = row.parent_id;
    }

    if (row.metadata) {
      entry.metadata = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
    }

    return entry;
  }
}