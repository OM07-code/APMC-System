import { readFileSync } from 'fs';
import { join } from 'path';
import { getPool, closePool } from './pool';
import { logger } from '@apmc/middleware';

async function runMigrations(): Promise<void> {
  const pool = getPool();
  const client = await pool.connect();
  
  try {
    // Create migrations table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255) UNIQUE NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    // Read all migration files (init-db is at repo root)
    const migrationDir = join(__dirname, '..', '..', '..', '..', 'init-db');
    const migrations = [
      '01-schema.sql',
    ];

    for (const filename of migrations) {
      // Check if already applied
      const check = await client.query(
        'SELECT 1 FROM schema_migrations WHERE filename = $1',
        [filename]
      );
      
      if (check.rows.length > 0) {
        logger.info(`Migration ${filename} already applied, skipping`);
        continue;
      }

      // Read and execute migration
      const sql = readFileSync(join(migrationDir, filename), 'utf-8');
      logger.info(`Applying migration: ${filename}`);
      
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (filename) VALUES ($1)',
          [filename]
        );
        await client.query('COMMIT');
        logger.info(`Migration ${filename} applied successfully`);
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
    
    logger.info('All migrations completed');
  } finally {
    client.release();
    await closePool();
  }
}

if (require.main === module) {
  runMigrations()
    .then(() => {
      logger.info('Migration script completed');
      process.exit(0);
    })
    .catch((err) => {
      logger.error({ err }, 'Migration failed');
      process.exit(1);
    });
}