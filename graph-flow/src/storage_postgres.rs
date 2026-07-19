use async_trait::async_trait;
use serde_json;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use crate::{Session, error::{Result, GraphError}, storage::SessionStorage};

/// PostgreSQL-backed [`SessionStorage`] implementation.
///
/// Session ids are stored as `TEXT`, so any string id works (pre-0.6 the
/// column was `UUID`; the migration converts it automatically).
///
/// Saves are protected by optimistic locking on [`Session::version`]: a save
/// whose version does not match the stored row fails with
/// [`GraphError::SessionConflict`] instead of silently overwriting newer data.
pub struct PostgresSessionStorage {
    pool: Pool<Postgres>,
}

impl PostgresSessionStorage {
    /// Connect with default pool settings (5 max connections) and run the
    /// schema migration. Use [`PostgresSessionStorage::with_pool`] for custom
    /// pool configuration.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to Postgres: {e}")))?;

        Self::with_pool(pool).await
    }

    /// Build the storage from a pre-configured pool (custom connection limits,
    /// timeouts, etc.) and run the schema migration.
    pub async fn with_pool(pool: Pool<Postgres>) -> Result<Self> {
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    async fn migrate(pool: &Pool<Postgres>) -> Result<()> {
        let statements = [
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                graph_id TEXT NOT NULL,
                current_task_id TEXT NOT NULL,
                status_message TEXT,
                context JSONB NOT NULL,
                version BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#,
            // Upgrade pre-0.6 tables in place: UUID ids become TEXT (a no-op
            // if already TEXT) and the version column is added.
            r#"ALTER TABLE sessions ALTER COLUMN id TYPE TEXT;"#,
            r#"ALTER TABLE sessions ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;"#,
        ];

        for statement in statements {
            sqlx::query(statement)
                .execute(pool)
                .await
                .map_err(|e| GraphError::StorageError(format!("Migration failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl SessionStorage for PostgresSessionStorage {
    async fn save(&self, session: Session) -> Result<()> {
        let context_json = serde_json::to_value(&session.context)
            .map_err(|e| GraphError::StorageError(format!("Context serialization failed: {e}")))?;
        let version = i64::try_from(session.version)
            .map_err(|e| GraphError::StorageError(format!("Session version overflow: {e}")))?;

        // Optimistic locking: the UPDATE only applies when the stored version
        // still matches the version this session was loaded with.
        let result = sqlx::query(
            r#"
            INSERT INTO sessions (id, graph_id, current_task_id, status_message, context, version, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6 + 1, NOW())
            ON CONFLICT (id) DO UPDATE
            SET graph_id = EXCLUDED.graph_id,
                current_task_id = EXCLUDED.current_task_id,
                status_message = EXCLUDED.status_message,
                context = EXCLUDED.context,
                version = sessions.version + 1,
                updated_at = NOW()
            WHERE sessions.version = $6
            "#,
        )
        .bind(&session.id)
        .bind(&session.graph_id)
        .bind(&session.current_task_id)
        .bind(&session.status_message)
        .bind(&context_json)
        .bind(version)
        .execute(&self.pool)
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to save session: {e}")))?;

        if result.rows_affected() == 0 {
            return Err(GraphError::SessionConflict(format!(
                "Session '{}' was modified concurrently (attempted save from version {})",
                session.id, session.version
            )));
        }

        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Session>> {
        let row = sqlx::query_as::<_, (String, String, String, Option<String>, serde_json::Value, i64)>(
            r#"
            SELECT id, graph_id, current_task_id, status_message, context, version
            FROM sessions
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to fetch session: {e}")))?;

        if let Some((session_id, graph_id, current_task_id, status_message, context_json, version)) = row {
            let context: crate::Context = serde_json::from_value(context_json)
                .map_err(|e| GraphError::StorageError(format!("Context deserialization failed: {e}")))?;
            Ok(Some(Session {
                id: session_id,
                graph_id,
                current_task_id,
                status_message,
                context,
                version: version as u64,
            }))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, id: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM sessions WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to delete session: {e}")))?;
        Ok(())
    }
}
