use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Context, error::{GraphError, Result}, graph::Graph};

/// Session information
///
/// Sessions carry an optimistic-locking `version`: storage backends reject a
/// save whose version does not match the stored one (see
/// [`GraphError::SessionConflict`]), so concurrent runs of the same session
/// fail loudly instead of silently losing updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub graph_id: String,
    pub current_task_id: String,
    /// Optional status message from the last executed task
    pub status_message: Option<String>,
    pub context: crate::context::Context,
    /// Optimistic-locking version, incremented by storage on every successful
    /// save. Defaults to 0 for new / pre-0.6 sessions.
    #[serde(default)]
    pub version: u64,
}

impl Session {
    /// Create a new session positioned at the given task.
    ///
    /// `graph_id` defaults to `"default"`; use [`Session::with_graph_id`] if
    /// you store multiple graphs.
    pub fn new_from_task(sid: String, task_name: &str) -> Self {
        Self {
            id: sid,
            graph_id: "default".to_string(),
            current_task_id: task_name.to_string(),
            status_message: None,
            context: Context::new(),
            version: 0,
        }
    }

    /// Set the graph this session belongs to.
    pub fn with_graph_id(mut self, graph_id: impl Into<String>) -> Self {
        self.graph_id = graph_id.into();
        self
    }
}

/// Trait for storing and retrieving graphs
#[async_trait]
pub trait GraphStorage: Send + Sync {
    async fn save(&self, id: String, graph: Arc<Graph>) -> Result<()>;
    async fn get(&self, id: &str) -> Result<Option<Arc<Graph>>>;
    async fn delete(&self, id: &str) -> Result<()>;
}

/// Trait for storing and retrieving sessions.
///
/// Implementations must enforce optimistic locking: `save` succeeds only when
/// the incoming session's `version` matches the stored version (or the
/// session does not exist yet), and increments the stored version on success.
/// A mismatch returns [`GraphError::SessionConflict`].
#[async_trait]
pub trait SessionStorage: Send + Sync {
    async fn save(&self, session: Session) -> Result<()>;
    async fn get(&self, id: &str) -> Result<Option<Session>>;
    async fn delete(&self, id: &str) -> Result<()>;
}

/// In-memory implementation of GraphStorage
pub struct InMemoryGraphStorage {
    graphs: DashMap<String, Arc<Graph>>,
}

impl Default for InMemoryGraphStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryGraphStorage {
    pub fn new() -> Self {
        Self {
            graphs: DashMap::new(),
        }
    }
}

#[async_trait]
impl GraphStorage for InMemoryGraphStorage {
    async fn save(&self, id: String, graph: Arc<Graph>) -> Result<()> {
        self.graphs.insert(id, graph);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Arc<Graph>>> {
        Ok(self.graphs.get(id).map(|entry| entry.clone()))
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.graphs.remove(id);
        Ok(())
    }
}

/// In-memory implementation of SessionStorage
pub struct InMemorySessionStorage {
    sessions: DashMap<String, Session>,
}

impl Default for InMemorySessionStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySessionStorage {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
        }
    }
}

#[async_trait]
impl SessionStorage for InMemorySessionStorage {
    async fn save(&self, mut session: Session) -> Result<()> {
        // Optimistic locking: the entry lock makes check-and-swap atomic.
        match self.sessions.entry(session.id.clone()) {
            dashmap::Entry::Occupied(mut occupied) => {
                let stored_version = occupied.get().version;
                if stored_version != session.version {
                    return Err(GraphError::SessionConflict(format!(
                        "Session '{}' was modified concurrently (stored version {}, \
                         attempted save from version {})",
                        session.id, stored_version, session.version
                    )));
                }
                session.version += 1;
                occupied.insert(session);
            }
            dashmap::Entry::Vacant(vacant) => {
                session.version += 1;
                vacant.insert(session);
            }
        }
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Session>> {
        Ok(self.sessions.get(id).map(|entry| entry.clone()))
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.sessions.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_version_conflict() {
        let storage = InMemorySessionStorage::new();

        let session = Session::new_from_task("s1".to_string(), "task");
        storage.save(session).await.unwrap(); // stored version becomes 1

        // Two clients load the same session (version 1)
        let a = storage.get("s1").await.unwrap().unwrap();
        let b = storage.get("s1").await.unwrap().unwrap();
        assert_eq!(a.version, 1);

        // First save wins...
        storage.save(a).await.unwrap(); // stored version becomes 2

        // ...second save conflicts
        let err = storage.save(b).await.unwrap_err();
        assert!(matches!(err, GraphError::SessionConflict(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn test_session_save_reload_cycle() {
        let storage = InMemorySessionStorage::new();

        let session = Session::new_from_task("s1".to_string(), "task");
        storage.save(session).await.unwrap();

        // Normal load -> save cycles keep working
        for expected_version in 1..4 {
            let s = storage.get("s1").await.unwrap().unwrap();
            assert_eq!(s.version, expected_version);
            storage.save(s).await.unwrap();
        }
    }
}
