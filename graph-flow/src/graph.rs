use dashmap::DashMap;
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::{
    context::Context,
    error::{GraphError, Result},
    storage::Session,
    task::{NextAction, Task, TaskResult},
};

/// Type alias for edge condition functions
pub type EdgeCondition = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

/// Edge between tasks in the graph
#[derive(Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub condition: Option<EdgeCondition>,
}

/// A graph of tasks that can be executed
pub struct Graph {
    pub id: String,
    tasks: DashMap<String, Arc<dyn Task>>,
    /// Outgoing edges indexed by source task id. Within a bucket, edges keep
    /// insertion order so the first matching conditional edge (or the first
    /// unconditional fallback) wins deterministically.
    edges: DashMap<String, Vec<Edge>>,
    start_task_id: Mutex<Option<String>>,
    task_timeout: Duration,
    /// Maximum number of chained `ContinueAndExecute` steps allowed within a
    /// single `execute_session` call. `None` (default) means unlimited.
    max_execution_steps: Option<usize>,
}

impl Graph {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            tasks: DashMap::new(),
            edges: DashMap::new(),
            start_task_id: Mutex::new(None),
            task_timeout: Duration::from_secs(300), // Default 5 minute timeout
            max_execution_steps: None,
        }
    }

    /// Set the timeout duration for task execution
    pub fn set_task_timeout(&mut self, timeout: Duration) {
        self.task_timeout = timeout;
    }

    /// Set the maximum number of chained `ContinueAndExecute` steps allowed
    /// within a single `execute_session` call. Guards against accidental
    /// infinite loops in cyclic graphs. `None` means unlimited.
    pub fn set_max_execution_steps(&mut self, max: Option<usize>) {
        self.max_execution_steps = max;
    }

    /// Add a task to the graph
    pub fn add_task(&self, task: Arc<dyn Task>) -> &Self {
        let task_id = task.id().to_string();
        let is_first = self.tasks.is_empty();
        self.tasks.insert(task_id.clone(), task);

        // Set as start task if it's the first one
        if is_first {
            *self.start_task_id.lock().unwrap() = Some(task_id);
        }

        self
    }

    /// Set the starting task.
    ///
    /// Logs a warning and leaves the current start task unchanged if the
    /// given task id has not been added to the graph.
    pub fn set_start_task(&self, task_id: impl Into<String>) -> &Self {
        let task_id = task_id.into();
        if self.tasks.contains_key(&task_id) {
            *self.start_task_id.lock().unwrap() = Some(task_id);
        } else {
            tracing::warn!(
                task_id = %task_id,
                "set_start_task called with unknown task id - start task unchanged"
            );
        }
        self
    }

    /// Add an edge between tasks
    pub fn add_edge(&self, from: impl Into<String>, to: impl Into<String>) -> &Self {
        let from = from.into();
        self.edges.entry(from.clone()).or_default().push(Edge {
            from,
            to: to.into(),
            condition: None,
        });
        self
    }

    /// Add a conditional edge with an explicit `else` branch.
    /// `yes` is taken when `condition(ctx)` returns `true`; otherwise `no` is chosen.
    pub fn add_conditional_edge<F>(
        &self,
        from: impl Into<String>,
        condition: F,
        yes: impl Into<String>,
        no: impl Into<String>,
    ) -> &Self
    where
        F: Fn(&Context) -> bool + Send + Sync + 'static,
    {
        let from = from.into();
        let predicate: EdgeCondition = Arc::new(condition);

        let mut bucket = self.edges.entry(from.clone()).or_default();

        // "yes" branch
        bucket.push(Edge {
            from: from.clone(),
            to: yes.into(),
            condition: Some(predicate),
        });

        // "else" branch (unconditional fallback)
        bucket.push(Edge {
            from,
            to: no.into(),
            condition: None,
        });

        self
    }

    /// Execute the graph with session management.
    ///
    /// Runs the session's current task. If the task returns
    /// [`NextAction::ContinueAndExecute`], execution proceeds to the next task
    /// within the same call, repeating until a task pauses, waits for input,
    /// ends, or the configured `max_execution_steps` limit is hit.
    ///
    /// Note: the session is only persisted by the *caller* (e.g.
    /// [`crate::FlowRunner::run`]) after this method returns, so context
    /// updates made by intermediate `ContinueAndExecute` steps are not durable
    /// until the whole chain finishes.
    #[allow(deprecated)] // NextAction::GoBack must still be matched until removed
    pub async fn execute_session(&self, session: &mut Session) -> Result<ExecutionResult> {
        tracing::info!(
            graph_id = %self.id,
            session_id = %session.id,
            current_task = %session.current_task_id,
            "Starting graph execution"
        );

        let mut steps = 0usize;

        loop {
            let result = self
                .execute_single_task(&session.current_task_id, session.context.clone())
                .await?;

            // Update session status message if provided
            session.status_message = result.status_message.clone();

            match &result.next_action {
                NextAction::Continue | NextAction::ContinueAndExecute => {
                    let Some(next_task_id) = self.find_next_task(&result.task_id, &session.context)
                    else {
                        // No outgoing edge found, stay at current task
                        session.current_task_id = result.task_id.clone();
                        return Ok(ExecutionResult {
                            response: result.response,
                            status: ExecutionStatus::Paused {
                                next_task_id: result.task_id.clone(),
                                reason: "No outgoing edge found from current task".to_string(),
                            },
                        });
                    };

                    session.current_task_id = next_task_id.clone();

                    if result.next_action == NextAction::ContinueAndExecute {
                        steps += 1;
                        if let Some(max) = self.max_execution_steps
                            && steps >= max
                        {
                            return Err(GraphError::TaskExecutionFailed(format!(
                                "Aborted after {} chained ContinueAndExecute steps \
                                 (max_execution_steps = {}). Possible cycle in graph '{}'",
                                steps, max, self.id
                            )));
                        }
                        // Execute the next task immediately within this call
                        continue;
                    }

                    return Ok(ExecutionResult {
                        response: result.response,
                        status: ExecutionStatus::Paused {
                            next_task_id,
                            reason: "Task completed, continuing to next task".to_string(),
                        },
                    });
                }
                NextAction::WaitForInput => {
                    // Stay at the current task
                    session.current_task_id = result.task_id.clone();
                    return Ok(ExecutionResult {
                        response: result.response,
                        status: ExecutionStatus::WaitingForInput,
                    });
                }
                NextAction::End => {
                    session.current_task_id = result.task_id.clone();
                    return Ok(ExecutionResult {
                        response: result.response,
                        status: ExecutionStatus::Completed,
                    });
                }
                NextAction::GoTo(target_id) => {
                    if !self.tasks.contains_key(target_id) {
                        return Err(GraphError::TaskNotFound(target_id.clone()));
                    }
                    session.current_task_id = target_id.clone();
                    return Ok(ExecutionResult {
                        response: result.response,
                        status: ExecutionStatus::Paused {
                            next_task_id: target_id.clone(),
                            reason: "Task requested jump to specific task".to_string(),
                        },
                    });
                }
                NextAction::GoBack => {
                    // Not implemented: stay at current task and wait for input
                    session.current_task_id = result.task_id.clone();
                    return Ok(ExecutionResult {
                        response: result.response,
                        status: ExecutionStatus::WaitingForInput,
                    });
                }
            }
        }
    }

    /// Execute a single task without following Continue actions
    async fn execute_single_task(&self, task_id: &str, context: Context) -> Result<TaskResult> {
        tracing::debug!(
            task_id = %task_id,
            "Executing single task"
        );

        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| GraphError::TaskNotFound(task_id.to_string()))?
            .clone();

        // Execute task with timeout
        let mut result = match timeout(self.task_timeout, task.run(context)).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                return Err(GraphError::TaskExecutionFailed(format!(
                    "Task '{}' failed: {}",
                    task_id, e
                )));
            }
            Err(_) => {
                return Err(GraphError::TaskExecutionFailed(format!(
                    "Task '{}' timed out after {:?}",
                    task_id, self.task_timeout
                )));
            }
        };

        // Set the task_id in the result to track which task generated it
        result.task_id = task_id.to_string();

        Ok(result)
    }

    /// Execute the graph starting from a specific task.
    ///
    /// Note that this method's `NextAction` semantics differ from
    /// [`Graph::execute_session`]: `Continue` recurses into the next task when
    /// the current task produced no response, and `ContinueAndExecute` stops.
    #[deprecated(
        since = "0.5.2",
        note = "use execute_session (or FlowRunner::run) instead; this method's NextAction \
                semantics are inconsistent with the documented behavior and it will be removed \
                in a future release"
    )]
    pub async fn execute(&self, task_id: &str, context: Context) -> Result<TaskResult> {
        let result = self.execute_single_task(task_id, context.clone()).await?;

        // Handle next action
        match &result.next_action {
            NextAction::Continue => {
                // If this task has a response, stop here and don't continue to next task
                // This allows the response to be returned to the user
                if result.response.is_some() {
                    Ok(result)
                } else {
                    // Find the next task based on edges
                    if let Some(next_task_id) = self.find_next_task(task_id, &context) {
                        Box::pin(self.execute(&next_task_id, context)).await
                    } else {
                        Ok(result)
                    }
                }
            }
            NextAction::GoTo(target_id) => {
                if self.tasks.contains_key(target_id) {
                    Box::pin(self.execute(target_id, context)).await
                } else {
                    Err(GraphError::TaskNotFound(target_id.clone()))
                }
            }
            _ => Ok(result),
        }
    }

    /// Find the next task based on edges and conditions
    pub fn find_next_task(&self, current_task_id: &str, context: &Context) -> Option<String> {
        let edges = self.edges.get(current_task_id)?;

        let mut fallback: Option<&Edge> = None;
        for edge in edges.iter() {
            match &edge.condition {
                Some(pred) if pred(context) => return Some(edge.to.clone()),
                None if fallback.is_none() => fallback = Some(edge),
                _ => {}
            }
        }
        fallback.map(|edge| edge.to.clone())
    }

    /// Get the start task ID
    pub fn start_task_id(&self) -> Option<String> {
        self.start_task_id.lock().unwrap().clone()
    }

    /// Get a task by ID
    pub fn get_task(&self, task_id: &str) -> Option<Arc<dyn Task>> {
        self.tasks.get(task_id).map(|entry| entry.clone())
    }
}

/// Builder for creating graphs
pub struct GraphBuilder {
    graph: Graph,
}

impl GraphBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            graph: Graph::new(id),
        }
    }

    pub fn add_task(self, task: Arc<dyn Task>) -> Self {
        self.graph.add_task(task);
        self
    }

    pub fn add_edge(self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.graph.add_edge(from, to);
        self
    }

    pub fn add_conditional_edge<F>(
        self,
        from: impl Into<String>,
        condition: F,
        yes: impl Into<String>,
        no: impl Into<String>,
    ) -> Self
    where
        F: Fn(&Context) -> bool + Send + Sync + 'static,
    {
        self.graph.add_conditional_edge(from, condition, yes, no);
        self
    }

    pub fn set_start_task(self, task_id: impl Into<String>) -> Self {
        self.graph.set_start_task(task_id);
        self
    }

    /// Set the timeout applied to each task execution (default: 5 minutes).
    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.graph.set_task_timeout(timeout);
        self
    }

    /// Limit the number of chained `ContinueAndExecute` steps within a single
    /// `execute_session` call, guarding against infinite loops in cyclic
    /// graphs (default: unlimited).
    pub fn with_max_execution_steps(mut self, max: usize) -> Self {
        self.graph.set_max_execution_steps(Some(max));
        self
    }

    pub fn build(self) -> Graph {
        // Validate the graph before returning
        if self.graph.tasks.is_empty() {
            tracing::warn!("Building graph with no tasks");
        }

        let mut connected_tasks = std::collections::HashSet::new();
        for bucket in self.graph.edges.iter() {
            for edge in bucket.value() {
                connected_tasks.insert(edge.from.clone());
                connected_tasks.insert(edge.to.clone());

                // Warn about edges pointing at tasks that were never added
                for endpoint in [&edge.from, &edge.to] {
                    if !self.graph.tasks.contains_key(endpoint) {
                        tracing::warn!(
                            task_id = %endpoint,
                            "Edge references a task that has not been added to the graph"
                        );
                    }
                }
            }
        }

        // Check for orphaned tasks (tasks with no incoming or outgoing edges)
        if self.graph.tasks.len() > 1 {
            for task in self.graph.tasks.iter() {
                if !connected_tasks.contains(task.key()) {
                    tracing::warn!(
                        task_id = %task.key(),
                        "Task has no edges - it may be unreachable"
                    );
                }
            }
        }

        self.graph
    }
}

/// Status of graph execution
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub response: Option<String>,
    pub status: ExecutionStatus,
}

#[derive(Debug, Clone)]
pub enum ExecutionStatus {
    /// Paused, will continue automatically to the specified next task
    Paused {
        next_task_id: String,
        reason: String,
    },
    /// Waiting for user input to continue
    WaitingForInput,
    /// Workflow completed successfully
    Completed,
    /// Error occurred during execution
    ///
    /// Note: the engine currently reports failures via `Err(GraphError)`
    /// rather than this variant; it is kept for API compatibility and may be
    /// removed in a future major release.
    Error(String),
}
