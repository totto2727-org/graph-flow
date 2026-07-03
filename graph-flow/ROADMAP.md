# graph-flow API Roadmap

This document tracks **planned breaking changes** identified during the engine
code review (2026-07). The review's non-breaking fixes shipped in `0.5.2`;
everything below changes the public API and is deferred to the next
semver-major (or `0.x+1`) release so it can land as one coordinated batch.

Status legend: 🔶 = mitigated in 0.5.2 (deprecation / additive API / docs), ⬜ = not started.

## 1. 🔶 Unify `Graph::execute` semantics (or remove it)

`Graph::execute` treats `NextAction` inconsistently with `execute_session`:
`Continue` recurses into the next task (when there is no response) while
`ContinueAndExecute` does nothing. `execute_session` implements the documented
behavior.

- **0.5.2**: `Graph::execute` is `#[deprecated]` and now applies the task
  timeout like `execute_session` does.
- **Next major**: remove `Graph::execute` entirely, or reimplement it as a thin
  session-less wrapper with the documented `NextAction` semantics.

## 2. 🔶 `Context::set` should return `Result` instead of panicking

`set` / `set_sync` panic when the value cannot be serialized to JSON (e.g. a
map with non-string keys).

- **0.5.2**: panic is documented; fallible `try_set` / `try_set_sync` added.
- **Next major**: make `set` / `set_sync` return `Result<()>` and remove the
  `try_*` variants.

## 3. ⬜ Remove fake-async `Context` methods (or the `_sync` duplicates)

`Context::set`, `get`, `remove`, `clear` and all chat-history methods are
`async` but never await; `get` and `get_sync` are identical. One of the two
families should go:

- Preferred: make the data methods sync (drop `async`), delete `get_sync` /
  `set_sync`, and keep `async` only where a future implementation could
  genuinely suspend.
- This breaks every `.await` call site, so it must be a major release.

## 4. ⬜ Builder/graph mutators should report failure

`Graph::set_start_task` (and `GraphBuilder::set_start_task`) silently no-ops
on an unknown task id (0.5.2 adds a `tracing::warn!`). `NextAction::GoTo` to a
missing task only fails at runtime.

- **Next major**: return `Result` from `set_start_task`, and make
  `GraphBuilder::build()` return `Result<Graph>` so edge/task reference
  validation (currently warnings) can be a hard error.

## 5. ⬜ Remove `ExecutionStatus::Error`

The engine reports failures via `Err(GraphError)`; the `Error(String)` variant
is never constructed, yet every consumer must match on it.

- **Next major**: delete the variant (or start using it for recoverable task
  errors - decide one way, not both).

## 6. 🔶 Remove `NextAction::GoBack`

Never implemented - behaves like `WaitForInput`.

- **0.5.2**: `#[deprecated]`.
- **Next major**: remove the variant. Note: it derives `Serialize`/
  `Deserialize`, so check no persisted `TaskResult` values contain it before
  removal.

## 7. ⬜ Make `Graph` immutable after `build()`

`Graph` uses `DashMap`/`Mutex` interior mutability only so `GraphBuilder` can
mutate through `&self`. After `build()` the graph is read-only.

- **Next major**: move the mutating methods (`add_task`, `add_edge`,
  `add_conditional_edge`, `set_start_task`) onto `GraphBuilder` only; `Graph`
  holds plain `HashMap`s with no locks. Removing `Graph`'s `pub` mutators is
  the breaking part. (0.5.2 already indexes edges by source task, so the
  performance motivation is mostly addressed.)

## 8. ⬜ `ChatHistory` ring-buffer storage

At the message cap, every append shifts the whole `Vec` (O(n)). A `VecDeque`
fixes this but `ChatHistory::messages() -> &[SerializableMessage]` cannot be
kept as-is (a deque is not contiguous).

- **Next major**: switch to `VecDeque` and change `messages()` to return an
  iterator (or `(&[..], &[..])` slices), or accept `&mut self` for
  `make_contiguous`.

## 9. ⬜ Optimistic locking for `PostgresSessionStorage`

Saves are last-write-wins; concurrent runs of the same session lose updates
(0.5.2 documents this on `FlowRunner::run` and the storage type). Real
protection needs a `version` column checked on update and a new error variant
(e.g. `GraphError::SessionConflict`) - a schema and behavior change.

## 10. ⬜ Type `Session::id` (UUID requirement)

`Session::id` is a free-form `String`, but the Postgres schema casts it to
`UUID`, so non-UUID ids fail at runtime (documented in 0.5.2). Either type the
field as `uuid::Uuid` (breaking) or change the column to `TEXT` (migration).
Also reconsider `Session::new_from_task` hardcoding `graph_id = "default"`.
