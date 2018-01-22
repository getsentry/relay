/// The project state snapshot represents a known server state of
/// a project.
///
/// This is generally used by an indirection of `ProjectState` which
/// manages a view over it which supports concurrent updates in the
/// background.
pub struct ProjectStateSnapshot {
    project_id: String,
}
