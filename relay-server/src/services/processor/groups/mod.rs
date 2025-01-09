//! How to create a new processing group:
//!
//! 1. Create a new module file for your group (e.g., `my_group.rs`)
//!
//! 2. Define your group type and implement necessary traits:
//!    ```text
//!    use crate::group;
//!
//!    // This creates MyGroup struct and implements required traits
//!    group!(MyGroup, MyGroupType);
//!    ```
//!
//! 3. Create a processing implementation:
//!    ```text
//!    pub struct ProcessMyGroup<'a> {
//!        payload: BasePayload<'a, MyGroupType>,
//!        processor: Arc<InnerProcessor>,
//!        rate_limits: Arc<RateLimits>,
//!        project_info: Arc<ProjectInfo>,
//!        project_id: ProjectId,
//!    }
//!
//!    impl<'a> ProcessGroup<'a> for ProcessMyGroup<'a> {
//!        type Group = MyGroupType;
//!        type Payload = BasePayload<'a, Self::Group>;
//!
//!        fn create(params: GroupParams<'a, Self::Group>) -> Self {
//!            Self {
//!                payload: Self::Payload::no_event(params.managed_envelope),
//!                processor: params.processor,
//!                rate_limits: params.rate_limits,
//!                project_info: params.project_info,
//!                project_id: params.project_id,
//!            }
//!        }
//!
//!        fn process(self) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError> {
//!            // Implement your processing logic here
//!        }
//!    }
//!    ```
//!
//! 4. Add your group to ProcessingGroup enum in processor.rs:
//!    ```text
//!    pub enum ProcessingGroup {
//!        MyGroup,
//!        // ... other variants
//!    }
//!    ```
//!
//! 5. Register your group in this macro call:
//!    ```text
//!    build_process_group!((MyGroup, ProcessMyGroup));
//!    ```
//!
//! The macro will automatically:
//! - Create a function to check if the group supports new processing
//! - Generate the processing logic to handle your group type
//! - Wire up error handling and metrics extraction

mod base;
mod base_payload;
mod check_in;

pub use base::*;
