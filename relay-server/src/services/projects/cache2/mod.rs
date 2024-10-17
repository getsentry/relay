mod handle;
mod refresh;
mod service;
mod state;

pub use self::handle::ProjectCacheHandle;
pub use self::service::{ProjectCache, ProjectCacheService};

// pub struct ProjectCacheService {
//     inner: Inner,
// }
//
// impl ProjectCacheService {
//     fn handle(&mut self, message: super::cache::ProjectCache) {
//         match message {
//             super::cache::ProjectCache::Get(_, _) => todo!(),
//             super::cache::ProjectCache::GetCached(_, _) => todo!(),
//             super::cache::ProjectCache::CheckEnvelope(_, _) => todo!(),
//             super::cache::ProjectCache::ValidateEnvelope(_) => todo!(),
//             super::cache::ProjectCache::UpdateRateLimits(_) => todo!(),
//             super::cache::ProjectCache::ProcessMetrics(_) => todo!(),
//             super::cache::ProjectCache::AddMetricMeta(_) => todo!(),
//             super::cache::ProjectCache::FlushBuckets(_) => todo!(),
//
//             // Spooling
//             super::cache::ProjectCache::UpdateSpoolIndex(_) => todo!(),
//             super::cache::ProjectCache::RefreshIndexCache(_) => todo!(),
//
//             // Sent by the envelope buffer to force update a project.
//             // Eventually the envelope buffer needs a response whether the project is ready/not
//             // ready.
//             //
//             // Like GetCached but with less params and no response.
//             super::cache::ProjectCache::UpdateProject(_) => todo!(),
//
//
//             // This is the typical fetch.
//             super::cache::ProjectCache::RequestUpdate(_) => todo!(),
//         }
//     }
//
// }
