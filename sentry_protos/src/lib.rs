pub mod events {
    pub mod v1 {
        include!("sentry_protos.kafka.events.v1.rs");
    }
}

pub mod options {
    pub mod v1 {
        include!("sentry_protos.options.v1.rs");
    }
}

pub mod seer {
    pub mod v1 {
        include!("sentry_protos.seer.v1.rs");
    }
}

pub mod relay {
    pub mod v1 {
        include!("sentry_protos.relay.v1.rs");
    }
}

pub mod sentry {
    pub mod v1 {
        include!("sentry_protos.sentry.v1.rs");
    }
}

pub mod taskbroker {
    pub mod v1 {
        include!("sentry_protos.taskbroker.v1.rs");
    }
}

pub mod snuba {
    pub mod v1 {
        include!("sentry_protos.snuba.v1.rs");
    }
}
