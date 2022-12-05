use std::net::IpAddr;

pub struct ReplayProcessor<'a> {
    config: Arc<StoreConfig>,
    client_ip_addr: Option<IpAddr>,
}
