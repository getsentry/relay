use std::io;
use std::net::{ToSocketAddrs, UdpSocket};

use cadence::{BufferedUdpMetricSink, MetricResult, MetricSink};
use rand::distributions::{Distribution, Uniform};
use rand::rngs::ThreadRng;

#[derive(Debug)]
pub struct BufferedMultiUdpMetricSink {
    inner_sinks: Vec<BufferedUdpMetricSink>,
    rng: ThreadRng,
    distr: Uniform<usize>,
}

impl BufferedMultiUdpMetricSink {
    pub fn from<A>(
        sink_addr: A,
        sockets: Vec<UdpSocket>,
    ) -> MetricResult<BufferedMultiUdpMetricSink>
    where
        A: ToSocketAddrs,
    {
        let mut inner_sinks = Vec::new();

        for socket in sockets {
            let sink = BufferedUdpMetricSink::from(&sink_addr, socket)?;
            inner_sinks.push(sink);
        }

        let rng = rand::thread_rng();
        let distr = Uniform::from(0..inner_sinks.len());
        Ok(BufferedMultiUdpMetricSink {
            inner_sinks,
            rng,
            distr,
        })
    }
}

impl MetricSink for BufferedMultiUdpMetricSink {
    fn emit(&self, metric: &str) -> io::Result<usize> {
        let mut rng = self.rng;
        self.inner_sinks[self.distr.sample(&mut rng)].emit(metric)
    }

    fn flush(&self) -> io::Result<()> {
        for sink in &self.inner_sinks {
            sink.flush()?;
        }
        Ok(())
    }
}
