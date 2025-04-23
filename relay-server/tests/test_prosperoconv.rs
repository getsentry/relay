// TODO: Although I hate it I think this is the best way to ensure that these tests are not run with --all-features
#[cfg(all(feature = "prosperoconv", not(feature = "not-prosperoconv")))]
mod tests {
    use prosperoconv::process_dump;
    use sentry::{protocol::Event, Envelope};
    use std::fs;

    #[test]
    fn test_prosperoconv() {
        let file =
            fs::File::open("../tests/integration/fixtures/native/playstation.prosperodmp").unwrap();

        // TODO: Make this test more elaborate
        process_dump(&mut Envelope::new(), Event::new(), file).unwrap();
    }
}
