// TODO: Although I hate it I think this is the best way to ensure that these tests are not run with --all-features
#[cfg(all(feature = "prosperoconv", not(feature = "not-prosperoconv")))]
mod tests {
    // TODO: This is a place holder test, replace with a better one and update the CI to run them.
    use relay_prosperoconv::verify;

    #[test]
    fn test_prosperoconv_verify() {
        // Call the verify function and check its return value
        let result = verify();
        assert_eq!(result, "library successfully included");
    }
}
