macro_rules! assert_eq_str {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left, right) => assert!(
                left == right,
                "`left == right` in line {}:\n{}\n{}",
                line!(),
                difference::Changeset::new("- left", "+ right", "\n"),
                difference::Changeset::new(&left, &right, "\n")
            ),
        }
    };
    ($left:expr, $right:expr,) => {
        assert_eq_str($left, $right)
    };
}

macro_rules! assert_eq_dbg {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left, right) => assert!(
                left == right,
                "`left == right` in line {}:\n{}\n{}",
                line!(),
                difference::Changeset::new("- left", "+ right", "\n"),
                difference::Changeset::new(&format!("{:#?}", left), &format!("{:#?}", right), "\n")
            ),
        }
    };
    ($left:expr, $right:expr,) => {
        assert_eq_dbg($left, $right)
    };
}
