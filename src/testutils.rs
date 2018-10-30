macro_rules! assert_eq_str {
    ($left:expr, $right:expr) => {{
        let left = &($left);
        let right = &$right;

        assert!(
            left == right,
            "`left == right` in line {}:\n{}\n{}",
            line!(),
            difference::Changeset::new("- left", "+ right", "\n"),
            difference::Changeset::new(&left, &right, "\n")
        )
    }};
}

macro_rules! assert_eq_dbg {
    ($left:expr, $right:expr) => {{
        let left = &$left;
        let right = &$right;

        assert!(
            left == right,
            "`left == right` in line {}:\n{}\n{}",
            line!(),
            difference::Changeset::new("- left", "+ right", "\n"),
            difference::Changeset::new(&format!("{:#?}", left), &format!("{:#?}", right), "\n")
        )
    }};
}
