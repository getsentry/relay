use std::str::FromStr;
use std::fmt::Display;

use dialoguer::Input;
use failure::Error;

/// Prompts for a value that has a default.
pub fn prompt_value<V: FromStr + Display>(name: &str, v: &mut V) -> Result<(), Error>
where
    <V as FromStr>::Err: Display,
{
    loop {
        let s = Input::new(name).default(&v.to_string()).interact()?;
        match s.parse() {
            Ok(value) => {
                *v = value;
                return Ok(());
            }
            Err(err) => {
                println!("  invalid input: {}", err);
                continue;
            }
        }
    }
}

/// Prompts for a value without a default.
pub fn prompt_value_no_default<V: FromStr + Display>(
    name: &str,
    v: &mut Option<V>,
) -> Result<(), Error>
where
    <V as FromStr>::Err: Display,
{
    loop {
        let s = Input::new(name).interact()?;
        match s.parse() {
            Ok(value) => {
                *v = Some(value);
                return Ok(());
            }
            Err(err) => {
                println!("  invalid input: {}", err);
                continue;
            }
        }
    }
}
