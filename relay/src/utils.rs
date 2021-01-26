use std::fmt::Display;
use std::str::FromStr;

use console::Style;
use dialoguer::Input;
use failure::Error;
use lazy_static::lazy_static;

use dialoguer::theme::{ColorfulTheme, Theme};

lazy_static! {
    static ref THEME: ColorfulTheme = ColorfulTheme {
        values_style: Style::new().cyan().dim(),
        indicator_style: Style::new().cyan().bold(),
        yes_style: Style::new().cyan().dim(),
        no_style: Style::new().cyan().dim(),
        ..ColorfulTheme::default()
    };
}

/// Returns the theme to use.
pub fn get_theme() -> &'static dyn Theme {
    &*THEME
}

/// Prompts for a value that has a default.
pub fn prompt_value<V: FromStr + Display>(name: &str, v: &mut V) -> Result<(), Error>
where
    <V as FromStr>::Err: Display,
{
    loop {
        let s = Input::with_theme(get_theme())
            .with_prompt(name)
            .default(v.to_string())
            .interact()?;
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
pub fn prompt_value_no_default<V: FromStr + Display>(name: &str) -> Result<V, Error>
where
    <V as FromStr>::Err: Display,
{
    loop {
        let s: String = Input::with_theme(get_theme())
            .with_prompt(name)
            .interact()?;
        match s.parse() {
            Ok(value) => {
                return Ok(value);
            }
            Err(err) => {
                println!("  invalid input: {}", err);
                continue;
            }
        }
    }
}
