mod args;
mod command;

use std::process::ExitCode;

use clap::Parser;

use args::{Cli, Command};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Sql(args) => command::sql(args),
        Command::Migrate(args) => command::migrate(args).await,
        Command::Status(args) => command::status(args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
