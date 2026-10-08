use std::fmt;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use jalari::{SCHEMA_VERSION, Schema};

#[derive(Debug, Parser)]
#[command(
    name = "jalari",
    version,
    about = "Create, export and inspect the jalari tables"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    #[command(about = "Print the migration SQL without connecting to a database")]
    Sql(SqlArgs),
    #[command(about = "Apply the missing migrations to a database")]
    Migrate(MigrateArgs),
    #[command(about = "Show the installed and the latest schema version")]
    Status(StatusArgs),
}

#[derive(Debug, Args)]
pub(crate) struct SchemaArgs {
    #[arg(long, default_value = "jalari", help = "Schema that holds the tables")]
    pub(crate) schema: String,
    #[arg(long, help = "Prefix added to every table name")]
    pub(crate) prefix: Option<String>,
}

impl SchemaArgs {
    pub(crate) fn build(&self) -> jalari::Result<Schema> {
        match &self.prefix {
            Some(prefix) => Schema::prefixed(&self.schema, prefix),
            None => Schema::named(&self.schema),
        }
    }
}

#[derive(Args)]
pub(crate) struct DatabaseArgs {
    #[arg(
        long,
        env = "DATABASE_URL",
        hide_env_values = true,
        help = "PostgreSQL connection URL"
    )]
    pub(crate) database_url: String,
}

impl fmt::Debug for DatabaseArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DatabaseArgs { database_url: <redacted> }")
    }
}

#[derive(Debug, Args)]
pub(crate) struct SqlArgs {
    #[command(flatten)]
    pub(crate) schema: SchemaArgs,
    #[arg(long, value_parser = version_parser(), help = "First version to print [default: 1]")]
    pub(crate) from: Option<i32>,
    #[arg(long, value_parser = version_parser(), help = "Last version to print [default: latest]")]
    pub(crate) to: Option<i32>,
    #[arg(
        long,
        help = "Write one <version>.sql file per version instead of printing"
    )]
    pub(crate) out_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct MigrateArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseArgs,
    #[command(flatten)]
    pub(crate) schema: SchemaArgs,
    #[arg(long, value_parser = version_parser(), help = "Stop at this version [default: latest]")]
    pub(crate) to: Option<i32>,
    #[arg(long, help = "Print the SQL that would run without applying it")]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseArgs,
    #[command(flatten)]
    pub(crate) schema: SchemaArgs,
}

fn version_parser() -> clap::builder::RangedI64ValueParser<i32> {
    clap::value_parser!(i32).range(1..=i64::from(SCHEMA_VERSION))
}
