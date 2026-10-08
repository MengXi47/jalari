use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::str::FromStr;
use std::time::Duration;

use jalari::{Migration, SCHEMA_VERSION, Schema};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};

use crate::args::{MigrateArgs, SqlArgs, StatusArgs};

type CommandResult<T = ()> = Result<T, Box<dyn Error>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn sql(args: &SqlArgs) -> CommandResult {
    let schema = args.schema.build()?;
    let from = args.from.unwrap_or(1);
    let to = args.to.unwrap_or(SCHEMA_VERSION);
    if from > to {
        return Err(format!("--from {from} is greater than --to {to}").into());
    }
    let selected = select(&schema, from, to);

    match &args.out_dir {
        Some(out_dir) => {
            fs::create_dir_all(out_dir)?;
            for migration in selected {
                let path = out_dir.join(format!("{:04}.sql", migration.version));
                fs::write(&path, migration.sql)?;
                eprintln!("wrote {}", path.display());
            }
        }
        None => print_sql(&selected)?,
    }
    Ok(())
}

pub(crate) async fn migrate(args: &MigrateArgs) -> CommandResult {
    let schema = args.schema.build()?;
    let to = args.to.unwrap_or(SCHEMA_VERSION);
    let pool = connect(&args.database.database_url).await?;
    let before = jalari::schema_version(&pool, &schema).await?;

    if args.dry_run {
        let mut statements = Vec::new();
        if !schema_exists(&pool, &schema).await? {
            statements.push(Migration {
                version: 0,
                sql: format!("CREATE SCHEMA \"{}\";\n", schema.name()),
            });
        }
        statements.extend(select(&schema, before.unwrap_or(0) + 1, to));
        if statements.is_empty() {
            eprintln!("{}: nothing to apply", describe(&schema, before));
        }
        return Ok(print_sql(&statements)?);
    }

    jalari::migrate_to(&pool, &schema, to).await?;
    let after = jalari::schema_version(&pool, &schema).await?;
    if after == before {
        println!("{}: nothing to apply", describe(&schema, before));
    } else {
        println!(
            "{}: migrated from {} to {}",
            schema_label(&schema),
            version_label(before),
            version_label(after)
        );
    }
    Ok(())
}

pub(crate) async fn status(args: &StatusArgs) -> CommandResult {
    let schema = args.schema.build()?;
    let pool = connect(&args.database.database_url).await?;
    let installed = jalari::schema_version(&pool, &schema).await?;

    let state = match installed {
        None => "not installed, run `jalari migrate`".to_owned(),
        Some(version) if version == SCHEMA_VERSION => "up to date".to_owned(),
        Some(version) if version > SCHEMA_VERSION => {
            "the database is newer than this CLI".to_owned()
        }
        Some(version) => format!("{} behind, run `jalari migrate`", SCHEMA_VERSION - version),
    };
    println!("schema:    {}", schema_label(&schema));
    println!("installed: {}", version_label(installed));
    println!("latest:    {SCHEMA_VERSION}");
    println!("status:    {state}");
    Ok(())
}

fn select(schema: &Schema, from: i32, to: i32) -> Vec<Migration> {
    jalari::migrations(schema)
        .into_iter()
        .filter(|migration| (from..=to).contains(&migration.version))
        .collect()
}

fn print_sql(migrations: &[Migration]) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    for (i, migration) in migrations.iter().enumerate() {
        if i > 0 {
            stdout.write_all(b"\n")?;
        }
        stdout.write_all(migration.sql.as_bytes())?;
    }
    stdout.flush()
}

async fn connect(database_url: &str) -> CommandResult<PgPool> {
    let options = PgConnectOptions::from_str(database_url).map_err(jalari::Error::from)?;
    let probe = tokio::time::timeout(CONNECT_TIMEOUT, PgConnection::connect_with(&options))
        .await
        .map_err(|_| format!("could not connect within {CONNECT_TIMEOUT:?}"))?
        .map_err(jalari::Error::from)?;
    probe.close().await.map_err(jalari::Error::from)?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect_with(options)
        .await
        .map_err(jalari::Error::from)?;
    Ok(pool)
}

async fn schema_exists(pool: &PgPool, schema: &Schema) -> jalari::Result<bool> {
    let exists =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(schema.name())
            .fetch_one(pool)
            .await?;
    Ok(exists)
}

fn describe(schema: &Schema, version: Option<i32>) -> String {
    format!("{} is at {}", schema_label(schema), version_label(version))
}

fn schema_label(schema: &Schema) -> String {
    if schema.prefix().is_empty() {
        schema.name().to_owned()
    } else {
        format!("{} (prefix {})", schema.name(), schema.prefix())
    }
}

fn version_label(version: Option<i32>) -> String {
    match version {
        Some(version) => format!("version {version}"),
        None => "no version (not installed)".to_owned(),
    }
}
