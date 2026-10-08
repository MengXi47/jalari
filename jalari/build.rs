use std::fmt::Write;
use std::path::Path;
use std::{env, fs};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let dir = Path::new(&manifest_dir).join("migrations");
    println!("cargo::rerun-if-changed={}", dir.display());

    let mut migrations: Vec<(i32, String)> = Vec::new();
    for entry in fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.ends_with(".sql") {
            continue;
        }
        migrations.push((parse_version(name), path.to_str().unwrap().to_owned()));
    }
    migrations.sort();

    let versions: Vec<i32> = migrations.iter().map(|(version, _)| *version).collect();
    let expected: Vec<i32> = (1..=migrations.len() as i32).collect();
    assert!(
        versions == expected,
        "migration versions in {} must run 1, 2, 3, ... without gaps, found {versions:?}",
        dir.display()
    );

    let mut code = format!("const TEMPLATES: [(i32, &str); {}] = [\n", migrations.len());
    for (version, path) in &migrations {
        writeln!(code, "    ({version}, include_str!({path:?})),").unwrap();
    }
    code.push_str("];\n");

    let out_dir = env::var("OUT_DIR").unwrap();
    fs::write(Path::new(&out_dir).join("migrations.rs"), code).unwrap();
}

fn parse_version(name: &str) -> i32 {
    let parsed = name
        .strip_suffix(".sql")
        .and_then(|stem| stem.split_once('_'))
        .filter(|(digits, description)| {
            !digits.is_empty()
                && digits.bytes().all(|b| b.is_ascii_digit())
                && !description.is_empty()
        })
        .and_then(|(digits, _)| digits.parse().ok());
    match parsed {
        Some(version) => version,
        None => panic!("migration file {name:?} must be named <version>_<description>.sql"),
    }
}
