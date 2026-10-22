#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Lock {
    pub(crate) packages: Vec<LockedPackage>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LockedPackage {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) source: Option<String>,
    pub(crate) dependencies: Vec<String>,
}

impl Lock {
    pub(crate) fn parse(text: &str) -> Self {
        let mut packages = Vec::new();
        let mut current: Option<LockedPackage> = None;
        let mut in_dependencies = false;
        for line in text.lines().map(str::trim) {
            if in_dependencies {
                let (body, closed) = match line.split_once(']') {
                    Some((body, _)) => (body, true),
                    None => (line, false),
                };
                if let Some(package) = current.as_mut() {
                    package.dependencies.extend(quoted(body));
                }
                in_dependencies = !closed;
                continue;
            }
            if line.starts_with('[') {
                packages.extend(current.take());
                if line == "[[package]]" {
                    current = Some(LockedPackage::default());
                }
                continue;
            }
            let Some(package) = current.as_mut() else {
                continue;
            };
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let value = value.trim();
            match key.trim() {
                "name" => package.name = unquote(value),
                "version" => package.version = unquote(value),
                "source" => package.source = Some(unquote(value)),
                "dependencies" => {
                    let body = value.strip_prefix('[').unwrap_or(value);
                    let (body, closed) = match body.split_once(']') {
                        Some((body, _)) => (body, true),
                        None => (body, false),
                    };
                    package.dependencies.extend(quoted(body));
                    in_dependencies = !closed;
                }
                _ => {}
            }
        }
        packages.extend(current);
        Self { packages }
    }

    pub(crate) fn pin(&self, owner: &str, package: &str) -> Option<String> {
        let wanted = self
            .packages
            .iter()
            .filter(|locked| locked.name == owner && locked.source.is_none())
            .flat_map(|locked| locked.dependencies.iter())
            .find(|dependency| dependency.split_whitespace().next() == Some(package));
        let locked = match wanted {
            Some(dependency) => {
                let mut parts = dependency.split_whitespace();
                parts.next();
                let version = parts.next();
                let source = parts
                    .next()
                    .map(|source| source.trim_start_matches('(').trim_end_matches(')'));
                self.packages.iter().find(|locked| {
                    locked.name == package
                        && version.is_none_or(|version| locked.version == version)
                        && source.is_none_or(|source| locked.source.as_deref() == Some(source))
                })?
            }
            None => {
                let mut named = self.packages.iter().filter(|locked| locked.name == package);
                let first = named.next()?;
                if named.next().is_some() {
                    return None;
                }
                first
            }
        };
        Some(format!(
            "{} {} {}",
            locked.name,
            locked.version,
            locked.source.as_deref().unwrap_or("")
        ))
    }
}

fn unquote(value: &str) -> String {
    value.trim().trim_matches('"').to_owned()
}

fn quoted(body: &str) -> Vec<String> {
    body.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(unquote)
        .collect()
}
