use jalari::{Job, JobResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct UsesHelper {
    count: u32,
}

#[jalari::job]
impl Job for UsesHelper {
    const NAME: &'static str = "uses_helper";

    async fn run(self) -> JobResult {
        let total = helper::compute() + self.count;
        serde_json::to_vec(&total)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Standalone {
    count: u32,
}

#[jalari::job]
impl Job for Standalone {
    const NAME: &'static str = "standalone";

    async fn run(self) -> JobResult {
        let _ = self.count + 1;
        Ok(())
    }
}
