//! Re-exports jalari for crates that do not depend on it.

#[doc(hidden)]
pub use jalari as __jalari;

pub use facade_macros::job_attr;

/// Declares a job through this crate, so the defining crate never names the path to jalari.
#[macro_export]
macro_rules! job {
    (($($arguments:tt)*) $($item:tt)*) => {
        #[$crate::__jalari::job(crate = $crate::__jalari, $($arguments)*)]
        $($item)*
    };
}
