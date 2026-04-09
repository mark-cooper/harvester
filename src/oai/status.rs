use std::fmt;
use std::str::FromStr;

/// Defines a string-backed enum: derives sqlx::Type with explicit per-variant
/// renames, plus `as_str`, `Display`, and `FromStr`.
macro_rules! status_enum {
    (
        $(#[$attr:meta])*
        $vis:vis enum $name:ident {
            $($variant:ident => $str:literal),* $(,)?
        }
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
        #[sqlx(type_name = "text")]
        $vis enum $name {
            $(
                #[sqlx(rename = $str)]
                $variant,
            )*
        }

        impl $name {
            pub fn as_str(&self) -> &'static str {
                match self { $(Self::$variant => $str),* }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = String;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($str => Ok(Self::$variant),)*
                    _ => Err(format!(
                        concat!("unknown ", stringify!($name), ": {}"),
                        value
                    )),
                }
            }
        }
    };
}

status_enum! {
    /// Harvest lifecycle states for `oai_records.status`.
    ///
    /// Expected transitions:
    /// - import: `* -> pending|deleted` for changed records (`failed` records are intentionally sticky)
    /// - download: `pending -> available|failed`
    /// - metadata: `available -> parsed|failed`
    /// - retry: `failed -> pending` (batch)
    ///
    /// Index lifecycle ownership:
    /// - metadata success also resets index lifecycle (`index_status -> pending`)
    /// - import of deleted records also requeues index lifecycle (`index_status -> pending`)
    pub enum OaiRecordStatus {
        Available => "available",
        Deleted   => "deleted",
        Failed    => "failed",
        Parsed    => "parsed",
        Pending   => "pending",
    }
}

status_enum! {
    /// Index lifecycle states for `oai_records.index_status`.
    ///
    /// Expected transitions:
    /// - metadata success: `* -> pending` when a record becomes `parsed`
    /// - import deleted: `* -> pending` when a record becomes `deleted`
    /// - index run: `pending|index_failed -> indexed|index_failed`
    /// - purge run: `pending|purge_failed -> purged|purge_failed`
    /// - CLI reindex: `* -> pending` for matching `parsed|deleted` records
    pub enum OaiIndexStatus {
        IndexFailed => "index_failed",
        Indexed     => "indexed",
        Pending     => "pending",
        Purged      => "purged",
        PurgeFailed => "purge_failed",
    }
}
