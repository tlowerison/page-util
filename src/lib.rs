#![feature(return_position_impl_trait_in_trait)]

#[macro_use]
extern crate cfg_if;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate serde;

mod cursor;
mod env;
mod offset;

pub use crate::cursor::*;
pub use crate::env::*;
pub use crate::offset::*;

cfg_if! { if #[cfg(feature = "diesel")] {
    mod diesel;
    pub use crate::diesel::*;
} }

cfg_if! { if #[cfg(any(
    feature = "async-graphql-4",
    feature = "async-graphql-5",
    feature = "async-graphql-6"
))] {
    mod graphql;
    pub(crate) use crate::graphql::*;

    #[cfg(feature = "async-graphql-4")]
    pub(crate) use async_graphql_4 as async_graphql;
    #[cfg(feature = "async-graphql-5")]
    pub(crate) use async_graphql_5 as async_graphql;
    #[cfg(feature = "async-graphql-6")]
    pub(crate) use async_graphql_6 as async_graphql;
} }

#[derive(
    AsVariant, AsVariantMut, Clone, Copy, Debug, Deserialize, Eq, Hash, IsVariant, Ord, PartialEq, PartialOrd, Serialize,
)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(async_graphql::OneofObject)
)]
pub enum Page {
    Cursor(PageCursor),
    Offset(PageOffset),
}

impl Page {
    pub fn count(&self) -> u32 {
        match self {
            Self::Cursor(cursor) => cursor.count,
            Self::Offset(offset) => offset.count,
        }
    }
}
