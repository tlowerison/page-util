use ::std::cmp::Ordering;

#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;
#[cfg(feature = "async-graphql-6")]
use async_graphql_6 as async_graphql;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(async_graphql::InputObject)
)]
pub struct PageOffset {
    #[cfg_attr(
        any(
            feature = "async-graphql-4",
            feature = "async-graphql-5",
            feature = "async-graphql-6"
        ),
        graphql(validator(custom = "crate::GraphqlPaginationCountValidator"))
    )]
    pub count: u32,
    pub index: u32,
}

impl PageOffset {
    pub fn with_count(count: u32) -> Self {
        Self { count, index: 0 }
    }
}

impl Ord for PageOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for PageOffset {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        if self.index != rhs.index {
            self.index.partial_cmp(&rhs.index)
        } else {
            self.count.partial_cmp(&rhs.count)
        }
    }
}
