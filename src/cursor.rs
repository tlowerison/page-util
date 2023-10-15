use ::chrono::NaiveDateTime;
use ::std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(crate::async_graphql::InputObject)
)]
pub struct PageCursor {
    #[cfg_attr(
        any(
            feature = "async-graphql-4",
            feature = "async-graphql-5",
            feature = "async-graphql-6"
        ),
        graphql(validator(custom = "crate::GraphqlPaginationCountValidator"))
    )]
    pub count: u32,
    pub cursor: NaiveDateTime,
    pub direction: CursorDirection,
    /// defaults to false
    pub is_comparator_inclusive: Option<bool>,
}

impl Ord for PageCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for PageCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.cursor != other.cursor {
            self.cursor.partial_cmp(&other.cursor)
        } else if self.count != other.count {
            self.count.partial_cmp(&other.count)
        } else if self.direction != other.direction {
            self.direction.partial_cmp(&other.direction)
        } else if self.is_comparator_inclusive != other.is_comparator_inclusive {
            if other.is_comparator_inclusive.unwrap_or_default() {
                Some(Ordering::Less)
            } else {
                Some(Ordering::Greater)
            }
        } else {
            Some(Ordering::Equal)
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(crate::async_graphql::Enum)
)]
pub enum CursorDirection {
    Following,
    Preceding,
}

