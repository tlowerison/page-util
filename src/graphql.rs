use crate::async_graphql;

#[cfg(any(
    feature = "async-graphql-4",
    feature = "async-graphql-5",
    feature = "async-graphql-6"
))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct GraphqlPaginationCountValidator;

#[cfg(feature = "async-graphql-4")]
impl async_graphql::CustomValidator<u32> for GraphqlPaginationCountValidator {
    fn check(&self, value: &u32) -> Result<(), String> {
        match crate::pagination_max_count() {
            None => Ok(()),
            Some(max) => match value <= max {
                true => Ok(()),
                false => Err(format!(
                    "DbPage count is too large, must be less than or equal to {max}"
                )),
            },
        }
    }
}

#[cfg(any(feature = "async-graphql-5", feature = "async-graphql-6"))]
impl async_graphql::CustomValidator<u32> for GraphqlPaginationCountValidator {
    fn check(&self, value: &u32) -> Result<(), async_graphql::InputValueError<u32>> {
        match crate::pagination_max_count() {
            None => Ok(()),
            Some(max) => match value <= max {
                true => Ok(()),
                false => Err(format!("DbPage count is too large, must be less than or equal to {max}").into()),
            },
        }
    }
}
