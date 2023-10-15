use crate::{CursorDirection, Page, PageCursor, PageOffset};
use ::diesel::backend::Backend;
use ::diesel::helper_types::{Asc, Desc};
use ::diesel::query_builder::*;
use ::diesel::query_dsl::methods::{FilterDsl, OrderDsl};
use ::diesel::serialize::ToSql;
use ::diesel::sql_types::{BigInt, Nullable, Text};
use ::diesel::Column;
use ::diesel::{prelude::*, FromSqlRow};
use ::dyn_clone::clone_box;
use ::either::Either::*;
use ::itertools::intersperse;
use ::itertools::Itertools;
use ::std::hash::Hash;
use ::std::marker::PhantomData;
use ::uuid::Uuid;

static PAGE_ID_COLUMN_NAME: &str = "page_id";
static OFFSET_COLUMN_NAME: &str = "offset";
static OFFSET_SUBQUERY1_ALIAS: &str = "q1";
static OFFSET_SUBQUERY2_ALIAS: &str = "q2";

pub type Paged<T> = (T, Option<i64>, Option<String>);

#[derive(Debug, Clone)]
pub struct PaginatedQuery<QS: ?Sized, Q0, Q2, P = (), PO = ()> {
    query: Q0,
    cursor_queries: Vec<Q2>,
    /// must include DbPageOffset so that its `left` and `right` fields
    /// can be referenced in `QueryFragment::walk_ast` (the values
    /// cannot be created within the scope of the method)
    pages: Option<Vec<DbPage<QS>>>,
    partition: Option<P>,
    partition_order: Option<PO>,
}

#[derive(Clone, Debug, FromSqlRow)]
pub struct PaginatedResult<T> {
    pub result: T,
    pub row_number: Option<i64>,
    pub page_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, From)]
pub struct PaginatedQueryWrapper<T, DB>(pub T, PhantomData<DB>);

pub trait Paginate<DB: Backend, QS: ?Sized>: AsQuery + Clone + Send + Sized {
    type Output: Paginated<DB>;

    fn paginate<P: AsDbPage<QS>>(self, page: P) -> PaginatedQueryWrapper<Self::Output, DB>;

    fn multipaginate<P, I>(self, pages: I) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: for<'a> DbPageRef<'a, QS>,
        I: Iterator<Item = P>;
}

impl<DB, QS, Q0, Q1, Q2, SqlType> Paginate<DB, QS> for Q0
where
    DB: Backend,
    QS: diesel::QuerySource,
    Q0: Clone
        + Query<SqlType = SqlType>
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<QS>>, Output = Q1>,
    Q1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<QS>>, Output = Q2>,
    Q2: Query<SqlType = SqlType> + QueryFragment<DB> + Send,
    SqlType: 'static,
{
    type Output = PaginatedQuery<QS, Q0, Q2, (), ()>;

    fn paginate<P>(self, page: P) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: AsDbPage<QS>,
    {
        let query = self.as_query();
        let pages = page.as_page().map(|page| vec![page.clone()]);

        let mut cursor_queries = Vec::<Q2>::default();
        if let Some(pages) = pages.as_ref() {
            let DbPageSplit { cursor_indices, .. } = DbPage::split(pages);

            let page_cursors: Vec<&DbPageCursor<QS>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            for page_cursor in page_cursors.iter() {
                let query: Q0 = query.clone();
                let query: Q1 = query.filter(clone_box(&*page_cursor.column.cursor_comparison_expression));
                let query: Q2 = query.order(clone_box(&*page_cursor.column.order_by_expression));
                cursor_queries.push(query);
            }
        }

        PaginatedQueryWrapper(
            PaginatedQuery::<QS, Q0, Q2, (), ()> {
                query,
                cursor_queries,
                pages,
                partition: None,
                partition_order: None,
            },
            Default::default(),
        )
    }

    fn multipaginate<P, I>(self, pages: I) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: for<'a> DbPageRef<'a, QS>,
        I: Iterator<Item = P>,
    {
        let query = self.as_query();

        let pages = DbPage::merge(pages.map(|page| page.page_ref().clone()))
            .into_iter()
            .map(Into::into)
            .collect_vec();

        let DbPageSplit { cursor_indices, .. } = DbPage::split(&pages);

        let page_cursors: Vec<&DbPageCursor<QS>> = cursor_indices
            .into_iter()
            .map(|i| pages[i].as_cursor().unwrap())
            .collect_vec();

        let mut cursor_queries = Vec::<Q2>::default();
        for page_cursor in page_cursors.iter() {
            let query: Q0 = query.clone();
            let query: Q1 = query.filter(clone_box(&*page_cursor.column.cursor_comparison_expression));
            let query: Q2 = query.order(clone_box(&*page_cursor.column.order_by_expression));
            cursor_queries.push(query);
        }

        PaginatedQueryWrapper(
            PaginatedQuery::<QS, Q0, Q2, (), ()> {
                query,
                cursor_queries,
                pages: Some(pages),
                partition: None,
                partition_order: None,
            },
            Default::default(),
        )
    }
}

#[allow(opaque_hidden_inferred_bound)]
pub trait Paginated<DB: Backend>:
    Query<SqlType = (Self::InternalSqlType, Nullable<BigInt>, Nullable<Text>)> + Send + Sized
{
    type QuerySource: diesel::QuerySource;
    type Q0<'q>: Clone
        + Query<SqlType = Self::InternalSqlType>
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = Self::Q1<'q>>
        + 'q
    where
        Self: 'q;
    type Q1<'q>: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = Self::Q2<'q>>
    where
        Self: 'q;
    type Q2<'q>: Send + Query<SqlType = Self::InternalSqlType> + QueryFragment<DB>
    where
        Self: 'q;
    type P: Partition + Send;
    type PO: PartitionOrder<DB> + Send;
    type QueryId: 'static;
    type InternalSqlType: 'static;

    type Partitioned<'q, Expr: Partition + Send + 'q>: Paginated<
            DB,
            Q0<'q> = Self::Q0<'q>,
            Q1<'q> = Self::Q1<'q>,
            Q2<'q> = Self::Q2<'q>,
            P = Expr,
            PO = Self::PO,
            QueryId = Self::QueryId,
            SqlType = Self::SqlType,
            InternalSqlType = Self::InternalSqlType,
        > + 'q
    where
        Self: 'q;

    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q>: Paginated<
            DB,
            Q0<'q> = Self::Q0<'q>,
            Q1<'q> = Self::Q1<'q>,
            Q2<'q> = Self::Q2<'q>,
            P = Self::P,
            PO = Expr,
            QueryId = Self::QueryId,
            SqlType = Self::SqlType,
            InternalSqlType = Self::InternalSqlType,
        > + 'q
    where
        Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q;
    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q;

    fn get_query(&self) -> &Self::Q0<'_>;
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>];
    fn get_partition(&self) -> Option<&Self::P>;
    fn get_partition_order(&self) -> Option<&Self::PO>;
    fn get_pages(&self) -> Option<&[DbPage<Self::QuerySource>]>;

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + QueryId + Send + 'q;
}

#[allow(opaque_hidden_inferred_bound)]
impl<DB, QS, Q0, Q1, Q2, P, PO> Paginated<DB> for PaginatedQuery<QS, Q0, Q2, P, PO>
where
    DB: Backend,
    QS: diesel::QuerySource,
    Q0: Clone
        + Query
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<QS>>, Output = Q1>,
    Q0::SqlType: 'static,
    Q1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<QS>>, Output = Q2>,
    Q2: Query<SqlType = Q0::SqlType> + QueryFragment<DB> + Send,
    P: Partition + Send,
    PO: PartitionOrder<DB> + Send,
{
    type QuerySource = QS;
    type Q0<'q> = Q0 where Self: 'q;
    type Q1<'q> = Q1 where Self: 'q;
    type Q2<'q> = Q2 where Self: 'q;
    type P = P;
    type PO = PO;
    type QueryId = Q0::QueryId;
    type InternalSqlType = Q0::SqlType;

    type Partitioned<'q, Expr: Partition + Send + 'q> = PaginatedQuery<QS, Q0, Q2, Expr, PO> where Self: 'q;
    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q> = PaginatedQuery<QS, Q0, Q2, P, Expr> where Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQuery {
            query: self.query,
            cursor_queries: self.cursor_queries,
            pages: self.pages,
            partition: Some(expr),
            partition_order: self.partition_order,
        }
    }

    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQuery {
            query: self.query,
            cursor_queries: self.cursor_queries,
            pages: self.pages,
            partition: self.partition,
            partition_order: Some(expr),
        }
    }

    fn get_query(&self) -> &Self::Q0<'_> {
        &self.query
    }
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>] {
        &self.cursor_queries
    }
    fn get_partition(&self) -> Option<&Self::P> {
        self.partition.as_ref()
    }
    fn get_partition_order(&self) -> Option<&Self::PO> {
        self.partition_order.as_ref()
    }
    fn get_pages(&self) -> Option<&[DbPage<Self::QuerySource>]> {
        self.pages.as_deref()
    }

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + Send + 'q,
    {
        PaginatedQuery::<QS, NewQ0, NewQ2, Self::P, Self::PO> {
            query: f0(self.query),
            cursor_queries: self.cursor_queries.into_iter().map(f2).collect_vec(),
            pages: self.pages,
            partition: self.partition,
            partition_order: self.partition_order,
        }
    }
}

#[allow(opaque_hidden_inferred_bound)]
impl<DB: Backend + Send, P: Paginated<DB>> Paginated<DB> for PaginatedQueryWrapper<P, DB> {
    type QuerySource = P::QuerySource;
    type Q0<'q> = P::Q0<'q> where Self: 'q;
    type Q1<'q> = P::Q1<'q> where Self: 'q;
    type Q2<'q> = P::Q2<'q> where Self: 'q;
    type P = P::P;
    type PO = P::PO;
    type QueryId = P::QueryId;
    type InternalSqlType = P::InternalSqlType;

    type Partitioned<'q, Expr: Partition + Send + 'q> = PaginatedQueryWrapper<P::Partitioned<'q, Expr>, DB> where Self: 'q;
    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q> = PaginatedQueryWrapper<P::PartitionOrdered<'q, Expr>, DB> where Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQueryWrapper(self.0.partition(expr), Default::default())
    }
    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQueryWrapper(self.0.partition_order(expr), Default::default())
    }

    fn get_query(&self) -> &Self::Q0<'_> {
        self.0.get_query()
    }
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>] {
        self.0.get_cursor_queries()
    }
    fn get_partition(&self) -> Option<&Self::P> {
        self.0.get_partition()
    }
    fn get_partition_order(&self) -> Option<&Self::PO> {
        self.0.get_partition_order()
    }
    fn get_pages(&self) -> Option<&[DbPage<Self::QuerySource>]> {
        self.0.get_pages()
    }

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + QueryId + Send + 'q,
    {
        PaginatedQueryWrapper(self.0.map_query(f0, f2), Default::default())
    }
}

pub trait Partition {
    fn encode(&self) -> Result<String, diesel::result::Error>;
}

pub trait PartitionOrder<DB>
where
    DB: Backend,
{
    fn encode<'a, 'b>(ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a;
}

impl Partition for () {
    fn encode(&self) -> Result<String, diesel::result::Error> {
        Ok("".into())
    }
}

impl<DB> PartitionOrder<DB> for ()
where
    DB: Backend,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" 1 ");
    }
}

#[allow(unused_parens)]
impl<DB, T> PartitionOrder<DB> for Asc<T>
where
    DB: Backend,
    T: Column,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" ");
        ast_pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
        ast_pass.push_sql(".");
        ast_pass.push_sql(<T as Column>::NAME.split('.').last().unwrap());
        ast_pass.push_sql(" asc ");
    }
}

#[allow(unused_parens)]
impl<DB, T> PartitionOrder<DB> for Desc<T>
where
    DB: Backend,
    T: Column,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" ");
        ast_pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
        ast_pass.push_sql(".");
        ast_pass.push_sql(<T as Column>::NAME.split('.').last().unwrap());
        ast_pass.push_sql(" desc ");
    }
}

macro_rules! intersperse_statement {
    ($separator:stmt; $stmt:stmt; $($stmts:stmt;)+) => {
        $stmt
        $separator
        intersperse_statement!($separator; $($stmts;)*);
    };
    ($separator:stmt; $stmt:stmt;) => {
        $stmt
    };
}

macro_rules! partition {
    ($($len:literal: $($gen:ident)+),*$(,)?) => {
        $(
            #[allow(unused_parens)]
            impl<$($gen),+> Partition for ($($gen,)+)
            where
                $($gen: Column),+
            {
                fn encode(&self) -> Result<String, diesel::result::Error> {
                    let unique_min_column_names = [$(
                        format!("{OFFSET_SUBQUERY1_ALIAS}.{}", <$gen as Column>::NAME.split(".").last().unwrap()),
                    )+]
                        .into_iter()
                        .unique()
                        .collect::<Vec<_>>();
                    if unique_min_column_names.len() < $len {
                        return Err(diesel::result::Error::QueryBuilderError("could not encode group by clause as a row number partition for pagination because the column names included in the group by clause have identical names".into()));
                    }
                    Ok(unique_min_column_names.join(", "))
                }
            }

            #[allow(unused_parens)]
            impl<DB, $($gen),+> PartitionOrder<DB> for ($($gen,)+)
            where
                DB: Backend,
                $($gen: PartitionOrder<DB>,)+
            {
                fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
                where
                    DB::QueryBuilder: 'a,
                    <DB as Backend>::BindCollector<'a>: 'a,
                    DB::MetadataLookup: 'a,
                    'b: 'a,
                {
                    intersperse_statement!(
                        ast_pass.push_sql(", ");
                        $(<$gen as PartitionOrder<DB>>::encode(ast_pass.reborrow());)+
                    );
                }
            }
        )*
    };
}

impl<QS, Q0, Q2, P, PO, DB> QueryFragment<DB> for PaginatedQuery<QS, Q0, Q2, P, PO>
where
    DB: Backend,
    Q0: QueryFragment<DB>,
    Q2: QueryFragment<DB>,
    P: Partition,
    PO: PartitionOrder<DB>,
    i64: ToSql<BigInt, DB>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();

        let query = &self.query;

        let pages = match self.pages.as_ref() {
            Some(pages) => pages,
            None => return query.walk_ast(pass),
        };
        if pages.is_empty() {
            return Err(diesel::result::Error::QueryBuilderError(
                "no pages specified for a paginated query".into(),
            ));
        }

        let DbPageSplit {
            cursor_indices,
            offset_indices,
        } = DbPage::split(pages);

        let has_cursor_pages = !cursor_indices.is_empty();
        let has_offset_pages = !offset_indices.is_empty();

        if has_cursor_pages {
            let cursor_queries = &self.cursor_queries;
            let page_cursors: Vec<&DbPageCursor<QS>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            pass.push_sql("with ");
            for (i, cursor_query) in cursor_queries.iter().enumerate() {
                pass.push_sql(&format!("query{i} as ("));
                cursor_query.walk_ast(pass.reborrow())?;
                pass.push_sql(")");
                if i < cursor_queries.len() - 1 {
                    pass.push_sql(", ");
                }
            }

            for item in intersperse(page_cursors.into_iter().enumerate().map(Left), Right(())) {
                match item {
                    Left((i, page_cursor)) => {
                        if let Some(partition) = self.partition.as_ref() {
                            pass.push_sql("(select *, row_number() over (partition by ");
                            pass.push_sql(&partition.encode()?);
                            if self.partition_order.is_some() {
                                pass.push_sql(" order by ");
                                PO::encode(pass.reborrow());
                            }
                            pass.push_sql(") as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        } else {
                            pass.push_sql("(select *, null as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        }

                        pass.push_sql(", ");
                        pass.push_sql(&format!("'{}' as ", page_cursor.id));
                        pass.push_sql(PAGE_ID_COLUMN_NAME);

                        static SUBQUERY_NAME: &str = "q";
                        pass.push_sql(&format!(" from query{i} as "));
                        pass.push_sql(SUBQUERY_NAME);

                        match self.partition.is_some() {
                            false => {
                                pass.push_sql(" limit ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                            true => {
                                pass.push_sql(" where ");
                                pass.push_sql(SUBQUERY_NAME);
                                pass.push_sql(".");
                                pass.push_sql(OFFSET_COLUMN_NAME);
                                pass.push_sql(" <= ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                        };

                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" union "),
                }
            }
        }

        // union
        if has_cursor_pages && has_offset_pages {
            pass.push_sql(" union ");
        }

        if has_offset_pages {
            let page_offsets: Vec<&DbPageOffset> = offset_indices
                .into_iter()
                .map(|i| pages[i].as_offset().unwrap())
                .collect_vec();

            pass.push_sql("select *, null as ");
            pass.push_sql(PAGE_ID_COLUMN_NAME);
            pass.push_sql(" from (select *, row_number() over (");
            if let Some(partition) = self.partition.as_ref() {
                pass.push_sql("partition by ");
                pass.push_sql(&partition.encode()?);
                if self.partition_order.is_some() {
                    pass.push_sql(" order by ");
                    PO::encode(pass.reborrow());
                }
            }
            pass.push_sql(") as ");
            pass.push_sql(OFFSET_COLUMN_NAME);
            pass.push_sql(" from ( ");
            query.walk_ast(pass.reborrow())?;

            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
            pass.push_sql(" where ");
            for item in intersperse(page_offsets.iter().map(Left), Right(())) {
                match item {
                    Left(page_offset) => {
                        pass.push_sql("(");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" > "); // row_number starts at 1
                        pass.push_bind_param::<BigInt, _>(&page_offset.left)?;
                        pass.push_sql(" and ");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" <= ");
                        pass.push_bind_param::<BigInt, _>(&page_offset.right)?;
                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" or "),
                }
            }
        }

        Ok(())
    }
}

impl<DB, P> QueryFragment<DB> for PaginatedQueryWrapper<P, DB>
where
    DB: Backend,
    P: Paginated<DB>,
    i64: ToSql<BigInt, DB>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();

        let query = self.0.get_query();

        let pages = match self.0.get_pages() {
            Some(pages) => pages,
            None => return query.walk_ast(pass),
        };
        if pages.is_empty() {
            return Err(diesel::result::Error::QueryBuilderError(
                "no pages specified for a paginated query".into(),
            ));
        }

        let DbPageSplit {
            cursor_indices,
            offset_indices,
        } = DbPage::split(pages);

        let has_cursor_pages = !cursor_indices.is_empty();
        let has_offset_pages = !offset_indices.is_empty();

        if has_cursor_pages {
            let cursor_queries = self.0.get_cursor_queries();
            let page_cursors: Vec<&DbPageCursor<P::QuerySource>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            pass.push_sql("with ");
            for (i, cursor_query) in cursor_queries.iter().enumerate() {
                pass.push_sql(&format!("query{i} as ("));
                cursor_query.walk_ast(pass.reborrow())?;
                pass.push_sql(")");
                if i < cursor_queries.len() - 1 {
                    pass.push_sql(", ");
                }
            }

            for item in intersperse(page_cursors.into_iter().enumerate().map(Left), Right(())) {
                match item {
                    Left((i, page_cursor)) => {
                        if let Some(partition) = self.0.get_partition() {
                            pass.push_sql("(select *, row_number() over (partition by ");
                            pass.push_sql(&partition.encode()?);
                            if self.0.get_partition_order().is_some() {
                                pass.push_sql(" order by ");
                                P::PO::encode(pass.reborrow());
                            }
                            pass.push_sql(") as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        } else {
                            pass.push_sql("(select *, null as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        }

                        pass.push_sql(", ");
                        pass.push_sql(&format!("'{}' as ", page_cursor.id));
                        pass.push_sql(PAGE_ID_COLUMN_NAME);

                        static SUBQUERY_NAME: &str = "q";
                        pass.push_sql(&format!(" from query{i} as "));
                        pass.push_sql(SUBQUERY_NAME);

                        match self.0.get_partition().is_some() {
                            false => {
                                pass.push_sql(" limit ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                            true => {
                                pass.push_sql(" where ");
                                pass.push_sql(SUBQUERY_NAME);
                                pass.push_sql(".");
                                pass.push_sql(OFFSET_COLUMN_NAME);
                                pass.push_sql(" <= ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                        };

                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" union "),
                }
            }
        }

        // union
        if has_cursor_pages && has_offset_pages {
            pass.push_sql(" union ");
        }

        if has_offset_pages {
            let page_offsets: Vec<&DbPageOffset> = offset_indices
                .into_iter()
                .map(|i| pages[i].as_offset().unwrap())
                .collect_vec();

            pass.push_sql("select *, null as ");
            pass.push_sql(PAGE_ID_COLUMN_NAME);
            pass.push_sql(" from (select *, row_number() over (");
            if let Some(partition) = self.0.get_partition() {
                pass.push_sql("partition by ");
                pass.push_sql(&partition.encode()?);
                if self.0.get_partition_order().is_some() {
                    pass.push_sql(" order by ");
                    P::PO::encode(pass.reborrow());
                }
            }
            pass.push_sql(") as ");
            pass.push_sql(OFFSET_COLUMN_NAME);
            pass.push_sql(" from ( ");
            query.walk_ast(pass.reborrow())?;

            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
            pass.push_sql(" where ");
            for item in intersperse(page_offsets.iter().map(Left), Right(())) {
                match item {
                    Left(page_offset) => {
                        pass.push_sql("(");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" > "); // row_number starts at 1
                        pass.push_bind_param::<BigInt, _>(&page_offset.left)?;
                        pass.push_sql(" and ");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" <= ");
                        pass.push_bind_param::<BigInt, _>(&page_offset.right)?;
                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" or "),
                }
            }
        }

        Ok(())
    }
}

partition!(
     1: A,
     2: A B,
     3: A B C,
     4: A B C D,
     5: A B C D E,
     6: A B C D E F,
     7: A B C D E F G,
     8: A B C D E F G H,
     9: A B C D E F G H I,
    10: A B C D E F G H I J,
    11: A B C D E F G H I J K,
    12: A B C D E F G H I J K L,
    13: A B C D E F G H I J K L M,
    14: A B C D E F G H I J K L M N,
    15: A B C D E F G H I J K L M N O,
    16: A B C D E F G H I J K L M N O P,
    17: A B C D E F G H I J K L M N O P Q,
    18: A B C D E F G H I J K L M N O P Q R,
    19: A B C D E F G H I J K L M N O P Q R S,
    20: A B C D E F G H I J K L M N O P Q R S T,
    21: A B C D E F G H I J K L M N O P Q R S T U,
    22: A B C D E F G H I J K L M N O P Q R S T U V,
    23: A B C D E F G H I J K L M N O P Q R S T U V W,
    24: A B C D E F G H I J K L M N O P Q R S T U V W X,
    25: A B C D E F G H I J K L M N O P Q R S T U V W X Y,
    26: A B C D E F G H I J K L M N O P Q R S T U V W X Y Z,
);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DbPaged<K, QS> {
    pub page: DbPage<QS>,
    pub key: K,
}

impl<K, QS> AsRef<DbPage<QS>> for DbPaged<K, QS> {
    fn as_ref(&self) -> &DbPage<QS> {
        &self.page
    }
}

mod paginated_query_impls {
    use super::*;
    use diesel::dsl::*;
    use diesel::query_dsl::methods::*;
    use diesel::sql_types::{Nullable, Text};

    // Query impl also produces auto-impl of AsQuery
    impl<QS, Q0: Query, Q2, P, PO> Query for PaginatedQuery<QS, Q0, Q2, P, PO> {
        type SqlType = (Q0::SqlType, Nullable<BigInt>, Nullable<Text>);
    }

    impl<QS, Q0: QueryId, Q2, P, PO> QueryId for PaginatedQuery<QS, Q0, Q2, P, PO> {
        type QueryId = Q0::QueryId;

        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl<C: Connection, QS, Q0, Q2, P, PO> RunQueryDsl<C> for PaginatedQuery<QS, Q0, Q2, P, PO> {}

    impl<P, PO, QS, Q0: QueryDsl, Q2: QueryDsl> QueryDsl for PaginatedQuery<QS, Q0, Q2, P, PO> {}

    impl<'a, P, PO, QS: ?Sized, Q0: BoxedDsl<'a, DB>, Q2: BoxedDsl<'a, DB>, DB> BoxedDsl<'a, DB>
        for PaginatedQuery<QS, Q0, Q2, P, PO>
    {
        type Output = PaginatedQuery<QS, IntoBoxed<'a, Q0, DB>, IntoBoxed<'a, Q2, DB>, P, PO>;
        fn internal_into_boxed(self) -> IntoBoxed<'a, Self, DB> {
            PaginatedQuery {
                query: self.query.internal_into_boxed(),
                cursor_queries: self
                    .cursor_queries
                    .into_iter()
                    .map(|q| q.internal_into_boxed())
                    .collect_vec(),
                pages: self.pages,
                partition: self.partition,
                partition_order: self.partition_order,
            }
        }
    }

    macro_rules! paginated_query_query_dsl_method {
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> $output_ty:ident<Self $(, $output_gen:ident)*>; }) => {
            impl<QS: ?Sized, P, PO, Q0: $trait$(<$($gen),+>)?, Q2: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQuery<QS, Q0, Q2, P, PO> {
                type Output = PaginatedQuery<QS, $output_ty<Q0 $(, $output_gen)*>, $output_ty<Q2 $(, $output_gen)*>, P, PO>;
                fn $fn_name(self $(, $param: $param_ty)*) -> $output_ty<Self $(, $output_gen)*> {
                    PaginatedQuery {
                        cursor_queries: self.cursor_queries.into_iter().map(|q| q.$fn_name($($param.clone()),*)).collect_vec(),
                        query: self.query.$fn_name($($param),*),
                        pages: self.pages,
                        partition: self.partition,
                        partition_order: self.partition_order,
                    }
                }
            }
        };
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> Self::Output; }) => {
            impl<QS: ?Sized, P, PO, Q0: $trait$(<$($gen),+>)?, Q2: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQuery<QS, Q0, Q2, P, PO> {
                type Output = PaginatedQuery<QS, <Q0 as $trait$(<$($gen),+>)?>::Output, <Q2 as $trait$(<$($gen),+>)?>::Output, P, PO>;
                fn $fn_name(self $(, $param: $param_ty)*) -> Self::Output {
                    PaginatedQuery {
                        cursor_queries: self.cursor_queries.into_iter().map(|q| q.$fn_name($($param.clone()),*)).collect_vec(),
                        query: self.query.$fn_name($($param),*),
                        pages: self.pages,
                        partition: self.partition,
                        partition_order: self.partition_order,
                    }
                }
            }
        };
    }

    paginated_query_query_dsl_method!(DistinctDsl { fn distinct(self) -> Distinct<Self>; });
    #[cfg(feature = "postgres")]
    paginated_query_query_dsl_method!(DistinctOnDsl<Selection: Clone> { fn distinct_on(self, selection: Selection) -> DistinctOn<Self, Selection>; });
    paginated_query_query_dsl_method!(SelectDsl<Selection: Clone + Expression> { fn select(self, selection: Selection) -> Self::Output; });
    paginated_query_query_dsl_method!(FilterDsl<Predicate: Clone> { fn filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_query_dsl_method!(OrFilterDsl<Predicate: Clone> { fn or_filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_query_dsl_method!(FindDsl<PK: Clone> { fn find(self, id: PK) -> Self::Output; });
    paginated_query_query_dsl_method!(GroupByDsl<Expr: Clone + Expression> { fn group_by(self, expr: Expr) -> GroupBy<Self, Expr>; });
    paginated_query_query_dsl_method!(HavingDsl<Predicate: Clone> { fn having(self, predicate: Predicate) -> Having<Self, Predicate>; });
    paginated_query_query_dsl_method!(LockingDsl<Lock: Clone> { fn with_lock(self, lock: Lock) -> Self::Output; });
    paginated_query_query_dsl_method!(ModifyLockDsl<Modifier: Clone> { fn modify_lock(self, modifier: Modifier) -> Self::Output; });
    paginated_query_query_dsl_method!(SingleValueDsl { fn single_value(self) -> Self::Output; });
    paginated_query_query_dsl_method!(SelectNullableDsl { fn nullable(self) -> Self::Output; });
}

mod paginated_query_wrapper_impls {
    use super::*;
    use diesel::dsl::*;
    use diesel::query_dsl::methods::*;

    // Query impl also produces auto-impl of AsQuery
    impl<DB: Backend, P: Paginated<DB>> Query for PaginatedQueryWrapper<P, DB> {
        type SqlType = P::SqlType;
    }

    impl<DB: Backend, P: Paginated<DB>> QueryId for PaginatedQueryWrapper<P, DB> {
        type QueryId = P::QueryId;

        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl<C: Connection, P: Paginated<C::Backend>> RunQueryDsl<C> for PaginatedQueryWrapper<P, C::Backend> {}

    impl<DB: Backend, P: Paginated<DB>> QueryDsl for PaginatedQueryWrapper<P, DB> {}

    impl<'a, DB: Backend, P: Paginated<DB>> BoxedDsl<'a, DB> for PaginatedQueryWrapper<P, DB>
    where
        P: BoxedDsl<'a, DB>,
    {
        type Output = IntoBoxed<'a, P, DB>;
        fn internal_into_boxed(self) -> IntoBoxed<'a, Self, DB> {
            self.0.internal_into_boxed()
        }
    }

    macro_rules! paginated_query_wrapper_query_dsl_method {
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> $output_ty:ident<Self $(, $output_gen:ident)*>; }) => {
            impl<DB: Backend, P: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQueryWrapper<P, DB> {
                type Output = PaginatedQueryWrapper<$output_ty<P $(, $output_gen)*>, DB>;
                fn $fn_name(self $(, $param: $param_ty)*) -> $output_ty<Self $(, $output_gen)*> {
                    PaginatedQueryWrapper(self.0.$fn_name($($param),*), self.1)
                }
            }
        };
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> Self::Output; }) => {
            impl<DB: Backend, P: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQueryWrapper<P, DB> {
                type Output = PaginatedQueryWrapper<<P as $trait$(<$($gen),+>)?>::Output, DB>;
                fn $fn_name(self $(, $param: $param_ty)*) -> Self::Output {
                    PaginatedQueryWrapper(self.0.$fn_name($($param),*), self.1)
                }
            }
        };
    }

    paginated_query_wrapper_query_dsl_method!(DistinctDsl { fn distinct(self) -> Distinct<Self>; });
    #[cfg(feature = "postgres")]
    paginated_query_wrapper_query_dsl_method!(DistinctOnDsl<Selection: Clone> { fn distinct_on(self, selection: Selection) -> DistinctOn<Self, Selection>; });
    paginated_query_wrapper_query_dsl_method!(SelectDsl<Selection: Clone + Expression> { fn select(self, selection: Selection) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(FilterDsl<Predicate: Clone> { fn filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(OrFilterDsl<Predicate: Clone> { fn or_filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(FindDsl<PK: Clone> { fn find(self, id: PK) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(GroupByDsl<Expr: Clone + Expression> { fn group_by(self, expr: Expr) -> GroupBy<Self, Expr>; });
    paginated_query_wrapper_query_dsl_method!(HavingDsl<Predicate: Clone> { fn having(self, predicate: Predicate) -> Having<Self, Predicate>; });
    paginated_query_wrapper_query_dsl_method!(LockingDsl<Lock: Clone> { fn with_lock(self, lock: Lock) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(ModifyLockDsl<Modifier: Clone> { fn modify_lock(self, modifier: Modifier) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(SingleValueDsl { fn single_value(self) -> Self::Output; });
    paginated_query_wrapper_query_dsl_method!(SelectNullableDsl { fn nullable(self) -> Self::Output; });
}

pub use db_page::*;
pub(super) mod db_page {
    use super::*;
    use ::chrono::NaiveDateTime;
    use ::derivative::Derivative;
    use ::diesel::dsl::{self, And, IsNotNull, Or};
    use ::diesel::expression::{is_aggregate::No, AsExpression, ValidGrouping};
    use ::diesel::helper_types::{Gt, GtEq, Lt, LtEq};
    use ::diesel::sql_types::is_nullable::{IsNullable, NotNull};
    use ::diesel::sql_types::BoolOrNullableBool;
    use ::diesel::sql_types::MaybeNullableType;
    use ::diesel::sql_types::OneIsNullable;
    use ::diesel::sql_types::{Bool, Nullable, SingleValue, SqlType};
    use ::diesel::{AppearsOnTable, BoolExpressionMethods, Column, Expression, QuerySource};
    use ::dyn_clone::DynClone;
    use ::itertools::Itertools;
    use ::std::borrow::{Borrow, Cow};
    use ::std::cmp::Ordering;
    use ::std::collections::{BTreeMap, HashMap};
    use ::uuid::Uuid;
    
    #[cfg(feature = "async-graphql-4")]
    use async_graphql_4 as async_graphql;
    #[cfg(feature = "async-graphql-5")]
    use async_graphql_5 as async_graphql;
    #[cfg(feature = "async-graphql-6")]
    use async_graphql_6 as async_graphql;
    
    #[derive(AsVariant, AsVariantMut, Derivative, IsVariant, Unwrap)]
    #[derivative(
        Clone(bound = ""),
        Debug(bound = ""),
        Eq(bound = ""),
        Hash(bound = ""),
        PartialEq(bound = "")
    )]
    pub enum DbPage<QS: ?Sized> {
        Cursor(DbPageCursor<QS>),
        Offset(DbPageOffset),
    }
    
    impl<QS: ?Sized> Ord for DbPage<QS> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }
    
    impl<QS: ?Sized> PartialOrd for DbPage<QS> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match self {
                DbPage::Cursor(lhs) => match other {
                    DbPage::Cursor(rhs) => lhs.partial_cmp(rhs),
                    DbPage::Offset(_) => Some(Ordering::Less),
                },
                DbPage::Offset(lhs) => match other {
                    DbPage::Cursor(_) => Some(Ordering::Greater),
                    DbPage::Offset(rhs) => lhs.partial_cmp(rhs),
                },
            }
        }
    }
    
    pub trait DbPageExt {
        fn is_empty(&self) -> bool;
        fn merge(items: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self>
        where
            Self: Sized;
    }
    
    impl<QS> DbPageExt for DbPage<QS> {
        fn is_empty(&self) -> bool {
            match self {
                Self::Cursor(page_cursor) => page_cursor.is_empty(),
                Self::Offset(page_offset) => page_offset.is_empty(),
            }
        }
        fn merge(pages: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
            let pages = pages.into_iter().collect_vec();
            let (page_cursors, page_offsets): (Vec<_>, Vec<_>) = pages
                .iter()
                .map(|page| {
                    let page = page.borrow();
                    (page.as_cursor(), page.as_offset())
                })
                .unzip();
            let page_cursors = page_cursors.into_iter().flatten().collect_vec();
            let page_offsets = page_offsets.into_iter().flatten().collect_vec();
    
            let mut pages = Vec::<Self>::new();
            pages.extend(&mut DbPageCursor::merge(page_cursors).into_iter().map(DbPage::Cursor));
            pages.extend(&mut DbPageOffset::merge(page_offsets).into_iter().map(DbPage::Offset));
    
            pages
        }
    }
    
    pub(crate) struct DbPageSplit {
        pub(crate) cursor_indices: Vec<usize>,
        pub(crate) offset_indices: Vec<usize>,
    }
    
    impl<QS> DbPage<QS> {
        pub(crate) fn split(pages: &[Self]) -> DbPageSplit {
            let (page_cursors, page_offsets): (Vec<_>, Vec<_>) = pages
                .iter()
                .enumerate()
                .map(|(i, page)| match page {
                    DbPage::Cursor(_) => (Some(i), None),
                    DbPage::Offset(_) => (None, Some(i)),
                })
                .unzip();
    
            DbPageSplit {
                cursor_indices: page_cursors.into_iter().flatten().collect(),
                offset_indices: page_offsets.into_iter().flatten().collect(),
            }
        }
    }
    
    pub fn split_multipaginated_results<T: ?Sized, R: Clone>(
        results: Vec<(R, Option<i64>, Option<String>)>,
        pages: Vec<DbPage<T>>,
    ) -> HashMap<DbPage<T>, Vec<R>> {
        use std::ops::Bound::*;
        use std::str::FromStr;
    
        let mut pages_by_cursor_id = HashMap::<Uuid, &DbPage<T>>::default();
        let mut pages_by_page_offset_range = BTreeMap::<Range, &DbPage<T>>::default();
        for page in &pages {
            match page {
                DbPage::Cursor(page_cursor) => {
                    pages_by_cursor_id.insert(page_cursor.id, page);
                }
                DbPage::Offset(page_offset) => {
                    pages_by_page_offset_range.insert(page_offset.into(), page);
                }
            }
        }
        let (range_min, range_max) = if !pages_by_page_offset_range.is_empty() {
            (
                pages_by_page_offset_range.first_key_value().unwrap().0.left_exclusive,
                pages_by_page_offset_range.last_key_value().unwrap().0.right_inclusive,
            )
        } else {
            (0, 0) // never used
        };
    
        let mut results_by_page = HashMap::<DbPage<T>, Vec<R>>::default();
        for (result, row_number, page_id) in results {
            if let Some(page_id) = page_id {
                let page_id = Uuid::from_str(&page_id).unwrap();
                let page = pages_by_cursor_id.get(&page_id).unwrap();
                if !results_by_page.contains_key(page) {
                    results_by_page.insert((*page).clone(), vec![]);
                }
                results_by_page.get_mut(page).unwrap().push(result);
            } else if let Some(row_number) = row_number {
                let mut matches = pages_by_page_offset_range.range((
                    Excluded(Range {
                        left_exclusive: range_min,
                        right_inclusive: row_number - 1,
                        kind: RangeKind::LowerBound,
                    }),
                    Excluded(Range {
                        left_exclusive: row_number,
                        right_inclusive: range_max,
                        kind: RangeKind::UpperBound,
                    }),
                ));
                if let Some((_, first_match)) = matches.next() {
                    let new_pages_and_results = matches.map(|(_, page)| (*page, result.clone())).collect::<Vec<_>>();
    
                    if !results_by_page.contains_key(first_match) {
                        results_by_page.insert((*first_match).clone(), vec![]);
                    }
                    results_by_page.get_mut(first_match).unwrap().push(result);
    
                    for (page, result) in new_pages_and_results {
                        if !results_by_page.contains_key(page) {
                            results_by_page.insert((*page).clone(), vec![]);
                        }
                        results_by_page.get_mut(page).unwrap().push(result);
                    }
                }
            }
        }
    
        results_by_page
    }
    
    impl Page {
        pub fn on_column<QS, C>(self, column: C) -> DbPage<QS>
        where
            QS: QuerySource,
            C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
            <C as Expression>::SqlType: SingleValue,
            NaiveDateTime: AsExpression<C::SqlType>,
    
            Gt<C, NaiveDateTime>: Expression,
            Lt<C, NaiveDateTime>: Expression,
            GtEq<C, NaiveDateTime>: Expression,
            LtEq<C, NaiveDateTime>: Expression,
    
            dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
        {
            match self {
                Self::Cursor(cursor) => DbPage::Cursor(cursor.on_column(column)),
                Self::Offset(offset) => DbPage::Offset(offset.into()),
            }
        }
    
        pub fn on_columns<QS, C1, C2, N1, N2>(self, column1: C1, column2: C2) -> DbPage<QS>
        where
            QS: QuerySource,
            C1: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
            C2: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync + 'static,
            <C1 as Expression>::SqlType: SingleValue + SqlType<IsNull = N1>,
            <C2 as Expression>::SqlType: SingleValue + SqlType<IsNull = N2>,
            NaiveDateTime: AsExpression<C1::SqlType> + AsExpression<C2::SqlType>,
            <NaiveDateTime as AsExpression<C2::SqlType>>::Expression: QF,
    
            N1: OneIsNullable<N1>,
            <N1 as OneIsNullable<N1>>::Out: MaybeNullableType<Bool>,
            N2: OneIsNullable<N2>,
            <N2 as OneIsNullable<N2>>::Out: MaybeNullableType<Bool>,
    
            dsl::Nullable<Gt<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<Lt<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<GtEq<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<LtEq<C1, NaiveDateTime>>: Expression,
    
            IsNotNull<C1>: Expression + BoolExpressionMethods,
            <IsNotNull<C1> as Expression>::SqlType: SqlType,
            <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
    
            dsl::Nullable<Gt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<Gt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<Gt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>,
                Gt<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<GtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<GtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<GtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
                GtEq<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<Lt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<Lt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<Lt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>,
                Lt<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<LtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<LtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<LtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
                LtEq<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
        {
            match self {
                Self::Cursor(cursor) => DbPage::Cursor(cursor.on_columns::<QS, C1, C2, N1, N2>(column1, column2)),
                Self::Offset(offset) => DbPage::Offset(offset.into()),
            }
        }
    
        pub fn map_on_column<QS, C>(column: C) -> impl (Fn(Self) -> DbPage<QS>) + 'static
        where
            QS: QuerySource,
            C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
            <C as Expression>::SqlType: SingleValue,
    
            NaiveDateTime: AsExpression<C::SqlType>,
    
            Gt<C, NaiveDateTime>: Expression,
            Lt<C, NaiveDateTime>: Expression,
            GtEq<C, NaiveDateTime>: Expression,
            LtEq<C, NaiveDateTime>: Expression,
            dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + dyn_clone::DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
                + 'static,
            dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + dyn_clone::DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
                + 'static,
            dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + dyn_clone::DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
                + 'static,
            dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + dyn_clone::DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
                + 'static,
        {
            move |page| page.on_column(column.clone())
        }
    }
    
    // necessary to use instead of something like Borrow
    // because coercion from double reference to single reference
    // does not occur automatically when passing in a value that needs
    // to implement Borrow<DbPage>
    // e.g. &[&DbPage].iter() == Iterator<Item = &&DbPage> and &&DbPage: !Borrow<DbPage>
    pub trait DbPageRef<'a, QS: ?Sized> {
        fn page_ref(&'a self) -> &'a DbPage<QS>;
    }
    
    pub trait AsDbPage<QS: ?Sized> {
        fn as_page(&self) -> Option<&DbPage<QS>>;
    }
    
    impl<QS> AsRef<DbPage<QS>> for DbPage<QS> {
        fn as_ref(&self) -> &DbPage<QS> {
            self
        }
    }
    
    impl<'a, T, QS> DbPageRef<'a, QS> for T
    where
        T: AsRef<DbPage<QS>>,
    {
        fn page_ref(&'a self) -> &'a DbPage<QS> {
            self.as_ref()
        }
    }
    
    impl<QS, P: Borrow<DbPage<QS>>> AsDbPage<QS> for P {
        fn as_page(&self) -> Option<&DbPage<QS>> {
            Some(self.borrow())
        }
    }
    
    impl<QS> AsDbPage<QS> for Option<DbPage<QS>> {
        fn as_page(&self) -> Option<&DbPage<QS>> {
            self.as_ref()
        }
    }
    
    impl<QS> AsDbPage<QS> for &Option<DbPage<QS>> {
        fn as_page(&self) -> Option<&DbPage<QS>> {
            self.as_ref()
        }
    }
    
    impl<QS> AsDbPage<QS> for Option<&DbPage<QS>> {
        fn as_page(&self) -> Option<&DbPage<QS>> {
            *self
        }
    }
    
    impl<QS> AsDbPage<QS> for &Option<&DbPage<QS>> {
        fn as_page(&self) -> Option<&DbPage<QS>> {
            **self
        }
    }
    
    pub trait OptDbPageRef<'a, QS: ?Sized> {
        fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>>;
    }
    
    impl<'a, T, QS: ?Sized> OptDbPageRef<'a, QS> for Option<T>
    where
        T: OptDbPageRef<'a, QS>,
    {
        fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
            match self {
                Some(some) => some.opt_page_ref(),
                None => None,
            }
        }
    }
    
    impl<'a, 'b: 'a, T, QS: ?Sized> OptDbPageRef<'a, QS> for &'b T
    where
        T: OptDbPageRef<'a, QS>,
    {
        fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
            OptDbPageRef::opt_page_ref(*self)
        }
    }
    
    impl<'a, QS: ?Sized> OptDbPageRef<'a, QS> for DbPage<QS> {
        fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
            Some(self)
        }
    }
    
    pub trait OptDbPageRefs<'a, QS: ?Sized> {
        type _IntoIter: IntoIterator<Item = Self::_Item> + 'a;
        type _Item: for<'b> DbPageRef<'b, QS>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter>;
    }
    
    #[derive(Clone, Debug)]
    pub enum OptDbPageRefsEither<L, R> {
        Left(L),
        Right(R),
    }
    #[derive(Clone, Debug)]
    pub enum OptDbPageRefsEitherIter<L, R> {
        Left(L),
        Right(R),
    }
    
    impl<L, R> IntoIterator for OptDbPageRefsEither<L, R>
    where
        L: IntoIterator,
        R: IntoIterator,
    {
        type IntoIter = OptDbPageRefsEitherIter<L::IntoIter, R::IntoIter>;
        type Item = OptDbPageRefsEither<L::Item, R::Item>;
    
        fn into_iter(self) -> Self::IntoIter {
            match self {
                Self::Left(left) => OptDbPageRefsEitherIter::Left(left.into_iter()),
                Self::Right(right) => OptDbPageRefsEitherIter::Right(right.into_iter()),
            }
        }
    }
    
    impl<L, R> Iterator for OptDbPageRefsEitherIter<L, R>
    where
        L: Iterator,
        R: Iterator,
    {
        type Item = OptDbPageRefsEither<L::Item, R::Item>;
    
        fn next(&mut self) -> Option<Self::Item> {
            match self {
                Self::Left(left) => left.next().map(OptDbPageRefsEither::Left),
                Self::Right(right) => right.next().map(OptDbPageRefsEither::Right),
            }
        }
    }
    
    impl<'a, L, R, QS: ?Sized> DbPageRef<'a, QS> for OptDbPageRefsEither<L, R>
    where
        L: DbPageRef<'a, QS>,
        R: DbPageRef<'a, QS>,
    {
        fn page_ref(&'a self) -> &'a DbPage<QS> {
            match self {
                Self::Left(left) => left.page_ref(),
                Self::Right(right) => right.page_ref(),
            }
        }
    }
    
    impl<'a, 'b: 'a, T, QS: ?Sized> OptDbPageRefs<'a, QS> for &'b T
    where
        T: OptDbPageRefs<'a, QS>,
    {
        type _IntoIter = T::_IntoIter;
        type _Item = T::_Item;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            OptDbPageRefs::opt_page_refs(*self)
        }
    }
    
    impl<'a, T, QS: ?Sized> OptDbPageRefs<'a, QS> for Option<T>
    where
        T: OptDbPageRefs<'a, QS>,
    {
        type _IntoIter = T::_IntoIter;
        type _Item = T::_Item;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            match self {
                Some(some) => OptDbPageRefs::opt_page_refs(some),
                None => None,
            }
        }
    }
    
    impl<'a, 'b: 'a, T: ToOwned, QS: ?Sized> OptDbPageRefs<'a, QS> for Cow<'b, T>
    where
        T: OptDbPageRefs<'a, QS> + IntoIterator,
        T::Owned: OptDbPageRefs<'a, QS> + IntoIterator,
        T::Item: 'b,
        <T::Owned as IntoIterator>::Item: 'b,
    {
        type _IntoIter = OptDbPageRefsEither<T::_IntoIter, <T::Owned as OptDbPageRefs<'a, QS>>::_IntoIter>;
        type _Item = OptDbPageRefsEither<T::_Item, <T::Owned as OptDbPageRefs<'a, QS>>::_Item>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            match self {
                Cow::Borrowed(borrowed) => OptDbPageRefs::opt_page_refs(borrowed).map(OptDbPageRefsEither::Left),
                Cow::Owned(owned) => OptDbPageRefs::opt_page_refs(owned).map(OptDbPageRefsEither::Right),
            }
        }
    }
    
    impl<'a, QS: 'a> OptDbPageRefs<'a, QS> for Vec<DbPage<QS>> {
        type _IntoIter = &'a [DbPage<QS>];
        type _Item = &'a DbPage<QS>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            Some(self)
        }
    }
    
    impl<'a, 'b: 'a, QS: 'a> OptDbPageRefs<'a, QS> for &'b [DbPage<QS>] {
        type _IntoIter = &'a [DbPage<QS>];
        type _Item = &'a DbPage<QS>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            Some(self)
        }
    }
    
    impl<'a, const N: usize, QS: 'a> OptDbPageRefs<'a, QS> for [DbPage<QS>; N] {
        type _IntoIter = &'a [DbPage<QS>];
        type _Item = &'a DbPage<QS>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            Some(self)
        }
    }
    
    impl<'a, 'b: 'a, QS: 'a> OptDbPageRefs<'a, QS> for Cow<'b, [DbPage<QS>]> {
        type _IntoIter = &'a [DbPage<QS>];
        type _Item = &'a DbPage<QS>;
        fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
            Some(&**self)
        }
    }
}

pub use db_page_cursor::*;
pub(super) mod db_page_cursor {
    use super::*;
    use ::chrono::NaiveDateTime;
    use ::derivative::Derivative;
    use ::diesel::dsl::{self, And, IsNotNull, Or};
    use ::diesel::expression::{expression_types::NotSelectable, is_aggregate::No, AsExpression, ValidGrouping};
    use ::diesel::expression_methods::NullableExpressionMethods;
    use ::diesel::helper_types::{Gt, GtEq, Lt, LtEq};
    use ::diesel::sql_types::is_nullable::{IsNullable, NotNull};
    use ::diesel::sql_types::BoolOrNullableBool;
    use ::diesel::sql_types::MaybeNullableType;
    use ::diesel::sql_types::OneIsNullable;
    use ::diesel::sql_types::{Bool, Nullable, SingleValue, SqlType};
    use ::diesel::{AppearsOnTable, BoolExpressionMethods, Column, Expression, ExpressionMethods, QuerySource};
    use ::dyn_clone::DynClone;
    use ::std::borrow::Borrow;
    use ::std::cmp::Ordering;
    use ::uuid::Uuid;
    
    #[derive(Derivative)]
    #[derivative(Debug(bound = ""), Eq(bound = ""), Hash(bound = ""), PartialEq(bound = ""))]
    pub struct DbPageCursor<QS: ?Sized> {
        pub count: i64,
        pub cursor: NaiveDateTime,
        pub column: DbPageCursorColumn<QS>,
        #[derivative(Hash = "ignore")]
        #[derivative(PartialEq = "ignore")]
        pub(crate) id: Uuid,
        pub(crate) direction: CursorDirection,
        pub(crate) is_comparator_inclusive: bool,
    }
    
    #[derive(Derivative)]
    #[derivative(Debug(bound = ""), Eq(bound = ""), Hash(bound = ""), PartialEq(bound = ""))]
    pub struct DbPageCursorColumn<QS: ?Sized> {
        pub name: &'static str,
        #[derivative(Debug = "ignore")]
        #[derivative(Hash = "ignore")]
        #[derivative(PartialEq = "ignore")]
        pub(crate) cursor_comparison_expression: Box<dyn ColumnCursorComparisonExpression<QS>>,
        #[derivative(Debug = "ignore")]
        #[derivative(Hash = "ignore")]
        #[derivative(PartialEq = "ignore")]
        pub(crate) order_by_expression: Box<dyn ColumnOrderByExpression<QS>>,
    }
    
    pub trait DbPageFrom<T> {
        fn page_from(value: T) -> Self;
    }
    
    impl<QS: ?Sized> Clone for DbPageCursor<QS> {
        fn clone(&self) -> Self {
            Self {
                count: self.count,
                cursor: self.cursor,
                id: self.id,
                direction: self.direction,
                is_comparator_inclusive: self.is_comparator_inclusive,
                column: DbPageCursorColumn {
                    name: self.column.name,
                    cursor_comparison_expression: dyn_clone::clone_box(&*self.column.cursor_comparison_expression),
                    order_by_expression: dyn_clone::clone_box(&*self.column.order_by_expression),
                },
            }
        }
    }
    
    pub trait ColumnCursorComparisonExpression<QS: ?Sized>:
        AppearsOnTable<QS>
        + DynClone
        + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static
    {
    }
    
    pub trait ColumnOrderByExpression<QS: ?Sized>:
        AppearsOnTable<QS> + DynClone + Expression<SqlType = (NotSelectable, NotSelectable)> + QF + Send + Sync + 'static
    {
    }
    
    impl<
            QS: QuerySource + ?Sized,
            CE: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
        > ColumnCursorComparisonExpression<QS> for CE
    {
    }
    
    impl<
            QS: QuerySource + ?Sized,
            CE: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = (NotSelectable, NotSelectable)>
                + QF
                + Send
                + Sync
                + 'static,
        > ColumnOrderByExpression<QS> for CE
    {
    }
    
    impl PageCursor {
        pub fn on_column<QS, C>(self, column: C) -> DbPageCursor<QS>
        where
            QS: QuerySource,
            C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
            <C as Expression>::SqlType: SingleValue,
            NaiveDateTime: AsExpression<C::SqlType>,
    
            Gt<C, NaiveDateTime>: Expression,
            Lt<C, NaiveDateTime>: Expression,
            GtEq<C, NaiveDateTime>: Expression,
            LtEq<C, NaiveDateTime>: Expression,
    
            dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
            dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
                + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
        {
            let is_comparator_inclusive = self.is_comparator_inclusive.unwrap_or_default();
    
            DbPageCursor {
                count: self.count as i64,
                id: Uuid::new_v4(),
                direction: self.direction,
                cursor: self.cursor,
                is_comparator_inclusive,
                column: DbPageCursorColumn {
                    name: C::NAME,
                    cursor_comparison_expression: match (is_comparator_inclusive, self.direction) {
                        (false, CursorDirection::Following) => Box::new(column.clone().gt(self.cursor).nullable()),
                        (false, CursorDirection::Preceding) => Box::new(column.clone().lt(self.cursor).nullable()),
                        (true, CursorDirection::Following) => Box::new(column.clone().ge(self.cursor).nullable()),
                        (true, CursorDirection::Preceding) => Box::new(column.clone().le(self.cursor).nullable()),
                    },
                    order_by_expression: match self.direction {
                        CursorDirection::Following => Box::new((column.clone().asc(), column.asc())),
                        CursorDirection::Preceding => Box::new((column.clone().desc(), column.desc())),
                    },
                },
            }
        }
    
        pub fn on_columns<QS, C1, C2, N1, N2>(self, column1: C1, column2: C2) -> DbPageCursor<QS>
        where
            QS: QuerySource,
            C1: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
            C2: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync + 'static,
            <C1 as Expression>::SqlType: SingleValue + SqlType<IsNull = N1>,
            <C2 as Expression>::SqlType: SingleValue + SqlType<IsNull = N2>,
            NaiveDateTime: AsExpression<C1::SqlType> + AsExpression<C2::SqlType>,
            <NaiveDateTime as AsExpression<C2::SqlType>>::Expression: QF,
    
            N1: OneIsNullable<N1>,
            <N1 as OneIsNullable<N1>>::Out: MaybeNullableType<Bool>,
            N2: OneIsNullable<N2>,
            <N2 as OneIsNullable<N2>>::Out: MaybeNullableType<Bool>,
    
            dsl::Nullable<Gt<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<Lt<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<GtEq<C1, NaiveDateTime>>: Expression,
            dsl::Nullable<LtEq<C1, NaiveDateTime>>: Expression,
    
            IsNotNull<C1>: Expression + BoolExpressionMethods,
            <IsNotNull<C1> as Expression>::SqlType: SqlType,
            <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
    
            dsl::Nullable<Gt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<Gt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<Gt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>,
                Gt<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<GtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<GtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<GtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
                GtEq<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<Lt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<Lt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<Lt<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>,
                Lt<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
    
            dsl::Nullable<LtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
            <dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
            <<<dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
            <And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
                SqlType + BoolOrNullableBool,
            <<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
                OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
            <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
            <<And<
                IsNotNull<C1>,
                dsl::Nullable<LtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
            <<<And<
                IsNotNull<C1>,
                dsl::Nullable<LtEq<C1, NaiveDateTime>>,
                Nullable<Bool>,
            > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
            dsl::Nullable<Or<
                And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
                LtEq<C2, NaiveDateTime>,
                <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
            >>: AppearsOnTable<QS>
                + DynClone
                + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
                + Send
                + Sync
                + ValidGrouping<(), IsAggregate = No>
                + 'static,
        {
            let is_comparator_inclusive = self.is_comparator_inclusive.unwrap_or_default();
            DbPageCursor {
                count: self.count as i64,
                id: Uuid::new_v4(),
                direction: self.direction,
                cursor: self.cursor,
                is_comparator_inclusive,
                column: DbPageCursorColumn {
                    name: C1::NAME,
                    cursor_comparison_expression: match (is_comparator_inclusive, self.direction) {
                        (false, CursorDirection::Following) => Box::new(
                            column1
                                .clone()
                                .is_not_null()
                                .and(column1.clone().gt(self.cursor).nullable())
                                .or(column2.clone().gt(self.cursor))
                                .nullable(),
                        ),
                        (false, CursorDirection::Preceding) => Box::new(
                            column1
                                .clone()
                                .is_not_null()
                                .and(column1.clone().lt(self.cursor).nullable())
                                .or(column2.clone().lt(self.cursor))
                                .nullable(),
                        ),
                        (true, CursorDirection::Following) => Box::new(
                            column1
                                .clone()
                                .is_not_null()
                                .and(column1.clone().ge(self.cursor).nullable())
                                .or(column2.clone().ge(self.cursor))
                                .nullable(),
                        ),
                        (true, CursorDirection::Preceding) => Box::new(
                            column1
                                .clone()
                                .is_not_null()
                                .and(column1.clone().le(self.cursor).nullable())
                                .or(column2.clone().le(self.cursor))
                                .nullable(),
                        ),
                    },
                    order_by_expression: match self.direction {
                        CursorDirection::Following => Box::new((column1.asc(), column2.asc())),
                        CursorDirection::Preceding => Box::new((column1.desc(), column2.desc())),
                    },
                },
            }
        }
    }
    
    impl<QS, C> From<(PageCursor, C)> for DbPageCursor<QS>
    where
        QS: QuerySource,
        C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync + ValidGrouping<(), IsAggregate = No> + 'static,
        <C as Expression>::SqlType: SingleValue,
        NaiveDateTime: AsExpression<C::SqlType>,
    
        Gt<C, NaiveDateTime>: Expression,
        Lt<C, NaiveDateTime>: Expression,
        GtEq<C, NaiveDateTime>: Expression,
        LtEq<C, NaiveDateTime>: Expression,
    
        dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
    {
        fn from((value, column): (PageCursor, C)) -> Self {
            PageCursor::on_column(value, column)
        }
    }
    
    impl<QS: ?Sized> PartialOrd for DbPageCursor<QS> {
        fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
            if self.cursor != rhs.cursor {
                self.cursor.partial_cmp(&rhs.cursor)
            } else {
                self.count.partial_cmp(&rhs.count)
            }
        }
    }
    
    impl<QS: ?Sized> Ord for DbPageCursor<QS> {
        fn cmp(&self, rhs: &Self) -> Ordering {
            self.partial_cmp(rhs).unwrap()
        }
    }
    
    impl<QS: ?Sized> DbPageExt for DbPageCursor<QS> {
        fn is_empty(&self) -> bool {
            self.count == 0
        }
        fn merge(page_cursors: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
            let mut page_cursors = page_cursors
                .into_iter()
                .map(|page_cursor| page_cursor.borrow().clone())
                .collect::<Vec<Self>>();
    
            page_cursors.sort();
    
            // no actual merging can occur for cursor based pagination because we
            // cannot know the actual density of records between cursors in advance
            page_cursors
        }
    }
    
    cfg_if! {
        if #[cfg(all(not(feature = "mysql"), not(feature = "postgres"), not(feature = "sqlite")))] {
            pub trait QF {}
            impl<T> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(feature = "mysql", not(feature = "postgres"), not(feature = "sqlite")))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(not(feature = "mysql"), feature = "postgres", not(feature = "sqlite")))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::pg::Pg> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::pg::Pg>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(not(feature = "mysql"), not(feature = "postgres"), feature = "sqlite"))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(feature = "mysql", feature = "postgres", not(feature = "sqlite")))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(not(feature = "mysql"), feature = "postgres", feature = "sqlite"))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(feature = "mysql", not(feature = "postgres"), feature = "sqlite"))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
        }
    }
    cfg_if! {
        if #[cfg(all(feature = "mysql", feature = "postgres", feature = "sqlite"))] {
            pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
            impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
        }
    }
}

pub use db_page_offset::*;
pub(super) mod db_page_offset {
    use super::*;
    use ::std::borrow::Borrow;
    use ::std::cmp::Ordering;
    
    #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
    pub struct DbPageOffset {
        pub left: i64,
        pub right: i64,
    }
    
    impl DbPageOffset {
        pub fn with_count(count: u32) -> Self {
            Self {
                left: 0,
                right: count as i64,
            }
        }
    }
    
    impl From<PageOffset> for DbPageOffset {
        fn from(value: PageOffset) -> Self {
            Self {
                left: (value.index * value.count) as i64,
                right: (value.count + value.index * value.count) as i64,
            }
        }
    }
    
    impl PartialOrd for DbPageOffset {
        fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
            if self.left != rhs.left {
                self.left.partial_cmp(&rhs.left)
            } else {
                self.right.partial_cmp(&rhs.right)
            }
        }
    }
    
    impl Ord for DbPageOffset {
        fn cmp(&self, rhs: &Self) -> Ordering {
            self.partial_cmp(rhs).unwrap()
        }
    }
    
    impl DbPageExt for DbPageOffset {
        fn is_empty(&self) -> bool {
            self.left == self.right
        }
        fn merge(page_offsets: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
            let mut page_offsets = page_offsets
                .into_iter()
                .map(|page_offset| *page_offset.borrow())
                .collect::<Vec<Self>>();
    
            page_offsets.sort();
    
            let mut merged = Vec::new();
            if !page_offsets.is_empty() {
                merged.push(page_offsets[0]);
            }
            for page in page_offsets.into_iter().skip(1) {
                let merged_len = merged.len();
                let last_merged = &mut merged[merged_len - 1];
                if page.left <= last_merged.right {
                    last_merged.right = last_merged.right.max(page.right);
                } else {
                    merged.push(page);
                }
            }
            merged
        }
    }
    
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct Range {
        pub(crate) left_exclusive: i64,
        pub(crate) right_inclusive: i64,
        pub(crate) kind: RangeKind,
    }
    
    impl From<&DbPageOffset> for Range {
        fn from(value: &DbPageOffset) -> Self {
            Self {
                left_exclusive: value.left,
                right_inclusive: value.right,
                kind: RangeKind::Basic,
            }
        }
    }
    
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum RangeKind {
        Basic,
        LowerBound,
        UpperBound,
    }
    
    impl PartialOrd for Range {
        fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
            use RangeKind::*;
            let (range, bound, bound_is_lower, parity) = match (&self.kind, &rhs.kind) {
                (Basic, LowerBound) => (self, rhs, true, false),
                (Basic, UpperBound) => (self, rhs, false, false),
                (LowerBound, Basic) => (rhs, self, true, true),
                (UpperBound, Basic) => (rhs, self, false, true),
                (LowerBound, UpperBound) => return Some(Ordering::Less),
                (UpperBound, LowerBound) => return Some(Ordering::Greater),
                _ => {
                    if self.left_exclusive != rhs.left_exclusive {
                        return self.left_exclusive.partial_cmp(&rhs.left_exclusive);
                    } else {
                        return self.right_inclusive.partial_cmp(&rhs.right_inclusive);
                    }
                }
            };
    
            Some(
                if (bound.left_exclusive <= range.left_exclusive && bound.right_inclusive >= range.right_inclusive)
                    ^ bound_is_lower
                    ^ parity
                {
                    Ordering::Greater
                } else {
                    Ordering::Less
                },
            )
        }
    }
    
    impl Ord for Range {
        fn cmp(&self, rhs: &Self) -> Ordering {
            self.partial_cmp(rhs).unwrap()
        }
    }
}
