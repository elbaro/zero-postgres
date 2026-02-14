use std::sync::Arc;

use diesel::connection::{
    AnsiTransactionManager, CacheSize, ConnectionSealed, DefaultLoadingMode, DynInstrumentation,
    Instrumentation, LoadConnection, SimpleConnection,
};
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{Query, QueryBuilder, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult, QueryResult};

use super::cursor::{CollectRawHandler, Cursor};
use crate::conversion::ToParams;
use crate::protocol::types::Oid;

pub struct Connection {
    conn: crate::sync::Conn,
    transaction_manager: AnsiTransactionManager,
    metadata_cache: PgMetadataCache,
    instrumentation: DynInstrumentation,
}

#[expect(unsafe_code)]
// SAFETY: sync::Conn owns a TCP stream and buffer set, both of which are Send.
unsafe impl Send for Connection {}

impl SimpleConnection for Connection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.conn.query_drop(query).map_err(into_diesel_error)?;
        Ok(())
    }
}

impl ConnectionSealed for Connection {}

impl diesel::connection::Connection for Connection {
    type Backend = Pg;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let opts = crate::Opts::try_from(database_url)
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        let conn = crate::sync::Conn::new(opts)
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        Ok(Self {
            conn,
            transaction_manager: AnsiTransactionManager::default(),
            metadata_cache: PgMetadataCache::new(),
            instrumentation: DynInstrumentation::default_instrumentation(),
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId,
    {
        let (sql, binds) = self.prepare_query(source)?;
        let params = DieselParams::new(&binds);
        let result = self
            .conn
            .exec_drop(&*sql, params)
            .map_err(into_diesel_error)?;
        Ok(result.unwrap_or(0) as usize)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_manager
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = instrumentation.into();
    }

    fn set_prepared_statement_cache_size(&mut self, _size: CacheSize) {
        // zero-postgres manages its own statement lifecycle
    }
}

impl LoadConnection<DefaultLoadingMode> for Connection {
    type Cursor<'conn, 'query> = Cursor;
    type Row<'conn, 'query> = super::row::ZeroPgRow;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Pg> + QueryId + 'query,
        Pg: QueryMetadata<T::SqlType>,
    {
        let (sql, binds) = self.prepare_query(&source)?;
        let params = DieselParams::new(&binds);
        let mut handler = CollectRawHandler::new();
        self.conn
            .exec(&*sql, params, &mut handler)
            .map_err(into_diesel_error)?;
        let columns: Arc<[_]> = handler.columns.into();
        Ok(Cursor::new(columns, handler.rows))
    }
}

impl GetPgMetadataCache for Connection {
    fn get_metadata_cache(&mut self) -> &mut PgMetadataCache {
        &mut self.metadata_cache
    }
}

impl Connection {
    fn prepare_query<T: QueryFragment<Pg>>(
        &mut self,
        source: &T,
    ) -> QueryResult<(String, RawBytesBindCollector<Pg>)> {
        let mut qb = diesel::pg::PgQueryBuilder::default();
        source.to_sql(&mut qb, &Pg)?;
        let sql = qb.finish();

        let mut bind_collector = RawBytesBindCollector::<Pg>::new();
        source.collect_binds(&mut bind_collector, self, &Pg)?;

        Ok((sql, bind_collector))
    }
}

/// Bridges diesel's `RawBytesBindCollector<Pg>` binds to zero-postgres `ToParams`.
struct DieselParams<'a> {
    binds: &'a RawBytesBindCollector<Pg>,
}

impl<'a> DieselParams<'a> {
    fn new(binds: &'a RawBytesBindCollector<Pg>) -> Self {
        Self { binds }
    }
}

impl ToParams for DieselParams<'_> {
    fn param_count(&self) -> usize {
        self.binds.binds.len()
    }

    fn natural_oids(&self) -> Vec<Oid> {
        self.binds
            .metadata
            .iter()
            .map(|m| m.oid().unwrap_or(0))
            .collect()
    }

    fn encode(&self, _target_oids: &[Oid], buf: &mut Vec<u8>) -> crate::Result<()> {
        for bind in &self.binds.binds {
            match bind {
                Some(bytes) => {
                    let len = i32::try_from(bytes.len())
                        .map_err(|_e| crate::Error::Encode("bind value too large".into()))?;
                    buf.extend_from_slice(&len.to_be_bytes());
                    buf.extend_from_slice(bytes);
                }
                None => {
                    buf.extend_from_slice(&(-1_i32).to_be_bytes());
                }
            }
        }
        Ok(())
    }
}

fn into_diesel_error(e: crate::Error) -> diesel::result::Error {
    match &e {
        crate::Error::Server(server_error) => {
            let kind = match server_error.code() {
                "23505" => diesel::result::DatabaseErrorKind::UniqueViolation,
                "23503" => diesel::result::DatabaseErrorKind::ForeignKeyViolation,
                "23502" => diesel::result::DatabaseErrorKind::NotNullViolation,
                "23514" => diesel::result::DatabaseErrorKind::CheckViolation,
                "40001" => diesel::result::DatabaseErrorKind::SerializationFailure,
                "23P01" => diesel::result::DatabaseErrorKind::ReadOnlyTransaction,
                _ => diesel::result::DatabaseErrorKind::Unknown,
            };
            diesel::result::Error::DatabaseError(
                kind,
                Box::new(ServerErrorInfo(server_error.clone())),
            )
        }
        _ => diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::Unknown,
            Box::new(e.to_string()),
        ),
    }
}

#[derive(Debug)]
struct ServerErrorInfo(crate::ServerError);

impl diesel::result::DatabaseErrorInformation for ServerErrorInfo {
    fn message(&self) -> &str {
        self.0.message()
    }

    fn details(&self) -> Option<&str> {
        self.0.detail()
    }

    fn hint(&self) -> Option<&str> {
        self.0.hint()
    }

    fn table_name(&self) -> Option<&str> {
        self.0.table()
    }

    fn column_name(&self) -> Option<&str> {
        self.0.column()
    }

    fn constraint_name(&self) -> Option<&str> {
        self.0.constraint()
    }

    fn statement_position(&self) -> Option<i32> {
        self.0.position().map(|p| p as i32)
    }
}
