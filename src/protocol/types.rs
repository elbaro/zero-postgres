//! Common PostgreSQL wire protocol types.

/// PostgreSQL Object Identifier (OID)
pub type Oid = u32;

/// Data format code in PostgreSQL protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u16)]
pub enum FormatCode {
    /// Text format (human-readable)
    #[default]
    Text = 0,
    /// Binary format (type-specific packed representation)
    Binary = 1,
}

impl FormatCode {
    /// Create a FormatCode from a raw u16 value.
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => FormatCode::Text,
            1 => FormatCode::Binary,
            _ => FormatCode::Text, // Default to text for unknown values
        }
    }
}

impl From<u16> for FormatCode {
    fn from(value: u16) -> Self {
        Self::from_u16(value)
    }
}

/// Returns the preferred format code for a given OID.
///
/// Most types use binary format for efficiency, but some types
/// (like NUMERIC) use text format because the binary encoding
/// is complex and text is equally efficient.
pub fn preferred_format(oid: Oid) -> FormatCode {
    match oid {
        oid::NUMERIC => FormatCode::Text,
        _ => FormatCode::Binary,
    }
}

/// Transaction status indicator from ReadyForQuery message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TransactionStatus {
    /// Idle (not in transaction block)
    #[default]
    Idle = b'I',
    /// In transaction block
    InTransaction = b'T',
    /// In failed transaction block (queries will be rejected until rollback)
    Failed = b'E',
}

impl TransactionStatus {
    /// Create a TransactionStatus from a raw byte value.
    pub fn from_byte(value: u8) -> Option<Self> {
        match value {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }

    /// Returns true if currently in a transaction (either active or failed).
    pub fn in_transaction(self) -> bool {
        matches!(
            self,
            TransactionStatus::InTransaction | TransactionStatus::Failed
        )
    }

    /// Returns true if the transaction has failed.
    pub fn is_failed(self) -> bool {
        matches!(self, TransactionStatus::Failed)
    }
}

/// Well-known PostgreSQL type OIDs.
pub mod oid {
    use super::Oid;

    /// boolean, format 't'/'f'
    pub const BOOL: Oid = 16;
    /// variable-length string, binary values escaped
    pub const BYTEA: Oid = 17;
    /// single character
    pub const CHAR: Oid = 18;
    /// 63-byte type for storing system identifiers
    pub const NAME: Oid = 19;
    /// ~18 digit integer, 8-byte storage
    pub const INT8: Oid = 20;
    /// -32 thousand to 32 thousand, 2-byte storage
    pub const INT2: Oid = 21;
    /// array of int2, used in system tables
    pub const INT2VECTOR: Oid = 22;
    /// -2 billion to 2 billion integer, 4-byte storage
    pub const INT4: Oid = 23;
    /// registered procedure
    pub const REGPROC: Oid = 24;
    /// variable-length string, no limit specified
    pub const TEXT: Oid = 25;
    /// object identifier(oid), maximum 4 billion
    pub const OID: Oid = 26;
    /// tuple physical location, format '(block,offset)'
    pub const TID: Oid = 27;
    /// transaction id
    pub const XID: Oid = 28;
    /// command identifier type, sequence in transaction id
    pub const CID: Oid = 29;
    /// array of oids, used in system tables
    pub const OIDVECTOR: Oid = 30;
    /// internal type for passing CollectedCommand
    pub const PG_DDL_COMMAND: Oid = 32;
    /// pg_type system catalog
    pub const PG_TYPE: Oid = 71;
    /// pg_attribute system catalog
    pub const PG_ATTRIBUTE: Oid = 75;
    /// pg_proc system catalog
    pub const PG_PROC: Oid = 81;
    /// pg_class system catalog
    pub const PG_CLASS: Oid = 83;
    /// JSON stored as text
    pub const JSON: Oid = 114;
    /// XML content
    pub const XML: Oid = 142;
    /// string representing an internal node tree
    pub const PG_NODE_TREE: Oid = 194;
    /// pseudo-type for the result of a table AM handler function
    pub const TABLE_AM_HANDLER: Oid = 269;
    /// pseudo-type for the result of an index AM handler function
    pub const INDEX_AM_HANDLER: Oid = 325;
    /// geometric point, format '(x,y)'
    pub const POINT: Oid = 600;
    /// geometric line segment, format '\[point1,point2\]'
    pub const LSEG: Oid = 601;
    /// geometric path, format '(point1,...)'
    pub const PATH: Oid = 602;
    /// geometric box, format 'lower left point,upper right point'
    pub const BOX: Oid = 603;
    /// geometric polygon, format '(point1,...)'
    pub const POLYGON: Oid = 604;
    /// geometric line, formats '{A,B,C}'/'\[point1,point2\]'
    pub const LINE: Oid = 628;
    /// network IP address/netmask, network address
    pub const CIDR: Oid = 650;
    /// single-precision floating point number, 4-byte storage
    pub const FLOAT4: Oid = 700;
    /// double-precision floating point number, 8-byte storage
    pub const FLOAT8: Oid = 701;
    /// pseudo-type representing an undetermined type
    pub const UNKNOWN: Oid = 705;
    /// geometric circle, format '<center point,radius>'
    pub const CIRCLE: Oid = 718;
    /// XX:XX:XX:XX:XX:XX:XX:XX, MAC address
    pub const MACADDR8: Oid = 774;
    /// monetary amounts, $d,ddd.cc
    pub const MONEY: Oid = 790;
    /// XX:XX:XX:XX:XX:XX, MAC address
    pub const MACADDR: Oid = 829;
    /// IP address/netmask, host address, netmask optional
    pub const INET: Oid = 869;
    /// access control list
    pub const ACLITEM: Oid = 1033;
    /// 'char(length)' blank-padded string, fixed storage length
    pub const BPCHAR: Oid = 1042;
    /// 'varchar(length)' non-blank-padded string, variable storage length
    pub const VARCHAR: Oid = 1043;
    /// date
    pub const DATE: Oid = 1082;
    /// time of day
    pub const TIME: Oid = 1083;
    /// date and time
    pub const TIMESTAMP: Oid = 1114;
    /// date and time with time zone
    pub const TIMESTAMPTZ: Oid = 1184;
    /// time interval, format 'number units ...'
    pub const INTERVAL: Oid = 1186;
    /// time of day with time zone
    pub const TIMETZ: Oid = 1266;
    /// fixed-length bit string
    pub const BIT: Oid = 1560;
    /// variable-length bit string
    pub const VARBIT: Oid = 1562;
    /// 'numeric(precision, scale)' arbitrary precision number
    pub const NUMERIC: Oid = 1700;
    /// reference to cursor (portal name)
    pub const REFCURSOR: Oid = 1790;
    /// registered procedure (with args)
    pub const REGPROCEDURE: Oid = 2202;
    /// registered operator
    pub const REGOPER: Oid = 2203;
    /// registered operator (with args)
    pub const REGOPERATOR: Oid = 2204;
    /// registered class
    pub const REGCLASS: Oid = 2205;
    /// registered type
    pub const REGTYPE: Oid = 2206;
    /// pseudo-type representing any composite type
    pub const RECORD: Oid = 2249;
    /// array of records
    pub const RECORD_ARRAY: Oid = 2287;
    /// C-style string
    pub const CSTRING: Oid = 2275;
    /// pseudo-type representing any type
    pub const ANY: Oid = 2276;
    /// pseudo-type representing a polymorphic array type
    pub const ANYARRAY: Oid = 2277;
    /// pseudo-type for the result of a function with no real result
    pub const VOID: Oid = 2278;
    /// pseudo-type for the result of a trigger function
    pub const TRIGGER: Oid = 2279;
    /// pseudo-type for the result of a language handler function
    pub const LANGUAGE_HANDLER: Oid = 2280;
    /// pseudo-type representing an internal data structure
    pub const INTERNAL: Oid = 2281;
    /// pseudo-type representing a polymorphic base type
    pub const ANYELEMENT: Oid = 2283;
    /// pseudo-type representing a polymorphic base type that is not an array
    pub const ANYNONARRAY: Oid = 2776;
    /// UUID
    pub const UUID: Oid = 2950;
    /// transaction snapshot
    pub const TXID_SNAPSHOT: Oid = 2970;
    /// pseudo-type for the result of an FDW handler function
    pub const FDW_HANDLER: Oid = 3115;
    /// PostgreSQL LSN
    pub const PG_LSN: Oid = 3220;
    /// pseudo-type for the result of a tablesample method function
    pub const TSM_HANDLER: Oid = 3310;
    /// multivariate ndistinct coefficients
    pub const PG_NDISTINCT: Oid = 3361;
    /// multivariate dependencies
    pub const PG_DEPENDENCIES: Oid = 3402;
    /// pseudo-type representing a polymorphic base type that is an enum
    pub const ANYENUM: Oid = 3500;
    /// text representation for text search
    pub const TSVECTOR: Oid = 3614;
    /// query representation for text search
    pub const TSQUERY: Oid = 3615;
    /// GiST index internal text representation for text search
    pub const GTSVECTOR: Oid = 3642;
    /// registered text search configuration
    pub const REGCONFIG: Oid = 3734;
    /// registered text search dictionary
    pub const REGDICTIONARY: Oid = 3769;
    /// Binary JSON
    pub const JSONB: Oid = 3802;
    /// pseudo-type representing a range over a polymorphic base type
    pub const ANYRANGE: Oid = 3831;
    /// pseudo-type for the result of an event trigger function
    pub const EVENT_TRIGGER: Oid = 3838;
    /// range of integers
    pub const INT4RANGE: Oid = 3904;
    /// range of numerics
    pub const NUMRANGE: Oid = 3906;
    /// range of timestamps without time zone
    pub const TSRANGE: Oid = 3908;
    /// range of timestamps with time zone
    pub const TSTZRANGE: Oid = 3910;
    /// range of dates
    pub const DATERANGE: Oid = 3912;
    /// range of bigints
    pub const INT8RANGE: Oid = 3926;
    /// JSON path
    pub const JSONPATH: Oid = 4072;
    /// registered namespace
    pub const REGNAMESPACE: Oid = 4089;
    /// registered role
    pub const REGROLE: Oid = 4096;
    /// registered collation
    pub const REGCOLLATION: Oid = 4191;
    /// multirange of integers
    pub const INT4MULTIRANGE: Oid = 4451;
    /// multirange of numerics
    pub const NUMMULTIRANGE: Oid = 4532;
    /// multirange of timestamps without time zone
    pub const TSMULTIRANGE: Oid = 4533;
    /// multirange of timestamps with time zone
    pub const TSTZMULTIRANGE: Oid = 4534;
    /// multirange of dates
    pub const DATEMULTIRANGE: Oid = 4535;
    /// multirange of bigints
    pub const INT8MULTIRANGE: Oid = 4536;
    /// pseudo-type representing a polymorphic base type that is a multirange
    pub const ANYMULTIRANGE: Oid = 4537;
    /// pseudo-type representing a multirange over a polymorphic common type
    pub const ANYCOMPATIBLEMULTIRANGE: Oid = 4538;
    /// pseudo-type representing BRIN bloom summary
    pub const PG_BRIN_BLOOM_SUMMARY: Oid = 4600;
    /// pseudo-type representing BRIN minmax-multi summary
    pub const PG_BRIN_MINMAX_MULTI_SUMMARY: Oid = 4601;
    /// multivariate MCV list
    pub const PG_MCV_LIST: Oid = 5017;
    /// transaction snapshot
    pub const PG_SNAPSHOT: Oid = 5038;
    /// full transaction id
    pub const XID8: Oid = 5069;
    /// pseudo-type representing a polymorphic common type
    pub const ANYCOMPATIBLE: Oid = 5077;
    /// pseudo-type representing an array of polymorphic common type elements
    pub const ANYCOMPATIBLEARRAY: Oid = 5078;
    /// pseudo-type representing a polymorphic common type that is not an array
    pub const ANYCOMPATIBLENONARRAY: Oid = 5079;
    /// pseudo-type representing a range over a polymorphic common type
    pub const ANYCOMPATIBLERANGE: Oid = 5080;
    /// registered database
    pub const REGDATABASE: Oid = 8326;
}
