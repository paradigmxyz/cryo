/// define datatypes
#[macro_export]
macro_rules! define_datatypes {
    ($($datatype:ident),* $(,)?) => {
        /// Datatypes
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
        pub enum Datatype {
            $(
                /// $datatype
                $datatype,
            )*
        }

        impl Datatype {
            /// name of datatype
            pub fn name(&self) -> String {
                let name = match *self {
                    $(Datatype::$datatype => stringify!($datatype),)*
                };
                format!("{}", heck::AsSnakeCase(name))
            }

            /// default sorting columns of datatype
            pub fn default_sort(&self) -> Vec<String> {
                match *self {
                    $(Datatype::$datatype => $datatype::default_sort(),)*
                }
            }

            /// default columns of datatype
            pub fn default_columns(&self) -> Vec<&'static str> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_default_columns(),)*
                }
            }

            /// default blocks of datatype
            pub fn default_blocks(&self) -> Option<String> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_default_blocks(),)*
                }
            }

            /// default column types of datatype
            pub fn column_types(&self) -> HashMap<&'static str, ColumnType> {
                match *self {
                    $(Datatype::$datatype => $datatype::column_types(),)*
                }
            }

            /// aliases of datatype
            pub fn arg_aliases(&self) -> HashMap<String, String> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_arg_aliases(),)*
                }
            }
        }

        const TX_ERROR: &str = "datatype cannot collect by transaction";

        /// collect by block
        pub async fn collect_by_block(
            datatype: MetaDatatype,
            partition: Partition,
            source: Source,
            schemas: HashMap<Datatype, Table>,
        ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
            let task = match datatype {
                MetaDatatype::Scalar(datatype) => match datatype {
                    $(
                        Datatype::$datatype => $datatype::collect_by_block(partition, source, &schemas),
                    )*
                },
                MetaDatatype::Multi(datatype) => match datatype {
                    MultiDatatype::BlocksAndTransactions => {
                        BlocksAndTransactions::collect_by_block(partition, source, &schemas)
                    }
                    MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR.to_string()))?,
                },
            };
            task.await
        }

        /// collect by transaction
        pub async fn collect_by_transaction(
            datatype: MetaDatatype,
            partition: Partition,
            source: Source,
            schemas: HashMap<Datatype, Table>,
        ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
            let task = match datatype {
                MetaDatatype::Scalar(datatype) => match datatype {
                    $(
                        Datatype::$datatype => $datatype::collect_by_transaction(partition, source, &schemas),
                    )*
                },
                MetaDatatype::Multi(datatype) => match datatype {
                    MultiDatatype::BlocksAndTransactions => {
                        Err(CollectError::CollectError(TX_ERROR.to_string()))?
                    }
                    MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR.to_string()))?,
                },
            };
            task.await
        }
    };
}

