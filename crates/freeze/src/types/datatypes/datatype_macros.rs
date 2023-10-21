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
            /// return Vec of all datatypes
            pub fn all() -> Vec<Self> {
                vec![
                    $(Datatype::$datatype,)*
                ]
            }

            /// name of datatype
            pub fn name(&self) -> String {
                let name = match *self {
                    $(Datatype::$datatype => stringify!($datatype),)*
                };
                format!("{}", heck::AsSnakeCase(name))
            }

            /// default sorting columns of datatype
            pub fn aliases(&self) -> Vec<&'static str> {
                match *self {
                    $(Datatype::$datatype => $datatype::aliases(),)*
                }
            }

            /// default sorting columns of datatype
            pub fn default_sort(&self) -> Vec<String> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_default_sort(),)*
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

            /// whether to use block ranges instead of individual blocks
            pub fn use_block_ranges(&self) -> bool {
                match *self {
                    $(Datatype::$datatype => $datatype::use_block_ranges(),)*
                }
            }

            /// aliases of datatype
            pub fn arg_aliases(&self) -> HashMap<Dim, Dim> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_arg_aliases(),)*
                }
            }

            /// required parameters of each datatype
            pub fn required_parameters(&self) -> Vec<Dim> {
                match *self {
                    $(Datatype::$datatype => $datatype::required_parameters(),)*
                }
            }

            /// optional parameters of each datatype
            pub fn optional_parameters(&self) -> Vec<Dim> {
                match *self {
                    $(Datatype::$datatype => $datatype::optional_parameters(),)*
                }
            }

            /// whether datatype can be collected by block
            pub fn can_collect_by_block(&self) -> bool {
                match *self {
                    $(Datatype::$datatype => $datatype::can_collect_by_block(),)*
                }
            }

            /// whether datatype can be collected by block
            pub fn can_collect_by_transaction(&self) -> bool {
                match *self {
                    $(Datatype::$datatype => $datatype::can_collect_by_transaction(),)*
                }
            }
        }

        /// collect by block
        pub async fn collect_by_block(
            datatype: MetaDatatype,
            partition: Partition,
            source: Arc<Source>,
            query: Arc<Query>
        ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
            let task = match datatype {
                MetaDatatype::Scalar(datatype) => {
                    let inner_request_size = if datatype.use_block_ranges() {
                        Some(source.inner_request_size)
                    } else {
                        None
                    };
                    match datatype {
                    $(
                        Datatype::$datatype => $datatype::collect_by_block(partition, source, query, inner_request_size),
                    )*
                    }
                },
                MetaDatatype::Multi(datatype) => match datatype {
                    MultiDatatype::BlocksAndTransactions => {
                        BlocksAndTransactions::collect_by_block(partition, source, query, None)
                    }
                    MultiDatatype::CallTraceDerivatives => {
                        CallTraceDerivatives::collect_by_block(partition, source, query, None)
                    }
                    MultiDatatype::GethStateDiffs => {
                        GethStateDiffs::collect_by_block(partition, source, query, None)
                    },
                    MultiDatatype::StateDiffs => {
                        StateDiffs::collect_by_block(partition, source, query, None)
                    },
                },
            };
            task.await
        }

        /// collect by transaction
        pub async fn collect_by_transaction(
            datatype: MetaDatatype,
            partition: Partition,
            source: Arc<Source>,
            query: Arc<Query>,
        ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
            let task = match datatype {
                MetaDatatype::Scalar(datatype) => {
                    let inner_request_size = if datatype.use_block_ranges() {
                        Some(source.inner_request_size)
                    } else {
                        None
                    };
                    match datatype {
                    $(
                        Datatype::$datatype => $datatype::collect_by_transaction(partition, source, query, inner_request_size),
                    )*
                    }
                },
                MetaDatatype::Multi(datatype) => {
                    let inner_request_size = None;
                    match datatype {
                        MultiDatatype::BlocksAndTransactions => {
                            BlocksAndTransactions::collect_by_transaction(partition, source, query, inner_request_size)
                        }
                        MultiDatatype::CallTraceDerivatives => {
                            CallTraceDerivatives::collect_by_transaction(partition, source, query, None)
                        }
                        MultiDatatype::GethStateDiffs => {
                            GethStateDiffs::collect_by_transaction(partition, source, query, None)
                        },
                        MultiDatatype::StateDiffs => {
                            StateDiffs::collect_by_transaction(partition, source, query, inner_request_size)
                        }
                    }
                },
            };
            task.await
        }
    };
}
