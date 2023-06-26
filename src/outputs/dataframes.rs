
//pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//    // let mut binary_columns: Vec<Series> = vec![];
//    // let mut binary_columns: Vec<ChunkedArray<Utf8Type>> = vec![];
//    // let mut binary_exprs: Vec<Expr> = vec![];
//    let columns: &[Series] = &df.get_columns();
//    let mut lazy = df.clone().lazy();
//    // let mut df_hex = df.clone();
//    columns.iter().for_each(|column| {
//        match column.dtype() {
//            polars::datatypes::DataType::Binary => {
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // let encoded = column.utf8().unwrap().hex_encode();
//                // let encoded = col("hi").binary().encode();
//                // binary_columns.push(encoded.into_series());
//                // binary_exprs.push(column.encode_hex());
//                // df_hex = df_hex.clone().with_column(encoded).unwrap().clone();
//                //
//                let expr = col(column.name()).bin().encode("hex");
//                lazy = lazy.clone().with_column(expr);
//            },
//            _ => ()
//        }
//    });
//    lazy.collect()
//    // Ok(df_hex)
//}

// pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//     let columns: &[Series] = &df.get_columns();
//     let mut lazy = df.clone().lazy();
//     columns.iter().for_each(|column| {
//         if column.dtype() == &polars::datatypes::DataType::Binary {
//             let expr = lit("0x") + col(column.name()).binary().encode("hex");
//             lazy = lazy.clone().with_column(expr);
//         }
//     });
//     lazy.collect()
// }

