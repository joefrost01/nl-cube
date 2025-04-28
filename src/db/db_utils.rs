use duckdb::types::ToSql;
use duckdb::{Result as DuckResult, Statement};
use std::convert::TryInto;

/// Executes a prepared statement with a dynamic slice of parameters,
/// supporting up to 27 parameters.
/// If you exceed 27 parameters, you will get an unimplemented!() panic.
pub fn execute_stmt(stmt: &mut Statement, params: &[&(dyn ToSql + Sync)]) -> DuckResult<usize> {
    macro_rules! match_params {
        ($($n:expr),*) => {
            match params.len() {
                0 => stmt.execute([]),
                $(
                    $n => {
                        let arr: [&(dyn ToSql + Sync); $n] = params.try_into().unwrap();
                        stmt.execute(arr)
                    }
                ),*,
                n => unimplemented!("Too many parameters: {} (max 27 allowed)", n),
            }
        };
    }

    match_params!(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27
    )
}
