use rusqlite::{functions::FunctionFlags, Error::UserFunctionError};
use serde_json::value::RawValue;
use smallvec::SmallVec;

type RawJsonArray<'a> = SmallVec<[&'a serde_json::value::RawValue; 5]>;

pub fn register_functions(conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.create_scalar_function(
        "json_array_append",
        2,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_INNOCUOUS,
        |ctx| {
            let json: String = ctx.get(0)?;
            let value: String = ctx.get(1)?;
            let mut json: RawJsonArray =
                serde_json::from_str(&json).map_err(|e| UserFunctionError(e.into()))?;

            let json_item =
                RawValue::from_string(value).map_err(|e| UserFunctionError(e.into()))?;
            json.push(&json_item);
            Ok(serde_json::to_string(&json).map_err(|e| UserFunctionError(e.into()))?)
        },
    )?;
    Ok(())
}
