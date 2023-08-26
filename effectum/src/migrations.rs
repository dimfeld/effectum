use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

use crate::Result;

const MIGRATIONS: [&str; 2] = [
    include_str!("../migrations/00001-init.sql"),
    include_str!("../migrations/00002-rename-column.sql"),
];

fn create_migrations() -> Migrations<'static> {
    let items = MIGRATIONS.iter().map(|m| M::up(m)).collect::<Vec<_>>();
    Migrations::new(items)
}

pub fn migrate(conn: &mut Connection) -> Result<()> {
    let migrations = create_migrations();
    migrations.to_latest(conn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_valid() {
        let migrations = create_migrations();
        migrations.validate().unwrap();
    }

    #[test]
    fn migrations_work() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
    }
}
