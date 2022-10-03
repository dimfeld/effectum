use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

use crate::Result;

const MIGRATIONS: [&str; 1] = [include_str!("../migrations/00001-init.sql")];

fn create_migrations() -> Migrations<'static> {
    let items = MIGRATIONS.iter().map(|m| M::up(m)).collect::<Vec<_>>();
    Migrations::new(items)
}

pub fn migrate(conn: &mut Connection) -> Result<()> {
    let migrations = create_migrations();
    migrations.to_latest(conn)?;
    Ok(())
}

mod tests {
    #[test]
    fn migrations_valid() {
        let migrations = super::create_migrations();
        migrations.validate().unwrap();
    }
}
