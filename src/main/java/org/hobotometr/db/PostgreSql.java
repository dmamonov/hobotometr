package org.hobotometr.db;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 12:25 AM
 */
class PostgreSql extends AbstractSqlDatabase {
    protected PostgreSql(final HikariDataSource hikariDataSource) {
        super(hikariDataSource);
    }

    @Override
    public void init() {
        sql.update("" +
                "CREATE TABLE IF NOT EXISTS hikari (\n" +
                "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                "  title TEXT NULL,\n" +
                "  val INTEGER NOT NULL DEFAULT 0\n" +
                ");");
        sql.update("CREATE INDEX NOT EXISTS i_hikari_title ON hikari(title);");
        sql.update("CREATE INDEX NOT EXISTS i_hikari_val ON hikari(val);");
    }
}
