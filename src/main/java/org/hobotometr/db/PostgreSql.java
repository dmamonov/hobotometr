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
        if (0 == sql.queryForObject("" +
                        "SELECT COUNT(*)\n" +
                        "  FROM information_schema.tables \n" +
                        " WHERE table_schema = 'public' \n" +
                        "   AND table_name = 'hikari';",
                Integer.class)) {
            sql.update("" +
                    "CREATE TABLE hikari (\n" +
                    "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                    "  title TEXT NULL,\n" +
                    "  val INTEGER NOT NULL DEFAULT 0\n" +
                    ");");
            if (false) { //note [DM] no additional indexed for now.
                sql.update("CREATE INDEX i_hikari_title ON hikari(title);");
                sql.update("CREATE INDEX i_hikari_val ON hikari(val);");
            }
        }
    }
}
