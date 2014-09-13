package org.hobotometr.db;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 12:25 AM
 */
class MySql extends AbstractSqlDatabase {
    public MySql(final HikariDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void init() {
        if (0==sql.queryForObject("" +
                        "SELECT COUNT(*)\n" +
                        "  FROM information_schema.tables \n" +
                        " WHERE table_schema = 'demo' \n" +
                        "   AND table_name = 'hikari';",
                Integer.class)) {
            sql.update("" +
                    "CREATE TABLE hikari (\n" +
                    "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                    "  title varchar(1024) NULL,\n" + //mysql TEXT is a way different to pgsql TEXT, this we use varchar here.
                    "  val INTEGER NOT NULL DEFAULT 0\n" +
                    ");");
            sql.update("CREATE INDEX i_hikari_title ON hikari(title);");
            sql.update("CREATE INDEX i_hikari_val ON hikari(val);");
        }
    }
}
