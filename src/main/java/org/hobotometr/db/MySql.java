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
}
