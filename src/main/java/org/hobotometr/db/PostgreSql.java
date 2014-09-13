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
}
