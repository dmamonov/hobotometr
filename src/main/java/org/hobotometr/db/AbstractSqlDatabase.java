package org.hobotometr.db;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 12:25 AM
 */
abstract class AbstractSqlDatabase extends AbstractDatabase {
    private final HikariDataSource dataSource;
    private final JdbcTemplate sql;

    protected AbstractSqlDatabase(final HikariDataSource dataSource) {
        this.dataSource = dataSource;
        this.sql = new JdbcTemplate(dataSource);
    }

    @Override
    public void init() {
        sql.update("" +
                "CREATE TABLE IF NOT EXISTS hikari (\n" +
                "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                "  title TEXT NULL,\n" +
                "  val INTEGER NOT NULL DEFAULT 0\n" +
                ");");
        sql.update("CREATE INDEX IF NOT EXISTS i_hikari_title ON hikari(title);");
        sql.update("CREATE INDEX IF NOT EXISTS i_hikari_val ON hikari(val);");
    }

    @Override
    public boolean insert() {
        try {
            sql.update("INSERT INTO hikari (title, val) VALUES (repeat(?, 64), ?);", String.format("%08d", random.nextInt(100_000_000)), random.nextInt(1_000_000_000));
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean updateTinyColumnById(final int rangeFrom, final int rangeTo) {
        try {
            final int idForTinyUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
            sql.update("UPDATE hikari SET val=? WHERE id=?;", random.nextInt(1_000_000_000), idForTinyUpdate);
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean updateWideColumnById(final int rangeFrom, final int rangeTo) {
        try {
            sql.update("UPDATE hikari SET title=(repeat(?, 64) WHERE id=?;", String.format("%08d", random.nextInt(100_000_000)), rangeFrom + (random.nextInt(rangeTo - rangeFrom)));
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean selectCpuLite(final int rangeFrom, final int rangeTo) {
        try {
            sql.queryForObject("SELECT val FROM hikari WHERE id=?", Integer.class, rangeFrom + (random.nextInt(rangeTo - rangeFrom)));
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean selectCpuHeavy(final int rangeFrom, final int rangeTo, final int size) {
        try {
            final int start = rangeFrom + random.nextInt(rangeTo - rangeFrom - size);
            final int end = start + size;
            sql.queryForObject("SELECT avg(val) FROM hikari WHERE id BETWEEN ? AND ?", Integer.class, start, end);
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public void close() {
        this.dataSource.close();
    }
}
