package org.hobotometr.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
* @author dmitry.mamonov
*         Created: 2014-09-14 12:36 AM
*/
public enum DatabaseType {
    postgres("postgres", "postgres") {
        @Override
        public Database createConnectionPool(final String host, final int poolSize) {
            final HikariConfig config = createDefaultPoolConfig(this, host, poolSize);

            config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

            return new PostgreSql(new HikariDataSource(config));
        }
    },
    mysql("root", "123") {
        @Override
        public Database createConnectionPool(final String host, final int poolSize) {
            final HikariConfig config = createDefaultPoolConfig(this, host, poolSize);

            config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
            config.addDataSourceProperty("port", "3306");
            config.addDataSourceProperty("user", this.user);
            config.addDataSourceProperty("password", this.password);

            return new MySql(new HikariDataSource(config));
        }
    },
    mongo("mongo", "mongo") {
        @Override
        public Database createConnectionPool(final String host, final int poolSize) {
            final MongoClient client;
            try {
                client = new MongoClient(host, new MongoClientOptions.Builder()
                        .connectionsPerHost(poolSize)
                        .build());
            } catch (final UnknownHostException e) {
                throw new RuntimeException(e);
            }

            return new MongoDb(client);
        }
    },
    redis("redis", "redis") {
        @Override
        public Database createConnectionPool(final String host, final int poolSize) {
            throw new RuntimeException("TODO [DM] not implemented yet");
        }
    };
    private final String user;
    private final String password;

    private DatabaseType(final String user, final String password) {
        this.user = user;
        this.password = password;
    }

    public abstract Database createConnectionPool(String host, int poolSize);

    private static HikariConfig createDefaultPoolConfig(final DatabaseType databaseType, final String host, final int poolSize) {
        final HikariConfig config = new HikariConfig();
        config.setAutoCommit(true);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(1));
        config.setIdleTimeout(MINUTES.toMillis(5));
        config.setMaxLifetime(HOURS.toMillis(1));
        config.setLeakDetectionThreshold(0);
        config.setInitializationFailFast(false);
        config.setJdbc4ConnectionTest(true);
        //config.setConnectionInitSql("SELECT 1");
        config.setMaximumPoolSize(poolSize);
        config.setIsolateInternalQueries(false);
        config.setRegisterMbeans(false);
        config.setUsername(databaseType.user);
        config.setPassword(databaseType.password);

        config.addDataSourceProperty("databaseName", "demo");
        config.setPoolName("demo-ds");
        config.addDataSourceProperty("serverName", host);

        return config;
    }
}
