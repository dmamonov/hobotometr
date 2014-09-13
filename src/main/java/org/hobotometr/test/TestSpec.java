package org.hobotometr.test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.hobotometr.db.DatabaseType;

import static com.google.common.base.Preconditions.checkNotNull;

/**
* @author dmitry.mamonov
*         Created: 2014-09-14 12:38 AM
*/
public class TestSpec {
    private DatabaseType databaseType = DatabaseType.postgres;
    private int sharedPoolSize;
    private int writePoolSize = 0;
    private int readLiteCpuThreads = 0;
    private int readHeavyCpuThreads = 0;
    private int writeInsertThreads = 0;
    private int writeUpdateTinyThreads = 0;
    private int writeUpdateWideThreads = 0;
    private int readRange = EnvSpec.readRangeDefault;
    private int readSize = 1000;
    private int updateRange = EnvSpec.updateRangeDefault;

    private TestSpec() {
        //note [DM] use Builder instead.
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public int getSharedPoolSize() {
        return sharedPoolSize;
    }

    public int getWritePoolSize() {
        return writePoolSize;
    }

    public int getReadLiteCpuThreads() {
        return readLiteCpuThreads;
    }

    public int getReadHeavyCpuThreads() {
        return readHeavyCpuThreads;
    }

    public int getWriteInsertThreads() {
        return writeInsertThreads;
    }

    public int getWriteUpdateTinyThreads() {
        return writeUpdateTinyThreads;
    }

    public int getWriteUpdateWideThreads() {
        return writeUpdateWideThreads;
    }

    public int getReadRange() {
        return readRange;
    }

    public int getReadSize() {
        return readSize;
    }

    public int getUpdateRange() {
        return updateRange;
    }

    @Override
    public String toString() {
        return Joiner.on(",").join(ImmutableList.of(
                String.format("sp=%04d", sharedPoolSize),
                String.format("wp=%04d", writePoolSize),
                String.format("r_lite=%04d", readLiteCpuThreads),
                String.format("r_heavy=%04d", readHeavyCpuThreads),
                String.format("w_ins=%04d", writeInsertThreads),
                String.format("w_up_tiny=%04d", writeUpdateTinyThreads),
                String.format("w_up_wide=%04d", writeUpdateWideThreads)
        ));
    }

    public static class Builder {
        private TestSpec delegate = new TestSpec();

        public Builder setDatabaseType(final DatabaseType databaseType) {
            delegate.databaseType = databaseType;
            return this;
        }

        public Builder setSharedPoolSize(final int sharedPoolSize) {
            delegate.sharedPoolSize = sharedPoolSize;
            return this;
        }

        public Builder setWritePoolSize(final int writePoolSize) {
            delegate.writePoolSize = writePoolSize;
            return this;
        }

        public Builder setReadLiteCpuThreads(final int readLiteCpuThreads) {
            delegate.readLiteCpuThreads = readLiteCpuThreads;
            return this;
        }

        public Builder setReadHeavyCpuThreads(final int readHeavyCpuThreads) {
            delegate.readHeavyCpuThreads = readHeavyCpuThreads;
            return this;
        }

        public Builder setWriteInsertThreads(final int writeInsertThreads) {
            delegate.writeInsertThreads = writeInsertThreads;
            return this;
        }

        public Builder setWriteUpdateTinyThreads(final int writeUpdateTinyThreads) {
            delegate.writeUpdateTinyThreads = writeUpdateTinyThreads;
            return this;
        }

        public Builder setWriteUpdateWideThreads(final int writeUpdateWideThreads) {
            delegate.writeUpdateWideThreads = writeUpdateWideThreads;
            return this;
        }

        public TestSpec build() {
            final TestSpec result = checkNotNull(this.delegate, "Builder closed");
            this.delegate = null; //close builder.
            return result;
        }
    }
}
