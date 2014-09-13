package org.hobotometr.test;

import com.google.common.collect.ImmutableList;
import org.hobotometr.db.DatabaseType;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 1:16 AM
 */
public class TestSuiteRunner {
    public static void runComplexTestSuite(final DatabaseType databaseType, final ImmutableList<Integer> sizes, final boolean insertOnly)  {
        for (final int maxPoolSize : sizes) { //test inserts:
            runInsertTest(databaseType, maxPoolSize);
        }

        for (final int maxPoolSize : sizes) { //test update tiny:
            if (!insertOnly) {
                runUpdateTinyTest(databaseType, maxPoolSize);
            }
        }

        for (final int maxPoolSize : sizes) { //test update wide:
            if (!insertOnly) {
                runUpdateWideTest(databaseType, maxPoolSize);
            }
        }

        for (final int maxPoolSize : sizes) { //test select lite:
            if (!insertOnly) {
                runSelectLiteTest(databaseType, maxPoolSize);
            }
        }

        for (final int maxPoolSize : sizes) { //test select heavy:
            if (!insertOnly) {
                runSelectHeavyTest(databaseType, maxPoolSize);
            }
        }

        if (!EnvSpec.simpleOnly) {
            for (final int maxPoolSize : sizes) { //test select tiny, insert:
                if (!insertOnly) {
                    TestRunner.runTest(new TestSpec.Builder()
                                    .setDatabaseType(databaseType)
                                    .setSharedPoolSize(maxPoolSize)
                                    .setReadLiteCpuThreads(Math.max(1, maxPoolSize / 2))
                                    .setWriteInsertThreads(Math.max(1, maxPoolSize / 2))
                                    .build()
                    );
                }
            }

            for (final int maxPoolSize : sizes) { //test select tiny, update tiny:
                if (!insertOnly) {
                    TestRunner.runTest(new TestSpec.Builder()
                                    .setDatabaseType(databaseType)
                                    .setSharedPoolSize(maxPoolSize)
                                    .setReadLiteCpuThreads(Math.max(1, maxPoolSize / 2))
                                    .setWriteUpdateTinyThreads(Math.max(1, maxPoolSize / 2))
                                    .build()
                    );
                }
            }

            for (final int maxPoolSize : sizes) { //test select heavy, update wide:
                if (!insertOnly) {
                    TestRunner.runTest(new TestSpec.Builder()
                                    .setDatabaseType(databaseType)
                                    .setSharedPoolSize(maxPoolSize)
                                    .setReadHeavyCpuThreads(Math.max(1, maxPoolSize / 2))
                                    .setWriteUpdateWideThreads(Math.max(1, maxPoolSize / 2))
                                    .build()
                    );
                }
            }

            for (final int maxPoolSize : sizes) { //test insert, update, select:
                if (!insertOnly) {
                    final int writePoolSize = Math.max(1, maxPoolSize / 2);
                    TestRunner.runTest(new TestSpec.Builder()
                                    .setDatabaseType(databaseType)
                                    .setSharedPoolSize(maxPoolSize)
                                    .setWritePoolSize(writePoolSize)
                                    .setReadLiteCpuThreads(maxPoolSize)
                                    .setWriteInsertThreads(Math.max(1, writePoolSize / 3))
                                    .setWriteUpdateTinyThreads(Math.max(1, writePoolSize / 3))
                                    .setWriteUpdateTinyThreads(Math.max(1, writePoolSize / 3))
                                    .build()
                    );
                }
            }
        }
    }

    private static void runSelectHeavyTest(final DatabaseType databaseType, final int maxPoolSize) {
        TestRunner.runTest(new TestSpec.Builder()
                        .setDatabaseType(databaseType)
                        .setSharedPoolSize(maxPoolSize)
                        .setReadHeavyCpuThreads(maxPoolSize)
                        .build()
        );
    }

    public static void runSelectLiteTest(final DatabaseType databaseType, final int maxPoolSize) {
        TestRunner.runTest(new TestSpec.Builder()
                        .setDatabaseType(databaseType)
                        .setSharedPoolSize(maxPoolSize)
                        .setReadLiteCpuThreads(maxPoolSize)
                        .build()
        );
    }

    private static void runUpdateWideTest(final DatabaseType databaseType, final int maxPoolSize) {
        TestRunner.runTest(new TestSpec.Builder()
                        .setDatabaseType(databaseType)
                        .setSharedPoolSize(maxPoolSize)
                        .setWriteUpdateWideThreads(maxPoolSize)
                        .build()
        );
    }

    private static void runUpdateTinyTest(final DatabaseType databaseType, final int maxPoolSize) {
        TestRunner.runTest(new TestSpec.Builder()
                        .setDatabaseType(databaseType)
                        .setSharedPoolSize(maxPoolSize)
                        .setWriteUpdateTinyThreads(maxPoolSize)
                        .build()
        );
    }

    private static void runInsertTest(final DatabaseType databaseType, final int maxPoolSize) {
        TestRunner.runTest(new TestSpec.Builder()
                        .setDatabaseType(databaseType)
                        .setSharedPoolSize(maxPoolSize)
                        .setWriteInsertThreads(maxPoolSize)
                        .build()
        );
    }
}
