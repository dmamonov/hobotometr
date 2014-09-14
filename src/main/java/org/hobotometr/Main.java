package org.hobotometr;

import com.google.common.collect.ImmutableList;
import org.hobotometr.db.DatabaseType;
import org.hobotometr.test.EnvSpec;
import org.hobotometr.test.TestSuiteRunner;

/**
 * -Xms1024m -Xmx1024m -verbose:gc -XX:+UseG1GC -Dhost=linux.env
 * @author dmitry.mamonov
 *         Created: 2014-08-13 11:24 PM
 */
public class Main {
    private static final ImmutableList<Integer> sizes = ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 24, 28, 32, 48, 64, 96, 128, 192, 256);
    public static void main(final String[] args) {
        System.out.println("Suggested vm settings: -server -Xms4g -Xmx4g -XX:NewSize=3g -XX:MaxNewSize=3g -verbose:gc");
        final String suite = System.getProperty("suite", "complex");
        switch (suite){
            case "select-lite":
                System.out.println("Run "+suite);
                runTestSeries(TestSuiteRunner::runSelectLiteTest);
                break;
            default:
                System.out.println("Run default (complex) suite:");
                runTestSeries((databaseType, poolSize) ->  TestSuiteRunner.runComplexTestSuite(databaseType, ImmutableList.of(poolSize), EnvSpec.insertFirst));
                runTestSeries((databaseType, poolSize) -> TestSuiteRunner.runComplexTestSuite(databaseType, ImmutableList.of(poolSize), false));
        }

        System.out.println("All Done");
    }

    private static void runTestSeries(final TestDelegate delegate){
        for(final int size:sizes) {
            for(final DatabaseType databaseType: DatabaseType.values()) {
                if (System.getProperties().containsKey(databaseType.name())) {
                    delegate.runTest(databaseType, size);
                }
            }
        }
    }

    private interface TestDelegate {
        void runTest(DatabaseType databaseType, int poolSize);
    }
}

