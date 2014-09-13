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
        rurTestSuiteHighLevel(EnvSpec.insertFirst);
        rurTestSuiteHighLevel(false);
        System.out.println("All Done");
    }

    private static void rurTestSuiteHighLevel(final boolean insertOnly) {
        for(final int size:sizes) {
            for(final DatabaseType databaseType: DatabaseType.values()) {
                if (System.getProperties().containsKey(databaseType.name())) {
                    TestSuiteRunner.runComplexTestSuite(databaseType, ImmutableList.of(size), insertOnly);
                }
            }
        }
    }





}

