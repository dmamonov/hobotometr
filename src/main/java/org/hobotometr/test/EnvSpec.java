package org.hobotometr.test;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 1:15 AM
 */
public class EnvSpec {
    static {
        System.out.println("Environment configuration:");
    }

    public static final String host = getSystemProperty("host", "localhost", "host/ip address of server with configured database(es) for testing");
    public static final boolean yield = Boolean.parseBoolean(getSystemProperty("yield", "false", "Make a Thread.yield() after each query (interesting in situation where pool=1 and consumers=2)"));
    public static final boolean insertFirst = Boolean.parseBoolean(getSystemProperty("insert.first", "true", "Will perform data inserts tests before queries tests (required for first run)"));
    public static final boolean simpleOnly = Boolean.parseBoolean(getSystemProperty("simple.only", "true", "Do not run composite tests where insert/update/select operations run in parallel)"));
    public static final boolean forceGcDuringTest = Boolean.parseBoolean(getSystemProperty("gc.in.test", "false", "Invoke System.gc() each 10 seconds during test run"));
    public static final long freeTime = Long.parseLong(getSystemProperty("free.time", "5000", "Amount of Milliseconds to sleep between tests (helps to eliminate CPU overheat throttling during test)"));
    public static final int readRangeDefault = Integer.parseInt(getSystemProperty("read.range", "100000", "first N entries will be used for query tests (make sure there is enough entries in database)"));
    public static final int updateRangeDefault = Integer.parseInt(getSystemProperty("update.range", "400000", "first N entries will be used for update tests (make sure there is enough entries in database)"));

    private static String getSystemProperty(final String key, final String byDefault, final String comment) {
        final String value = System.getProperty(key, byDefault);
        System.out.printf("  %-30s %-12s # %s\n", " -D" + key + "=" + value, (byDefault.equals(value) ? "(default)" : "(parameter)"), comment);
        return value;
    }
}
