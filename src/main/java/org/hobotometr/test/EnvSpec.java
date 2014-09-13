package org.hobotometr.test;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 1:15 AM
 */
public class EnvSpec {
    public static final String host = System.getProperty("host", "localhost");
    public static final boolean yield = System.getProperties().containsKey("yield");
    public static final boolean insertFirst = Boolean.parseBoolean(System.getProperty("insert.first", "false"));
    public static final boolean simpleOnly = Boolean.parseBoolean(System.getProperty("simple.only", "false"));
}
