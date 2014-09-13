package org.hobotometr.db;

/**
* @author dmitry.mamonov
*         Created: 2014-09-14 12:26 AM
*/
public interface Database {
    void init();

    boolean insert();

    boolean updateTinyColumnById(int rangeFrom, int rangeTo);

    boolean updateWideColumnById(int rangeFrom, int rangeTo);

    boolean selectCpuLite(int rangeFrom, int rangeTo);

    boolean selectCpuHeavy(int rangeFrom, int rangeTo, int size);

    void close();
}
