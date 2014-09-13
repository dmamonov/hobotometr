package org.hobotometr.test;

import com.google.common.base.Charsets;
import org.hobotometr.db.Database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 12:38 AM
 */
public class TestRunner {
    private static boolean javaWarmed = false;

    public static void runTest(final TestSpec config) {
        try {
            runTestImpl(config);
        } catch (final Exception oops) {
            oops.printStackTrace();
        }
    }

    private static void runTestImpl(final TestSpec config) throws InterruptedException, IOException {
        final String testName = config.toString()+(EnvSpec.yield?",yield=1":"");
        final File csvFile = new File(String.format("data/%s/%s/%s.csv", config.getDatabaseType(), EnvSpec.host, config.toString()));
        if (csvFile.exists()) {
            return;
        }
        if (javaWarmed){
            Thread.sleep(EnvSpec.freeTime);
        }


        final Database sharedTestQueries = config.getDatabaseType().createConnectionPool(EnvSpec.host, config.getSharedPoolSize());
        final Database writeTestQueries = config.getWritePoolSize() > 0
                ? config.getDatabaseType().createConnectionPool(EnvSpec.host, config.getWritePoolSize())
                : sharedTestQueries;

        sharedTestQueries.init();


        final AtomicInteger readProgress = new AtomicInteger();
        final AtomicInteger readFailures = new AtomicInteger();
        final AtomicInteger writeProgress = new AtomicInteger();
        final AtomicInteger writeFailures = new AtomicInteger();

        final AtomicBoolean stop = new AtomicBoolean(false);

        abstract class AbstractLoadThread extends Thread {
            protected final AtomicInteger successCounter;
            protected final AtomicInteger failuresCounter;

            AbstractLoadThread(final AtomicInteger successCounter, final AtomicInteger failuresCounter) {
                this.successCounter = successCounter;
                this.failuresCounter = failuresCounter;
            }

            @Override
            public void run() {
                while (!stop.get()) {
                    if (perform()) {
                        successCounter.incrementAndGet();
                    } else {
                        failuresCounter.incrementAndGet();
                    }
                    if (EnvSpec.yield){
                        yield();
                    }
                }
            }

            abstract boolean perform();
        }

        abstract class AbstractReadLoadThread extends AbstractLoadThread {
            protected AbstractReadLoadThread() {
                super(readProgress, readFailures);
            }
        }

        abstract class AbstractWriteLoadThread extends AbstractLoadThread {
            protected AbstractWriteLoadThread() {
                super(writeProgress, writeFailures);
            }
        }

        for (int i = 0; i < config.getReadLiteCpuThreads(); i++) {
            new AbstractReadLoadThread() {
                @Override
                boolean perform() {
                    return sharedTestQueries.selectCpuLite(1, config.getReadRange());
                }
            }.start();
        }

        for (int i = 0; i < config.getReadHeavyCpuThreads(); i++) {
            new AbstractReadLoadThread() {
                @Override
                boolean perform() {
                    return sharedTestQueries.selectCpuHeavy(1, config.getReadRange(), config.getReadSize());
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteInsertThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.insert();
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteUpdateTinyThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.updateTinyColumnById(1, config.getUpdateRange());
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteUpdateWideThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.updateWideColumnById(1, config.getUpdateRange());
                }
            }.start();
        }


        if (javaWarmed) {
            Thread.sleep(15000);
        } else {
            System.out.println("Warm JVM (40 sec)");
            Thread.sleep(40000);
            javaWarmed = true;
        }

        final StringBuilder csv = new StringBuilder("'Time','ReadOps','ReadErr','WriteOps','WriteErr'\n");

        System.gc(); //cleanup heap before test.
        Thread.sleep(10); //wait a bit after gc.

        //start tracking:
        System.out.println("Start tracking ("+config.getDatabaseType()+"): " + testName);
        for (int time = 0; time < 60; time++) {
            if (EnvSpec.forceGcDuringTest) {
                if (time % 10 == 0) {
                    //cleanup environment:
                    System.gc();
                    for (final AtomicInteger reset : new AtomicInteger[]{readProgress, readFailures, writeProgress, writeFailures}) {
                        reset.set(0);
                    }
                }
            }


            //reset counters before test.
            readProgress.set(0);
            readFailures.set(0);
            writeProgress.set(0);
            writeFailures.set(0);

            //actual test step:
            final long secondStart = System.currentTimeMillis();
            Thread.sleep(1000);
            final double duration = (System.currentTimeMillis() - secondStart) / 1000.0;

            //fetch metrics after test:
            final int readOpsSnapshot = readProgress.get();
            final int readErrorsSnapshot = readFailures.get();
            final int writeOpsSnapshot = writeProgress.get();
            final int writeErrorsSnapshot = writeFailures.get();

            //render and print metrics:
            System.out.printf("T %4d, R %5d/%5d, W %5d/%5d, dur=%.3f, Pools(%d/%d)\n",
                    time,
                    readOpsSnapshot, readErrorsSnapshot,
                    writeOpsSnapshot, writeErrorsSnapshot,
                    duration,
                    config.getSharedPoolSize(), config.getWritePoolSize());
            csv.append(String.format("%d,%d,%d,%d,%d\n", time, readOpsSnapshot, readErrorsSnapshot, writeOpsSnapshot, writeErrorsSnapshot));
        }

        if (!csvFile.getParentFile().exists()) {
            checkState(csvFile.getParentFile().mkdirs());
        }
        Files.write(csvFile.toPath(), csv.toString().getBytes(Charsets.UTF_8));
        System.out.println("Done");

        //cleanup after test
        stop.set(true);
        Thread.sleep(500);

        sharedTestQueries.close();
        if (writeTestQueries != sharedTestQueries) {
            writeTestQueries.close();
        }
    }
}
