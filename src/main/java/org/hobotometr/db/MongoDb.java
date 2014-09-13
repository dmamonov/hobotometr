package org.hobotometr.db;

import com.google.common.base.Charsets;
import com.mongodb.MongoClient;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author dmitry.mamonov
 *         Created: 2014-09-14 12:25 AM
 */
class MongoDb extends AbstractDatabase {
    private final MongoClient client;
    private final MongoTemplate mongo;

    public MongoDb(final MongoClient client) {
        this.client = client;
        this.mongo = new MongoTemplate(new SimpleMongoDbFactory(client,"demo"));
    }

    @Override
    public void init() {
        try {
            mongo.createCollection("hikari");
            persistentId.reset();
        } catch (final RuntimeException oops){
            if (!oops.getMessage().contains("exists")){
                throw oops;
            }
        }
        try {
            mongo.insert(new MongoDomain(), "hikari");
        } catch (final DuplicateKeyException ignore){
            //ignore.
        }
        mongo.indexOps("hikari").ensureIndex(new Index().on("id", Sort.Direction.ASC));
        mongo.indexOps("hikari").ensureIndex(new Index().on("title", Sort.Direction.ASC));
        mongo.indexOps("hikari").ensureIndex(new Index().on("val", Sort.Direction.ASC));
    }

    @Override
    public boolean insert() {
        try {
            mongo.insert(new MongoDomain(), "hikari");
            return true;
        } catch (final RuntimeException re) {
            //System.out.println(re.getMessage());
            return false;
        }
    }

    @Override
    public boolean updateTinyColumnById(final int rangeFrom, final int rangeTo) {
        try {
            final int idForTinyUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
            mongo.updateFirst(new Query(Criteria.where("id").is(idForTinyUpdate)), new Update().set("val", random.nextInt()), "hikari");
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean updateWideColumnById(final int rangeFrom, final int rangeTo) {
        try {
            final int idForWideUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
            final StringBuilder titleBuilder = new StringBuilder();
            {
                final String titlePart = String.format("%08d", random.nextInt(100_000_000));
                for (int i = 0; i < 16; i++) {
                    titleBuilder.append(titlePart);
                }
            }
            mongo.updateFirst(new Query(Criteria.where("id").is(idForWideUpdate)), new Update().set("title", titleBuilder.toString()), "hikari");
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public boolean selectCpuLite(final int rangeFrom, final int rangeTo) {
        try {
            final int idToFind = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
            mongo.find(new Query(Criteria.where("id").is(idToFind)), MongoDomain.class);
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
            mongo.aggregate(Aggregation.newAggregation(
                            Aggregation.match(new Criteria("id").lte(end).gte(start)),
                            Aggregation.group("val").sum("val").as("val")
                    ),
                    "hikari", MongoDomain.class);
            return true;
        } catch (final RuntimeException re) {
            return false;
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private static final PersistentId persistentId = new PersistentId();
    private static final class PersistentId {
        private static final File idFile = new File("id.seq");
        private final AtomicInteger idSeq = new AtomicInteger(0);

        PersistentId() {
            if (idFile.exists()){
                try {
                    idSeq.set(Integer.parseInt(new String(Files.readAllBytes(idFile.toPath())).trim()));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public int nextId(){
            final int result = idSeq.incrementAndGet();
            if (result%1000==0) {
                try {
                    Files.write(idFile.toPath(), String.valueOf(result).getBytes(Charsets.UTF_8));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        public void reset() {
            if (idFile.exists()) {
                checkState(idFile.delete(), "Failed to delete id file: %s", idFile);
            }
        }
    }

    public static class MongoDomain {
        public long id;
        public String title;
        public int val;

        public MongoDomain() {
            this.id=persistentId.nextId();
            final StringBuilder titleBuilder = new StringBuilder();
            {
                final String titlePart = String.format("%08d", random.nextInt(100_000_000));
                for (int i = 0; i < 16; i++) {
                    titleBuilder.append(titlePart);
                }
            }
            this.title = titleBuilder.toString();
            this.val = random.nextInt();

        }
    }
}
