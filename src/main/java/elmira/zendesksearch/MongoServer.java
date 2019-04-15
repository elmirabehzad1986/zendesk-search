package elmira.zendesksearch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoServer implements ZendeskRepository {

    static Logger logger = Logger.getLogger(MongoServer.class.getName());

    private String ip;
    private int port;
    private MongodExecutable mongodExecutable;
    MongoDatabase db;

    class Join {
        public String leftCollection;
        public String leftField;
        public String rightCollection;
        public String rightField;
    }
    List<Join> joinDefenitions;


    public MongoServer(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.mongodExecutable = null;
        this.joinDefenitions = new ArrayList<>();
    }

    @Override
    public void start(String repositoryName) throws IOException {

        MongodStarter starter = MongodStarter.getDefaultInstance();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(this.ip, this.port, Network.localhostIsIPv6()))
                    .build();

        this.mongodExecutable = starter.prepare(mongodConfig);
        MongodProcess mongod = this.mongodExecutable.start();

        MongoClient mongoClient = new MongoClient(getIp(), getPort());

        // create codec registry for POJOs
        CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        // get handle to "mydb" database
        this.db = mongoClient.getDatabase(repositoryName).withCodecRegistry(pojoCodecRegistry);

    }

    @Override
    public void stop() {
        if (mongodExecutable != null) {
            mongodExecutable.stop();
            mongodExecutable = null;
        }

    }

    @Override
    public <E> void load(Class<E> entity_class, String jsonFile) throws FileNotFoundException {

        String entityName = entity_class.getSimpleName().toLowerCase();

        logger.log(Level.INFO, String.format("Loading %ss into database ...", entityName));
        //db.createCollection(entityName);
        //col.save(new BasicDBObject("testDoc", new Date()));

        MongoCollection<E> collection = db.getCollection(entityName, entity_class);

        AtomicInteger count = new AtomicInteger();
        int batch = 100;

        //List<InsertOneModel<E>> docs = new ArrayList<>();
        List<E> docs = new ArrayList<>();

        readStream(entity_class, jsonFile, u-> {
            docs.add(u);
            if (count.incrementAndGet() == batch) {
                //collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                collection.insertMany(docs);
                docs.clear();
                count.set(0);
            }
        });

        if (count.get() > 0) {
            //collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
            collection.insertMany(docs);
        }


        logger.log(Level.FINE, String.format("Loading %ss is finished", entityName));

    }

    @Override
    public <E> AggregateIterable<Document> search(Class<E> entity_class, Map<String, String> searchClauses) {

        String entityName = entity_class.getSimpleName().toLowerCase();

        Document where = new Document();
        searchClauses.forEach(where::append);

        Bson match = new Document("$match", where);

        List<Bson> filters = new ArrayList<>();
        filters.add(match);

        joinDefenitions.stream().filter(j -> j.leftCollection.equals(entityName)).forEach(join -> {
            Bson lookup = new Document("$lookup",
                    new Document("from", join.rightCollection)
                            .append("localField", join.leftField)
                            .append("foreignField", join.rightField)
                            .append("as", entityName+"_"+join.rightCollection));
            filters.add(lookup);
        });

        return db.getCollection(entityName).aggregate(filters);
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    private static <E> void  readStream(Class<E> entity_class, String jsonFilePath, Consumer<E> entityHandler) throws FileNotFoundException {
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(jsonFilePath));
        } catch (FileNotFoundException e) {
            logger.log(Level.SEVERE, String.format("JSON file not found(%s)", jsonFilePath));
            throw e;
        }

        Gson gson = new GsonBuilder().create();

        // Read file in stream mode
        try {
            reader.beginArray();
            while (reader.hasNext()) {
                // Read data into object model
                E entity = null;
                try {
                    entity = gson.fromJson(reader, entity_class);
                    entityHandler.accept(entity);
                } catch (JsonIOException e) {
                    logger.log(Level.SEVERE, String.format("IO exception during reading JSON file(%s): %s", jsonFilePath, e.getMessage()));
                    throw e;
                } catch (JsonSyntaxException e) {
                    logger.log(Level.SEVERE, String.format("Syntax error during reading JSON file(%s): %s", jsonFilePath, e.getMessage()));
                    throw e;
                }
            }
            reader.close();

        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("IO exception on reading JSON file(%s): %s", jsonFilePath, e.getMessage()));
        }
    }



    public <E1, E2> void leftjoin(Class<E1> entity1_class, String entity1_foreign_field, Class<E2> entity2_class, String entity2_foreign_field) {
        Join join = new Join();
        join.leftCollection = entity1_class.getSimpleName().toLowerCase();
        join.rightCollection = entity2_class.getSimpleName().toLowerCase();
        join.leftField = entity1_foreign_field;
        join.rightField = entity2_foreign_field;

        joinDefenitions.add(join);
    }


}


