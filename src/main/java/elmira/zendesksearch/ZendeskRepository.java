package elmira.zendesksearch;

import com.mongodb.client.AggregateIterable;
import org.bson.Document;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public interface ZendeskRepository {

    void start(String repositoryName) throws IOException;
    void stop();
    <E> void load(Class<E> entity_class, String jsonFile) throws FileNotFoundException;
    <E> AggregateIterable<Document> search(Class<E> entity_class, Map<String, String> searchClauses);
    <E1, E2> void leftjoin(Class<E1> entity1_class, String entity1_fograin_field, Class<E2> entity2_class, String entity2_fograin_field);
}
