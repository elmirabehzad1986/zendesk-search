package elmira.zendesksearch;

import com.mongodb.client.AggregateIterable;
import elmira.zendesksearch.model.Organization;
import elmira.zendesksearch.model.Ticket;
import elmira.zendesksearch.model.User;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class MongoServerTest {

    MongoServer repo;
    @Before
    public void setUp() throws Exception {
         this.repo = new MongoServer("127.0.0.1", 1234);

        ClassLoader classLoader = MongoServerTest.class.getClassLoader();

         repo.start("zendest_test");
         repo.load( User.class, classLoader.getResource("users.json").getPath());
         repo.load(Organization.class, classLoader.getResource("organizations.json").getPath());
         repo.load(Ticket.class, classLoader.getResource("tickets.json").getPath());
         repo.leftjoin(User.class, "organization_id", Organization.class, "_id");
         repo.leftjoin(Ticket.class, "submitter_id", User.class, "_id");
    }

    @After
    public void tearDown() throws Exception {
        repo.stop();
    }

    @Test
    public void search() {

        Map<String, String> clause = new HashMap<>();
        clause.put("alias", "Miss Coffey");

        AggregateIterable<Document> it = repo.search(User.class, clause);

        Assert.assertEquals(
                " \"active\": true, \"alias\": \"Miss Coffey\", \"created_at\": \"2016-04-15T05:19:46 -10:00\", \"email\": \"coffeyrasmussen@flotonic.com\", \"external_id\": \"74341f74-9c79-49d5-9611-87ef9b6eb75f\", \"last_login_at\": \"2013-08-04T01:03:27 -10:00\", \"locale\": \"en-AU\", \"name\": \"Francisca Rasmussen\", \"organization_id\": 119, \"phone\": \"8335-422-718\", \"role\": \"admin\", \"shared\": false, \"signature\": \"Don't Worry Be Happy!\", \"suspended\": true, \"tags\": [\"Springville\", \"Sutton\", \"Hartsville/Hartley\", \"Diaperville\"], \"timezone\": \"Sri Lanka\", \"url\": \"http://initech.zendesk.com/api/v2/users/1.json\", \"verified\": true, \"user_organization\": []}"
                , it.first().toJson().substring(45)); // Ignore the generated _id from the begining of the json document

        for (Document row : it) {
            System.out.println(row.toJson());
        }
    }
}