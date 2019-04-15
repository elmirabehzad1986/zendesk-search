package elmira.zendesksearch;


import elmira.zendesksearch.model.Organization;
import elmira.zendesksearch.model.Ticket;
import elmira.zendesksearch.model.User;

import javax.naming.ConfigurationException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZendeskSearchApplication {

    private static Logger logger = Logger.getLogger(ZendeskSearchApplication.class.getName());

    private Properties config;

    public ZendeskSearchApplication(Properties config) {
        this.config = config;
    }

    public static void main(String[] args) {

        Properties prop = new Properties();

        if(args.length > 2) {
            String configFile = args[1];
            try (InputStream input = new FileInputStream(configFile)) {
                // load a properties file
                prop.load(input);
            } catch (FileNotFoundException e) {
                logger.log(Level.SEVERE, String.format("Given properties file not found: %s", configFile));
                e.printStackTrace();
                return;
            } catch (IOException e) {
                logger.log(Level.SEVERE, String.format("Could not open the given properties file: %s", configFile));
                e.printStackTrace();
                return;
            }
        } else {
            loadDefaultProperties(prop);
        }

        ZendeskSearchApplication app = new ZendeskSearchApplication(prop);
        try {
            app.run();
        } catch (Exception e) {
            logger.severe("Exit because of fatal error");
            e.printStackTrace();
        }
    }

    private static void loadDefaultProperties(Properties prop) {
        prop.setProperty("repository.engine", "embeded-mongodb");
        prop.setProperty("repository.name", "zendesk-data");
        prop.setProperty("db.ip", "127.0.0.1");
        prop.setProperty("db.port", "12345");
        prop.setProperty("json.user", "entities/users.json");
        prop.setProperty("json.organization", "entities/organizations.json");
        prop.setProperty("json.ticket", "entities/tickets.json");
    }

    private ZendeskRepository createRepository() throws ConfigurationException {
        if("embeded-mongodb".equals(config.getProperty("repository.engine"))) {
            return new MongoServer(
                    config.getProperty("db.ip"),
                    Integer.valueOf(config.getProperty("db.port"))
            );
        }

        String msg = String.format("Unknown repository engine  in configuration: %s", config.getProperty("repository.engine"));
        logger.log(Level.SEVERE, msg);
        throw new ConfigurationException(msg);
    }

    public void run() throws ConfigurationException, IOException {

        System.out.println("Welcome to Zendesk Search(A code challenge written by Elmira)");

        logger.log(Level.INFO, "Starting database server ...");

        ZendeskRepository repo = createRepository();

        ClassLoader classLoader = ZendeskSearchApplication.class.getClassLoader();

        try {
            repo.start(config.getProperty("repository.name"));
            logger.log(Level.FINE, "Database server started");

            logger.log(Level.INFO, "Loading users ...");
            repo.load( User.class, classLoader.getResource(config.getProperty("json.user")).getPath());
            logger.log(Level.FINE, "Users loaded");

            logger.log(Level.INFO, "Loading organizations ...");
            repo.load(Organization.class, classLoader.getResource(config.getProperty("json.organization")).getPath());
            logger.log(Level.FINE, "Organizations loaded");

            logger.log(Level.INFO, "Loading tickets ...");
            repo.load(Ticket.class, classLoader.getResource(config.getProperty("json.ticket")).getPath());
            logger.log(Level.FINE, "Tickets loaded");

            Scanner scanner = new Scanner(System.in);
            String option = "";
            boolean jumpToMenu = false;
            Map<String, String> searchCauses = new HashMap<>();
            String clause;

            while(true) {
                jumpToMenu = false;

                System.out.println("*** MENU ***");
                System.out.println("\tPress 1 to search");
                System.out.println("\tPress 2 to view searchable fields");
                System.out.println("\tPress q to exit");
                System.out.print("\nSelect one of the above options: ");



                try {
                    option = scanner.next("\\s*[A-Za-z1-2]\\s*").trim();
                } catch (InputMismatchException e) {
                    option = "";
                }

                if("1".equals(option)) {

                    System.out.println("\tEnter query in format \"field1\"=\"value\"[&/|]\"field2\"=\"value\"...");

                    searchCauses.clear();
                    try {
                        while (!(clause = scanner.next("\\w+=[^&]+")).isEmpty()) {
                            String[] keyValue = clause.split("=");
                            if(keyValue.length != 2) {
                                System.out.print("\nQuery format is invalid");
                                jumpToMenu = true;
                                break;
                            }
                            if(searchCauses.containsKey(keyValue[0])) {
                                logger.log(Level.WARNING, String.format("Duplicate fields found in the search clause for field '%s'", keyValue[0]));
                            } else {
                                searchCauses.put(keyValue[0], keyValue[1]);
                            }
                        }
                    } catch (InputMismatchException e) {
                        System.out.print("\nQuery format is invalid");
                        jumpToMenu = true;
                    }

                    if(jumpToMenu){
                        continue;
                    }

                    //repo.search(searchCauses);

                } else if("2".equals(option)) {

                } else if("q".equals(option)) {
                    break;
                } else {
                    System.out.println("The selected option does not exist, try again.");
                }
            }

            System.out.println("THE END");

        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("Could not start mongodb server: %s", e.getMessage()));
            throw e;

        } finally {
            repo.stop();
        }

    }

}
