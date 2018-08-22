package com.cliquedom;

import com.cliquedom.cassandra.connector.CassandraConnector;
import com.cliquedom.cassandra.tables.BookRepository;
import com.cliquedom.cassandra.tables.Book;
import com.cliquedom.cassandra.keyspace.KeyspaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import java.util.ArrayList;
import java.util.List;

public class CassandraClient {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
    public static final String keyspace = "vanhack_wafer_cassandra";

    public static void main(String args[]) {
        //Connect to cassandra using the default IP address and port 9042
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", null);
        Session session = connector.getSession();

        //Create a new keyspace
        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.createKeyspace(keyspace, "SimpleStrategy", 1);
        sr.useKeyspace(keyspace);

        //Create the book Table
        BookRepository br = new BookRepository(session);
        br.createTable();
        br.alterTablebooks("publisher", "text");
        br.createTableBooksByTitle();

        //Insert a new entry
        Book book = new Book(UUIDs.timeBased(), "Effective Cassandra And Java", "Idisimagha Bernard Dublin-Green", "CASSANDRA");
        Book book2 = new Book(UUIDs.timeBased(), "Effective JAVA", "Idisimagha Bernard Dublin-Green", "JAVA");
        Book book3 = new Book(UUIDs.timeBased(), "Effective PHP", "Idisimagha Bernard Dublin-Green", "PHP");
        br.insertBookBatch(book);
        br.insertBookBatch(book2);
        br.insertBookBatch(book3);

        br.selectAll().forEach(o -> LOG.info("Title in books: " + o.getTitle()));
        br.selectAllBookByTitle().forEach(o -> LOG.info("Title in booksByTitle: " + o.getTitle()));
                
        //br.deletebookByTitle("CASSANDRA");
        //br.deleteTable("books");
        //br.deleteTable("booksByTitle");
        
        //Drop keyspace
        //sr.deleteKeyspace(keyspace);
        
        //Close connection to cassandra
        connector.close();
    }
}
