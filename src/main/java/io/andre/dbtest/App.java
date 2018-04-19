package io.andre.dbtest;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class App {
    private static final int TIMES = 5000;
    private static String createTable = "create table test(a, b, c, d, e)";
    //private static String createTable = "create table test(a VARCHAR(255), b VARCHAR(255), c VARCHAR(255), d VARCHAR(255), e VARCHAR(255))";
    private static String insert = "insert into test values ('1', '2', '3', '4', '5')";
    //private static String url = "jdbc:sqlite::memory:";
    private static String url = "jdbc:sqlite:db.sqlite";
    //private static String url = "jdbc:mysql://localhost:3306/test?user=root&useSSL=false";
    private static Properties props = new Properties();

    public static void main(String[] args) {
        props.setProperty("rewriteBatchedStatements", "true");

        for (int i = 0; i < 100; i++) {
            jdbc();
            jdbi();
            jdbi2();
            System.out.println("");
        }
    }

    private static void jdbc() {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(url, props);
            statement = connection.createStatement();
            //statement.executeUpdate(createTable);
            long start = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                statement.addBatch(insert);
            }
            statement.executeBatch();
            getTimeElapsed("jdbc", start);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void jdbi() {
        Jdbi jdbi = Jdbi.create(url, props);
        Handle handle = jdbi.open();
        //handle.execute(createTable);
        Batch batch = handle.createBatch();
        long start = System.currentTimeMillis();
        for (int i = 0; i < TIMES; i++) {
            batch.add(insert);
        }
        batch.execute();
        getTimeElapsed("jdbi", start);

    }

    private static void jdbi2() {
        Jdbi jdbi = Jdbi.create(url, props);
        jdbi.useHandle(handle -> {
            //handle.execute(createTable);
            long start = System.currentTimeMillis();
            for (int i = 0; i < TIMES; i++) {
                handle.execute(insert);
            }
            getTimeElapsed("jdbi2", start);
        });
}

    private static void getTimeElapsed(String label, long start) {
        double timeElapsed = System.currentTimeMillis() - start;
        System.out.print(label + ": ");
        System.out.print(timeElapsed / 1000);
        System.out.print("s | ");
        System.out.print(Math.round(TIMES / (timeElapsed / 1000)));
        System.out.println(" ops/sec");

    }

}
