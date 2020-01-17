package org.example.jdbc;

import java.sql.*;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class ReturnClosedConnectionTest {
    private final DataSource datasource = new DataSource();

    @BeforeEach
    void init() {
        datasource.setUrl("jdbc:postgresql://localhost/TEST");
        datasource.setUsername("TEST");
        datasource.setPassword("TEST");
    }

    @Test
    void returnClosedConnection() throws SQLException {
        assumeFalse(datasource.getPoolProperties().isTestOnBorrow());
        assumeFalse(datasource.getPoolProperties().isTestOnReturn());
        assumeFalse(datasource.getPoolProperties().isTestWhileIdle());

        // Code which causes connection to be closed

        final int numberOfParameters = 0x10000;
        try (Connection connection = datasource.getConnection()) {
            String query = "select " + String.join("+", Collections.nCopies(numberOfParameters, "?"));
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                for (int i = 1; i <= numberOfParameters; ++i) {
                    ps.setInt(i, i);
                }
                try (ResultSet resultSet = ps.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(numberOfParameters * ( numberOfParameters + 1 ) / 2, resultSet.getInt(1));
                }
            } catch (SQLException e) {
                Logger.getLogger(getClass().getName()).log(Level.INFO, "Ignore possible SQLException", e);
            }
        }

        // Other code which gets that closed connection

        try (Connection connection = datasource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery("select 1")) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getInt(1));
                }
            }
        }
    }
}
