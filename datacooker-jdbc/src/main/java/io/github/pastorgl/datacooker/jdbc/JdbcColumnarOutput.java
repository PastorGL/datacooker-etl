/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.jdbc;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.Input;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.PathOutput;
import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.OutputAdapter;
import org.sparkproject.guava.collect.Iterators;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@SuppressWarnings("unused")
public class JdbcColumnarOutput extends OutputAdapter {
    public static final String BATCH_SIZE = "batch_size";

    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    private int batchSize;

    private String[] columns;
    private DataStream dataStream;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder("jdbcColumnar", "JDBC adapter which performs batch INSERT VALUES of" +
                " attributes (in order of incidence) into a table in the configured database.")
                .outputAdapter(new String[]{"output_table_name"})
                .input(StreamType.COLUMNAR, "Columnar DS")
                .def(JDBCStorage.JDBC_DRIVER, "JDBC driver, fully qualified class name")
                .def(JDBCStorage.JDBC_URL, "JDBC connection string URL")
                .def(JDBCStorage.JDBC_USER, "JDBC connection user", null, "By default, user isn't set")
                .def(JDBCStorage.JDBC_PASSWORD, "JDBC connection password", null, "By default, use no password")
                .def(BATCH_SIZE, "Batch size for SQL INSERTs", Integer.class,
                        500, "By default, use 500 records")
                .build();
    }

    @Override
    public void initialize(Input input, PathOutput output) throws InvalidConfigurationException {
        super.initialize(input, output);

        if ((output.requested != null) && output.requested.containsKey(ObjLvl.VALUE)) {
            columns = output.requested.get(ObjLvl.VALUE).toArray(new String[0]);
        } else {
            columns = input.dataStream.attributes(ObjLvl.VALUE).toArray(new String[0]);
        }

        dataStream = input.dataStream;
    }

    @Override
    public void configure(Configuration params) {
        dbDriver = params.get(JDBCStorage.JDBC_DRIVER);
        dbUrl = params.get(JDBCStorage.JDBC_URL);
        dbUser = params.get(JDBCStorage.JDBC_USER);
        dbPassword = params.get(JDBCStorage.JDBC_PASSWORD);

        batchSize = params.get(BATCH_SIZE);
    }

    @Override
    public void execute() {
        final String _dbDriver = dbDriver;
        final String _dbUrl = dbUrl;
        final String _dbUser = dbUser;
        final String _dbPassword = dbPassword;

        int _batchSize = batchSize;

        final String[] _cols = columns;
        final String _table = path;

        dataStream.rdd().mapPartitions(partition -> {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                Class.forName(_dbDriver);

                Properties properties = new Properties();
                properties.setProperty("user", _dbUser);
                properties.setProperty("password", _dbPassword);

                conn = DriverManager.getConnection(_dbUrl, properties);

                StringBuilder sb = new StringBuilder("INSERT INTO " + _table + " VALUES ");
                sb.append("(");
                for (int i = 0, j = 0; i < _cols.length; i++) {
                    if (j > 0) {
                        sb.append(",");
                    }
                    sb.append("?");
                    j++;
                }
                sb.append(")");

                ps = conn.prepareStatement(sb.toString());
                int b = 0;
                while (partition.hasNext()) {
                    DataRecord<?> row = partition.next()._2;

                    for (int i = 0, j = 1; i < _cols.length; i++) {
                        ps.setObject(j++, row.asIs(_cols[i]));
                    }
                    ps.addBatch();

                    if (b == _batchSize) {
                        ps.executeBatch();

                        ps.clearBatch();
                        b = 0;
                    }

                    b++;
                }
                if (b != 0) {
                    ps.executeBatch();
                }

                return Iterators.emptyIterator();
            } catch (SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
        }).count();
    }
}
