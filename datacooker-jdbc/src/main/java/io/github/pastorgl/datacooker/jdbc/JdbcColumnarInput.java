/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.jdbc;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Output;
import io.github.pastorgl.datacooker.config.PathInput;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.InputAdapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.*;
import java.util.*;


@SuppressWarnings("unused")
public class JdbcColumnarInput extends InputAdapter {
    static final String VERB = "jdbcColumnar";
    private JavaSparkContext ctx;
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String delimiter;
    private DataStream result;
    private String name;
    private List<String> columns;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "JDBC adapter for reading Columnar data from an" +
                " SQL SELECT query against a configured database. Must use numeric boundaries for each part denoted" +
                " by two ? placeholders, from 0 to (part_count - 1). Supports only PARTITION BY" +
                " HASHCODE and RANDOM.")
                .inputAdapter(new String[]{"SELECT *, weeknum - 1 AS part_num FROM weekly_table WHERE part_num BETWEEN ? AND ?"})
                .output(StreamType.COLUMNAR, "Columnar DS")
                .def(JDBCStorage.JDBC_DRIVER, "JDBC driver, fully qualified class name")
                .def(JDBCStorage.JDBC_URL, "JDBC connection string URL")
                .def(JDBCStorage.JDBC_USER, "JDBC connection user", null, "By default, user isn't set")
                .def(JDBCStorage.JDBC_PASSWORD, "JDBC connection password", null, "By default, use no password")
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        dbDriver = params.get(JDBCStorage.JDBC_DRIVER);
        dbUrl = params.get(JDBCStorage.JDBC_URL);
        dbUser = params.get(JDBCStorage.JDBC_USER);
        dbPassword = params.get(JDBCStorage.JDBC_PASSWORD);
    }

    @Override
    public void initialize(PathInput input, Output output) throws InvalidConfigurationException {
        super.initialize(input, output);

        columns = (output.requested != null) ? output.requested.get(ObjLvl.VALUE) : null;

        name = output.name;
    }

    @Override
    public void execute() {
        result = new DataStreamBuilder(name, Collections.emptyMap())
                .created(VERB, path, StreamType.Columnar, partitioning.name())
                .build(
                        new JdbcRDD<Tuple2>(
                                ctx.sc(),
                                new DbConnection(dbDriver, dbUrl, dbUser, dbPassword),
                                path,
                                0, Math.max(partCount, 0),
                                Math.max(partCount, 1),
                                new RecordRowMapper(partitioning, columns),
                                ClassManifestFactory$.MODULE$.fromClass(Tuple2.class)
                        ).toJavaRDD().mapToPair(r -> r)
                );
    }

    @Override
    public Map<String, DataStream> result() {
        return Map.of(name, result);
    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {
        final String _dbDriver;
        final String _dbUrl;
        final String _dbUser;
        final String _dbPassword;

        DbConnection(String _dbDriver, String _dbUrl, String _dbUser, String _dbPassword) {
            this._dbDriver = _dbDriver;
            this._dbUrl = _dbUrl;
            this._dbUser = _dbUser;
            this._dbPassword = _dbPassword;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(_dbDriver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            if (_dbUser != null) {
                properties.setProperty("user", _dbUser);
            }
            if (_dbPassword != null) {
                properties.setProperty("password", _dbPassword);
            }

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(_dbUrl, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return connection;
        }
    }

    static class RecordRowMapper extends AbstractFunction1<ResultSet, Tuple2> implements Serializable {
        private final Random random;
        final List<String> _columns;

        public RecordRowMapper(Partitioning partitioning, List<String> _columns) {
            this.random = (partitioning == Partitioning.RANDOM) ? new Random() : null;
            this._columns = _columns;
        }

        @Override
        public Tuple2 apply(ResultSet row) {
            try {
                ResultSetMetaData metaData = row.getMetaData();

                Columnar obj;
                if (_columns != null) {
                    obj = new Columnar(_columns);
                    for (String column : _columns) {
                        obj.put(column, row.getObject(column));
                    }
                } else {
                    int columnCount = metaData.getColumnCount();
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < columnCount; i++) {
                        map.put(metaData.getColumnName(i), row.getObject(i));
                    }
                    obj = new Columnar().put(map);
                }

                return new Tuple2((random == null) ? obj.hashCode() : random.nextInt(), obj);
            } catch (SQLException ignore) {
                return null;
            }
        }
    }
}
