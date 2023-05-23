/**<p>*********************************************************************************************************************
 * <h1>DesktopDatabaseManager</h1>
 * @since 20150926
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20150926  Initial version       @author M Rafay Aleem
 * 20230323  @Deprecated: "String sql" might be subject for SQL Injection, if this will be the case it has to be deprecated.
 *           This needs to be modified and use other alternatives, such as Prepared Statement, etc.
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E519
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop;

import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.sql.*;
import com.badlogic.gdx.sql.builder.SqlBuilderDelete;
import com.badlogic.gdx.sql.builder.SqlBuilderInsert;
import com.badlogic.gdx.sql.builder.SqlBuilderSelect;
import com.badlogic.gdx.sql.builder.SqlBuilderUpdate;
import com.badlogic.gdx.utils.GdxRuntimeException;

import java.sql.*;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class DesktopDatabaseManager implements DatabaseManager {
    private static final String TAG = DesktopDatabaseManager.class.getCanonicalName();
    public static final String NAME = TAG;

    /* ERROR CODES */
    private static final String E519 = "There was an error counting the number of results";

    private static class DesktopDatabase implements Database {
        private SQLiteDatabaseHelper helper = null;
        private final String dbName;
        private final int dbVersion;
        private final String dbOnCreateQuery;
        private final String dbOnUpgradeQuery;
        private Connection connection = null;
        private Statement stmt = null;

        private DesktopDatabase (String dbName, int dbVersion, String dbOnCreateQuery, String dbOnUpgradeQuery) {
            this.dbName = dbName;
            this.dbVersion = dbVersion;
            this.dbOnCreateQuery = dbOnCreateQuery;
            this.dbOnUpgradeQuery = dbOnUpgradeQuery;
        }

        @Override
        public void setupDatabase () {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                Gdx.app.log(DatabaseFactory.ERROR_TAG, E519, e);
                throw new GdxRuntimeException(e);
            }
        }

        @Override
        public void openOrCreateDatabase () throws SQLiteGdxException {
            if (helper == null) helper = new SQLiteDatabaseHelper(dbName, dbVersion, dbOnCreateQuery, dbOnUpgradeQuery);
            try {
                connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
                stmt = connection.createStatement();
                helper.onCreate(stmt);
            } catch (SQLException e) {
                throw new SQLiteGdxException(e);
            }
        }

        @Override
        public void closeDatabase () throws SQLiteGdxException {
            try {
                stmt.close();
                connection.close();
            } catch (SQLException e) {
                throw new SQLiteGdxException(e);
            }
        }

        @Override
        @Deprecated
        public void execSQL (String sql) throws SQLiteGdxException {
            try {
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                throw new SQLiteGdxException(e);
            }
        }

        @Override
        @Deprecated
        public DatabaseCursor rawQuery(String sql) throws SQLiteGdxException {
            DesktopCursor lCursor = new DesktopCursor();
            try {
                ResultSet resultSetRef = stmt.executeQuery(sql);
                lCursor.setNativeCursor(resultSetRef);
                return lCursor;
            } catch (SQLException e) {
                throw new SQLiteGdxException(e);
            }
        }

        @Override
        @Deprecated
        public DatabaseCursor rawQuery (DatabaseCursor cursor, String sql) throws SQLiteGdxException {
            DesktopCursor lCursor = (DesktopCursor)cursor;
            try {
                ResultSet resultSetRef = stmt.executeQuery(sql);
                lCursor.setNativeCursor(resultSetRef);
                return lCursor;
            } catch (SQLException e) {
                throw new SQLiteGdxException(e);
            }
        }

        @Override
        public DatabaseCursor getCursor(SqlBuilderSelect builder) throws SQLiteGdxException, SQLException {
            return  builder.getCursor(connection);
        }

        @Override
        public DatabaseCursor getCursor(DatabaseCursor cursor, SqlBuilderSelect builder) throws SQLiteGdxException, SQLException {
            return builder.getCursor(cursor, connection);
        }

        @Override
        public OptionalLong insert(SqlBuilderInsert builder) throws SQLiteGdxException, SQLException {
            return builder.insert(connection);
        }

        @Override
        public OptionalInt delete(SqlBuilderDelete builder) throws SQLiteGdxException, SQLException {
            return builder.delete(connection);
        }

        @Override
        public OptionalInt update(SqlBuilderUpdate builder) throws SQLiteGdxException, SQLException {
            return builder.update(connection);
        }

    }

    @Override
    public Database getNewDatabase (String dbName, int dbVersion, String dbOnCreateQuery, String dbOnUpgradeQuery) {
        return new DesktopDatabase(dbName, dbVersion, dbOnCreateQuery, dbOnUpgradeQuery);
    }
}
