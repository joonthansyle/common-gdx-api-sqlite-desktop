/**<p>*********************************************************************************************************************
 * <h1>BuildSqlSelect</h1>
 * @since 20230328
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230328  original author       evanwht1@gmail.com
 *           Initial version
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E502, E503
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop.builder;

import com.badlogic.gdx.sql.DatabaseCursor;
import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.ResultMapper;
import com.badlogic.gdx.sql.builder.SqlBuilderSelect;
import com.badlogic.gdx.sqlite.desktop.DesktopCursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class BuildSqlSelect extends SqlBuilderSelect {
    private static final String TAG = BuildSqlSelect.class.getCanonicalName();
    public static final String NAME = TAG;

    /* ERROR CODES */
    private final String E502 = "Table is undefined";
    private final String E503 = "Operating System not Supported";
    public BuildSqlSelect(ResultMapper resultMapper) {
        super(resultMapper);
    }

    /* SqliteDatabase-Android not Supported in Windows */
    @Override
    protected Object preparedStatementAndroid(Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    @Override
    protected Object preparedStatementWin(Connection connection) throws SQLException, SQLiteGdxException {
        if (table == null || table.isEmpty()) {
            throw new SQLiteGdxException(E502);
        }
        String create = createStatement();
        final PreparedStatement statement = connection.prepareStatement(create);
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p!= null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        return statement;
    }

    @Override
    public DatabaseCursor getCursor(Connection connection) throws SQLiteGdxException, SQLException {
        DesktopCursor lCursor = new DesktopCursor();
        ResultSet resultSetRef = ((PreparedStatement) preparedStatementWin(connection)).executeQuery();
        lCursor.setNativeCursor(resultSetRef);
        return lCursor;
    }

    @Override
    public DatabaseCursor getCursor(DatabaseCursor cursor, Connection connection) throws SQLiteGdxException, SQLException {
        DesktopCursor lCursor = (DesktopCursor)cursor;
        ResultSet resultSetRef = ((PreparedStatement) preparedStatementWin(connection)).executeQuery();
        lCursor.setNativeCursor(resultSetRef);
        return lCursor;
    }

    /* SqliteDatabase-Android not Supported in Windows */
    @Override
    public DatabaseCursor getCursor(Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }

    /* SqliteDatabase-Android not Supported in Windows */
    @Override
    public DatabaseCursor getCursor(DatabaseCursor cursor, Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }
}
