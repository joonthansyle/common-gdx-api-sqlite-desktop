/**<p>*********************************************************************************************************************
 * <h1>BuildSqlUpdate</h1>
 * @since 20230329
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230329  original author       evanwht1@gmail.com
 *
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E502, E503
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop.builder;

import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.SqlBuilderUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;

public class BuildSqlUpdate extends SqlBuilderUpdate {
    private static final String TAG = BuildSqlUpdate.class.getCanonicalName();
    public static final String NAME = TAG;

    /* ERROR CODES */
    private final String E502 = "Table is undefined";
    private final String E503 = "Operating System not Supported";

    @Override
    public OptionalInt update(Connection connection) throws SQLException, SQLiteGdxException {
        if (table == null || values.isEmpty()) {
            throw new SQLiteGdxException(E502);
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : values.entrySet()) {
            if (p.getValue() == null) {
                statement.setNull(index++, p.getKey().getType());
            } else {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p != null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        final int rows = statement.executeUpdate();
        if (rows > 0) {
            return OptionalInt.of(rows);
        }
        return OptionalInt.empty();
    }

    /** Runs on Android */
    @Override
    public OptionalInt update(Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }
}
