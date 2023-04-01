/**<p>*********************************************************************************************************************
 * <h1>BuildSqlDelete</h1>
 * @since 20230329
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230329  @version 01           @author ORIGINAL AUTHOR
 *
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E502, E503
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop.builder;

import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.SqlBuilderDelete;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;

public class BuildSqlDelete extends SqlBuilderDelete {
    private static final String TAG = BuildSqlDelete.class.getCanonicalName();
    public static final String NAME = TAG;

    /* ERROR CODES */
    private final String E502 = "Table is undefined";
    private final String E503 = "Operating System not Supported";

    @Override
    public OptionalInt delete(Connection connection) throws SQLiteGdxException, SQLException {
        if (table == null || clauses.isEmpty()) {
            throw new SQLiteGdxException(E502);
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement());
        int index = 1;
        for (Map.Entry<Column, Object> p : clauses.entrySet()) {
            if (p.getValue() != null) {
                statement.setObject(index++, p.getValue(), p.getKey().getType());
            }
        }
        final int rows = statement.executeUpdate();
        return rows > 0 ? OptionalInt.of(rows) : OptionalInt.empty();
    }

    /** Runs on Android */
    @Override
    public OptionalInt delete(Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }
}
