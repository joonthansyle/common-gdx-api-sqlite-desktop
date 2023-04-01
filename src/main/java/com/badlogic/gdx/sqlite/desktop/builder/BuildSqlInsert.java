/**<p>*********************************************************************************************************************
 * <h1>BuildSqlInsert</h1>
 * @since 20230328
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20230328  original author       evanwht1@gmail.com
 *
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E502, E503
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop.builder;

import com.badlogic.gdx.sql.SQLiteGdxException;
import com.badlogic.gdx.sql.builder.Column;
import com.badlogic.gdx.sql.builder.SqlBuilderInsert;

import java.sql.*;
import java.util.Map;
import java.util.OptionalLong;
public class BuildSqlInsert extends SqlBuilderInsert {
    private static final String TAG = BuildSqlInsert.class.getCanonicalName();
    public static final String NAME = TAG;

    /* ERROR CODES */
    private final String E502 = "Table is undefined";
    private final String E503 = "Operating System not Supported";
    public OptionalLong insert(final Connection connection) throws SQLException, SQLiteGdxException {
        if (table == null) {
            throw new SQLiteGdxException(E502);
        }
        final PreparedStatement statement = connection.prepareStatement(createStatement(), Statement.RETURN_GENERATED_KEYS);
        int index = 1;
        for (Map.Entry<Column, Object> p : values.entrySet()) {
            statement.setObject(index++, p.getValue(), p.getKey().getType());
        }
        final int rows = statement.executeUpdate();
        if (rows > 0) {
            try (final ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return OptionalLong.of(generatedKeys.getLong(1));
                }
            }
        }
        return OptionalLong.empty();
    }

    /** Runs on Android */
    @Override
    public OptionalLong insert(Object androidDatabase) throws SQLiteGdxException {
        throw new SQLiteGdxException(E503);
    }
}
