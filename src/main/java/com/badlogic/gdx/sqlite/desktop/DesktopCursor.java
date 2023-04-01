/**<p>*********************************************************************************************************************
 * <h1>DesktopCursor</h1>
 * @since 20230323
 * =====================================================================================================================
 * DATE      VSN/MOD               BY....
 * =====================================================================================================================
 * 20150926  Initial version       @author M Rafay Aleem
 *           This is a Desktop implementation of the public interface {@link DatabaseCursor}.
 *           Note that columns in JDBC are not zero-based
 *           and hence +1 has been added to accommodate for this difference.
 *
 * 20230323  Encountering IllegalAccessError
 *           Replaced com.sun.rowset CachedRowSetImpl with javax.sql.rowset CachedRowSet
 * =====================================================================================================================
 * INFO, ERRORS AND WARNINGS:
 * E509, E510, E511, E512, E513, E514, E515, E516, E517, E518
 **********************************************************************************************************************</p>*/
package com.badlogic.gdx.sqlite.desktop;

import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.sql.DatabaseCursor;
import com.badlogic.gdx.sql.DatabaseFactory;
import com.badlogic.gdx.sql.SQLiteGdxRuntimeException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DesktopCursor implements DatabaseCursor {

    /**
     * Reference of {@code CachedRowSetImpl} Class Type created for both forward, backward, and random traversing the records, as for ResultSet Class Type
     * sqlite does not support other than forward traversing
     */
//    private CachedRowSetImpl resultSet = null;
    private CachedRowSet resultSet = null;

    /* ERROR CODES */
    private final String E509 = "There was an error in getting the blob";
    private final String E510 = "There was an error in getting the double";
    private final String E511 = "There was an error in getting the float";
    private final String E512 = "There was an error in getting the int";
    private final String E513 = "There was an error in getting the long";
    private final String E514 = "There was an error in getting the short";
    private final String E515 = "There was an error in getting the string";
    private final String E516 = "There was an error in moving the cursor to next";
    private final String E517 = "There was an error in closing the cursor";
    private final String E518 = "There was an error counting the number of results";
    @Override
    public byte[] getBlob (int columnIndex) {
        try {
            Blob blob = resultSet.getBlob(columnIndex + 1);
            return blob.getBytes(1, (int)blob.length());
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E509, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public double getDouble (int columnIndex) {
        try {
            return resultSet.getDouble(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E510, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public float getFloat (int columnIndex) {
        try {
            return resultSet.getFloat(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E511, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public int getInt (int columnIndex) {
        try {
            return resultSet.getInt(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E512, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public long getLong (int columnIndex) {
        try {
            return resultSet.getLong(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E513, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public short getShort (int columnIndex) {
        try {
            return resultSet.getShort(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E514, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public String getString (int columnIndex) {
        try {
            return resultSet.getString(columnIndex + 1);
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E515, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public boolean next () {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E516, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    @Override
    public int getCount () {
        return getRowCount(resultSet);
    }

    @Override
    public void close () {
        try {
            resultSet.close();
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E517, e);
            throw new SQLiteGdxRuntimeException(e);
        }
    }

    private int getRowCount (ResultSet resultSet) {
        if (resultSet == null) {
            return 0;
        }
        try {
            resultSet.last();
            return resultSet.getRow();
        } catch (SQLException e) {
            Gdx.app.log(DatabaseFactory.ERROR_TAG, E518, e);
            throw new SQLiteGdxRuntimeException(e);
        } finally {
            try {
                resultSet.beforeFirst();
            } catch (SQLException e) {
                Gdx.app.log(DatabaseFactory.ERROR_TAG, E518, e);
            }
        }
    }

    public void setNativeCursor (ResultSet resultSetRef) {
        try {
//            resultSet = new CachedRowSetImpl();
            resultSet = RowSetProvider.newFactory().createCachedRowSet();
            resultSet.populate (resultSetRef);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Error.....");
            e.printStackTrace();
        }
    }
}
