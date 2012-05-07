package net.hydromatic.optiq.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link ResultSet}
 * for the OPTIQ engine.
 */
public class OptiqResultSet implements ResultSet {
    private final OptiqStatement statement;
    private OptiqEnumerator<Object> enumerator;
    private boolean wasNull;
    private final List<Accessor> accessorList = new ArrayList<Accessor>();
    private Map<String, Accessor> accessorMap = new HashMap<String, Accessor>();
    private final Map<String, Integer> columnNameMap =
        new HashMap<String, Integer>();
    private int row = -1;
    private boolean afterLast;
    private int direction;
    private int fetchSize;
    private int type;
    private int concurrency;
    private int holdability;
    private boolean closed;
    private OptiqPrepare.PrepareResult prepareResult;

    OptiqResultSet(
        OptiqStatement statement,
        OptiqPrepare.PrepareResult prepareResult)
    {
        this.statement = statement;
        this.prepareResult = prepareResult;

        for (OptiqResultSetMetaData.ColumnMetaData columnMetaData
            : prepareResult.resultSetMetaData.columnMetaDataList)
        {
            Accessor accessor = createAccessor(columnMetaData);
            accessorList.add(accessor);
            accessorMap.put(columnMetaData.label, accessor);
            columnNameMap.put(columnMetaData.label, columnNameMap.size());
        }
    }

    private Accessor createAccessor(
        OptiqResultSetMetaData.ColumnMetaData columnMetaData)
    {
        switch (columnMetaData.type) {
        case Types.TINYINT:
            return new LongAccessor() {
                public byte getByte() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return 0;
                    }
                    return (Byte) o;
                }

                public long getLong() {
                    return getByte();
                }
            };
        case Types.SMALLINT:
            return new LongAccessor() {
                public short getShort() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return 0;
                    }
                    return (Short) o;
                }

                public long getLong() {
                    return getShort();
                }
            };
        case Types.INTEGER:
            return new LongAccessor() {
                public int getInt() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return 0;
                    }
                    return (Integer) o;
                }

                public long getLong() {
                    return getInt();
                }
            };
        case Types.BIGINT:
            return new LongAccessor() {
                public long getLong() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return 0;
                    }
                    return (Long) o;
                }
            };
        case Types.BOOLEAN:
            return new LongAccessor() {
                public boolean getBoolean() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return false;
                    }
                    return (Boolean) o;
                }

                public long getLong() {
                    return getBoolean() ? 1 : 0;
                }

                public String getString() {
                    return isNull() ? null : Boolean.toString(getBoolean());
                }
            };
        case Types.FLOAT:
            return new DoubleAccessor() {
                public float getFloat() {
                    Object o = enumerator.current();
                    if (o == null) {
                        wasNull = true;
                        return 0;
                    }
                    return (Float) o;
                }

                public double getDouble() {
                    return getFloat();
                }
            };
        case Types.CHAR:
        case Types.VARCHAR:
            return new Accessor() {
                public String getString() {
                    return (String) getObject();
                }
            };
        case Types.BINARY:
        case Types.VARBINARY:
            return new Accessor() {
                public byte[] getBytes() {
                    return (byte[]) getObject();
                }
            };
        default:
            throw new RuntimeException("unknown type " + columnMetaData.type);
        }
    }

    private Accessor getAccessor(int columnIndex) {
        return accessorList.get(columnIndex);
    }

    private Accessor getAccessor(String columnLabel) {
        return accessorMap.get(columnLabel);
    }

    public void close() {
        throw new UnsupportedOperationException();
    }

    // not JDBC
    void cancel() {
        // TODO:
    }

    /**
     * Executes this result set. (Not a JDBC method.)
     *
     * <p>Note that execute cannot occur in the
     * constructor, because the constructor occurs while the statement is
     * locked, to make sure that execute/cancel don't happen at the same
     * time.</p>
     */
    public void execute() {
        enumerator = prepareResult.execute();
    }

    public boolean next() throws SQLException {
        if (enumerator.moveNext()) {
            ++row;
            return true;
        } else {
            afterLast = true;
            return false;
        }
    }

    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    public String getString(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getString();
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getBoolean();
    }

    public byte getByte(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getByte();
    }

    public short getShort(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getShort();
    }

    public int getInt(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getInt();
    }

    public long getLong(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getLong();
    }

    public float getFloat(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getFloat();
    }

    public double getDouble(int columnIndex) throws SQLException {
        return getAccessor(columnIndex).getDouble();
    }

    public BigDecimal getBigDecimal(
        int columnIndex, int scale) throws SQLException
    {
        return getAccessor(columnIndex - 1).getBigDecimal(scale);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getBytes();
    }

    public Date getDate(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getDate();
    }

    public Time getTime(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getTime();
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getTimestamp();
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getAsciiStream();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getUnicodeStream();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getBinaryStream();
    }

    public String getString(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getString();
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getBoolean();
    }

    public byte getByte(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getByte();
    }

    public short getShort(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getShort();
    }

    public int getInt(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getInt();
    }

    public long getLong(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getLong();
    }

    public float getFloat(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getFloat();
    }

    public double getDouble(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getDouble();
    }

    public BigDecimal getBigDecimal(
        String columnLabel, int scale) throws SQLException
    {
        return getAccessor(columnLabel).getBigDecimal(scale);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getBytes();
    }

    public Date getDate(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getDate();
    }

    public Time getTime(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getTime();
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getTimestamp();
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getAsciiStream();
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException
    {
        return getAccessor(columnLabel).getUnicodeStream();
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getBinaryStream();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return prepareResult.resultSetMetaData;
    }

    public Object getObject(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getObject();
    }

    public Object getObject(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getObject();
    }

    public int findColumn(String columnLabel) throws SQLException {
        return columnNameMap.get(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getCharacterStream();
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getCharacterStream();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getAccessor(columnIndex - 1).getBigDecimal();
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getAccessor(columnLabel).getBigDecimal();
    }

    public boolean isBeforeFirst() throws SQLException {
        return row < 0;
    }

    public boolean isAfterLast() throws SQLException {
        return afterLast;
    }

    public boolean isFirst() throws SQLException {
        return row == 0;
    }

    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getRow() throws SQLException {
        return row;
    }

    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        this.direction = direction;
    }

    public int getFetchDirection() throws SQLException {
        return direction;
    }

    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    public int getType() throws SQLException {
        return type;
    }

    public int getConcurrency() throws SQLException {
        return concurrency;
    }

    public boolean rowUpdated() throws SQLException {
        return false;
    }

    public boolean rowInserted() throws SQLException {
        return false;
    }

    public boolean rowDeleted() throws SQLException {
        return false;
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
        int columnIndex, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
        int columnIndex, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        int columnIndex, Reader x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
        int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(
        String columnLabel, boolean x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
        String columnLabel, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
        String columnLabel, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        String columnLabel, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
        String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(
        int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(
        String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
        int columnIndex, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
        String columnLabel, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws SQLException {
        return holdability;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public void updateNString(
        int columnIndex, String nString) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNString(
        String columnLabel, String nString) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(
        String columnLabel, NClob nClob) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(
        int columnIndex, SQLXML xmlObject) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(
        String columnLabel, SQLXML xmlObject) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(
        int columnIndex, Reader x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(
        String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        int columnIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        int columnIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        int columnIndex, Reader x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        String columnLabel, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        String columnLabel, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(
        int columnIndex,
        InputStream inputStream,
        long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(
        String columnLabel,
        InputStream inputStream,
        long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateClob(
        int columnIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateClob(
        String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(
        int columnIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(
        String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(
        int columnIndex, Reader x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(
        String columnLabel, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        int columnIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        int columnIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        int columnIndex, Reader x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        String columnLabel, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        String columnLabel, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        String columnLabel, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(
        int columnIndex, InputStream inputStream) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(
        String columnLabel, InputStream inputStream) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(
        String columnLabel, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(
        int columnIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(
        String columnLabel, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(
        String columnLabel, Class<T> type) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw statement.connection.helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    class Accessor {
        public String getString() {
            throw cannotConvert("String");
        }

        public boolean getBoolean() {
            throw cannotConvert("boolean");
        }

        public byte getByte() {
            throw cannotConvert("byte");
        }

        public short getShort() {
            throw cannotConvert("short");
        }

        public int getInt() {
            throw cannotConvert("int");
        }

        public long getLong() {
            throw cannotConvert("long");
        }

        public float getFloat() {
            throw cannotConvert("float");
        }

        public double getDouble() {
            throw cannotConvert("double");
        }

        public BigDecimal getBigDecimal() {
            throw cannotConvert("BigDecimal");
        }

        public BigDecimal getBigDecimal(int scale) {
            throw cannotConvert("BigDecimal with scale");
        }

        public byte[] getBytes() {
            throw cannotConvert("byte[]");
        }

        public Date getDate() {
            throw cannotConvert("Date");
        }

        public Time getTime() {
            throw cannotConvert("Time");
        }

        public Timestamp getTimestamp() {
            throw cannotConvert("Timestamp");
        }

        public InputStream getAsciiStream() {
            throw cannotConvert("InputStream (ascii)");
        }

        public InputStream getUnicodeStream() {
            throw cannotConvert("InputStream (unicode)");
        }

        public InputStream getBinaryStream() {
            throw cannotConvert("InputStream (binary)");
        }

        public Object getObject() {
            return enumerator.current();
        }

        public Reader getCharacterStream() {
            throw cannotConvert("Reader");
        }

        private RuntimeException cannotConvert(String targetType) {
            return new RuntimeException("cannot convert to " + targetType);
        }

        protected boolean isNull() {
            return getObject() == null;
        }
    }

    /**
     * Accessor of values that are {@link Long} or null.
     */
    private class LongAccessor extends Accessor {
        public BigDecimal getBigDecimal(int scale) {
            return isNull()
                ? null
                : BigDecimal.valueOf(getDouble())
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            return isNull() ? null : BigDecimal.valueOf(getLong());
        }

        public double getDouble() {
            return getLong();
        }

        public float getFloat() {
            return getLong();
        }

        public long getLong() {
            Object o = getObject();
            if (o == null) {
                return 0;
            }
            return (Long) o;
        }

        public int getInt() {
            return (int) getLong();
        }

        public short getShort() {
            return (short) getLong();
        }

        public byte getByte() {
            return (byte) getLong();
        }

        public boolean getBoolean() {
            return getLong() != 0;
        }

        public String getString() {
            return isNull() ? null : String.valueOf(getLong());
        }
    }

    /**
     * Accessor of values that are {@link Double} or null.
     */
    private class DoubleAccessor extends Accessor {
        public BigDecimal getBigDecimal(int scale) {
            return isNull()
                ? null
                : BigDecimal.valueOf(getDouble())
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            return isNull() ? null : BigDecimal.valueOf(getDouble());
        }

        public double getDouble() {
            Object o = getObject();
            if (o == null) {
                return 0;
            }
            return (Double) o;
        }

        public float getFloat() {
            return (float) getDouble();
        }

        public long getLong() {
            return (long) getDouble();
        }

        public int getInt() {
            return (int) getDouble();
        }

        public short getShort() {
            return (short) getDouble();
        }

        public byte getByte() {
            return (byte) getDouble();
        }

        public boolean getBoolean() {
            return getDouble() != 0;
        }

        public String getString() {
            return isNull() ? null : String.valueOf(getDouble());
        }
    }
}

// End OptiqResultSet.java
