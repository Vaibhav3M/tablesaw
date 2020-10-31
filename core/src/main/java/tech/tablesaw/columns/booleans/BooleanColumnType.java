package tech.tablesaw.columns.booleans;

import java.util.Map;

import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.ByteIndex;
import tech.tablesaw.index.Index;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class BooleanColumnType extends AbstractColumnType {

    public static final BooleanParser DEFAULT_PARSER = new BooleanParser(ColumnType.BOOLEAN);

    public static final byte MISSING_VALUE = (Byte) missingValueIndicator();

    public static final byte BYTE_TRUE = 1;
    public static final byte BYTE_FALSE = 0;

    private static final byte BYTE_SIZE = 1;

    private static BooleanColumnType INSTANCE;

    private BooleanColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static BooleanColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new BooleanColumnType(BYTE_SIZE, "BOOLEAN", "Boolean");
        }
        return INSTANCE;
    }


    @Override
    public BooleanColumn create(String name) {
        return BooleanColumn.create(name);
    }

    @Override
    public BooleanParser customParser(ReadOptions readOptions) {
        return new BooleanParser(this, readOptions);
    }

    public static byte missingValueIndicator() {
        return Byte.MIN_VALUE;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		ByteIndex index = (ByteIndex) columnIndexMap.get(column);
		BooleanColumn col1 = (BooleanColumn) table1Column;
		byte value = col1.getByte(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		ByteIndex index = new ByteIndex(result.booleanColumn(col2Name));
		BooleanColumn col2 = (BooleanColumn) table2.column(col2Name);
		byte value = col2.getByte(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new ByteIndex(table2.booleanColumn(col2Name));
	}
}
