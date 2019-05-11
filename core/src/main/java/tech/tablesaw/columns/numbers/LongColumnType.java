package tech.tablesaw.columns.numbers;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.LongIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class LongColumnType extends AbstractColumnType {

    public static final LongParser DEFAULT_PARSER = new LongParser(ColumnType.LONG);

    private static final int BYTE_SIZE = 8;

    private static LongColumnType INSTANCE;

    private LongColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static LongColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new LongColumnType(BYTE_SIZE, "LONG", "Long");
        }
        return INSTANCE;
    }
    @Override
    public LongColumn create(String name) {
        return LongColumn.create(name);
    }

    public LongParser defaultParser() {
        return DEFAULT_PARSER;
    }

    @Override
    public LongParser customParser(ReadOptions options) {
        return new LongParser(this, options);
    }

    public static boolean isMissingValue(long value) {
        return value == missingValueIndicator();
    }

    public static long missingValueIndicator() {
        return Long.MIN_VALUE;
    }
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = (LongIndex) columnIndexMap.get(column);
		LongColumn col1 = (LongColumn) table1Column;
		long value = col1.getLong(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = new LongIndex(result.longColumn(col2Name));
		LongColumn col2 = (LongColumn) table2.column(col2Name);
		long value = col2.getLong(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new LongIndex(table2.longColumn(col2Name));
	}
}
