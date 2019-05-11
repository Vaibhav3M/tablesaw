package tech.tablesaw.columns.numbers;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.IntIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class IntColumnType extends AbstractColumnType {

    public static final IntParser DEFAULT_PARSER = new IntParser(ColumnType.INTEGER);

    private static final int BYTE_SIZE = 4;

    private static IntColumnType INSTANCE;

    private IntColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static IntColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new IntColumnType(BYTE_SIZE, "INTEGER", "Integer");
        }
        return INSTANCE;
    }

    @Override
    public IntColumn create(String name) {
        return IntColumn.create(name);
    }

    @Override
    public IntParser customParser(ReadOptions options) {
        return new IntParser(this, options);
    }

    public static boolean isMissingValue(int value) {
        return value == missingValueIndicator();
    }

    public static int missingValueIndicator() {
        return Integer.MIN_VALUE;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = (IntIndex) columnIndexMap.get(column);
		IntColumn col1 = (IntColumn) table1Column;
		int value = col1.getInt(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = new IntIndex(result.intColumn(col2Name));
		IntColumn col2 = (IntColumn) table2.column(col2Name);
		int value = col2.getInt(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
    public Index createIndex(Table table2, String col2Name) {
		return new IntIndex(table2.intColumn(col2Name));
	}
}
