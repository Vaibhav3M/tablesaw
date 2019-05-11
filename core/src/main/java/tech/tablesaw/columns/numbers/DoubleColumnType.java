package tech.tablesaw.columns.numbers;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.DoubleIndex;
import tech.tablesaw.index.Index;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class DoubleColumnType extends AbstractColumnType {

    private static final int BYTE_SIZE = 8;

    public static final DoubleParser DEFAULT_PARSER = new DoubleParser(ColumnType.DOUBLE);

    private static DoubleColumnType INSTANCE =
            new DoubleColumnType(BYTE_SIZE, "DOUBLE", "Double");

    public static DoubleColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new DoubleColumnType(BYTE_SIZE, "DOUBLE", "Double");
        }
        return INSTANCE;
    }

    private DoubleColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    @Override
    public DoubleColumn create(String name) {
        return DoubleColumn.create(name);
    }

    @Override
    public DoubleParser customParser(ReadOptions options) {
        return new DoubleParser(this, options);
    }

    public static boolean isMissingValue(double value) {
        return Double.isNaN(value);
    }

    public static double missingValueIndicator() {
        return Double.NaN;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		DoubleIndex index = (DoubleIndex) columnIndexMap.get(column);
		DoubleColumn col1 = (DoubleColumn) table1Column;
		double value = col1.getDouble(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		DoubleIndex index = new DoubleIndex(result.doubleColumn(col2Name));
		DoubleColumn col2 = (DoubleColumn) table2.column(col2Name);
		double value = col2.getDouble(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new DoubleIndex(table2.doubleColumn(col2Name));
	}
}
