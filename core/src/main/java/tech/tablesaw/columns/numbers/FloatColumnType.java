package tech.tablesaw.columns.numbers;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.FloatIndex;
import tech.tablesaw.index.Index;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class FloatColumnType extends AbstractColumnType {

    public static final int BYTE_SIZE = 4;

    public static final FloatParser DEFAULT_PARSER = new FloatParser(ColumnType.FLOAT);

    private static FloatColumnType INSTANCE;

    private FloatColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static FloatColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new FloatColumnType(BYTE_SIZE, "FLOAT", "float");
        }
        return INSTANCE;
    }

    @Override
    public FloatColumn create(String name) {
        return FloatColumn.create(name);
    }

    @Override
    public FloatParser customParser(ReadOptions options) {
        return new FloatParser(this, options);
    }

    public static boolean isMissingValue(float value) {
        return Float.isNaN(value);
    }

    public static float missingValueIndicator() {
        return Float.NaN;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		FloatIndex index = (FloatIndex) columnIndexMap.get(column);
		FloatColumn col1 = (FloatColumn) table1Column;
		float value = col1.getFloat(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		FloatIndex index = new FloatIndex(result.floatColumn(col2Name));
		FloatColumn col2 = (FloatColumn) table2.column(col2Name);
		float value = col2.getFloat(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new FloatIndex(table2.floatColumn(col2Name));
	}
}
