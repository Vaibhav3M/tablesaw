package tech.tablesaw.columns.numbers;

import java.util.Map;

import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.ShortIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class ShortColumnType extends AbstractColumnType {

    public static final ShortParser DEFAULT_PARSER = new ShortParser(ShortColumnType.INSTANCE);

    private static final int BYTE_SIZE = 2;

    private static ShortColumnType INSTANCE;

    private ShortColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static ShortColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new ShortColumnType(BYTE_SIZE, "SHORT", "Short");
        }
        return INSTANCE;
    }

    @Override
    public ShortColumn create(String name) {
        return ShortColumn.create(name);
    }

    @Override
    public ShortParser customParser(ReadOptions options) {
        return new ShortParser(this, options);
    }

    public static boolean isMissingValue(int value) {
	return value == missingValueIndicator();
    }

    public static short missingValueIndicator() {
        return Short.MIN_VALUE;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		ShortIndex index = (ShortIndex) columnIndexMap.get(column);
		ShortColumn col1 = (ShortColumn) table1Column;
		short value = col1.getShort(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		ShortIndex index = new ShortIndex(result.shortColumn(col2Name));
		ShortColumn col2 = (ShortColumn) table2.column(col2Name);
		short value = col2.getShort(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new ShortIndex(table2.shortColumn(col2Name));
	}
}
