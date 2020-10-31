package tech.tablesaw.columns.dates;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.IntIndex;
import tech.tablesaw.columns.AbstractColumnParser;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

import java.time.LocalDate;
import java.util.Map;

public class DateColumnType extends AbstractColumnType {

    public static final int BYTE_SIZE = 4;
    public static final DateParser DEFAULT_PARSER = new DateParser(ColumnType.LOCAL_DATE);

    private static DateColumnType INSTANCE;

    private DateColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static DateColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new DateColumnType(BYTE_SIZE, "LOCAL_DATE", "Date");
        }
        return INSTANCE;
    }

    @Override
    public DateColumn create(String name) {
        return DateColumn.create(name);
    }

    @Override
    public AbstractColumnParser<LocalDate> customParser(ReadOptions options) {
        return new DateParser(this, options);
    }

    public static int missingValueIndicator() {
        return Integer.MIN_VALUE;
    }
    
    
    @Override
	public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = (IntIndex) columnIndexMap.get(column);
		DateColumn col1 = (DateColumn) table1Column;
		int value = col1.getIntInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
	@Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = new IntIndex(result.dateColumn(col2Name));
		DateColumn col2 = (DateColumn) table2.column(col2Name);
		int value = col2.getIntInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new IntIndex(table2.dateColumn(col2Name));
	}
}
