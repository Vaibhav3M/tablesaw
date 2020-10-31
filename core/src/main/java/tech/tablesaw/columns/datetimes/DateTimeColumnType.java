package tech.tablesaw.columns.datetimes;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.LongIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class DateTimeColumnType extends AbstractColumnType {

    public static int BYTE_SIZE = 8;

    public static final DateTimeParser DEFAULT_PARSER = new DateTimeParser(ColumnType.LOCAL_DATE_TIME);

    private static DateTimeColumnType INSTANCE =
            new DateTimeColumnType(BYTE_SIZE, "LOCAL_DATE_TIME", "DateTime");

    private DateTimeColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static DateTimeColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new DateTimeColumnType(BYTE_SIZE, "LOCAL_DATE_TIME", "DateTime");
        }
        return INSTANCE;
    }

    @Override
    public DateTimeColumn create(String name) {
        return DateTimeColumn.create(name);
    }

    @Override
    public DateTimeParser customParser(ReadOptions options) {
        return new DateTimeParser(this, options);
    }

    public static long missingValueIndicator() {
        return Long.MIN_VALUE;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = (LongIndex) columnIndexMap.get(column);
		DateTimeColumn col1 = (DateTimeColumn) table1Column;
		long value = col1.getLongInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = new LongIndex(result.dateTimeColumn(col2Name));
		DateTimeColumn col2 = (DateTimeColumn) table2.column(col2Name);
		long value = col2.getLongInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new LongIndex(table2.dateTimeColumn(col2Name));
	}
}
