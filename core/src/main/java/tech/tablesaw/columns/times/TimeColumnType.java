package tech.tablesaw.columns.times;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TimeColumn;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.IntIndex;
import tech.tablesaw.columns.AbstractColumnParser;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

import java.time.LocalTime;
import java.util.Map;

public class TimeColumnType extends AbstractColumnType {

    public static final int BYTE_SIZE = 4;

    public static final TimeParser DEFAULT_PARSER = new TimeParser(ColumnType.LOCAL_TIME);

    private static TimeColumnType INSTANCE;

    private TimeColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static TimeColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new TimeColumnType(BYTE_SIZE, "LOCAL_TIME", "Time");
        }
        return INSTANCE;
    }

    @Override
    public TimeColumn create(String name) {
        return TimeColumn.create(name);
    }

    @Override
    public AbstractColumnParser<LocalTime> customParser(ReadOptions options) {
        return new TimeParser(this, options);
    }

    public static int missingValueIndicator() {
        return Integer.MIN_VALUE;
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = (IntIndex) columnIndexMap.get(column);
		TimeColumn col1 = (TimeColumn) table1Column;
		int value = col1.getIntInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		IntIndex index = new IntIndex(result.timeColumn(col2Name));
		TimeColumn col2 = (TimeColumn) table2.column(col2Name);
		int value = col2.getIntInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
    public Index createIndex(Table table2, String col2Name) {
		return new IntIndex(table2.timeColumn(col2Name));
	}
}
