package tech.tablesaw.columns.instant;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.IntIndex;
import tech.tablesaw.index.LongIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class InstantColumnType extends AbstractColumnType {

    public static int BYTE_SIZE = 8;

    public static final InstantParser DEFAULT_PARSER = new InstantParser(ColumnType.LOCAL_DATE_TIME);

    private static InstantColumnType INSTANCE =
            new InstantColumnType(BYTE_SIZE, "INSTANT", "Instant");

    private InstantColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static InstantColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new InstantColumnType(BYTE_SIZE, "INSTANT", "Instant");
        }
        return INSTANCE;
    }

    @Override
    public DateTimeColumn create(String name) {
        return DateTimeColumn.create(name);
    }

    @Override
    public InstantParser customParser(ReadOptions options) {
        return new InstantParser(this);
    }

    public static long missingValueIndicator() {
        return Long.MIN_VALUE;
    }
    
	@Override
	public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = (LongIndex) columnIndexMap.get(column);
		InstantColumn col1 = (InstantColumn) table1Column;
		long value = col1.getLongInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
	
	@Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		LongIndex index = new LongIndex(result.instantColumn(col2Name));
		InstantColumn col2 = (InstantColumn) table2.column(col2Name);
		long value = col2.getLongInternal(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
	
    @Override
	public Index createIndex(Table table2, String col2Name) {
    	return new LongIndex(table2.instantColumn(col2Name));
	}
}
