package tech.tablesaw.columns.strings;

import java.util.Map;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.api.TextColumn;
import tech.tablesaw.columns.AbstractColumnType;
import tech.tablesaw.columns.Column;
import tech.tablesaw.index.Index;
import tech.tablesaw.index.StringIndex;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.selection.Selection;

public class TextColumnType extends AbstractColumnType {

    public static final int BYTE_SIZE = 4;
    public static final StringParser DEFAULT_PARSER = new StringParser(ColumnType.STRING);

    private static TextColumnType INSTANCE;

    private TextColumnType(int byteSize, String name, String printerFriendlyName) {
        super(byteSize, name, printerFriendlyName);
    }

    public static TextColumnType instance() {
        if (INSTANCE == null) {
            INSTANCE = new TextColumnType(BYTE_SIZE, "TEXT", "Text");
        }
        return INSTANCE;
    }

    @Override
    public TextColumn create(String name) {
        return TextColumn.create(name);
    }

    @Override
    public StringParser customParser(ReadOptions options) {
        return new StringParser(this, options);
    }

    public static String missingValueIndicator() {
        return "";
    }
    
    
    @Override
    public Selection getRowBitMapOneCol(Map<Column<?>, Index> columnIndexMap, int ri, Column<?> column,
			Column<?> table1Column) {
		Selection rowBitMapOneCol;
		StringIndex index = (StringIndex) columnIndexMap.get(column);
		StringColumn col1 = (StringColumn) table1Column;
		String value = col1.get(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    
    @Override
	public Selection getRowBitMapOneCol(Table table2, Table result, int ri, String col2Name, Column<?> table1Column) {
		Selection rowBitMapOneCol;
		StringIndex index = new StringIndex(result.stringColumn(col2Name));
		StringColumn col2 = (StringColumn) table2.column(col2Name);
		String value = col2.get(ri);
		rowBitMapOneCol = index.get(value);
		return rowBitMapOneCol;
	}
    
    @Override
	public Index createIndex(Table table2, String col2Name) {
		return new StringIndex(table2.stringColumn(col2Name));
	}
}
