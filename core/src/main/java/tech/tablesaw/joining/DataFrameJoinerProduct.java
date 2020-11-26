package tech.tablesaw.joining;

import com.google.common.collect.Streams;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFrameJoinerProduct {
    private AtomicInteger joinTableId = new AtomicInteger(2);

    public void renameColumnsWithDuplicateNames(Table table1, Table table2, String... col2Names) {
        String table2Alias = DataFrameJoiner.TABLE_ALIAS + joinTableId.getAndIncrement();
        List<String> list = Arrays.asList(col2Names);
        for (Column<?> table2Column : table2.columns()) {
            String columnName = table2Column.name();
            if (table1.columnNames().stream().anyMatch(columnName::equalsIgnoreCase)
                    && !(list.stream().anyMatch(columnName::equalsIgnoreCase))) {
                table2Column.setName(newName(table2Alias, columnName));
            }
        }
    }

    public String newName(String table2Alias, String columnName) {
        return table2Alias + "." + columnName;
    }

    public Table emptyTableFromColumns(Table table1, Table table2, String... col2Names) {
        Column<?>[] cols = Streams.concat(
                table1.columns().stream(),
                table2.columns().stream().filter(c -> !Arrays.asList(col2Names).stream().anyMatch(c.name()::equalsIgnoreCase))
        ).map(Column::emptyCopy).toArray(Column[]::new);
        return Table.create(table1.name(), cols);
    }
}