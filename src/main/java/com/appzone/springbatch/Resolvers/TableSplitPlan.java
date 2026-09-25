package com.appzone.springbatch.Resolvers;

/**
 * Decides how a very wide CSV (many columns) is split into several smaller tables.
 *
 * Why: some databases cannot handle one table / one INSERT with hundreds of columns
 * (MySQL "Row size too large", Oracle driver stack overflow on a very long INSERT).
 *
 * Rules:
 *  - CSV with MAX_COLUMNS_PER_TABLE columns or fewer -> ONE table, exactly like before.
 *  - CSV with more columns -> several tables: <name>_part1, <name>_part2, ...
 *    Every part table has an extra key column (csv_row_id) so the parts can be joined back.
 */
public final class TableSplitPlan {

    // If a CSV has more columns than this, it is split into several tables.
    // 150 is a safe starting value. Lower it (for example 100) if your database still complains.
    public static final int MAX_COLUMNS_PER_TABLE = 150;

    // Extra key column added to every part table (only when the CSV is split)
    public static final String ROW_ID_COLUMN = "csv_row_id";

    // After the parts are joined into the single table successfully,
    // delete the part tables (they are only temporary helpers).
    public static final boolean DROP_PART_TABLES_AFTER_MERGE = true;

    private TableSplitPlan() {
    }

    public static boolean needsSplit(int columnCount) {
        return columnCount > MAX_COLUMNS_PER_TABLE;
    }

    // How many tables are needed
    public static int partCount(int columnCount) {
        if (!needsSplit(columnCount)) {
            return 1;
        }
        return (columnCount + MAX_COLUMNS_PER_TABLE - 1) / MAX_COLUMNS_PER_TABLE;
    }

    // How many columns go into each table (the last table may get a few less)
    public static int partSize(int columnCount) {
        int parts = partCount(columnCount);
        return (columnCount + parts - 1) / parts;
    }

    // First column number of a part (0-based, included)
    public static int startIndex(int columnCount, int part) {
        return part * partSize(columnCount);
    }

    // Last column number of a part (0-based, NOT included)
    public static int endIndex(int columnCount, int part) {
        return Math.min(columnCount, (part + 1) * partSize(columnCount));
    }

    // Name of a part table: long_table -> long_table_part1, long_table_part2, ...
    public static String partTableName(String baseTableName, int part) {
        return baseTableName + "_part" + (part + 1);
    }
}
