package com.appzone.springbatch.util;

import java.util.Set;

/**
 * A column name that happens to be a reserved SQL keyword (SELECT, FROM, NUMBER,
 * VARCHAR, ORDER, ...) makes CREATE TABLE fail on most databases unless it is
 * quoted everywhere it is used. Since this project also builds plain (unquoted)
 * SQL in a few places, the safer fix is to simply never let a reserved word
 * become a column name in the first place.
 *
 * This list is a UNION of the reserved words of every database this project
 * supports (Oracle, MySQL/MariaDB, PostgreSQL, SQL Server) plus the ANSI SQL
 * reserved word list, plus common data type names (NUMBER, VARCHAR, VARCHAR2,
 * DATE, ...). It is intentionally broader than any single database needs,
 * because the target database is not always known at header-cleaning time and
 * being over-cautious here is free (it only costs a "_col" suffix).
 */
public final class ReservedSqlKeywords {

    private ReservedSqlKeywords() {
        // utility class
    }

    private static final Set<String> RESERVED_WORDS = Set.of(
            // ---- DML / DQL ----
            "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "FROM", "WHERE", "INTO",
            "VALUES", "SET", "JOIN", "INNER", "OUTER", "LEFT", "RIGHT", "FULL", "CROSS",
            "ON", "USING", "GROUP", "ORDER", "BY", "HAVING", "UNION", "INTERSECT",
            "EXCEPT", "MINUS", "ALL", "DISTINCT", "AS", "AND", "OR", "NOT", "NULL",
            "IS", "IN", "LIKE", "BETWEEN", "EXISTS", "ANY", "SOME", "CASE", "WHEN",
            "THEN", "ELSE", "END", "WITH", "RECURSIVE", "LIMIT", "OFFSET", "TOP",
            "FETCH", "FIRST", "NEXT", "ROWS", "ROW", "OVER", "PARTITION", "MATCHED",

            // ---- DDL ----
            "CREATE", "DROP", "ALTER", "TABLE", "VIEW", "INDEX", "SEQUENCE", "SYNONYM",
            "DATABASE", "SCHEMA", "TABLESPACE", "COLUMN", "COLUMNS", "CONSTRAINT",
            "PRIMARY", "FOREIGN", "KEY", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
            "CASCADE", "RESTRICT", "TRUNCATE", "RENAME", "COMMENT",

            // ---- Transactions / procedural ----
            "COMMIT", "ROLLBACK", "TRANSACTION", "SAVEPOINT", "BEGIN", "DECLARE",
            "PROCEDURE", "FUNCTION", "TRIGGER", "RETURN", "RETURNS", "CALL", "EXEC",
            "EXECUTE", "GRANT", "REVOKE", "USE", "PRINT", "RAISERROR", "WAITFOR",
            "OUTPUT", "IDENTITY", "AUTOINCREMENT", "AUTO_INCREMENT",

            // ---- Data types (very commonly mistaken for column names) ----
            "NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "SMALLINT", "BIGINT",
            "TINYINT", "FLOAT", "DOUBLE", "REAL", "CHAR", "NCHAR", "VARCHAR",
            "VARCHAR2", "NVARCHAR", "NVARCHAR2", "TEXT", "NTEXT", "CLOB", "BLOB",
            "BINARY", "VARBINARY", "BOOLEAN", "BOOL", "BIT", "DATE", "TIME",
            "TIMESTAMP", "DATETIME", "DATETIME2", "INTERVAL", "YEAR", "MONTH", "DAY",
            "HOUR", "MINUTE", "SECOND", "ZONE",

            // ---- Functions / misc identifiers that are reserved on at least one DB ----
            "CAST", "CONVERT", "COALESCE", "NULLIF", "EXTRACT", "DUAL", "ROWID",
            "ROWNUM", "LEVEL", "CONNECT", "START", "USER", "SESSION", "CURRENT",
            "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "SYSDATE", "TRUE", "FALSE", "UNKNOWN", "LOCK", "MODE", "EXPLAIN",
            "ANALYZE", "VACUUM", "TO", "FOR", "OF", "GO"
    );

    /**
     * True if the given identifier (case-insensitive) is a reserved SQL keyword
     * on at least one of the supported databases.
     */
    public static boolean isReserved(String identifier) {
        return identifier != null && RESERVED_WORDS.contains(identifier.toUpperCase());
    }
}