/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import static io.github.pastorgl.datacooker.scripting.TDL.*;

public enum Highlight {
    OPERATOR,
    KEYWORD,
    NULL,
    BOOLEAN,
    TYPE,
    SIGIL,
    IDENTIFIER,
    NUMERIC,
    STRING,
    COMMENT;

    static public Highlight get(int tokenType) {
        return switch (tokenType) {
            case S_AMPERSAND, S_AND, S_ARRAY, S_BANG, S_BETWEEN, S_CARET, S_CLOSE_BRACKET, S_CLOSE_PAR, S_COLON,
                 S_COMMA, S_CONCAT, S_CAT, S_DEFAULT, S_DIGEST, S_DOT, S_EQ, S_GT, S_HASH, S_HASHCODE, S_IN, S_IS, S_LT,
                 S_MINUS, S_NOT, S_OPEN_BRACKET, S_OPEN_PAR, S_OR, S_PERCENT, S_PIPE, S_PLUS, S_RANDOM, S_RANGE,
                 S_REGEXP, S_QUESTION, S_SLASH, S_STAR, S_TILDE, S_XOR -> OPERATOR;
            case S_SCOL, K_ALTER, K_ANALYZE, K_ANTI, K_AS, K_BEGIN, K_BY, K_CALL, K_COLUMNS, K_COMMENT, K_COPY, K_CREATE,
                 K_DISTINCT, K_DROP, K_DS, K_ELSE, K_END, K_FETCH, K_FROM, K_FUNCTION, K_IF, K_INNER, K_INPUT, K_INTO,
                 K_JOIN, K_KEY, K_LEFT, K_LET, K_LIMIT, K_LOOP, K_OPTIONS, K_OUTER, K_OUTPUT, K_PARTITION, K_PROCEDURE,
                 K_RAISE, K_RECORD, K_REPLACE, K_RETURN, K_RIGHT, K_SELECT, K_SET, K_SOURCE, K_THEN, K_TRANSFORM,
                 K_UNION, K_WHERE, K_YIELD -> KEYWORD;
            case S_NULL -> NULL;
            case S_FALSE, S_TRUE -> BOOLEAN;
            case T_MSGLVL, T_OBJLVL, T_TYPE_SIMPLE, T_STREAM_TYPE -> TYPE;
            case L_IDENTIFIER -> IDENTIFIER;
            case S_DOLLAR, S_AT, L_UNARY -> SIGIL;
            case L_NUMERIC -> NUMERIC;
            case L_STRING -> STRING;
            case L_COMMENT -> COMMENT;
            default -> null;
        };
    }
}
