/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text: case check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import static io.github.pastorgl.datacooker.scripting.TDLLexicon.*;

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
        switch (tokenType) {
            case S_ARRAY: case S_BANG: case S_BETWEEN: case S_CARET: case S_CLOSE_BRACKET: case S_CLOSE_PAR: case S_COLON: case
                 S_COMMA: case S_CONCAT: case S_CAT: case S_DEFAULT: case S_DIGEST: case S_DOT: case S_EQ: case S_GT: case S_HASH: case S_HASHCODE: case S_IN: case S_IS: case S_LT: case
                 S_MINUS: case S_NOT: case S_OPEN_BRACKET: case S_OPEN_PAR: case S_OR: case S_PERCENT: case S_PIPE: case S_PLUS: case S_RANDOM: case S_RANGE: case
                 S_REGEXP: case S_QUESTION: case S_SLASH: case S_STAR: case S_TILDE: case S_XOR: case S_AND: case S_AMPERSAND: return OPERATOR;
            case S_SCOL: case K_ALTER: case K_ANALYZE: case K_ANTI: case K_AS: case K_BEGIN: case K_BY: case K_CALL: case K_COLUMNS: case K_COMMENT: case K_COPY: case
                 K_CREATE: case K_DISTINCT: case K_DROP: case K_DS: case K_ELSE: case K_END: case K_FETCH: case K_FROM: case K_FUNCTION: case K_IF: case K_INNER: case K_INPUT: case
                 K_INTO: case K_JOIN: case K_KEY: case K_LEFT: case K_LET: case K_LIMIT: case K_LOOP: case K_OPTIONS: case K_OUTER: case K_OUTPUT: case K_PARTITION: case
                 K_PROCEDURE: case K_RAISE: case K_RECORD: case K_REPLACE: case K_RETURN: case K_RIGHT: case K_SELECT: case K_SET: case K_SOURCE: case K_THEN: case
                 K_TRANSFORM: case K_UNION: case K_WHERE: case K_YIELD: return KEYWORD;
            case S_NULL: return NULL;
            case S_FALSE: case S_TRUE: return BOOLEAN;
            case T_MSG_ERROR: case T_MSG_INFO: case T_MSG_WARN: case T_COLUMNAR: case T_PASSTHRU: case T_POINT: case T_POLYGON: case T_RAW: case T_SEGMENT: case
                 T_SIMPLE: case T_STRUCT: case T_TRACK: case T_VALUE: return TYPE;
            case S_DOLLAR: case S_AT: return SIGIL;
            case L_IDENTIFIER: return IDENTIFIER;
            case L_NUMERIC: return NUMERIC;
            case L_STRING: return STRING;
            case L_COMMENT: return COMMENT;
            default: return null;
        }
    }
}
