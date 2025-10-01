/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
parser grammar TDL;

options { tokenVocab=TDLLexicon; }

script
 : statements EOF
 ;

statements
 : ( statement S_SCOL )*
 ;

loose_expression
 : expression EOF
 ;

statement
 : create_stmt | alter_stmt | copy_stmt | let_stmt | loop_stmt | if_stmt | select_stmt | call_stmt | analyze_stmt
 | options_stmt | create_proc | create_func | create_transform | raise_stmt | drop_stmt
 ;

create_stmt
 : K_CREATE K_DS? ds_name S_STAR? func_expr columns_item* K_FROM expression ds_parts? ( K_BY partition_by )?
 ;

columns_item
 : K_SET? obj_lvl K_COLUMNS? S_OPEN_PAR
  ( L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* | var_name )
  S_CLOSE_PAR
 ;

obj_lvl
 : T_POINT | T_POLYGON | T_SEGMENT | T_TRACK | T_VALUE
 ;

partition_by
 : S_HASHCODE | K_SOURCE | S_RANDOM
 ;

alter_stmt
 : K_ALTER K_DS?
  ( ds_name S_STAR? alter_item+ | from_scope alter_item+ K_INTO ds_name | from_wildcard alter_item+ K_INTO ds_name S_STAR )
 ;

alter_item
 : K_TRANSFORM? ( func_expr columns_item* | columns_item+ )
 | key_item
 | K_PARTITION expression?
 ;

key_item
 : K_KEY expression
 ;

copy_stmt
 : K_COPY K_DS? ( from_wildcard | from_scope ( S_COMMA from_scope )* ) func_expr columns_item* K_INTO expression
 ;

params_expr
 : param ( S_COMMA param )*
 ;

param
 : S_AT L_IDENTIFIER S_EQ expression
 ;

select_stmt
 : K_SELECT K_DISTINCT? ( S_STAR | what_expr ( S_COMMA what_expr )* )
  select_io+
  ( K_WHERE where_expr )?
  ( K_LIMIT limit_expr )?
 ;

select_io
 : K_INTO ds_name S_STAR? | K_FROM from_scope | K_FROM from_wildcard
 ;

limit_expr
 : expression S_PERCENT?
 ;

what_expr
 : expression ( K_AS obj_lvl? alias )?
 ;

alias
 : L_IDENTIFIER
 ;

expression
 : ( is_op | between_op | in_op | kw_op | sym_op | var_name | literal | func_call | array | attr )+
 ;

func_call
 : func S_OPEN_PAR ( expression ( S_COMMA expression )* )? S_CLOSE_PAR
 | S_OPEN_PAR expression S_CLOSE_PAR
 ;

from_scope
 : ds_name_parts
 | join_op ds_name_parts ( S_COMMA ds_name_parts )+
 | union_op ( ds_name_parts ( S_COMMA ds_name_parts )+ | ds_name S_STAR ds_parts? )
 ;

from_wildcard
 : ds_name S_STAR ds_parts?
 ;

ds_name
 : L_IDENTIFIER
 ;

ds_parts
 : K_PARTITION expression
 ;

ds_name_parts
 : L_IDENTIFIER ( K_PARTITION expression )?
 ;

union_op
 : K_UNION ( S_CAT | S_XOR | S_AND )?
 ;

join_op
 : ( K_INNER | K_LEFT K_ANTI? | K_RIGHT K_ANTI? | K_OUTER )? K_JOIN
 ;

where_expr
 : obj_lvl? expression
 ;

call_stmt
 : K_CALL func_expr operation_io*
 ;

func_expr
 : func ( S_OPEN_PAR params_expr? S_CLOSE_PAR )?
 ;

func
 : L_IDENTIFIER
 ;

operation_io
 : input_anonymous | input_wildcard | input_named | output_anonymous | output_wildcard | output_named
 ;

input_anonymous
 : K_INPUT K_FROM? from_scope ( S_COMMA from_scope )*
 ;

input_wildcard
 : K_INPUT K_FROM? from_wildcard
 ;

input_named
 : K_INPUT ds_alias K_FROM? from_scope ( S_COMMA ds_alias K_FROM? from_scope )*
 ;

output_anonymous
 : K_OUTPUT K_INTO? ds_name ( S_COMMA ds_name )*
 ;

output_wildcard
 : K_OUTPUT K_INTO? ds_name S_STAR
 ;

output_named
 : K_OUTPUT ds_alias K_INTO? ds_name ( S_COMMA ds_alias K_INTO? ds_name )*
 ;

ds_alias
 : L_IDENTIFIER
 ;

let_stmt
 : K_LET? var_name S_EQ ( expression | sub_query )
 | K_LET? expression
 ;

sub_query
 : K_SELECT K_DISTINCT? what_expr K_FROM from_scope ( K_WHERE where_expr )? ( K_LIMIT limit_expr )?
 ;

let_func
 : K_LET? var_name S_EQ expression
 | K_LET? expression
 ;

loop_stmt
 : K_LOOP var_name S_IN? expression K_BEGIN statements ( K_ELSE statements )? K_END K_LOOP?
 ;

loop_func
 : K_LOOP var_name S_IN? expression K_BEGIN func_stmts ( K_ELSE func_stmts )? K_END K_LOOP?
 ;

loop_transform
 : K_LOOP var_name S_IN? expression K_BEGIN transform_stmts ( K_ELSE transform_stmts )? K_END K_LOOP?
 ;

attr
 : L_IDENTIFIER
 ;

if_stmt
 : K_IF expression K_THEN statements ( K_ELSE statements )? K_END K_IF?
 ;

if_func
 : K_IF expression K_THEN func_stmts ( K_ELSE func_stmts )? K_END K_IF?
 ;

if_transform
 : K_IF expression K_THEN transform_stmts ( K_ELSE transform_stmts )? K_END K_IF?
 ;

analyze_stmt
 : K_ANALYZE K_DS? ds_name S_STAR? key_item? K_PARTITION?
 ;

options_stmt
 : K_OPTIONS params_expr
 ;

create_proc
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_PROCEDURE func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )?
  comment?
  K_AS? K_BEGIN statements K_END K_PROCEDURE?
 ;

create_func
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_FUNCTION func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )?
  K_RECORD? comment?
  K_AS? ( K_RETURN? expression | K_BEGIN func_stmts K_END K_FUNCTION? )
 ;

func_stmts
 : ( func_stmt S_SCOL )*
 ;

func_stmt
 : let_func | loop_func | if_func | return_func | raise_stmt
 ;

return_func
 : K_RETURN expression
 ;

proc_param
 : param_decl ( S_EQ expression comment? )?
 ;

param_decl
 : S_AT L_IDENTIFIER comment?
 ;

create_transform
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_TRANSFORM func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )?
  from_stream_type into_stream_type comment?
  K_AS? K_LOOP? K_BEGIN transform_stmts K_END K_TRANSFORM?
 ;

from_stream_type
 : K_FROM stream_type ( S_COMMA stream_type )*
 ;

into_stream_type
 : K_INTO stream_type
 ;

stream_type
 : T_COLUMNAR | T_PASSTHRU | T_POINT | T_POLYGON | T_RAW | T_STRUCT | T_TRACK
 ;

transform_stmts
 : ( transform_stmt S_SCOL )*
 ;

transform_stmt
 : let_func | loop_transform | if_transform | fetch_stmt | yield_stmt | return_transform | raise_stmt
 ;

return_transform
 : K_RETURN
 ;

fetch_stmt
 : K_FETCH ( K_INTO? S_AT L_IDENTIFIER ( S_COMMA S_AT L_IDENTIFIER )? )?
 ;

yield_stmt
 : K_YIELD expression S_COMMA expression
 ;

raise_stmt
 : K_RAISE msg_lvl? expression
 ;

msg_lvl
 : T_MSG_ERROR | T_MSG_INFO | T_MSG_WARN
 ;

drop_stmt
 : K_DROP ( K_PROCEDURE | K_FUNCTION | K_TRANSFORM ) func ( S_COMMA func )*
 ;

is_op
 : S_IS S_NOT? S_NULL
 ;

between_op
 : S_NOT? S_BETWEEN L_NUMERIC S_AND L_NUMERIC
 ;

in_op
 : S_NOT? S_IN
 ;

kw_op
 : T_RAW | T_SIMPLE | T_STRUCT | S_REGEXP | S_NOT | S_AND | S_OR | S_XOR | S_DEFAULT | S_DIGEST | S_HASHCODE | S_RANDOM
 ;

sym_op
 : S_CONCAT
 | ( S_PLUS | S_MINUS | S_STAR | S_SLASH | S_PERCENT | S_PIPE | S_CARET | S_TILDE | S_HASH | S_AMPERSAND | S_QUESTION | S_COLON | S_BANG | S_GT | S_LT | S_EQ )+
 ;

var_name
 : S_DOLLAR L_IDENTIFIER
 ;

literal
 : L_STRING | L_NUMERIC | S_TRUE | S_FALSE | S_NULL
 ;

array
 : S_ARRAY? S_OPEN_BRACKET ( literal ( S_COMMA literal )* | L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* )? S_CLOSE_BRACKET
 | S_RANGE S_OPEN_BRACKET L_NUMERIC S_COMMA L_NUMERIC S_CLOSE_BRACKET
 ;

comment
 : K_COMMENT L_STRING
 ;
