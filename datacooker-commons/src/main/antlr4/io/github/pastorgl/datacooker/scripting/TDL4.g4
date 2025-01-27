parser grammar TDL4;

options { tokenVocab=TDL4Lexicon; }

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
 : create_stmt | transform_stmt | copy_stmt | let_stmt | loop_stmt | if_stmt | select_stmt | call_stmt | analyze_stmt
 | options_stmt | create_proc | drop_proc | create_func | drop_func
 ;

create_stmt
 : K_CREATE K_DS? ds_name func_expr K_FROM expression ( K_PARTITION expression )? ( K_BY ( S_HASHCODE | K_SOURCE | S_RANDOM ) )?
 ;

transform_stmt
 : K_TRANSFORM K_DS? ds_name S_STAR? func_expr columns_item* key_item? ( K_PARTITION expression? )?
 ;

columns_item
 : K_SET? type_columns K_COLUMNS? S_OPEN_PAR L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* S_CLOSE_PAR
 | K_SET? type_columns K_COLUMNS? S_OPEN_PAR var_name S_CLOSE_PAR
 ;

type_columns
 : T_POINT | T_POLYGON | T_SEGMENT | T_TRACK | T_VALUE
 ;

key_item
 : K_KEY attr_expr
 ;

copy_stmt
 : K_COPY K_DS? ds_parts S_STAR? func_expr K_INTO expression
 ;

params_expr
 : param ( S_COMMA param )*
 ;

param
 : S_AT L_IDENTIFIER S_EQ attr_expr
 ;

select_stmt
 : K_SELECT K_DISTINCT? ( S_STAR | what_expr ( S_COMMA what_expr )* )
  ( K_INTO ds_name K_FROM from_scope | K_FROM from_scope K_INTO ds_name )
  ( K_WHERE where_expr )?
  ( K_LIMIT limit_expr )?
 ;

limit_expr
 : expression S_PERCENT?
 ;

what_expr
 : attr_expr ( K_AS type_alias? alias )?
 ;

alias
 : L_IDENTIFIER
 ;

expression
 : ( is_op | between_op | in_op | kw_op | sym_op | var_name | literal | func_call | array )+
 ;

attr_expr
 : ( is_op | between_op | in_op | kw_op | sym_op | var_name | literal | func_attr | array | attr )+
 ;

func_call
 : func S_OPEN_PAR expression ( S_COMMA expression )* S_CLOSE_PAR
 | func S_OPEN_PAR S_CLOSE_PAR
 | S_OPEN_PAR expression S_CLOSE_PAR
 ;

func_attr
 : func S_OPEN_PAR attr_expr ( S_COMMA attr_expr )* S_CLOSE_PAR
 | func S_OPEN_PAR S_CLOSE_PAR
 | S_OPEN_PAR attr_expr S_CLOSE_PAR
 ;

type_alias
 : T_POINT | T_POLYGON | T_SEGMENT | T_TRACK | T_VALUE
 ;

from_scope
 : ds_parts
 | join_op ds_parts ( S_COMMA ds_parts )+
 | union_op ds_parts ( S_COMMA ds_parts )+
 | union_op ds_parts S_STAR
 ;

ds_parts
 : L_IDENTIFIER ( K_PARTITION expression )?
 ;

union_op
 : K_UNION ( S_CAT | S_XOR | S_AND )?
 ;

join_op
 : ( K_INNER | K_LEFT K_ANTI? | K_RIGHT K_ANTI? | K_OUTER )? K_JOIN
 ;

ds_name
 : L_IDENTIFIER
 ;

where_expr
 : type_alias? attr_expr
 ;

call_stmt
 : K_CALL func_expr operation_io
 | K_CALL func_expr
 ;

func_expr
 : func ( S_OPEN_PAR params_expr? S_CLOSE_PAR )?
 ;

func
 : L_IDENTIFIER
 ;

operation_io
 : ( from_positional | from_named ) ( into_positional | into_named )
 | ( into_positional | into_named ) ( from_positional | from_named )
 ;

from_positional
 : K_INPUT K_FROM? ds_parts S_STAR?
 | K_INPUT K_FROM? ds_parts ( S_COMMA ds_parts )*
 ;

from_named
 : K_INPUT ds_alias K_FROM? ds_parts ( S_COMMA ds_alias K_FROM? ds_parts )*
 ;

into_positional
 : K_OUTPUT K_INTO? ds_name S_STAR?
 | K_OUTPUT K_INTO? ds_name ( S_COMMA ds_name )*
 ;

into_named
 : K_OUTPUT ds_alias K_INTO? ds_name ( S_COMMA ds_alias K_INTO? ds_name )*
 ;

ds_alias
 : L_IDENTIFIER
 ;

let_stmt
 : K_LET var_name S_EQ expression
 | K_LET var_name S_EQ sub_query
 ;

sub_query
 : K_SELECT K_DISTINCT? what_expr K_FROM ds_parts ( K_WHERE where_expr )? ( K_LIMIT limit_expr )?
 ;

loop_stmt
 : K_LOOP var_name S_IN? expression K_BEGIN statements ( K_ELSE statements )? K_END K_LOOP?
 ;

attr
 : L_IDENTIFIER
 ;

if_stmt
 : K_IF expression K_THEN statements ( K_ELSE statements )? K_END K_IF?
 ;

analyze_stmt
 : K_ANALYZE K_DS? ds_name S_STAR? key_item? K_PARTITION?
 ;

options_stmt
 : K_OPTIONS params_expr
 ;

create_proc
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_PROCEDURE func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )? K_AS? K_BEGIN statements K_END K_PROCEDURE?
 ;

create_func
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_FUNCTION func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )? K_AS? K_RETURN? expression
 | ( K_CREATE ( S_OR K_REPLACE )? )? K_FUNCTION func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )? K_RECORD K_AS? K_RETURN? attr_expr
 ;

proc_param
 : param
 | S_AT L_IDENTIFIER
 ;

drop_proc
 : K_DROP K_PROCEDURE func ( S_COMMA func )*
 ;

drop_func
 : K_DROP K_FUNCTION func ( S_COMMA func )*
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
 : T_BOOL | T_DOUBLE | T_INT | T_LONG | T_STRING | T_STRUCT | S_REGEXP | S_NOT | S_AND | S_OR | S_XOR | S_DEFAULT | S_DIGEST | S_HASHCODE | S_RANDOM
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
 : S_ARRAY? S_OPEN_BRACKET S_CLOSE_BRACKET
 | S_ARRAY? S_OPEN_BRACKET literal ( S_COMMA literal )* S_CLOSE_BRACKET
 | S_ARRAY? S_OPEN_BRACKET L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* S_CLOSE_BRACKET
 | S_RANGE S_OPEN_BRACKET L_NUMERIC S_COMMA L_NUMERIC S_CLOSE_BRACKET
 ;
