parser grammar TDL4;

options { tokenVocab=TDL4Lexicon; }

script
 : ( statement S_SCOL )* EOF
 ;

loose_expression
 : ( is_op | between_op | in_op | comparison_op | var_name | L_NUMERIC | L_STRING | K_NULL | K_TRUE | K_FALSE | S_OPEN_PAR | S_CLOSE_PAR | expression_op | digest_op | bool_op | default_op )+ EOF
 ;

statement
 : create_stmt | transform_stmt | copy_stmt | let_stmt | loop_stmt | if_stmt | select_stmt | call_stmt | analyze_stmt | options_stmt
 ;

create_stmt
 : K_CREATE K_DS? ds_name S_OPEN_PAR params_expr S_CLOSE_PAR partition_by?
 ;

partition_by
 : K_PARTITION? K_BY? ( K_HASHCODE | K_SOURCE | K_RANDOM )
 ;

transform_stmt
 : K_TRANSFORM K_DS? ds_name func_expr columns_item* key_item?
 ;

columns_item
 : K_SET? type_columns K_COLUMNS? S_OPEN_PAR L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* S_CLOSE_PAR
 ;

type_columns
 : T_POINT | T_POLYGON | T_SEGMENT | T_TRACK | K_VALUE
 ;

key_item
 : K_KEY expression
 ;

copy_stmt
 : K_COPY K_DS? ds_name S_STAR? S_OPEN_PAR params_expr S_CLOSE_PAR
 ;

params_expr
 : param ( S_COMMA param )*
 ;

param
 : S_AT L_IDENTIFIER S_EQ expression
 | S_AT L_IDENTIFIER S_EQ array
 ;

select_stmt
 : K_SELECT K_DISTINCT? ( S_STAR | what_expr ( S_COMMA what_expr )* )
  ( K_INTO ds_name K_FROM from_scope | K_FROM from_scope K_INTO ds_name )
  ( K_WHERE where_expr )?
  ( K_LIMIT limit_expr )?
 ;

limit_expr
 : L_NUMERIC S_PERCENT?
 ;

what_expr
 : expression ( K_AS type_alias? alias )?
 ;

alias
 : L_IDENTIFIER
 ;

expression
 : ( is_op | between_op | in_op | comparison_op | var_name | property_name | L_NUMERIC | L_STRING | K_NULL | K_TRUE | K_FALSE | S_OPEN_PAR | S_CLOSE_PAR | expression_op | digest_op | bool_op | default_op )+
 ;

type_alias
 : T_POINT | T_POLYGON | T_SEGMENT | T_TRACK | K_VALUE
 ;

from_scope
 : ds_name
 | join_op ds_name ( S_COMMA ds_name )+
 | union_op ds_name ( S_COMMA ds_name )+
 | union_op ds_name S_STAR
 ;

union_op
 : K_UNION ( K_CONCAT | K_XOR | K_AND )?
 ;

join_op
 : ( K_INNER | K_LEFT K_ANTI? | K_RIGHT K_ANTI? | K_OUTER )? K_JOIN
 ;

ds_name
 : L_IDENTIFIER
 ;

where_expr
 : type_alias? expression
 ;

call_stmt
 : K_CALL func_expr ( from_positional | from_named ) ( into_positional | into_named )
 | K_CALL func_expr ( into_positional | into_named ) ( from_positional | from_named )
 ;

func_expr
 : func ( S_OPEN_PAR params_expr? S_CLOSE_PAR )?
 ;

func
 : L_IDENTIFIER
 ;

from_positional
 : K_INPUT K_FROM? ds_name S_STAR?
 | K_INPUT K_FROM? ds_name ( S_COMMA ds_name )*
 ;

from_named
 : K_INPUT ds_alias K_FROM? ds_name ( S_COMMA ds_alias K_FROM? ds_name )*
 ;

into_positional
 : K_OUTPUT K_INTO? ds_name
 ;

into_named
 : K_OUTPUT ds_alias K_INTO? ds_name ( S_COMMA ds_alias K_INTO? ds_name )*
 ;

ds_alias
 : L_IDENTIFIER
 ;

let_stmt
 : K_LET var_name S_EQ let_expr
 | K_LET var_name S_EQ array
 | K_LET var_name S_EQ sub_query
 ;

sub_query
 : K_SELECT K_DISTINCT? what_expr K_FROM ds_name ( K_WHERE where_expr )? ( K_LIMIT limit_expr )?
 ;

let_expr
 : ( is_op | between_op | in_op | comparison_op | var_name | L_NUMERIC | L_STRING | K_NULL | K_TRUE | K_FALSE | S_OPEN_PAR | S_CLOSE_PAR | expression_op | digest_op | bool_op | default_op )+
 ;

loop_stmt
 : K_LOOP var_name K_IN? array K_BEGIN then_item ( K_ELSE else_item )? K_END K_LOOP?
 | K_LOOP var_name K_IN? var_name K_BEGIN then_item ( K_ELSE else_item )? K_END K_LOOP?
 ;

property_name
 : L_IDENTIFIER
 ;

if_stmt
 : K_IF let_expr K_THEN then_item ( K_ELSE else_item )? K_END K_IF?
 ;

then_item
 : ( statement S_SCOL )*
 ;

else_item
 : ( statement S_SCOL )*
 ;

analyze_stmt
 : K_ANALYZE K_DS? ds_name S_STAR? ( K_KEY property_name )?
 ;

options_stmt
 : K_OPTIONS params_expr
 ;

is_op
 : K_IS K_NOT? K_NULL
 ;

between_op
 : K_NOT? K_BETWEEN L_NUMERIC K_AND L_NUMERIC
 ;

in_op
 : K_NOT? K_IN array
 | K_NOT? K_IN var_name
 | K_NOT? K_IN property_name
 ;

comparison_op
 : K_REGEXP
 ;

bool_op
 : K_NOT | K_AND | K_OR | K_XOR
 ;

var_name
 : S_DOLLAR L_IDENTIFIER
 ;

expression_op
 : S_CONCAT
 | ( S_PLUS | S_MINUS | S_STAR | S_SLASH | S_PERCENT | S_PIPE | S_CARET | S_TILDE | S_HASH | S_AMPERSAND | S_QUESTION | S_BANG | S_GT | S_LT | S_EQ )+
 ;

default_op
 : K_DEFAULT | S_COLON
 ;

digest_op
 : K_DIGEST
 ;

array
 : K_ARRAY? S_OPEN_BRACKET L_STRING ( S_COMMA L_STRING )* S_CLOSE_BRACKET
 | K_ARRAY? S_OPEN_BRACKET L_NUMERIC ( S_COMMA L_NUMERIC )* S_CLOSE_BRACKET
 | K_ARRAY? S_OPEN_BRACKET L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* S_CLOSE_BRACKET
 ;
