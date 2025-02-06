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
 | options_stmt | create_proc | drop_proc | create_func | drop_func | raise_stmt
 ;

create_stmt
 : K_CREATE K_DS? ds_name func_expr K_FROM expression ds_parts? ( K_BY ( S_HASHCODE | K_SOURCE | S_RANDOM ) )?
 ;

transform_stmt
 : K_TRANSFORM K_DS? ds_name S_STAR? func_expr columns_item* key_item? ( K_PARTITION expression? )?
 ;

columns_item
 : K_SET? T_OBJLVL K_COLUMNS? S_OPEN_PAR L_IDENTIFIER ( S_COMMA L_IDENTIFIER )* S_CLOSE_PAR
 | K_SET? T_OBJLVL K_COLUMNS? S_OPEN_PAR var_name S_CLOSE_PAR
 ;

key_item
 : K_KEY expression
 ;

copy_stmt
 : K_COPY K_DS? ds_name S_STAR? ds_parts? func_expr K_INTO expression
 ;

params_expr
 : param ( S_COMMA param )*
 ;

param
 : S_AT L_IDENTIFIER S_EQ expression
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
 : expression ( K_AS T_OBJLVL? alias )?
 ;

alias
 : L_IDENTIFIER
 ;

expression
 : ( is_op | between_op | in_op | kw_op | sym_op | var_name | literal | func_call | array | attr )+
 ;

func_call
 : func S_OPEN_PAR expression ( S_COMMA expression )* S_CLOSE_PAR
 | func S_OPEN_PAR S_CLOSE_PAR
 | S_OPEN_PAR expression S_CLOSE_PAR
 ;

from_scope
 : ds_name_parts
 | join_op ds_name_parts ( S_COMMA ds_name_parts )+
 | union_op ds_name_parts ( S_COMMA ds_name_parts )+
 | union_op ds_name S_STAR ds_parts?
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
 : T_OBJLVL? expression
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
 : K_INPUT K_FROM? ds_name S_STAR ds_parts?
 | K_INPUT K_FROM? ds_name_parts ( S_COMMA ds_name_parts )*
 ;

from_named
 : K_INPUT ds_alias K_FROM? ds_name_parts ( S_COMMA ds_alias K_FROM? ds_name_parts )*
 ;

into_positional
 : K_OUTPUT K_INTO? ds_name S_STAR
 | K_OUTPUT K_INTO? ds_name ( S_COMMA ds_name )*
 ;

into_named
 : K_OUTPUT ds_alias K_INTO? ds_name ( S_COMMA ds_alias K_INTO? ds_name )*
 ;

ds_alias
 : L_IDENTIFIER
 ;

let_stmt
 : K_LET? var_name S_EQ expression
 | K_LET? var_name S_EQ sub_query
 ;

sub_query
 : K_SELECT K_DISTINCT? what_expr K_FROM ds_name_parts ( K_WHERE where_expr )? ( K_LIMIT limit_expr )?
 ;

let_func
 : K_LET? var_name S_EQ expression
 ;

loop_stmt
 : K_LOOP var_name S_IN? expression K_BEGIN statements ( K_ELSE statements )? K_END K_LOOP?
 ;

loop_func
 : K_LOOP var_name S_IN? expression K_BEGIN func_stmts ( K_ELSE func_stmts )? K_END K_LOOP?
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

analyze_stmt
 : K_ANALYZE K_DS? ds_name S_STAR? key_item? K_PARTITION?
 ;

options_stmt
 : K_OPTIONS params_expr
 ;

create_proc
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_PROCEDURE func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )?
  K_AS? K_BEGIN statements K_END K_PROCEDURE?
 ;

create_func
 : ( K_CREATE ( S_OR K_REPLACE )? )? K_FUNCTION func ( S_OPEN_PAR proc_param ( S_COMMA proc_param )* S_CLOSE_PAR )?
  K_RECORD?
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
 : param
 | S_AT L_IDENTIFIER
 ;

drop_proc
 : K_DROP K_PROCEDURE func ( S_COMMA func )*
 ;

drop_func
 : K_DROP K_FUNCTION func ( S_COMMA func )*
 ;

raise_stmt
 : K_RAISE T_MSGLVL? expression
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
 : T_SIMPLE | S_REGEXP | S_NOT | S_AND | S_OR | S_XOR | S_DEFAULT | S_DIGEST | S_HASHCODE | S_RANDOM
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
