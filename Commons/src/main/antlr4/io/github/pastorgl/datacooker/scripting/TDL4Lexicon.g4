lexer grammar TDL4Lexicon;

// Symbols, prefixed S_
S_AMPERSAND : '&';
S_AT : '@';
S_BACKSLASH : '\\';
S_BACKTICK : '`';
S_BANG : '!';
S_CARET : '^';
S_CLOSE_BRACKET : ']';
S_CLOSE_PAR : ')';
S_COLON : ':';
S_COMMA : ',';
S_CONCAT : '||';
S_DOLLAR : '$';
S_DOT : '.';
S_EQ : '=';
S_GT : '>';
S_HASH : '#';
S_LT : '<';
S_MINUS : '-';
S_MINUSMINUS : '--';
S_OPEN_BRACKET : '[';
S_OPEN_PAR : '(';
S_PERCENT : '%';
S_PIPE : '|';
S_PLUS : '+';
S_QUESTION : '?';
S_SCOL : ';';
S_SLASH : '/';
S_STAR : '*';
S_TILDE : '~';

// Keywords, prefixed K_
K_ANALYZE : A N A L Y Z E;
K_ARRAY : A R R A Y;
K_AND : A N D;
K_ANTI : A N T I;
K_AS : A S;
K_BEGIN : B E G I N;
K_BETWEEN : B E T W E E N;
K_CALL : C A L L | E X E C;
K_COLUMNS: C O L U M N S;
K_CONCAT : C O N C A T | C A T;
K_COPY : C O P Y;
K_CREATE : C R E A T E;
K_DEFAULT: D E F A U L T;
K_DIGEST : D I G E S T | H A S H;
K_DISTINCT : D I S T I N C T;
K_DS : D S;
K_ELSE : E L S E;
K_END : E N D;
K_FALSE : F A L S E;
K_FROM : F R O M;
K_IF : I F;
K_IN : I N;
K_INNER : I N N E R;
K_INPUT : I N P U T;
K_INTO : I N T O;
K_IS : I S;
K_JOIN : J O I N;
K_KEY: K E Y;
K_LEFT : L E F T;
K_LET : L E T;
K_LIMIT : L I M I T;
K_LOOP : L O O P;
K_NOT : N O T;
K_NULL : N U L L;
K_OR : O R;
K_OPTIONS : O P T I O N S;
K_OUTER : O U T E R;
K_OUTPUT : O U T P U T;
K_REGEXP : R E G E X P | R E G E X | M A T C H | L I K E;
K_RIGHT : R I G H T;
K_SELECT : S E L E C T;
K_SET : S E T;
K_THEN : T H E N;
K_TRANSFORM : T R A N S F O R M | A L T E R;
K_TRUE : T R U E;
K_UNION : U N I O N;
K_VALUE: V A L U E;
K_WHERE : W H E R E;
K_XOR : X O R;

// Types, prefixed T_
T_POINT : P O I N T | P O I;
T_POLYGON : P O L Y G O N;
T_SEGMENT : T R A C K S E G M E N T | S E G M E N T;
T_TRACK : S E G M E N T E D T R A C K | T R A C K;

// Literals, prefixed L_
L_IDENTIFIER
 : '"' ( ~'"' | '""' )+ '"'
 | [a-zA-Z_] [a-zA-Z_0-9]*
 ;

L_UNARY
 : S_PLUS
 | S_MINUS
 ;

L_NUMERIC
 : L_UNARY? DIGIT+ ( S_DOT DIGIT* )? ( E L_UNARY? DIGIT+ )? D?
 | L_UNARY? S_DOT DIGIT+ ( E L_UNARY? DIGIT+ )? D?
 | L_UNARY? DIGIT+ L?
 | '0' X ( DIGIT | A | B | C | D | E | F )+
 ;

L_STRING
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

L_SPACES
 : [ \u000B\u000C\t\r\n] -> channel(2)
 ;

L_COMMENT
 : ( S_MINUSMINUS .*? '\n' | '/*' .*? '*/' ) -> channel(2)
 ;


fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
