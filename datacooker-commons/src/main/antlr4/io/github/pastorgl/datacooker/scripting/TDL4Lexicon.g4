lexer grammar TDL4Lexicon;

channels { COMMENTS }

// Symbols and operators, prefixed S_
S_AMPERSAND : '&';
S_AND : A N D;
S_ARRAY : A R R A Y;
S_AT : '@';
S_BANG : '!';
S_BETWEEN : B E T W E E N;
S_CARET : '^';
S_CLOSE_BRACKET : ']';
S_CLOSE_PAR : ')';
S_COLON : ':';
S_COMMA : ',';
S_CONCAT : '||';
S_CAT : C A T | C O N C A T;
S_DEFAULT: D E F A U L T;
S_DIGEST : D I G E S T | H A S H;
S_DOLLAR : '$';
S_DOT : '.';
S_EQ : '=';
S_FALSE : F A L S E;
S_GT : '>';
S_HASH : '#';
S_HASHCODE : H A S H C O D E;
S_IN : I N;
S_IS : I S;
S_LT : '<';
S_MINUS : '-';
S_MINUSMINUS : '--';
S_NOT : N O T;
S_NULL : N U L L;
S_OPEN_BRACKET : '[';
S_OPEN_PAR : '(';
S_OR : O R;
S_PERCENT : '%';
S_PIPE : '|';
S_PLUS : '+';
S_RANDOM : R A N D O M | R A N D;
S_RANGE : R A N G E;
S_REGEXP : R E G E X P | R E G E X | M A T C H | L I K E;
S_QUESTION : '?';
S_SCOL : ';';
S_SLASH : '/';
S_STAR : '*';
S_TILDE : '~';
S_TRUE : T R U E;
S_XOR : X O R;

// Statements and keywords, prefixed K_
K_ALTER : A L T E R | K_TRANSFORM;
K_ANALYZE : A N A L Y Z E;
K_ANTI : A N T I;
K_AS : A S;
K_BEGIN : B E G I N;
K_BY : B Y;
K_CALL : C A L L | E X E C;
K_COLUMNS : C O L U M N S | A T T R I B U T E S | P R O P E R T I E S;
K_COPY : C O P Y | S A V E;
K_CREATE : C R E A T E | L O A D;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_DS : D S;
K_ELSE : E L S E;
K_END : E N D;
K_FROM : F R O M;
K_FUNCTION : F U N C T I O N;
K_IF : I F;
K_INNER : I N N E R;
K_INPUT : I N P U T;
K_INTO : I N T O;
K_JOIN : J O I N;
K_KEY : K E Y;
K_LEFT : L E F T;
K_LET : L E T;
K_LIMIT : L I M I T;
K_LOOP : L O O P;
K_OPTIONS : O P T I O N S;
K_OUTER : O U T E R;
K_OUTPUT : O U T P U T;
K_PARTITION : P A R T I T I O N;
K_PROCEDURE : P R O C E D U R E | P R O C;
K_RAISE : R A I S E;
K_RECORD : R E C O R D;
K_REPLACE : R E P L A C E;
K_RETURN : R E T U R N;
K_RIGHT : R I G H T;
K_SELECT : S E L E C T;
K_SET : S E T;
K_SOURCE : S O U R C E;
K_THEN : T H E N;
K_TRANSFORM : T R A N S F O R M;
K_UNION : U N I O N;
K_WHERE : W H E R E;

// Types, prefixed T_
T_MSGLVL : D E B U G | L O G | I N F O | N O T I C E | W A R N | W A R N I N G | E R R O R | E X C E P T I O N;
T_OBJLVL : V A L U E | P O I N T | P O I | P O L Y G O N | T R A C K S E G M E N T | S E G M E N T | S E G M E N T E D T R A C K | T R A C K;
T_SIMPLE : B O O L | B O O L E A N | D O U B L E | I N T | I N T E G E R | L O N G | S T R I N G | S T R U C T | J S O N;

// Literals, prefixed L_
L_IDENTIFIER
 : '"' ( ~'"' | '""' )+ '"'
 | [a-zA-Z_] [a-zA-Z_0-9.]*
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
 | DIGIT ( DIGIT | A | B | C | D | E | F )* H
 ;

L_STRING
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

L_SPACES
 : [\p{White_Space}]+ -> channel(HIDDEN)
 ;

L_COMMENT
 : ( S_MINUSMINUS .*? '\n' | '/*' .*? '*/' ) -> channel(COMMENTS)
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
