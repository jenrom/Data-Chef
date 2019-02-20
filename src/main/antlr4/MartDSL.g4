grammar MartDSL;

@header { package de.areto.datachef.parser.antlr4; }

compilationUnit:
    configuration
    mapping
    sql
    ;

mapping:
    'mapping' '{'
        (columns += martColumn)+
    '}'
    ;

martColumn:
    name = ID ':' params += columnParam (',' columnParam)*
    ;

columnParam:
    keyParam | ddParam | cmntParam | identityParam
    ;

identityParam:
    key = 'id' // Special: identity column
    ;

keyParam:
    key = 'kc' 	 // Part of tables PK
    ;

ddParam:
    key = 'dd' '=' value = ID
    ;

cmntParam:
    key = 'cmnt' '=' value = STRING
    ;

sql:
    'sql' '{'
    code = STRING
    '}'
    ;

configuration:
    'config' '{'
        (params += configParam)+
    '}'
    ;

configParam:
    typeConfig | triggerConfig
    ;

typeConfig:
    'type' ':' type = ( 'merged' | 'historical' | 'load' | 'reload' )
    ;

triggerConfig:
    'trigger' ':' (
          cronTrigger
        | mousetrapTrigger
    )
    ;

cronTrigger:
    'cron' cron = STRING
    ;

mousetrapTrigger:
    'after' depMappings += ID (',' ID)* 'timeout' timeout = INT unit = ( 'sec' | 'min' | 'hour' | 'day' )
    ;

ID : ('a'..'z' | '_') ('a'..'z' | '_' | '0'..'9')* ;
INT: ('0'..'9')+;

/*STRING
 : '"' ( '\\' [btnfr"'\\] | ~[\r\n\\"] )* '"'
 ;
*/
STRING : DQUOTE ( STR_TEXT | EOL )* DQUOTE ;

WS
    : [ \t\r\n]+ -> channel(HIDDEN)
;

COMMENT
    : '/*' .*? '*/' -> skip
;

LINE_COMMENT
    : '//' ~[\r\n]* -> skip
;

fragment STR_TEXT: ( ~["\r\n\\] | ESC_SEQ )+ ;
fragment ESC_SEQ : '\\' ( [btf"\\] | EOF ) ;
fragment DQUOTE  : '"' ;
fragment EOL     : '\r'? '\n' ;

