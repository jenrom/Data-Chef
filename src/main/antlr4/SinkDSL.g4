grammar SinkDSL;

@header { package de.areto.datachef.parser.antlr4; }

compilationUnit:
    configuration?
    mapping
    sql?
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
    loadTypeConfig | stageConfig | triggerConfig
    ;

loadTypeConfig:
    'load' ':' type = ( 'full' | 'partial' )
    ;

stageConfig:
    'stage' ':' (
          mod = 'import' 'from' connectionType = ( 'exa' | 'ora' | 'jdbc' ) connectionName = ID
        | mod = 'insert'
        | mod = 'file' ('of' 'type' csvType = ID)?
    )
    ;

triggerConfig:
    'trigger' ':' (
          defaultTrigger
        | cronTrigger
        | mousetrapTrigger
    )
    ;

defaultTrigger:
    'default'
;

cronTrigger:
    'cron' cron = STRING
    ;

mousetrapTrigger:
    'after' depMappings += ID (',' ID)* 'timeout' timeout = INT unit = ( 'sec' | 'min' | 'hour' | 'day' )
    ;

mapping:
    'mapping' '{'
        (columns += mappingColumn)+
    '}'
    ( mappingParams += mappingParam (',' mappingParam)* )?
    ;

mappingColumn:
    name = ID ':' params += columnParam (',' columnParam)*
    ;

columnParam:
    singleKey | keyAndId | keyAndString
    ;

mappingParam:
    mappingKeyAndId | mappingKeyAndString | linkRelations
;

mappingKeyAndId:
    key = ( 'on' | 'oa' )
    '=' value = ID
    ;

mappingKeyAndString:
    key = 'ocmnt'
    '=' value = STRING
    ;

linkRelations:
    'lk' '=' '{'
        relations += relation ( ',' relation) *
    '}'
    ;

relation:
    references += reference ( '--' reference ) * ( '+' reference ) * '(' name = ID ( historized = 'H' )? ')'
    ;

reference:
    hub = ID ( driving = '*' ) ?
    ;

singleKey:
    key = (
	  'kc' 	 // Key column
	| 'ign'	 // Ignore
	)
    ;

keyAndId:
    key = ( 'on' | 'oa' | 'rn' | 'dd' | 'sn' | 'ls' | 'rl' )
    '=' value = ID
    ;

keyAndString:
    key = ( 'ocmnt' | 'cmnt' | 'clc' )
    '=' value = STRING
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