// Generated from edu/stanford/futuredata/macrobase/SqlBase.g4 by ANTLR 4.7
package edu.stanford.futuredata.macrobase;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, ADD=5, ALL=6, ALTER=7, ANALYZE=8, AND=9, 
		ANY=10, ARRAY=11, AS=12, ASC=13, AT=14, BERNOULLI=15, BETWEEN=16, BY=17, 
		CALL=18, CASCADE=19, CASE=20, CAST=21, CATALOGS=22, COALESCE=23, COLUMN=24, 
		COLUMNS=25, COMBO=26, COMMENT=27, COMMIT=28, COMMITTED=29, COMPARE=30, 
		CONSTRAINT=31, COUNT=32, CREATE=33, CSV=34, CROSS=35, CUBE=36, CURRENT=37, 
		DATA=38, DATE=39, DAY=40, DEALLOCATE=41, DELETE=42, DESC=43, DESCRIBE=44, 
		DIFF=45, DISTINCT=46, DISTRIBUTED=47, DROP=48, ELSE=49, ENCLOSED=50, END=51, 
		ESCAPE=52, ESCAPED=53, EXCEPT=54, EXCLUDING=55, EXECUTE=56, EXISTS=57, 
		EXPLAIN=58, EXTRACT=59, FALSE=60, FIELDS=61, FILE=62, FILTER=63, FIRST=64, 
		FOLLOWING=65, FOR=66, FORMAT=67, FROM=68, FULL=69, FUNCTIONS=70, GRANT=71, 
		GRANTS=72, GRAPHVIZ=73, GROUP=74, GROUPING=75, HAVING=76, HOUR=77, IF=78, 
		IMPORT=79, IN=80, INCLUDING=81, INNER=82, INPUT=83, INSERT=84, INTEGER=85, 
		INTERSECT=86, INTERVAL=87, INTO=88, IS=89, ISOLATION=90, JOIN=91, LAST=92, 
		LATERAL=93, LEFT=94, LEVEL=95, LIKE=96, LIMIT=97, LINES=98, LOGICAL=99, 
		MAP=100, MAX=101, MIN=102, MIN_SUPPORT=103, MINUTE=104, MONTH=105, NATURAL=106, 
		NFC=107, NFD=108, NFKC=109, NFKD=110, NO=111, NOT=112, NULL=113, NULLIF=114, 
		NULLS=115, ON=116, ONLY=117, OPTION=118, OPTIONALLY=119, OR=120, ORDER=121, 
		ORDINALITY=122, OUTER=123, OUTFILE=124, OUTPUT=125, OVER=126, PARTITION=127, 
		PARTITIONS=128, POSITION=129, PRECEDING=130, PREPARE=131, PRIVILEGES=132, 
		PROPERTIES=133, PUBLIC=134, RANGE=135, RATIO=136, TRENDDIFF=137, READ=138, 
		RECURSIVE=139, RENAME=140, REPEATABLE=141, REPLACE=142, RESET=143, RESTRICT=144, 
		REVOKE=145, RIGHT=146, ROLLBACK=147, ROLLUP=148, ROW=149, ROWS=150, SCHEMA=151, 
		SCHEMAS=152, SECOND=153, SELECT=154, SESSION=155, SET=156, SETS=157, SHOW=158, 
		SMALLINT=159, SOME=160, SPLIT=161, START=162, STARTING=163, STATS=164, 
		SUBSTRING=165, SUM=166, SYSTEM=167, TABLE=168, TABLES=169, TABLESAMPLE=170, 
		TERMINATED=171, TEXT=172, THEN=173, TIME=174, TIMESTAMP=175, TINYINT=176, 
		TO=177, TRUE=178, TRY_CAST=179, TYPE=180, UESCAPE=181, UNBOUNDED=182, 
		UNCOMMITTED=183, UNION=184, UNNEST=185, USE=186, USING=187, VALIDATE=188, 
		VALUES=189, VERBOSE=190, VIEW=191, WHEN=192, WHERE=193, WITH=194, WORK=195, 
		WRITE=196, YEAR=197, ZONE=198, EQ=199, NEQ=200, LT=201, LTE=202, GT=203, 
		GTE=204, PLUS=205, MINUS=206, ASTERISK=207, SLASH=208, PERCENT=209, CONCAT=210, 
		STRING=211, UNICODE_STRING=212, BINARY_LITERAL=213, INTEGER_VALUE=214, 
		DECIMAL_VALUE=215, IDENTIFIER=216, DIGIT_IDENTIFIER=217, QUOTED_IDENTIFIER=218, 
		BACKQUOTED_IDENTIFIER=219, TIME_WITH_TIME_ZONE=220, TIMESTAMP_WITH_TIME_ZONE=221, 
		DOUBLE_PRECISION=222, SIMPLE_COMMENT=223, BRACKETED_COMMENT=224, WS=225, 
		UNRECOGNIZED=226, DELIMITER=227;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_statement = 2, 
		RULE_query = 3, RULE_queryTerm = 4, RULE_queryPrimary = 5, RULE_querySpecification = 6, 
		RULE_diffQuerySpecification = 7, RULE_splitQuery = 8, RULE_columnDefinition = 9, 
		RULE_sortItem = 10, RULE_minRatioExpression = 11, RULE_minTrendDiffExpression = 12, 
		RULE_minSupportExpression = 13, RULE_ratioMetricExpression = 14, RULE_aggregateExpression = 15, 
		RULE_aggregate = 16, RULE_setQuantifier = 17, RULE_selectItem = 18, RULE_exportClause = 19, 
		RULE_delimiterClause = 20, RULE_escapeClause = 21, RULE_relation = 22, 
		RULE_joinType = 23, RULE_joinCriteria = 24, RULE_aliasedRelation = 25, 
		RULE_columnAliases = 26, RULE_relationPrimary = 27, RULE_expression = 28, 
		RULE_booleanExpression = 29, RULE_predicated = 30, RULE_predicate = 31, 
		RULE_valueExpression = 32, RULE_primaryExpression = 33, RULE_string = 34, 
		RULE_comparisonOperator = 35, RULE_comparisonQuantifier = 36, RULE_booleanValue = 37, 
		RULE_type = 38, RULE_typeParameter = 39, RULE_baseType = 40, RULE_whenClause = 41, 
		RULE_filter = 42, RULE_qualifiedName = 43, RULE_identifier = 44, RULE_number = 45, 
		RULE_nonReserved = 46;
	public static final String[] ruleNames = {
		"singleStatement", "singleExpression", "statement", "query", "queryTerm", 
		"queryPrimary", "querySpecification", "diffQuerySpecification", "splitQuery", 
		"columnDefinition", "sortItem", "minRatioExpression", "minTrendDiffExpression", 
		"minSupportExpression", "ratioMetricExpression", "aggregateExpression", 
		"aggregate", "setQuantifier", "selectItem", "exportClause", "delimiterClause", 
		"escapeClause", "relation", "joinType", "joinCriteria", "aliasedRelation", 
		"columnAliases", "relationPrimary", "expression", "booleanExpression", 
		"predicated", "predicate", "valueExpression", "primaryExpression", "string", 
		"comparisonOperator", "comparisonQuantifier", "booleanValue", "type", 
		"typeParameter", "baseType", "whenClause", "filter", "qualifiedName", 
		"identifier", "number", "nonReserved"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "','", "')'", "'.'", "'ADD'", "'ALL'", "'ALTER'", "'ANALYZE'", 
		"'AND'", "'ANY'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'BERNOULLI'", "'BETWEEN'", 
		"'BY'", "'CALL'", "'CASCADE'", "'CASE'", "'CAST'", "'CATALOGS'", "'COALESCE'", 
		"'COLUMN'", "'COLUMNS'", "'COMBO'", "'COMMENT'", "'COMMIT'", "'COMMITTED'", 
		"'COMPARE'", "'CONSTRAINT'", "'COUNT'", "'CREATE'", "'CSV'", "'CROSS'", 
		"'CUBE'", "'CURRENT'", "'DATA'", "'DATE'", "'DAY'", "'DEALLOCATE'", "'DELETE'", 
		"'DESC'", "'DESCRIBE'", "'DIFF'", "'DISTINCT'", "'DISTRIBUTED'", "'DROP'", 
		"'ELSE'", "'ENCLOSED'", "'END'", "'ESCAPE'", "'ESCAPED'", "'EXCEPT'", 
		"'EXCLUDING'", "'EXECUTE'", "'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", 
		"'FIELDS'", "'FILE'", "'FILTER'", "'FIRST'", "'FOLLOWING'", "'FOR'", "'FORMAT'", 
		"'FROM'", "'FULL'", "'FUNCTIONS'", "'GRANT'", "'GRANTS'", "'GRAPHVIZ'", 
		"'GROUP'", "'GROUPING'", "'HAVING'", "'HOUR'", "'IF'", "'IMPORT'", "'IN'", 
		"'INCLUDING'", "'INNER'", "'INPUT'", "'INSERT'", "'INTEGER'", "'INTERSECT'", 
		"'INTERVAL'", "'INTO'", "'IS'", "'ISOLATION'", "'JOIN'", "'LAST'", "'LATERAL'", 
		"'LEFT'", "'LEVEL'", "'LIKE'", "'LIMIT'", "'LINES'", "'LOGICAL'", "'MAP'", 
		"'MAX'", "'MIN'", "'MIN SUPPORT'", "'MINUTE'", "'MONTH'", "'NATURAL'", 
		"'NFC'", "'NFD'", "'NFKC'", "'NFKD'", "'NO'", "'NOT'", "'NULL'", "'NULLIF'", 
		"'NULLS'", "'ON'", "'ONLY'", "'OPTION'", "'OPTIONALLY'", "'OR'", "'ORDER'", 
		"'ORDINALITY'", "'OUTER'", "'OUTFILE'", "'OUTPUT'", "'OVER'", "'PARTITION'", 
		"'PARTITIONS'", "'POSITION'", "'PRECEDING'", "'PREPARE'", "'PRIVILEGES'", 
		"'PROPERTIES'", "'PUBLIC'", "'RANGE'", "'RATIO'", "'TRENDDIFF'", "'READ'", 
		"'RECURSIVE'", "'RENAME'", "'REPEATABLE'", "'REPLACE'", "'RESET'", "'RESTRICT'", 
		"'REVOKE'", "'RIGHT'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'", "'SCHEMA'", 
		"'SCHEMAS'", "'SECOND'", "'SELECT'", "'SESSION'", "'SET'", "'SETS'", "'SHOW'", 
		"'SMALLINT'", "'SOME'", "'SPLIT'", "'START'", "'STARTING'", "'STATS'", 
		"'SUBSTRING'", "'SUM'", "'SYSTEM'", "'TABLE'", "'TABLES'", "'TABLESAMPLE'", 
		"'TERMINATED'", "'TEXT'", "'THEN'", "'TIME'", "'TIMESTAMP'", "'TINYINT'", 
		"'TO'", "'TRUE'", "'TRY_CAST'", "'TYPE'", "'UESCAPE'", "'UNBOUNDED'", 
		"'UNCOMMITTED'", "'UNION'", "'UNNEST'", "'USE'", "'USING'", "'VALIDATE'", 
		"'VALUES'", "'VERBOSE'", "'VIEW'", "'WHEN'", "'WHERE'", "'WITH'", "'WORK'", 
		"'WRITE'", "'YEAR'", "'ZONE'", "'='", null, "'<'", "'<='", "'>'", "'>='", 
		"'+'", "'-'", "'*'", "'/'", "'%'", "'||'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, "ADD", "ALL", "ALTER", "ANALYZE", "AND", 
		"ANY", "ARRAY", "AS", "ASC", "AT", "BERNOULLI", "BETWEEN", "BY", "CALL", 
		"CASCADE", "CASE", "CAST", "CATALOGS", "COALESCE", "COLUMN", "COLUMNS", 
		"COMBO", "COMMENT", "COMMIT", "COMMITTED", "COMPARE", "CONSTRAINT", "COUNT", 
		"CREATE", "CSV", "CROSS", "CUBE", "CURRENT", "DATA", "DATE", "DAY", "DEALLOCATE", 
		"DELETE", "DESC", "DESCRIBE", "DIFF", "DISTINCT", "DISTRIBUTED", "DROP", 
		"ELSE", "ENCLOSED", "END", "ESCAPE", "ESCAPED", "EXCEPT", "EXCLUDING", 
		"EXECUTE", "EXISTS", "EXPLAIN", "EXTRACT", "FALSE", "FIELDS", "FILE", 
		"FILTER", "FIRST", "FOLLOWING", "FOR", "FORMAT", "FROM", "FULL", "FUNCTIONS", 
		"GRANT", "GRANTS", "GRAPHVIZ", "GROUP", "GROUPING", "HAVING", "HOUR", 
		"IF", "IMPORT", "IN", "INCLUDING", "INNER", "INPUT", "INSERT", "INTEGER", 
		"INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "LAST", "LATERAL", 
		"LEFT", "LEVEL", "LIKE", "LIMIT", "LINES", "LOGICAL", "MAP", "MAX", "MIN", 
		"MIN_SUPPORT", "MINUTE", "MONTH", "NATURAL", "NFC", "NFD", "NFKC", "NFKD", 
		"NO", "NOT", "NULL", "NULLIF", "NULLS", "ON", "ONLY", "OPTION", "OPTIONALLY", 
		"OR", "ORDER", "ORDINALITY", "OUTER", "OUTFILE", "OUTPUT", "OVER", "PARTITION", 
		"PARTITIONS", "POSITION", "PRECEDING", "PREPARE", "PRIVILEGES", "PROPERTIES", 
		"PUBLIC", "RANGE", "RATIO", "TRENDDIFF", "READ", "RECURSIVE", "RENAME", 
		"REPEATABLE", "REPLACE", "RESET", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", 
		"ROLLUP", "ROW", "ROWS", "SCHEMA", "SCHEMAS", "SECOND", "SELECT", "SESSION", 
		"SET", "SETS", "SHOW", "SMALLINT", "SOME", "SPLIT", "START", "STARTING", 
		"STATS", "SUBSTRING", "SUM", "SYSTEM", "TABLE", "TABLES", "TABLESAMPLE", 
		"TERMINATED", "TEXT", "THEN", "TIME", "TIMESTAMP", "TINYINT", "TO", "TRUE", 
		"TRY_CAST", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNION", "UNNEST", 
		"USE", "USING", "VALIDATE", "VALUES", "VERBOSE", "VIEW", "WHEN", "WHERE", 
		"WITH", "WORK", "WRITE", "YEAR", "ZONE", "EQ", "NEQ", "LT", "LTE", "GT", 
		"GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "STRING", 
		"UNICODE_STRING", "BINARY_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", 
		"IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"TIME_WITH_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE", "DOUBLE_PRECISION", 
		"SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(94);
			statement();
			setState(95);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(97);
			expression();
			setState(98);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ImportCsvContext extends StatementContext {
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode CSV() { return getToken(SqlBaseParser.CSV, 0); }
		public TerminalNode FILE() { return getToken(SqlBaseParser.FILE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<ColumnDefinitionContext> columnDefinition() {
			return getRuleContexts(ColumnDefinitionContext.class);
		}
		public ColumnDefinitionContext columnDefinition(int i) {
			return getRuleContext(ColumnDefinitionContext.class,i);
		}
		public ImportCsvContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterImportCsv(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitImportCsv(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitImportCsv(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_statement);
		int _la;
		try {
			setState(121);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case SELECT:
			case TABLE:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(100);
				query();
				}
				break;
			case IMPORT:
				_localctx = new ImportCsvContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(101);
				match(IMPORT);
				setState(102);
				match(FROM);
				setState(103);
				match(CSV);
				setState(104);
				match(FILE);
				setState(105);
				match(STRING);
				setState(106);
				match(INTO);
				setState(107);
				qualifiedName();
				setState(119);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(108);
					match(T__0);
					setState(109);
					columnDefinition();
					setState(114);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(110);
						match(T__1);
						setState(111);
						columnDefinition();
						}
						}
						setState(116);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(117);
					match(T__2);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_query);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			queryTerm();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		QueryTermContext _localctx = new QueryTermContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_queryTerm);
		try {
			_localctx = new QueryTermDefaultContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(125);
			queryPrimary();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DiffQueryContext extends QueryPrimaryContext {
		public DiffQuerySpecificationContext diffQuerySpecification() {
			return getRuleContext(DiffQuerySpecificationContext.class,0);
		}
		public DiffQueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDiffQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDiffQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDiffQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_queryPrimary);
		try {
			setState(135);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(127);
				querySpecification();
				}
				break;
			case 2:
				_localctx = new DiffQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(128);
				diffQuerySpecification();
				}
				break;
			case 3:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(129);
				match(TABLE);
				setState(130);
				qualifiedName();
				}
				break;
			case 4:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(131);
				match(T__0);
				setState(132);
				query();
				setState(133);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public Token limit;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public ExportClauseContext exportClause() {
			return getRuleContext(ExportClauseContext.class,0);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(229);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(137);
				match(SELECT);
				setState(139);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(138);
					setQuantifier();
					}
					break;
				}
				setState(141);
				selectItem();
				setState(146);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(142);
						match(T__1);
						setState(143);
						selectItem();
						}
						} 
					}
					setState(148);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
				}
				setState(158);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM) {
					{
					setState(149);
					match(FROM);
					setState(150);
					relation(0);
					setState(155);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(151);
							match(T__1);
							setState(152);
							relation(0);
							}
							} 
						}
						setState(157);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
					}
					}
				}

				setState(162);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
				case 1:
					{
					setState(160);
					match(WHERE);
					setState(161);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(174);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(164);
					match(ORDER);
					setState(165);
					match(BY);
					setState(166);
					sortItem();
					setState(171);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(167);
							match(T__1);
							setState(168);
							sortItem();
							}
							} 
						}
						setState(173);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
					}
					}
				}

				setState(178);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(176);
					match(LIMIT);
					setState(177);
					((QuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((QuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(181);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(180);
					exportClause();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(183);
				match(SELECT);
				setState(185);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
				case 1:
					{
					setState(184);
					setQuantifier();
					}
					break;
				}
				setState(187);
				selectItem();
				setState(192);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(188);
						match(T__1);
						setState(189);
						selectItem();
						}
						} 
					}
					setState(194);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
				}
				setState(196);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(195);
					exportClause();
					}
				}

				setState(207);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM) {
					{
					setState(198);
					match(FROM);
					setState(199);
					relation(0);
					setState(204);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(200);
							match(T__1);
							setState(201);
							relation(0);
							}
							} 
						}
						setState(206);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
					}
					}
				}

				setState(211);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
				case 1:
					{
					setState(209);
					match(WHERE);
					setState(210);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(223);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(213);
					match(ORDER);
					setState(214);
					match(BY);
					setState(215);
					sortItem();
					setState(220);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(216);
							match(T__1);
							setState(217);
							sortItem();
							}
							} 
						}
						setState(222);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
					}
					}
				}

				setState(227);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
				case 1:
					{
					setState(225);
					match(LIMIT);
					setState(226);
					((QuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((QuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DiffQuerySpecificationContext extends ParserRuleContext {
		public Token maxCombo;
		public BooleanExpressionContext where;
		public Token limit;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode DIFF() { return getToken(SqlBaseParser.DIFF, 0); }
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode COMPARE() { return getToken(SqlBaseParser.COMPARE, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public RatioMetricExpressionContext ratioMetricExpression() {
			return getRuleContext(RatioMetricExpressionContext.class,0);
		}
		public TerminalNode MAX() { return getToken(SqlBaseParser.MAX, 0); }
		public TerminalNode COMBO() { return getToken(SqlBaseParser.COMBO, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public ExportClauseContext exportClause() {
			return getRuleContext(ExportClauseContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public MinTrendDiffExpressionContext minTrendDiffExpression() {
			return getRuleContext(MinTrendDiffExpressionContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public MinRatioExpressionContext minRatioExpression() {
			return getRuleContext(MinRatioExpressionContext.class,0);
		}
		public MinSupportExpressionContext minSupportExpression() {
			return getRuleContext(MinSupportExpressionContext.class,0);
		}
		public SplitQueryContext splitQuery() {
			return getRuleContext(SplitQueryContext.class,0);
		}
		public DiffQuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_diffQuerySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDiffQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDiffQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDiffQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DiffQuerySpecificationContext diffQuerySpecification() throws RecognitionException {
		DiffQuerySpecificationContext _localctx = new DiffQuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_diffQuerySpecification);
		int _la;
		try {
			int _alt;
			setState(532);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(231);
				match(SELECT);
				setState(233);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
				case 1:
					{
					setState(232);
					setQuantifier();
					}
					break;
				}
				setState(235);
				selectItem();
				setState(240);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(236);
					match(T__1);
					setState(237);
					selectItem();
					}
					}
					setState(242);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(243);
				match(FROM);
				setState(244);
				match(DIFF);
				setState(245);
				queryTerm();
				setState(247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXPLAIN) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (DIGIT_IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(246);
					qualifiedName();
					}
				}

				setState(249);
				match(T__1);
				setState(250);
				queryTerm();
				setState(252);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXPLAIN) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (DIGIT_IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(251);
					qualifiedName();
					}
				}

				setState(254);
				match(ON);
				setState(257);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ADD:
				case ALL:
				case ANALYZE:
				case ANY:
				case ARRAY:
				case ASC:
				case AT:
				case BERNOULLI:
				case CALL:
				case CASCADE:
				case CATALOGS:
				case COALESCE:
				case COLUMN:
				case COLUMNS:
				case COMMENT:
				case COMMIT:
				case COMMITTED:
				case CURRENT:
				case DATA:
				case DATE:
				case DAY:
				case DESC:
				case DISTRIBUTED:
				case EXCLUDING:
				case EXPLAIN:
				case FILTER:
				case FIRST:
				case FOLLOWING:
				case FORMAT:
				case FUNCTIONS:
				case GRANT:
				case GRANTS:
				case GRAPHVIZ:
				case HOUR:
				case IF:
				case INCLUDING:
				case INPUT:
				case INTEGER:
				case INTERVAL:
				case ISOLATION:
				case LAST:
				case LATERAL:
				case LEVEL:
				case LIMIT:
				case LOGICAL:
				case MAP:
				case MINUTE:
				case MONTH:
				case NFC:
				case NFD:
				case NFKC:
				case NFKD:
				case NO:
				case NULLIF:
				case NULLS:
				case ONLY:
				case OPTION:
				case ORDINALITY:
				case OUTPUT:
				case OVER:
				case PARTITION:
				case PARTITIONS:
				case POSITION:
				case PRECEDING:
				case PRIVILEGES:
				case PROPERTIES:
				case PUBLIC:
				case RANGE:
				case READ:
				case RENAME:
				case REPEATABLE:
				case REPLACE:
				case RESET:
				case RESTRICT:
				case REVOKE:
				case ROLLBACK:
				case ROW:
				case ROWS:
				case SCHEMA:
				case SCHEMAS:
				case SECOND:
				case SESSION:
				case SET:
				case SETS:
				case SHOW:
				case SMALLINT:
				case SOME:
				case START:
				case STATS:
				case SUBSTRING:
				case SYSTEM:
				case TABLES:
				case TABLESAMPLE:
				case TEXT:
				case TIME:
				case TIMESTAMP:
				case TINYINT:
				case TO:
				case TRY_CAST:
				case TYPE:
				case UNBOUNDED:
				case UNCOMMITTED:
				case USE:
				case VALIDATE:
				case VERBOSE:
				case VIEW:
				case WORK:
				case WRITE:
				case YEAR:
				case ZONE:
				case IDENTIFIER:
				case DIGIT_IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(255);
					columnAliases();
					}
					break;
				case ASTERISK:
					{
					setState(256);
					match(ASTERISK);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(275);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(259);
					match(WITH);
					setState(273);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
					case 1:
						{
						setState(261);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(260);
							minRatioExpression();
							}
						}

						setState(264);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(263);
							minSupportExpression();
							}
						}

						}
						break;
					case 2:
						{
						setState(267);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(266);
							minSupportExpression();
							}
						}

						setState(270);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(269);
							minRatioExpression();
							}
						}

						}
						break;
					case 3:
						{
						setState(272);
						minTrendDiffExpression();
						}
						break;
					}
					}
				}

				setState(280);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMPARE) {
					{
					setState(277);
					match(COMPARE);
					setState(278);
					match(BY);
					setState(279);
					ratioMetricExpression();
					}
				}

				setState(285);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MAX) {
					{
					setState(282);
					match(MAX);
					setState(283);
					match(COMBO);
					setState(284);
					((DiffQuerySpecificationContext)_localctx).maxCombo = match(INTEGER_VALUE);
					}
				}

				setState(289);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
				case 1:
					{
					setState(287);
					match(WHERE);
					setState(288);
					((DiffQuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(301);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(291);
					match(ORDER);
					setState(292);
					match(BY);
					setState(293);
					sortItem();
					setState(298);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(294);
							match(T__1);
							setState(295);
							sortItem();
							}
							} 
						}
						setState(300);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
					}
					}
				}

				setState(305);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
				case 1:
					{
					setState(303);
					match(LIMIT);
					setState(304);
					((DiffQuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((DiffQuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(308);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(307);
					exportClause();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(310);
				match(SELECT);
				setState(312);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
				case 1:
					{
					setState(311);
					setQuantifier();
					}
					break;
				}
				setState(314);
				selectItem();
				setState(319);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(315);
					match(T__1);
					setState(316);
					selectItem();
					}
					}
					setState(321);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(323);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(322);
					exportClause();
					}
				}

				setState(325);
				match(FROM);
				setState(326);
				match(DIFF);
				setState(327);
				queryTerm();
				setState(329);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXPLAIN) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (DIGIT_IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(328);
					qualifiedName();
					}
				}

				setState(331);
				match(T__1);
				setState(332);
				queryTerm();
				setState(334);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXPLAIN) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (DIGIT_IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(333);
					qualifiedName();
					}
				}

				setState(336);
				match(ON);
				setState(339);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ADD:
				case ALL:
				case ANALYZE:
				case ANY:
				case ARRAY:
				case ASC:
				case AT:
				case BERNOULLI:
				case CALL:
				case CASCADE:
				case CATALOGS:
				case COALESCE:
				case COLUMN:
				case COLUMNS:
				case COMMENT:
				case COMMIT:
				case COMMITTED:
				case CURRENT:
				case DATA:
				case DATE:
				case DAY:
				case DESC:
				case DISTRIBUTED:
				case EXCLUDING:
				case EXPLAIN:
				case FILTER:
				case FIRST:
				case FOLLOWING:
				case FORMAT:
				case FUNCTIONS:
				case GRANT:
				case GRANTS:
				case GRAPHVIZ:
				case HOUR:
				case IF:
				case INCLUDING:
				case INPUT:
				case INTEGER:
				case INTERVAL:
				case ISOLATION:
				case LAST:
				case LATERAL:
				case LEVEL:
				case LIMIT:
				case LOGICAL:
				case MAP:
				case MINUTE:
				case MONTH:
				case NFC:
				case NFD:
				case NFKC:
				case NFKD:
				case NO:
				case NULLIF:
				case NULLS:
				case ONLY:
				case OPTION:
				case ORDINALITY:
				case OUTPUT:
				case OVER:
				case PARTITION:
				case PARTITIONS:
				case POSITION:
				case PRECEDING:
				case PRIVILEGES:
				case PROPERTIES:
				case PUBLIC:
				case RANGE:
				case READ:
				case RENAME:
				case REPEATABLE:
				case REPLACE:
				case RESET:
				case RESTRICT:
				case REVOKE:
				case ROLLBACK:
				case ROW:
				case ROWS:
				case SCHEMA:
				case SCHEMAS:
				case SECOND:
				case SESSION:
				case SET:
				case SETS:
				case SHOW:
				case SMALLINT:
				case SOME:
				case START:
				case STATS:
				case SUBSTRING:
				case SYSTEM:
				case TABLES:
				case TABLESAMPLE:
				case TEXT:
				case TIME:
				case TIMESTAMP:
				case TINYINT:
				case TO:
				case TRY_CAST:
				case TYPE:
				case UNBOUNDED:
				case UNCOMMITTED:
				case USE:
				case VALIDATE:
				case VERBOSE:
				case VIEW:
				case WORK:
				case WRITE:
				case YEAR:
				case ZONE:
				case IDENTIFIER:
				case DIGIT_IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(337);
					columnAliases();
					}
					break;
				case ASTERISK:
					{
					setState(338);
					match(ASTERISK);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(341);
					match(WITH);
					setState(354);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
					case 1:
						{
						setState(343);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(342);
							minRatioExpression();
							}
						}

						setState(346);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(345);
							minSupportExpression();
							}
						}

						}
						break;
					case 2:
						{
						setState(349);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(348);
							minSupportExpression();
							}
						}

						setState(352);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(351);
							minRatioExpression();
							}
						}

						}
						break;
					}
					}
				}

				setState(361);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMPARE) {
					{
					setState(358);
					match(COMPARE);
					setState(359);
					match(BY);
					setState(360);
					ratioMetricExpression();
					}
				}

				setState(366);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MAX) {
					{
					setState(363);
					match(MAX);
					setState(364);
					match(COMBO);
					setState(365);
					((DiffQuerySpecificationContext)_localctx).maxCombo = match(INTEGER_VALUE);
					}
				}

				setState(370);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
				case 1:
					{
					setState(368);
					match(WHERE);
					setState(369);
					((DiffQuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(382);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(372);
					match(ORDER);
					setState(373);
					match(BY);
					setState(374);
					sortItem();
					setState(379);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,56,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(375);
							match(T__1);
							setState(376);
							sortItem();
							}
							} 
						}
						setState(381);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,56,_ctx);
					}
					}
				}

				setState(386);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
				case 1:
					{
					setState(384);
					match(LIMIT);
					setState(385);
					((DiffQuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((DiffQuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(388);
				match(SELECT);
				setState(390);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
				case 1:
					{
					setState(389);
					setQuantifier();
					}
					break;
				}
				setState(392);
				selectItem();
				setState(397);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(393);
					match(T__1);
					setState(394);
					selectItem();
					}
					}
					setState(399);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(400);
				match(FROM);
				setState(401);
				match(DIFF);
				setState(402);
				match(T__0);
				setState(403);
				splitQuery();
				setState(404);
				match(T__2);
				setState(405);
				match(ON);
				setState(408);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ADD:
				case ALL:
				case ANALYZE:
				case ANY:
				case ARRAY:
				case ASC:
				case AT:
				case BERNOULLI:
				case CALL:
				case CASCADE:
				case CATALOGS:
				case COALESCE:
				case COLUMN:
				case COLUMNS:
				case COMMENT:
				case COMMIT:
				case COMMITTED:
				case CURRENT:
				case DATA:
				case DATE:
				case DAY:
				case DESC:
				case DISTRIBUTED:
				case EXCLUDING:
				case EXPLAIN:
				case FILTER:
				case FIRST:
				case FOLLOWING:
				case FORMAT:
				case FUNCTIONS:
				case GRANT:
				case GRANTS:
				case GRAPHVIZ:
				case HOUR:
				case IF:
				case INCLUDING:
				case INPUT:
				case INTEGER:
				case INTERVAL:
				case ISOLATION:
				case LAST:
				case LATERAL:
				case LEVEL:
				case LIMIT:
				case LOGICAL:
				case MAP:
				case MINUTE:
				case MONTH:
				case NFC:
				case NFD:
				case NFKC:
				case NFKD:
				case NO:
				case NULLIF:
				case NULLS:
				case ONLY:
				case OPTION:
				case ORDINALITY:
				case OUTPUT:
				case OVER:
				case PARTITION:
				case PARTITIONS:
				case POSITION:
				case PRECEDING:
				case PRIVILEGES:
				case PROPERTIES:
				case PUBLIC:
				case RANGE:
				case READ:
				case RENAME:
				case REPEATABLE:
				case REPLACE:
				case RESET:
				case RESTRICT:
				case REVOKE:
				case ROLLBACK:
				case ROW:
				case ROWS:
				case SCHEMA:
				case SCHEMAS:
				case SECOND:
				case SESSION:
				case SET:
				case SETS:
				case SHOW:
				case SMALLINT:
				case SOME:
				case START:
				case STATS:
				case SUBSTRING:
				case SYSTEM:
				case TABLES:
				case TABLESAMPLE:
				case TEXT:
				case TIME:
				case TIMESTAMP:
				case TINYINT:
				case TO:
				case TRY_CAST:
				case TYPE:
				case UNBOUNDED:
				case UNCOMMITTED:
				case USE:
				case VALIDATE:
				case VERBOSE:
				case VIEW:
				case WORK:
				case WRITE:
				case YEAR:
				case ZONE:
				case IDENTIFIER:
				case DIGIT_IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(406);
					columnAliases();
					}
					break;
				case ASTERISK:
					{
					setState(407);
					match(ASTERISK);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(425);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(410);
					match(WITH);
					setState(423);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
					case 1:
						{
						setState(412);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(411);
							minRatioExpression();
							}
						}

						setState(415);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(414);
							minSupportExpression();
							}
						}

						}
						break;
					case 2:
						{
						setState(418);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(417);
							minSupportExpression();
							}
						}

						setState(421);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(420);
							minRatioExpression();
							}
						}

						}
						break;
					}
					}
				}

				setState(430);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMPARE) {
					{
					setState(427);
					match(COMPARE);
					setState(428);
					match(BY);
					setState(429);
					ratioMetricExpression();
					}
				}

				setState(435);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MAX) {
					{
					setState(432);
					match(MAX);
					setState(433);
					match(COMBO);
					setState(434);
					((DiffQuerySpecificationContext)_localctx).maxCombo = match(INTEGER_VALUE);
					}
				}

				setState(439);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(437);
					match(WHERE);
					setState(438);
					((DiffQuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(441);
					match(ORDER);
					setState(442);
					match(BY);
					setState(443);
					sortItem();
					setState(448);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,71,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(444);
							match(T__1);
							setState(445);
							sortItem();
							}
							} 
						}
						setState(450);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,71,_ctx);
					}
					}
				}

				setState(455);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
				case 1:
					{
					setState(453);
					match(LIMIT);
					setState(454);
					((DiffQuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((DiffQuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(458);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(457);
					exportClause();
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(460);
				match(SELECT);
				setState(462);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
				case 1:
					{
					setState(461);
					setQuantifier();
					}
					break;
				}
				setState(464);
				selectItem();
				setState(469);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(465);
					match(T__1);
					setState(466);
					selectItem();
					}
					}
					setState(471);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(473);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTO) {
					{
					setState(472);
					exportClause();
					}
				}

				setState(475);
				match(FROM);
				setState(476);
				match(DIFF);
				setState(477);
				match(T__0);
				setState(478);
				splitQuery();
				setState(479);
				match(T__2);
				setState(480);
				match(ON);
				setState(483);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ADD:
				case ALL:
				case ANALYZE:
				case ANY:
				case ARRAY:
				case ASC:
				case AT:
				case BERNOULLI:
				case CALL:
				case CASCADE:
				case CATALOGS:
				case COALESCE:
				case COLUMN:
				case COLUMNS:
				case COMMENT:
				case COMMIT:
				case COMMITTED:
				case CURRENT:
				case DATA:
				case DATE:
				case DAY:
				case DESC:
				case DISTRIBUTED:
				case EXCLUDING:
				case EXPLAIN:
				case FILTER:
				case FIRST:
				case FOLLOWING:
				case FORMAT:
				case FUNCTIONS:
				case GRANT:
				case GRANTS:
				case GRAPHVIZ:
				case HOUR:
				case IF:
				case INCLUDING:
				case INPUT:
				case INTEGER:
				case INTERVAL:
				case ISOLATION:
				case LAST:
				case LATERAL:
				case LEVEL:
				case LIMIT:
				case LOGICAL:
				case MAP:
				case MINUTE:
				case MONTH:
				case NFC:
				case NFD:
				case NFKC:
				case NFKD:
				case NO:
				case NULLIF:
				case NULLS:
				case ONLY:
				case OPTION:
				case ORDINALITY:
				case OUTPUT:
				case OVER:
				case PARTITION:
				case PARTITIONS:
				case POSITION:
				case PRECEDING:
				case PRIVILEGES:
				case PROPERTIES:
				case PUBLIC:
				case RANGE:
				case READ:
				case RENAME:
				case REPEATABLE:
				case REPLACE:
				case RESET:
				case RESTRICT:
				case REVOKE:
				case ROLLBACK:
				case ROW:
				case ROWS:
				case SCHEMA:
				case SCHEMAS:
				case SECOND:
				case SESSION:
				case SET:
				case SETS:
				case SHOW:
				case SMALLINT:
				case SOME:
				case START:
				case STATS:
				case SUBSTRING:
				case SYSTEM:
				case TABLES:
				case TABLESAMPLE:
				case TEXT:
				case TIME:
				case TIMESTAMP:
				case TINYINT:
				case TO:
				case TRY_CAST:
				case TYPE:
				case UNBOUNDED:
				case UNCOMMITTED:
				case USE:
				case VALIDATE:
				case VERBOSE:
				case VIEW:
				case WORK:
				case WRITE:
				case YEAR:
				case ZONE:
				case IDENTIFIER:
				case DIGIT_IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(481);
					columnAliases();
					}
					break;
				case ASTERISK:
					{
					setState(482);
					match(ASTERISK);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(500);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(485);
					match(WITH);
					setState(498);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
					case 1:
						{
						setState(487);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(486);
							minRatioExpression();
							}
						}

						setState(490);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(489);
							minSupportExpression();
							}
						}

						}
						break;
					case 2:
						{
						setState(493);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN_SUPPORT) {
							{
							setState(492);
							minSupportExpression();
							}
						}

						setState(496);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MIN) {
							{
							setState(495);
							minRatioExpression();
							}
						}

						}
						break;
					}
					}
				}

				setState(505);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMPARE) {
					{
					setState(502);
					match(COMPARE);
					setState(503);
					match(BY);
					setState(504);
					ratioMetricExpression();
					}
				}

				setState(510);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MAX) {
					{
					setState(507);
					match(MAX);
					setState(508);
					match(COMBO);
					setState(509);
					((DiffQuerySpecificationContext)_localctx).maxCombo = match(INTEGER_VALUE);
					}
				}

				setState(514);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
				case 1:
					{
					setState(512);
					match(WHERE);
					setState(513);
					((DiffQuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(526);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(516);
					match(ORDER);
					setState(517);
					match(BY);
					setState(518);
					sortItem();
					setState(523);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(519);
							match(T__1);
							setState(520);
							sortItem();
							}
							} 
						}
						setState(525);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
					}
					}
				}

				setState(530);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
				case 1:
					{
					setState(528);
					match(LIMIT);
					setState(529);
					((DiffQuerySpecificationContext)_localctx).limit = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
						((DiffQuerySpecificationContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SplitQueryContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public TerminalNode SPLIT() { return getToken(SqlBaseParser.SPLIT, 0); }
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public SplitQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_splitQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSplitQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSplitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSplitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SplitQueryContext splitQuery() throws RecognitionException {
		SplitQueryContext _localctx = new SplitQueryContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_splitQuery);
		try {
			setState(544);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(534);
				match(SPLIT);
				setState(535);
				relation(0);
				setState(536);
				match(WHERE);
				setState(537);
				((SplitQueryContext)_localctx).where = booleanExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(539);
				match(SPLIT);
				setState(540);
				queryTerm();
				setState(541);
				match(WHERE);
				setState(542);
				((SplitQueryContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnDefinitionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public ColumnDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnDefinitionContext columnDefinition() throws RecognitionException {
		ColumnDefinitionContext _localctx = new ColumnDefinitionContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_columnDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(546);
			identifier();
			setState(547);
			type(0);
			setState(550);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(548);
				match(COMMENT);
				setState(549);
				string();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552);
			expression();
			setState(554);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				{
				setState(553);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			setState(558);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				{
				setState(556);
				match(NULLS);
				setState(557);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MinRatioExpressionContext extends ParserRuleContext {
		public Token minRatio;
		public TerminalNode MIN() { return getToken(SqlBaseParser.MIN, 0); }
		public TerminalNode RATIO() { return getToken(SqlBaseParser.RATIO, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public MinRatioExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minRatioExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMinRatioExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMinRatioExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMinRatioExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MinRatioExpressionContext minRatioExpression() throws RecognitionException {
		MinRatioExpressionContext _localctx = new MinRatioExpressionContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_minRatioExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560);
			match(MIN);
			setState(561);
			match(RATIO);
			setState(562);
			((MinRatioExpressionContext)_localctx).minRatio = match(DECIMAL_VALUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MinTrendDiffExpressionContext extends ParserRuleContext {
		public Token minTrendDiff;
		public TerminalNode MIN() { return getToken(SqlBaseParser.MIN, 0); }
		public TerminalNode TRENDDIFF() { return getToken(SqlBaseParser.TRENDDIFF, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public MinTrendDiffExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minTrendDiffExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMinTrendDiffExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMinTrendDiffExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMinTrendDiffExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MinTrendDiffExpressionContext minTrendDiffExpression() throws RecognitionException {
		MinTrendDiffExpressionContext _localctx = new MinTrendDiffExpressionContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_minTrendDiffExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(564);
			match(MIN);
			setState(565);
			match(TRENDDIFF);
			setState(566);
			((MinTrendDiffExpressionContext)_localctx).minTrendDiff = match(DECIMAL_VALUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MinSupportExpressionContext extends ParserRuleContext {
		public Token minSupport;
		public TerminalNode MIN_SUPPORT() { return getToken(SqlBaseParser.MIN_SUPPORT, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public MinSupportExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minSupportExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMinSupportExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMinSupportExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMinSupportExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MinSupportExpressionContext minSupportExpression() throws RecognitionException {
		MinSupportExpressionContext _localctx = new MinSupportExpressionContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_minSupportExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568);
			match(MIN_SUPPORT);
			setState(569);
			((MinSupportExpressionContext)_localctx).minSupport = match(DECIMAL_VALUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RatioMetricExpressionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public AggregateExpressionContext aggregateExpression() {
			return getRuleContext(AggregateExpressionContext.class,0);
		}
		public RatioMetricExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ratioMetricExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRatioMetricExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRatioMetricExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRatioMetricExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RatioMetricExpressionContext ratioMetricExpression() throws RecognitionException {
		RatioMetricExpressionContext _localctx = new RatioMetricExpressionContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_ratioMetricExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(571);
			identifier();
			setState(572);
			match(T__0);
			setState(573);
			aggregateExpression();
			setState(574);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregateExpressionContext extends ParserRuleContext {
		public AggregateContext aggregate() {
			return getRuleContext(AggregateContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public AggregateExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregateExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregateExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregateExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAggregateExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregateExpressionContext aggregateExpression() throws RecognitionException {
		AggregateExpressionContext _localctx = new AggregateExpressionContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_aggregateExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(576);
			aggregate();
			setState(577);
			match(T__0);
			setState(578);
			match(ASTERISK);
			setState(579);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregateContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(SqlBaseParser.COUNT, 0); }
		public TerminalNode MIN() { return getToken(SqlBaseParser.MIN, 0); }
		public TerminalNode MAX() { return getToken(SqlBaseParser.MAX, 0); }
		public TerminalNode SUM() { return getToken(SqlBaseParser.SUM, 0); }
		public AggregateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAggregate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregateContext aggregate() throws RecognitionException {
		AggregateContext _localctx = new AggregateContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_aggregate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(581);
			_la = _input.LA(1);
			if ( !(_la==COUNT || _la==MAX || _la==MIN || _la==SUM) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(583);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SelectAllContext extends SelectItemContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectAll(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectSingle(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectSingle(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_selectItem);
		int _la;
		try {
			setState(597);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(585);
				expression();
				setState(590);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
				case 1:
					{
					setState(587);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(586);
						match(AS);
						}
					}

					setState(589);
					identifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(592);
				qualifiedName();
				setState(593);
				match(T__3);
				setState(594);
				match(ASTERISK);
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(596);
				match(ASTERISK);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExportClauseContext extends ParserRuleContext {
		public Token filename;
		public Token fieldsFormat;
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode OUTFILE() { return getToken(SqlBaseParser.OUTFILE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public List<DelimiterClauseContext> delimiterClause() {
			return getRuleContexts(DelimiterClauseContext.class);
		}
		public DelimiterClauseContext delimiterClause(int i) {
			return getRuleContext(DelimiterClauseContext.class,i);
		}
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public ExportClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exportClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExportClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExportClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExportClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExportClauseContext exportClause() throws RecognitionException {
		ExportClauseContext _localctx = new ExportClauseContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_exportClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(599);
			match(INTO);
			setState(600);
			match(OUTFILE);
			setState(601);
			((ExportClauseContext)_localctx).filename = match(STRING);
			setState(604);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
			case 1:
				{
				setState(602);
				((ExportClauseContext)_localctx).fieldsFormat = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==COLUMNS || _la==FIELDS) ) {
					((ExportClauseContext)_localctx).fieldsFormat = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(603);
				delimiterClause();
				}
				break;
			}
			setState(608);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LINES) {
				{
				setState(606);
				match(LINES);
				setState(607);
				delimiterClause();
				}
			}

			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DelimiterClauseContext extends ParserRuleContext {
		public Token delimiter;
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public DelimiterClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delimiterClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDelimiterClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDelimiterClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDelimiterClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DelimiterClauseContext delimiterClause() throws RecognitionException {
		DelimiterClauseContext _localctx = new DelimiterClauseContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_delimiterClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(610);
			match(TERMINATED);
			setState(611);
			match(BY);
			setState(612);
			((DelimiterClauseContext)_localctx).delimiter = match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EscapeClauseContext extends ParserRuleContext {
		public Token escaping;
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public EscapeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_escapeClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEscapeClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEscapeClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEscapeClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EscapeClauseContext escapeClause() throws RecognitionException {
		EscapeClauseContext _localctx = new EscapeClauseContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_escapeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(614);
			match(ESCAPED);
			setState(615);
			match(BY);
			setState(616);
			((EscapeClauseContext)_localctx).escaping = match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RelationDefaultContext extends RelationContext {
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelationDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelationDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public AliasedRelationContext right;
		public RelationContext rightRelation;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 44;
		enterRecursionRule(_localctx, 44, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(619);
			aliasedRelation();
			}
			_ctx.stop = _input.LT(-1);
			setState(639);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,102,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(621);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(635);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case CROSS:
						{
						setState(622);
						match(CROSS);
						setState(623);
						match(JOIN);
						setState(624);
						((JoinRelationContext)_localctx).right = aliasedRelation();
						}
						break;
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case RIGHT:
						{
						setState(625);
						joinType();
						setState(626);
						match(JOIN);
						setState(627);
						((JoinRelationContext)_localctx).rightRelation = relation(0);
						setState(628);
						joinCriteria();
						}
						break;
					case NATURAL:
						{
						setState(630);
						match(NATURAL);
						setState(631);
						joinType();
						setState(632);
						match(JOIN);
						setState(633);
						((JoinRelationContext)_localctx).right = aliasedRelation();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(641);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,102,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_joinType);
		int _la;
		try {
			setState(657);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(643);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(642);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(645);
				match(LEFT);
				setState(647);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(646);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(649);
				match(RIGHT);
				setState(651);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(650);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(653);
				match(FULL);
				setState(655);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(654);
					match(OUTER);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_joinCriteria);
		int _la;
		try {
			setState(673);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(659);
				match(ON);
				setState(660);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(661);
				match(USING);
				setState(662);
				match(T__0);
				setState(663);
				identifier();
				setState(668);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(664);
					match(T__1);
					setState(665);
					identifier();
					}
					}
					setState(670);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(671);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(675);
			relationPrimary();
			setState(686);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				{
				setState(677);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(676);
					match(AS);
					}
				}

				setState(679);
				identifier();
				setState(684);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
				case 1:
					{
					setState(680);
					match(T__0);
					setState(681);
					columnAliases();
					setState(682);
					match(T__2);
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnAliases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnAliases(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_columnAliases);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(688);
			identifier();
			setState(693);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,113,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(689);
					match(T__1);
					setState(690);
					identifier();
					}
					} 
				}
				setState(695);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,113,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_relationPrimary);
		try {
			setState(705);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(696);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(697);
				match(T__0);
				setState(698);
				query();
				setState(699);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new ParenthesizedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(701);
				match(T__0);
				setState(702);
				relation(0);
				setState(703);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(707);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanDefaultContext extends BooleanExpressionContext {
		public PredicatedContext predicated() {
			return getRuleContext(PredicatedContext.class,0);
		}
		public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 58;
		enterRecursionRule(_localctx, 58, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(713);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case ADD:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CASCADE:
			case CATALOGS:
			case COALESCE:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DESC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case FALSE:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTS:
			case GRAPHVIZ:
			case HOUR:
			case IF:
			case INCLUDING:
			case INPUT:
			case INTEGER:
			case INTERVAL:
			case ISOLATION:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MINUTE:
			case MONTH:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NULL:
			case NULLIF:
			case NULLS:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case PUBLIC:
			case RANGE:
			case READ:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESTRICT:
			case REVOKE:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SMALLINT:
			case SOME:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TINYINT:
			case TO:
			case TRUE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case USE:
			case VALIDATE:
			case VERBOSE:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case PLUS:
			case MINUS:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case DOUBLE_PRECISION:
				{
				_localctx = new BooleanDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(710);
				predicated();
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(711);
				match(NOT);
				setState(712);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(723);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,117,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(721);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(715);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(716);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(717);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(718);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(719);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(720);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(725);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,117,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicatedContext extends ParserRuleContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicated; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicatedContext predicated() throws RecognitionException {
		PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_predicated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(726);
			((PredicatedContext)_localctx).valueExpression = valueExpression(0);
			setState(728);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				{
				setState(727);
				predicate(((PredicatedContext)_localctx).valueExpression);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDistinctFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDistinctFrom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInList(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBetween(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QuantifiedComparisonContext extends PredicateContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ComparisonQuantifierContext comparisonQuantifier() {
			return getRuleContext(ComparisonQuantifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuantifiedComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedComparison(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 62, RULE_predicate);
		int _la;
		try {
			setState(791);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(730);
				comparisonOperator();
				setState(731);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new QuantifiedComparisonContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(733);
				comparisonOperator();
				setState(734);
				comparisonQuantifier();
				setState(735);
				match(T__0);
				setState(736);
				query();
				setState(737);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(740);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(739);
					match(NOT);
					}
				}

				setState(742);
				match(BETWEEN);
				setState(743);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(744);
				match(AND);
				setState(745);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 4:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(748);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(747);
					match(NOT);
					}
				}

				setState(750);
				match(IN);
				setState(751);
				match(T__0);
				setState(752);
				expression();
				setState(757);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(753);
					match(T__1);
					setState(754);
					expression();
					}
					}
					setState(759);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(760);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(763);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(762);
					match(NOT);
					}
				}

				setState(765);
				match(IN);
				setState(766);
				match(T__0);
				setState(767);
				query();
				setState(768);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(771);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(770);
					match(NOT);
					}
				}

				setState(773);
				match(LIKE);
				setState(774);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(777);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,124,_ctx) ) {
				case 1:
					{
					setState(775);
					match(ESCAPE);
					setState(776);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 7:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(779);
				match(IS);
				setState(781);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(780);
					match(NOT);
					}
				}

				setState(783);
				match(NULL);
				}
				break;
			case 8:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(784);
				match(IS);
				setState(786);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(785);
					match(NOT);
					}
				}

				setState(788);
				match(DISTINCT);
				setState(789);
				match(FROM);
				setState(790);
				((DistinctFromContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(SqlBaseParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 64;
		enterRecursionRule(_localctx, 64, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(797);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case ADD:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CASCADE:
			case CATALOGS:
			case COALESCE:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DESC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case FALSE:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTS:
			case GRAPHVIZ:
			case HOUR:
			case IF:
			case INCLUDING:
			case INPUT:
			case INTEGER:
			case INTERVAL:
			case ISOLATION:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MINUTE:
			case MONTH:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NULL:
			case NULLIF:
			case NULLS:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case PUBLIC:
			case RANGE:
			case READ:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESTRICT:
			case REVOKE:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SMALLINT:
			case SOME:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TINYINT:
			case TO:
			case TRUE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case USE:
			case VALIDATE:
			case VERBOSE:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case DOUBLE_PRECISION:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(794);
				primaryExpression(0);
				}
				break;
			case PLUS:
			case MINUS:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(795);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(796);
				valueExpression(4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(810);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,130,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(808);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,129,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(799);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(800);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 207)) & ~0x3f) == 0 && ((1L << (_la - 207)) & ((1L << (ASTERISK - 207)) | (1L << (SLASH - 207)) | (1L << (PERCENT - 207)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(801);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(802);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(803);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(804);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(805);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(806);
						match(CONCAT);
						setState(807);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(812);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,130,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BinaryLiteralContext extends PrimaryExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(SqlBaseParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBinaryLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBinaryLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullLiteralContext extends PrimaryExpressionContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends PrimaryExpressionContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode DOUBLE_PRECISION() { return getToken(SqlBaseParser.DOUBLE_PRECISION, 0); }
		public TypeConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public FilterContext filter() {
			return getRuleContext(FilterContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends PrimaryExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends PrimaryExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(864);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				_localctx = new NullLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(814);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(815);
				identifier();
				setState(816);
				string();
				}
				break;
			case 3:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(818);
				match(DOUBLE_PRECISION);
				setState(819);
				string();
				}
				break;
			case 4:
				{
				_localctx = new NumericLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(820);
				number();
				}
				break;
			case 5:
				{
				_localctx = new BooleanLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(821);
				booleanValue();
				}
				break;
			case 6:
				{
				_localctx = new StringLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(822);
				string();
				}
				break;
			case 7:
				{
				_localctx = new BinaryLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(823);
				match(BINARY_LITERAL);
				}
				break;
			case 8:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(824);
				qualifiedName();
				setState(825);
				match(T__0);
				setState(826);
				match(ASTERISK);
				setState(827);
				match(T__2);
				setState(829);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
				case 1:
					{
					setState(828);
					filter();
					}
					break;
				}
				}
				break;
			case 9:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(831);
				qualifiedName();
				setState(832);
				match(T__0);
				setState(844);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTINCT) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXISTS) | (1L << EXPLAIN) | (1L << FALSE) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRUE - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)) | (1L << (PLUS - 195)) | (1L << (MINUS - 195)) | (1L << (STRING - 195)) | (1L << (UNICODE_STRING - 195)) | (1L << (BINARY_LITERAL - 195)) | (1L << (INTEGER_VALUE - 195)) | (1L << (DECIMAL_VALUE - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (DIGIT_IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)) | (1L << (DOUBLE_PRECISION - 195)))) != 0)) {
					{
					setState(834);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,132,_ctx) ) {
					case 1:
						{
						setState(833);
						setQuantifier();
						}
						break;
					}
					setState(836);
					expression();
					setState(841);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(837);
						match(T__1);
						setState(838);
						expression();
						}
						}
						setState(843);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(846);
				match(T__2);
				setState(848);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,135,_ctx) ) {
				case 1:
					{
					setState(847);
					filter();
					}
					break;
				}
				}
				break;
			case 10:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(850);
				match(T__0);
				setState(851);
				query();
				setState(852);
				match(T__2);
				}
				break;
			case 11:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(854);
				match(EXISTS);
				setState(855);
				match(T__0);
				setState(856);
				query();
				setState(857);
				match(T__2);
				}
				break;
			case 12:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(859);
				identifier();
				}
				break;
			case 13:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(860);
				match(T__0);
				setState(861);
				expression();
				setState(862);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(871);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,137,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
					((DereferenceContext)_localctx).base = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
					setState(866);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(867);
					match(T__3);
					setState(868);
					((DereferenceContext)_localctx).fieldName = identifier();
					}
					} 
				}
				setState(873);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,137,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class StringContext extends ParserRuleContext {
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	 
		public StringContext() { }
		public void copyFrom(StringContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class UnicodeStringLiteralContext extends StringContext {
		public TerminalNode UNICODE_STRING() { return getToken(SqlBaseParser.UNICODE_STRING, 0); }
		public TerminalNode UESCAPE() { return getToken(SqlBaseParser.UESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public UnicodeStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnicodeStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnicodeStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnicodeStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BasicStringLiteralContext extends StringContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public BasicStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBasicStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBasicStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBasicStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_string);
		try {
			setState(880);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				_localctx = new BasicStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(874);
				match(STRING);
				}
				break;
			case UNICODE_STRING:
				_localctx = new UnicodeStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(875);
				match(UNICODE_STRING);
				setState(878);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,138,_ctx) ) {
				case 1:
					{
					setState(876);
					match(UESCAPE);
					setState(877);
					match(STRING);
					}
					break;
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(882);
			_la = _input.LA(1);
			if ( !(((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & ((1L << (EQ - 199)) | (1L << (NEQ - 199)) | (1L << (LT - 199)) | (1L << (LTE - 199)) | (1L << (GT - 199)) | (1L << (GTE - 199)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonQuantifierContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public ComparisonQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonQuantifierContext comparisonQuantifier() throws RecognitionException {
		ComparisonQuantifierContext _localctx = new ComparisonQuantifierContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_comparisonQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(884);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(886);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeContext extends ParserRuleContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public BaseTypeContext baseType() {
			return getRuleContext(BaseTypeContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		return type(0);
	}

	private TypeContext type(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TypeContext _localctx = new TypeContext(_ctx, _parentState);
		TypeContext _prevctx = _localctx;
		int _startState = 76;
		enterRecursionRule(_localctx, 76, RULE_type, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(930);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				{
				setState(889);
				match(ARRAY);
				setState(890);
				match(LT);
				setState(891);
				type(0);
				setState(892);
				match(GT);
				}
				break;
			case 2:
				{
				setState(894);
				match(MAP);
				setState(895);
				match(LT);
				setState(896);
				type(0);
				setState(897);
				match(T__1);
				setState(898);
				type(0);
				setState(899);
				match(GT);
				}
				break;
			case 3:
				{
				setState(901);
				match(ROW);
				setState(902);
				match(T__0);
				setState(903);
				identifier();
				setState(904);
				type(0);
				setState(911);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(905);
					match(T__1);
					setState(906);
					identifier();
					setState(907);
					type(0);
					}
					}
					setState(913);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(914);
				match(T__2);
				}
				break;
			case 4:
				{
				setState(916);
				baseType();
				setState(928);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
				case 1:
					{
					setState(917);
					match(T__0);
					setState(918);
					typeParameter();
					setState(923);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(919);
						match(T__1);
						setState(920);
						typeParameter();
						}
						}
						setState(925);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(926);
					match(T__2);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(936);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new TypeContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_type);
					setState(932);
					if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
					setState(933);
					match(ARRAY);
					}
					} 
				}
				setState(938);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_typeParameter);
		try {
			setState(941);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(939);
				match(INTEGER_VALUE);
				}
				break;
			case ADD:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CASCADE:
			case CATALOGS:
			case COALESCE:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DESC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTS:
			case GRAPHVIZ:
			case HOUR:
			case IF:
			case INCLUDING:
			case INPUT:
			case INTEGER:
			case INTERVAL:
			case ISOLATION:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MINUTE:
			case MONTH:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NULLIF:
			case NULLS:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case PUBLIC:
			case RANGE:
			case READ:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESTRICT:
			case REVOKE:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SMALLINT:
			case SOME:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TINYINT:
			case TO:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case USE:
			case VALIDATE:
			case VERBOSE:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
			case TIME_WITH_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
			case DOUBLE_PRECISION:
				enterOuterAlt(_localctx, 2);
				{
				setState(940);
				type(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BaseTypeContext extends ParserRuleContext {
		public TerminalNode TIME_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIME_WITH_TIME_ZONE, 0); }
		public TerminalNode TIMESTAMP_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIMESTAMP_WITH_TIME_ZONE, 0); }
		public TerminalNode DOUBLE_PRECISION() { return getToken(SqlBaseParser.DOUBLE_PRECISION, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public BaseTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_baseType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBaseType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBaseType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBaseType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BaseTypeContext baseType() throws RecognitionException {
		BaseTypeContext _localctx = new BaseTypeContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_baseType);
		try {
			setState(947);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TIME_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(943);
				match(TIME_WITH_TIME_ZONE);
				}
				break;
			case TIMESTAMP_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 2);
				{
				setState(944);
				match(TIMESTAMP_WITH_TIME_ZONE);
				}
				break;
			case DOUBLE_PRECISION:
				enterOuterAlt(_localctx, 3);
				{
				setState(945);
				match(DOUBLE_PRECISION);
				}
				break;
			case ADD:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CASCADE:
			case CATALOGS:
			case COALESCE:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DESC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTS:
			case GRAPHVIZ:
			case HOUR:
			case IF:
			case INCLUDING:
			case INPUT:
			case INTEGER:
			case INTERVAL:
			case ISOLATION:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MINUTE:
			case MONTH:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NULLIF:
			case NULLS:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case PUBLIC:
			case RANGE:
			case READ:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESTRICT:
			case REVOKE:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SMALLINT:
			case SOME:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TINYINT:
			case TO:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case USE:
			case VALIDATE:
			case VERBOSE:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 4);
				{
				setState(946);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(949);
			match(WHEN);
			setState(950);
			((WhenClauseContext)_localctx).condition = expression();
			setState(951);
			match(THEN);
			setState(952);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FilterContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public FilterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFilter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFilter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterContext filter() throws RecognitionException {
		FilterContext _localctx = new FilterContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_filter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(954);
			match(FILTER);
			setState(955);
			match(T__0);
			setState(956);
			match(WHERE);
			setState(957);
			booleanExpression(0);
			setState(958);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(960);
			identifier();
			setState(965);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,147,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(961);
					match(T__3);
					setState(962);
					identifier();
					}
					} 
				}
				setState(967);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,147,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_identifier);
		try {
			setState(972);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(968);
				match(IDENTIFIER);
				}
				break;
			case ADD:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case BERNOULLI:
			case CALL:
			case CASCADE:
			case CATALOGS:
			case COALESCE:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DESC:
			case DISTRIBUTED:
			case EXCLUDING:
			case EXPLAIN:
			case FILTER:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTS:
			case GRAPHVIZ:
			case HOUR:
			case IF:
			case INCLUDING:
			case INPUT:
			case INTEGER:
			case INTERVAL:
			case ISOLATION:
			case LAST:
			case LATERAL:
			case LEVEL:
			case LIMIT:
			case LOGICAL:
			case MAP:
			case MINUTE:
			case MONTH:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NULLIF:
			case NULLS:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case PARTITION:
			case PARTITIONS:
			case POSITION:
			case PRECEDING:
			case PRIVILEGES:
			case PROPERTIES:
			case PUBLIC:
			case RANGE:
			case READ:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESTRICT:
			case REVOKE:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SMALLINT:
			case SOME:
			case START:
			case STATS:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TIME:
			case TIMESTAMP:
			case TINYINT:
			case TO:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case USE:
			case VALIDATE:
			case VERBOSE:
			case VIEW:
			case WORK:
			case WRITE:
			case YEAR:
			case ZONE:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(969);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(970);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(971);
				match(DIGIT_IDENTIFIER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_number);
		try {
			setState(976);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DECIMAL_VALUE:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(974);
				match(DECIMAL_VALUE);
				}
				break;
			case INTEGER_VALUE:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(975);
				match(INTEGER_VALUE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode COALESCE() { return getToken(SqlBaseParser.COALESCE, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATE() { return getToken(SqlBaseParser.DATE, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public TerminalNode INTEGER() { return getToken(SqlBaseParser.INTEGER, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NULLIF() { return getToken(SqlBaseParser.NULLIF, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PUBLIC() { return getToken(SqlBaseParser.PUBLIC, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SMALLINT() { return getToken(SqlBaseParser.SMALLINT, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TINYINT() { return getToken(SqlBaseParser.TINYINT, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(978);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALL) | (1L << ANALYZE) | (1L << ANY) | (1L << ARRAY) | (1L << ASC) | (1L << AT) | (1L << BERNOULLI) | (1L << CALL) | (1L << CASCADE) | (1L << CATALOGS) | (1L << COALESCE) | (1L << COLUMN) | (1L << COLUMNS) | (1L << COMMENT) | (1L << COMMIT) | (1L << COMMITTED) | (1L << CURRENT) | (1L << DATA) | (1L << DATE) | (1L << DAY) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << EXCLUDING) | (1L << EXPLAIN) | (1L << FILTER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (FIRST - 64)) | (1L << (FOLLOWING - 64)) | (1L << (FORMAT - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (GRANT - 64)) | (1L << (GRANTS - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (HOUR - 64)) | (1L << (IF - 64)) | (1L << (INCLUDING - 64)) | (1L << (INPUT - 64)) | (1L << (INTEGER - 64)) | (1L << (INTERVAL - 64)) | (1L << (ISOLATION - 64)) | (1L << (LAST - 64)) | (1L << (LATERAL - 64)) | (1L << (LEVEL - 64)) | (1L << (LIMIT - 64)) | (1L << (LOGICAL - 64)) | (1L << (MAP - 64)) | (1L << (MINUTE - 64)) | (1L << (MONTH - 64)) | (1L << (NFC - 64)) | (1L << (NFD - 64)) | (1L << (NFKC - 64)) | (1L << (NFKD - 64)) | (1L << (NO - 64)) | (1L << (NULLIF - 64)) | (1L << (NULLS - 64)) | (1L << (ONLY - 64)) | (1L << (OPTION - 64)) | (1L << (ORDINALITY - 64)) | (1L << (OUTPUT - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (PARTITIONS - 128)) | (1L << (POSITION - 128)) | (1L << (PRECEDING - 128)) | (1L << (PRIVILEGES - 128)) | (1L << (PROPERTIES - 128)) | (1L << (PUBLIC - 128)) | (1L << (RANGE - 128)) | (1L << (READ - 128)) | (1L << (RENAME - 128)) | (1L << (REPEATABLE - 128)) | (1L << (REPLACE - 128)) | (1L << (RESET - 128)) | (1L << (RESTRICT - 128)) | (1L << (REVOKE - 128)) | (1L << (ROLLBACK - 128)) | (1L << (ROW - 128)) | (1L << (ROWS - 128)) | (1L << (SCHEMA - 128)) | (1L << (SCHEMAS - 128)) | (1L << (SECOND - 128)) | (1L << (SESSION - 128)) | (1L << (SET - 128)) | (1L << (SETS - 128)) | (1L << (SHOW - 128)) | (1L << (SMALLINT - 128)) | (1L << (SOME - 128)) | (1L << (START - 128)) | (1L << (STATS - 128)) | (1L << (SUBSTRING - 128)) | (1L << (SYSTEM - 128)) | (1L << (TABLES - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (TEXT - 128)) | (1L << (TIME - 128)) | (1L << (TIMESTAMP - 128)) | (1L << (TINYINT - 128)) | (1L << (TO - 128)) | (1L << (TRY_CAST - 128)) | (1L << (TYPE - 128)) | (1L << (UNBOUNDED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (USE - 128)) | (1L << (VALIDATE - 128)) | (1L << (VERBOSE - 128)) | (1L << (VIEW - 128)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (WORK - 195)) | (1L << (WRITE - 195)) | (1L << (YEAR - 195)) | (1L << (ZONE - 195)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 22:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 29:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 32:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 33:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 38:
			return type_sempred((TypeContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 2);
		case 2:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 3);
		case 4:
			return precpred(_ctx, 2);
		case 5:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 6:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean type_sempred(TypeContext _localctx, int predIndex) {
		switch (predIndex) {
		case 7:
			return precpred(_ctx, 5);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u00e5\u03d7\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4s\n\4\f\4\16\4v\13\4\3\4\3\4\5\4"+
		"z\n\4\5\4|\n\4\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u008a"+
		"\n\7\3\b\3\b\5\b\u008e\n\b\3\b\3\b\3\b\7\b\u0093\n\b\f\b\16\b\u0096\13"+
		"\b\3\b\3\b\3\b\3\b\7\b\u009c\n\b\f\b\16\b\u009f\13\b\5\b\u00a1\n\b\3\b"+
		"\3\b\5\b\u00a5\n\b\3\b\3\b\3\b\3\b\3\b\7\b\u00ac\n\b\f\b\16\b\u00af\13"+
		"\b\5\b\u00b1\n\b\3\b\3\b\5\b\u00b5\n\b\3\b\5\b\u00b8\n\b\3\b\3\b\5\b\u00bc"+
		"\n\b\3\b\3\b\3\b\7\b\u00c1\n\b\f\b\16\b\u00c4\13\b\3\b\5\b\u00c7\n\b\3"+
		"\b\3\b\3\b\3\b\7\b\u00cd\n\b\f\b\16\b\u00d0\13\b\5\b\u00d2\n\b\3\b\3\b"+
		"\5\b\u00d6\n\b\3\b\3\b\3\b\3\b\3\b\7\b\u00dd\n\b\f\b\16\b\u00e0\13\b\5"+
		"\b\u00e2\n\b\3\b\3\b\5\b\u00e6\n\b\5\b\u00e8\n\b\3\t\3\t\5\t\u00ec\n\t"+
		"\3\t\3\t\3\t\7\t\u00f1\n\t\f\t\16\t\u00f4\13\t\3\t\3\t\3\t\3\t\5\t\u00fa"+
		"\n\t\3\t\3\t\3\t\5\t\u00ff\n\t\3\t\3\t\3\t\5\t\u0104\n\t\3\t\3\t\5\t\u0108"+
		"\n\t\3\t\5\t\u010b\n\t\3\t\5\t\u010e\n\t\3\t\5\t\u0111\n\t\3\t\5\t\u0114"+
		"\n\t\5\t\u0116\n\t\3\t\3\t\3\t\5\t\u011b\n\t\3\t\3\t\3\t\5\t\u0120\n\t"+
		"\3\t\3\t\5\t\u0124\n\t\3\t\3\t\3\t\3\t\3\t\7\t\u012b\n\t\f\t\16\t\u012e"+
		"\13\t\5\t\u0130\n\t\3\t\3\t\5\t\u0134\n\t\3\t\5\t\u0137\n\t\3\t\3\t\5"+
		"\t\u013b\n\t\3\t\3\t\3\t\7\t\u0140\n\t\f\t\16\t\u0143\13\t\3\t\5\t\u0146"+
		"\n\t\3\t\3\t\3\t\3\t\5\t\u014c\n\t\3\t\3\t\3\t\5\t\u0151\n\t\3\t\3\t\3"+
		"\t\5\t\u0156\n\t\3\t\3\t\5\t\u015a\n\t\3\t\5\t\u015d\n\t\3\t\5\t\u0160"+
		"\n\t\3\t\5\t\u0163\n\t\5\t\u0165\n\t\5\t\u0167\n\t\3\t\3\t\3\t\5\t\u016c"+
		"\n\t\3\t\3\t\3\t\5\t\u0171\n\t\3\t\3\t\5\t\u0175\n\t\3\t\3\t\3\t\3\t\3"+
		"\t\7\t\u017c\n\t\f\t\16\t\u017f\13\t\5\t\u0181\n\t\3\t\3\t\5\t\u0185\n"+
		"\t\3\t\3\t\5\t\u0189\n\t\3\t\3\t\3\t\7\t\u018e\n\t\f\t\16\t\u0191\13\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u019b\n\t\3\t\3\t\5\t\u019f\n\t\3"+
		"\t\5\t\u01a2\n\t\3\t\5\t\u01a5\n\t\3\t\5\t\u01a8\n\t\5\t\u01aa\n\t\5\t"+
		"\u01ac\n\t\3\t\3\t\3\t\5\t\u01b1\n\t\3\t\3\t\3\t\5\t\u01b6\n\t\3\t\3\t"+
		"\5\t\u01ba\n\t\3\t\3\t\3\t\3\t\3\t\7\t\u01c1\n\t\f\t\16\t\u01c4\13\t\5"+
		"\t\u01c6\n\t\3\t\3\t\5\t\u01ca\n\t\3\t\5\t\u01cd\n\t\3\t\3\t\5\t\u01d1"+
		"\n\t\3\t\3\t\3\t\7\t\u01d6\n\t\f\t\16\t\u01d9\13\t\3\t\5\t\u01dc\n\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01e6\n\t\3\t\3\t\5\t\u01ea\n\t\3\t"+
		"\5\t\u01ed\n\t\3\t\5\t\u01f0\n\t\3\t\5\t\u01f3\n\t\5\t\u01f5\n\t\5\t\u01f7"+
		"\n\t\3\t\3\t\3\t\5\t\u01fc\n\t\3\t\3\t\3\t\5\t\u0201\n\t\3\t\3\t\5\t\u0205"+
		"\n\t\3\t\3\t\3\t\3\t\3\t\7\t\u020c\n\t\f\t\16\t\u020f\13\t\5\t\u0211\n"+
		"\t\3\t\3\t\5\t\u0215\n\t\5\t\u0217\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\5\n\u0223\n\n\3\13\3\13\3\13\3\13\5\13\u0229\n\13\3\f\3\f\5\f"+
		"\u022d\n\f\3\f\3\f\5\f\u0231\n\f\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3"+
		"\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\22\3"+
		"\22\3\23\3\23\3\24\3\24\5\24\u024e\n\24\3\24\5\24\u0251\n\24\3\24\3\24"+
		"\3\24\3\24\3\24\5\24\u0258\n\24\3\25\3\25\3\25\3\25\3\25\5\25\u025f\n"+
		"\25\3\25\3\25\5\25\u0263\n\25\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27"+
		"\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
		"\3\30\3\30\3\30\5\30\u027e\n\30\7\30\u0280\n\30\f\30\16\30\u0283\13\30"+
		"\3\31\5\31\u0286\n\31\3\31\3\31\5\31\u028a\n\31\3\31\3\31\5\31\u028e\n"+
		"\31\3\31\3\31\5\31\u0292\n\31\5\31\u0294\n\31\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\7\32\u029d\n\32\f\32\16\32\u02a0\13\32\3\32\3\32\5\32\u02a4"+
		"\n\32\3\33\3\33\5\33\u02a8\n\33\3\33\3\33\3\33\3\33\3\33\5\33\u02af\n"+
		"\33\5\33\u02b1\n\33\3\34\3\34\3\34\7\34\u02b6\n\34\f\34\16\34\u02b9\13"+
		"\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u02c4\n\35\3\36"+
		"\3\36\3\37\3\37\3\37\3\37\5\37\u02cc\n\37\3\37\3\37\3\37\3\37\3\37\3\37"+
		"\7\37\u02d4\n\37\f\37\16\37\u02d7\13\37\3 \3 \5 \u02db\n \3!\3!\3!\3!"+
		"\3!\3!\3!\3!\3!\3!\5!\u02e7\n!\3!\3!\3!\3!\3!\3!\5!\u02ef\n!\3!\3!\3!"+
		"\3!\3!\7!\u02f6\n!\f!\16!\u02f9\13!\3!\3!\3!\5!\u02fe\n!\3!\3!\3!\3!\3"+
		"!\3!\5!\u0306\n!\3!\3!\3!\3!\5!\u030c\n!\3!\3!\5!\u0310\n!\3!\3!\3!\5"+
		"!\u0315\n!\3!\3!\3!\5!\u031a\n!\3\"\3\"\3\"\3\"\5\"\u0320\n\"\3\"\3\""+
		"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\7\"\u032b\n\"\f\"\16\"\u032e\13\"\3#\3#\3"+
		"#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u0340\n#\3#\3#\3#\5#\u0345"+
		"\n#\3#\3#\3#\7#\u034a\n#\f#\16#\u034d\13#\5#\u034f\n#\3#\3#\5#\u0353\n"+
		"#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u0363\n#\3#\3#\3#\7#\u0368"+
		"\n#\f#\16#\u036b\13#\3$\3$\3$\3$\5$\u0371\n$\5$\u0373\n$\3%\3%\3&\3&\3"+
		"\'\3\'\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3("+
		"\7(\u0390\n(\f(\16(\u0393\13(\3(\3(\3(\3(\3(\3(\3(\7(\u039c\n(\f(\16("+
		"\u039f\13(\3(\3(\5(\u03a3\n(\5(\u03a5\n(\3(\3(\7(\u03a9\n(\f(\16(\u03ac"+
		"\13(\3)\3)\5)\u03b0\n)\3*\3*\3*\3*\5*\u03b6\n*\3+\3+\3+\3+\3+\3,\3,\3"+
		",\3,\3,\3,\3-\3-\3-\7-\u03c6\n-\f-\16-\u03c9\13-\3.\3.\3.\3.\5.\u03cf"+
		"\n.\3/\3/\5/\u03d3\n/\3\60\3\60\3\60\2\7.<BDN\61\2\4\6\b\n\f\16\20\22"+
		"\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^\2\16\4\2"+
		"\b\b\u00d8\u00d8\4\2\17\17--\4\2BB^^\5\2\"\"gh\u00a8\u00a8\4\2\b\b\60"+
		"\60\4\2\33\33??\3\2\u00cf\u00d0\3\2\u00d1\u00d3\3\2\u00c9\u00ce\5\2\b"+
		"\b\f\f\u00a2\u00a2\4\2>>\u00b4\u00b4\63\2\7\b\n\n\f\r\17\21\24\25\30\33"+
		"\35\37\'*--\61\6199<<ACEEHKOPSSUUWWYY\\\\^_aaccefjkmqtuwx||\177\u0084"+
		"\u0086\u0089\u008c\u008c\u008e\u0093\u0095\u0095\u0097\u009b\u009d\u00a2"+
		"\u00a4\u00a4\u00a6\u00a7\u00a9\u00a9\u00ab\u00ac\u00ae\u00ae\u00b0\u00b3"+
		"\u00b5\u00b6\u00b8\u00b9\u00bc\u00bc\u00be\u00be\u00c0\u00c1\u00c5\u00c8"+
		"\2\u045f\2`\3\2\2\2\4c\3\2\2\2\6{\3\2\2\2\b}\3\2\2\2\n\177\3\2\2\2\f\u0089"+
		"\3\2\2\2\16\u00e7\3\2\2\2\20\u0216\3\2\2\2\22\u0222\3\2\2\2\24\u0224\3"+
		"\2\2\2\26\u022a\3\2\2\2\30\u0232\3\2\2\2\32\u0236\3\2\2\2\34\u023a\3\2"+
		"\2\2\36\u023d\3\2\2\2 \u0242\3\2\2\2\"\u0247\3\2\2\2$\u0249\3\2\2\2&\u0257"+
		"\3\2\2\2(\u0259\3\2\2\2*\u0264\3\2\2\2,\u0268\3\2\2\2.\u026c\3\2\2\2\60"+
		"\u0293\3\2\2\2\62\u02a3\3\2\2\2\64\u02a5\3\2\2\2\66\u02b2\3\2\2\28\u02c3"+
		"\3\2\2\2:\u02c5\3\2\2\2<\u02cb\3\2\2\2>\u02d8\3\2\2\2@\u0319\3\2\2\2B"+
		"\u031f\3\2\2\2D\u0362\3\2\2\2F\u0372\3\2\2\2H\u0374\3\2\2\2J\u0376\3\2"+
		"\2\2L\u0378\3\2\2\2N\u03a4\3\2\2\2P\u03af\3\2\2\2R\u03b5\3\2\2\2T\u03b7"+
		"\3\2\2\2V\u03bc\3\2\2\2X\u03c2\3\2\2\2Z\u03ce\3\2\2\2\\\u03d2\3\2\2\2"+
		"^\u03d4\3\2\2\2`a\5\6\4\2ab\7\2\2\3b\3\3\2\2\2cd\5:\36\2de\7\2\2\3e\5"+
		"\3\2\2\2f|\5\b\5\2gh\7Q\2\2hi\7F\2\2ij\7$\2\2jk\7@\2\2kl\7\u00d5\2\2l"+
		"m\7Z\2\2my\5X-\2no\7\3\2\2ot\5\24\13\2pq\7\4\2\2qs\5\24\13\2rp\3\2\2\2"+
		"sv\3\2\2\2tr\3\2\2\2tu\3\2\2\2uw\3\2\2\2vt\3\2\2\2wx\7\5\2\2xz\3\2\2\2"+
		"yn\3\2\2\2yz\3\2\2\2z|\3\2\2\2{f\3\2\2\2{g\3\2\2\2|\7\3\2\2\2}~\5\n\6"+
		"\2~\t\3\2\2\2\177\u0080\5\f\7\2\u0080\13\3\2\2\2\u0081\u008a\5\16\b\2"+
		"\u0082\u008a\5\20\t\2\u0083\u0084\7\u00aa\2\2\u0084\u008a\5X-\2\u0085"+
		"\u0086\7\3\2\2\u0086\u0087\5\b\5\2\u0087\u0088\7\5\2\2\u0088\u008a\3\2"+
		"\2\2\u0089\u0081\3\2\2\2\u0089\u0082\3\2\2\2\u0089\u0083\3\2\2\2\u0089"+
		"\u0085\3\2\2\2\u008a\r\3\2\2\2\u008b\u008d\7\u009c\2\2\u008c\u008e\5$"+
		"\23\2\u008d\u008c\3\2\2\2\u008d\u008e\3\2\2\2\u008e\u008f\3\2\2\2\u008f"+
		"\u0094\5&\24\2\u0090\u0091\7\4\2\2\u0091\u0093\5&\24\2\u0092\u0090\3\2"+
		"\2\2\u0093\u0096\3\2\2\2\u0094\u0092\3\2\2\2\u0094\u0095\3\2\2\2\u0095"+
		"\u00a0\3\2\2\2\u0096\u0094\3\2\2\2\u0097\u0098\7F\2\2\u0098\u009d\5.\30"+
		"\2\u0099\u009a\7\4\2\2\u009a\u009c\5.\30\2\u009b\u0099\3\2\2\2\u009c\u009f"+
		"\3\2\2\2\u009d\u009b\3\2\2\2\u009d\u009e\3\2\2\2\u009e\u00a1\3\2\2\2\u009f"+
		"\u009d\3\2\2\2\u00a0\u0097\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1\u00a4\3\2"+
		"\2\2\u00a2\u00a3\7\u00c3\2\2\u00a3\u00a5\5<\37\2\u00a4\u00a2\3\2\2\2\u00a4"+
		"\u00a5\3\2\2\2\u00a5\u00b0\3\2\2\2\u00a6\u00a7\7{\2\2\u00a7\u00a8\7\23"+
		"\2\2\u00a8\u00ad\5\26\f\2\u00a9\u00aa\7\4\2\2\u00aa\u00ac\5\26\f\2\u00ab"+
		"\u00a9\3\2\2\2\u00ac\u00af\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ae\3\2"+
		"\2\2\u00ae\u00b1\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\u00a6\3\2\2\2\u00b0"+
		"\u00b1\3\2\2\2\u00b1\u00b4\3\2\2\2\u00b2\u00b3\7c\2\2\u00b3\u00b5\t\2"+
		"\2\2\u00b4\u00b2\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b7\3\2\2\2\u00b6"+
		"\u00b8\5(\25\2\u00b7\u00b6\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00e8\3\2"+
		"\2\2\u00b9\u00bb\7\u009c\2\2\u00ba\u00bc\5$\23\2\u00bb\u00ba\3\2\2\2\u00bb"+
		"\u00bc\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd\u00c2\5&\24\2\u00be\u00bf\7\4"+
		"\2\2\u00bf\u00c1\5&\24\2\u00c0\u00be\3\2\2\2\u00c1\u00c4\3\2\2\2\u00c2"+
		"\u00c0\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2"+
		"\2\2\u00c5\u00c7\5(\25\2\u00c6\u00c5\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7"+
		"\u00d1\3\2\2\2\u00c8\u00c9\7F\2\2\u00c9\u00ce\5.\30\2\u00ca\u00cb\7\4"+
		"\2\2\u00cb\u00cd\5.\30\2\u00cc\u00ca\3\2\2\2\u00cd\u00d0\3\2\2\2\u00ce"+
		"\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d2\3\2\2\2\u00d0\u00ce\3\2"+
		"\2\2\u00d1\u00c8\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d5\3\2\2\2\u00d3"+
		"\u00d4\7\u00c3\2\2\u00d4\u00d6\5<\37\2\u00d5\u00d3\3\2\2\2\u00d5\u00d6"+
		"\3\2\2\2\u00d6\u00e1\3\2\2\2\u00d7\u00d8\7{\2\2\u00d8\u00d9\7\23\2\2\u00d9"+
		"\u00de\5\26\f\2\u00da\u00db\7\4\2\2\u00db\u00dd\5\26\f\2\u00dc\u00da\3"+
		"\2\2\2\u00dd\u00e0\3\2\2\2\u00de\u00dc\3\2\2\2\u00de\u00df\3\2\2\2\u00df"+
		"\u00e2\3\2\2\2\u00e0\u00de\3\2\2\2\u00e1\u00d7\3\2\2\2\u00e1\u00e2\3\2"+
		"\2\2\u00e2\u00e5\3\2\2\2\u00e3\u00e4\7c\2\2\u00e4\u00e6\t\2\2\2\u00e5"+
		"\u00e3\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00e8\3\2\2\2\u00e7\u008b\3\2"+
		"\2\2\u00e7\u00b9\3\2\2\2\u00e8\17\3\2\2\2\u00e9\u00eb\7\u009c\2\2\u00ea"+
		"\u00ec\5$\23\2\u00eb\u00ea\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ed\3\2"+
		"\2\2\u00ed\u00f2\5&\24\2\u00ee\u00ef\7\4\2\2\u00ef\u00f1\5&\24\2\u00f0"+
		"\u00ee\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3\2"+
		"\2\2\u00f3\u00f5\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f5\u00f6\7F\2\2\u00f6"+
		"\u00f7\7/\2\2\u00f7\u00f9\5\n\6\2\u00f8\u00fa\5X-\2\u00f9\u00f8\3\2\2"+
		"\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00fc\7\4\2\2\u00fc\u00fe"+
		"\5\n\6\2\u00fd\u00ff\5X-\2\u00fe\u00fd\3\2\2\2\u00fe\u00ff\3\2\2\2\u00ff"+
		"\u0100\3\2\2\2\u0100\u0103\7v\2\2\u0101\u0104\5\66\34\2\u0102\u0104\7"+
		"\u00d1\2\2\u0103\u0101\3\2\2\2\u0103\u0102\3\2\2\2\u0104\u0115\3\2\2\2"+
		"\u0105\u0113\7\u00c4\2\2\u0106\u0108\5\30\r\2\u0107\u0106\3\2\2\2\u0107"+
		"\u0108\3\2\2\2\u0108\u010a\3\2\2\2\u0109\u010b\5\34\17\2\u010a\u0109\3"+
		"\2\2\2\u010a\u010b\3\2\2\2\u010b\u0114\3\2\2\2\u010c\u010e\5\34\17\2\u010d"+
		"\u010c\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110\3\2\2\2\u010f\u0111\5\30"+
		"\r\2\u0110\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0114\3\2\2\2\u0112"+
		"\u0114\5\32\16\2\u0113\u0107\3\2\2\2\u0113\u010d\3\2\2\2\u0113\u0112\3"+
		"\2\2\2\u0114\u0116\3\2\2\2\u0115\u0105\3\2\2\2\u0115\u0116\3\2\2\2\u0116"+
		"\u011a\3\2\2\2\u0117\u0118\7 \2\2\u0118\u0119\7\23\2\2\u0119\u011b\5\36"+
		"\20\2\u011a\u0117\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u011f\3\2\2\2\u011c"+
		"\u011d\7g\2\2\u011d\u011e\7\34\2\2\u011e\u0120\7\u00d8\2\2\u011f\u011c"+
		"\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u0123\3\2\2\2\u0121\u0122\7\u00c3\2"+
		"\2\u0122\u0124\5<\37\2\u0123\u0121\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u012f"+
		"\3\2\2\2\u0125\u0126\7{\2\2\u0126\u0127\7\23\2\2\u0127\u012c\5\26\f\2"+
		"\u0128\u0129\7\4\2\2\u0129\u012b\5\26\f\2\u012a\u0128\3\2\2\2\u012b\u012e"+
		"\3\2\2\2\u012c\u012a\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u0130\3\2\2\2\u012e"+
		"\u012c\3\2\2\2\u012f\u0125\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u0133\3\2"+
		"\2\2\u0131\u0132\7c\2\2\u0132\u0134\t\2\2\2\u0133\u0131\3\2\2\2\u0133"+
		"\u0134\3\2\2\2\u0134\u0136\3\2\2\2\u0135\u0137\5(\25\2\u0136\u0135\3\2"+
		"\2\2\u0136\u0137\3\2\2\2\u0137\u0217\3\2\2\2\u0138\u013a\7\u009c\2\2\u0139"+
		"\u013b\5$\23\2\u013a\u0139\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u013c\3\2"+
		"\2\2\u013c\u0141\5&\24\2\u013d\u013e\7\4\2\2\u013e\u0140\5&\24\2\u013f"+
		"\u013d\3\2\2\2\u0140\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0141\u0142\3\2"+
		"\2\2\u0142\u0145\3\2\2\2\u0143\u0141\3\2\2\2\u0144\u0146\5(\25\2\u0145"+
		"\u0144\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0148\7F"+
		"\2\2\u0148\u0149\7/\2\2\u0149\u014b\5\n\6\2\u014a\u014c\5X-\2\u014b\u014a"+
		"\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014d\3\2\2\2\u014d\u014e\7\4\2\2\u014e"+
		"\u0150\5\n\6\2\u014f\u0151\5X-\2\u0150\u014f\3\2\2\2\u0150\u0151\3\2\2"+
		"\2\u0151\u0152\3\2\2\2\u0152\u0155\7v\2\2\u0153\u0156\5\66\34\2\u0154"+
		"\u0156\7\u00d1\2\2\u0155\u0153\3\2\2\2\u0155\u0154\3\2\2\2\u0156\u0166"+
		"\3\2\2\2\u0157\u0164\7\u00c4\2\2\u0158\u015a\5\30\r\2\u0159\u0158\3\2"+
		"\2\2\u0159\u015a\3\2\2\2\u015a\u015c\3\2\2\2\u015b\u015d\5\34\17\2\u015c"+
		"\u015b\3\2\2\2\u015c\u015d\3\2\2\2\u015d\u0165\3\2\2\2\u015e\u0160\5\34"+
		"\17\2\u015f\u015e\3\2\2\2\u015f\u0160\3\2\2\2\u0160\u0162\3\2\2\2\u0161"+
		"\u0163\5\30\r\2\u0162\u0161\3\2\2\2\u0162\u0163\3\2\2\2\u0163\u0165\3"+
		"\2\2\2\u0164\u0159\3\2\2\2\u0164\u015f\3\2\2\2\u0165\u0167\3\2\2\2\u0166"+
		"\u0157\3\2\2\2\u0166\u0167\3\2\2\2\u0167\u016b\3\2\2\2\u0168\u0169\7 "+
		"\2\2\u0169\u016a\7\23\2\2\u016a\u016c\5\36\20\2\u016b\u0168\3\2\2\2\u016b"+
		"\u016c\3\2\2\2\u016c\u0170\3\2\2\2\u016d\u016e\7g\2\2\u016e\u016f\7\34"+
		"\2\2\u016f\u0171\7\u00d8\2\2\u0170\u016d\3\2\2\2\u0170\u0171\3\2\2\2\u0171"+
		"\u0174\3\2\2\2\u0172\u0173\7\u00c3\2\2\u0173\u0175\5<\37\2\u0174\u0172"+
		"\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0180\3\2\2\2\u0176\u0177\7{\2\2\u0177"+
		"\u0178\7\23\2\2\u0178\u017d\5\26\f\2\u0179\u017a\7\4\2\2\u017a\u017c\5"+
		"\26\f\2\u017b\u0179\3\2\2\2\u017c\u017f\3\2\2\2\u017d\u017b\3\2\2\2\u017d"+
		"\u017e\3\2\2\2\u017e\u0181\3\2\2\2\u017f\u017d\3\2\2\2\u0180\u0176\3\2"+
		"\2\2\u0180\u0181\3\2\2\2\u0181\u0184\3\2\2\2\u0182\u0183\7c\2\2\u0183"+
		"\u0185\t\2\2\2\u0184\u0182\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0217\3\2"+
		"\2\2\u0186\u0188\7\u009c\2\2\u0187\u0189\5$\23\2\u0188\u0187\3\2\2\2\u0188"+
		"\u0189\3\2\2\2\u0189\u018a\3\2\2\2\u018a\u018f\5&\24\2\u018b\u018c\7\4"+
		"\2\2\u018c\u018e\5&\24\2\u018d\u018b\3\2\2\2\u018e\u0191\3\2\2\2\u018f"+
		"\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0192\3\2\2\2\u0191\u018f\3\2"+
		"\2\2\u0192\u0193\7F\2\2\u0193\u0194\7/\2\2\u0194\u0195\7\3\2\2\u0195\u0196"+
		"\5\22\n\2\u0196\u0197\7\5\2\2\u0197\u019a\7v\2\2\u0198\u019b\5\66\34\2"+
		"\u0199\u019b\7\u00d1\2\2\u019a\u0198\3\2\2\2\u019a\u0199\3\2\2\2\u019b"+
		"\u01ab\3\2\2\2\u019c\u01a9\7\u00c4\2\2\u019d\u019f\5\30\r\2\u019e\u019d"+
		"\3\2\2\2\u019e\u019f\3\2\2\2\u019f\u01a1\3\2\2\2\u01a0\u01a2\5\34\17\2"+
		"\u01a1\u01a0\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01aa\3\2\2\2\u01a3\u01a5"+
		"\5\34\17\2\u01a4\u01a3\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5\u01a7\3\2\2\2"+
		"\u01a6\u01a8\5\30\r\2\u01a7\u01a6\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01aa"+
		"\3\2\2\2\u01a9\u019e\3\2\2\2\u01a9\u01a4\3\2\2\2\u01aa\u01ac\3\2\2\2\u01ab"+
		"\u019c\3\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u01b0\3\2\2\2\u01ad\u01ae\7 "+
		"\2\2\u01ae\u01af\7\23\2\2\u01af\u01b1\5\36\20\2\u01b0\u01ad\3\2\2\2\u01b0"+
		"\u01b1\3\2\2\2\u01b1\u01b5\3\2\2\2\u01b2\u01b3\7g\2\2\u01b3\u01b4\7\34"+
		"\2\2\u01b4\u01b6\7\u00d8\2\2\u01b5\u01b2\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6"+
		"\u01b9\3\2\2\2\u01b7\u01b8\7\u00c3\2\2\u01b8\u01ba\5<\37\2\u01b9\u01b7"+
		"\3\2\2\2\u01b9\u01ba\3\2\2\2\u01ba\u01c5\3\2\2\2\u01bb\u01bc\7{\2\2\u01bc"+
		"\u01bd\7\23\2\2\u01bd\u01c2\5\26\f\2\u01be\u01bf\7\4\2\2\u01bf\u01c1\5"+
		"\26\f\2\u01c0\u01be\3\2\2\2\u01c1\u01c4\3\2\2\2\u01c2\u01c0\3\2\2\2\u01c2"+
		"\u01c3\3\2\2\2\u01c3\u01c6\3\2\2\2\u01c4\u01c2\3\2\2\2\u01c5\u01bb\3\2"+
		"\2\2\u01c5\u01c6\3\2\2\2\u01c6\u01c9\3\2\2\2\u01c7\u01c8\7c\2\2\u01c8"+
		"\u01ca\t\2\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u01cc\3\2"+
		"\2\2\u01cb\u01cd\5(\25\2\u01cc\u01cb\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd"+
		"\u0217\3\2\2\2\u01ce\u01d0\7\u009c\2\2\u01cf\u01d1\5$\23\2\u01d0\u01cf"+
		"\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\u01d7\5&\24\2\u01d3"+
		"\u01d4\7\4\2\2\u01d4\u01d6\5&\24\2\u01d5\u01d3\3\2\2\2\u01d6\u01d9\3\2"+
		"\2\2\u01d7\u01d5\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8\u01db\3\2\2\2\u01d9"+
		"\u01d7\3\2\2\2\u01da\u01dc\5(\25\2\u01db\u01da\3\2\2\2\u01db\u01dc\3\2"+
		"\2\2\u01dc\u01dd\3\2\2\2\u01dd\u01de\7F\2\2\u01de\u01df\7/\2\2\u01df\u01e0"+
		"\7\3\2\2\u01e0\u01e1\5\22\n\2\u01e1\u01e2\7\5\2\2\u01e2\u01e5\7v\2\2\u01e3"+
		"\u01e6\5\66\34\2\u01e4\u01e6\7\u00d1\2\2\u01e5\u01e3\3\2\2\2\u01e5\u01e4"+
		"\3\2\2\2\u01e6\u01f6\3\2\2\2\u01e7\u01f4\7\u00c4\2\2\u01e8\u01ea\5\30"+
		"\r\2\u01e9\u01e8\3\2\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01ec\3\2\2\2\u01eb"+
		"\u01ed\5\34\17\2\u01ec\u01eb\3\2\2\2\u01ec\u01ed\3\2\2\2\u01ed\u01f5\3"+
		"\2\2\2\u01ee\u01f0\5\34\17\2\u01ef\u01ee\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0"+
		"\u01f2\3\2\2\2\u01f1\u01f3\5\30\r\2\u01f2\u01f1\3\2\2\2\u01f2\u01f3\3"+
		"\2\2\2\u01f3\u01f5\3\2\2\2\u01f4\u01e9\3\2\2\2\u01f4\u01ef\3\2\2\2\u01f5"+
		"\u01f7\3\2\2\2\u01f6\u01e7\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01fb\3\2"+
		"\2\2\u01f8\u01f9\7 \2\2\u01f9\u01fa\7\23\2\2\u01fa\u01fc\5\36\20\2\u01fb"+
		"\u01f8\3\2\2\2\u01fb\u01fc\3\2\2\2\u01fc\u0200\3\2\2\2\u01fd\u01fe\7g"+
		"\2\2\u01fe\u01ff\7\34\2\2\u01ff\u0201\7\u00d8\2\2\u0200\u01fd\3\2\2\2"+
		"\u0200\u0201\3\2\2\2\u0201\u0204\3\2\2\2\u0202\u0203\7\u00c3\2\2\u0203"+
		"\u0205\5<\37\2\u0204\u0202\3\2\2\2\u0204\u0205\3\2\2\2\u0205\u0210\3\2"+
		"\2\2\u0206\u0207\7{\2\2\u0207\u0208\7\23\2\2\u0208\u020d\5\26\f\2\u0209"+
		"\u020a\7\4\2\2\u020a\u020c\5\26\f\2\u020b\u0209\3\2\2\2\u020c\u020f\3"+
		"\2\2\2\u020d\u020b\3\2\2\2\u020d\u020e\3\2\2\2\u020e\u0211\3\2\2\2\u020f"+
		"\u020d\3\2\2\2\u0210\u0206\3\2\2\2\u0210\u0211\3\2\2\2\u0211\u0214\3\2"+
		"\2\2\u0212\u0213\7c\2\2\u0213\u0215\t\2\2\2\u0214\u0212\3\2\2\2\u0214"+
		"\u0215\3\2\2\2\u0215\u0217\3\2\2\2\u0216\u00e9\3\2\2\2\u0216\u0138\3\2"+
		"\2\2\u0216\u0186\3\2\2\2\u0216\u01ce\3\2\2\2\u0217\21\3\2\2\2\u0218\u0219"+
		"\7\u00a3\2\2\u0219\u021a\5.\30\2\u021a\u021b\7\u00c3\2\2\u021b\u021c\5"+
		"<\37\2\u021c\u0223\3\2\2\2\u021d\u021e\7\u00a3\2\2\u021e\u021f\5\n\6\2"+
		"\u021f\u0220\7\u00c3\2\2\u0220\u0221\5<\37\2\u0221\u0223\3\2\2\2\u0222"+
		"\u0218\3\2\2\2\u0222\u021d\3\2\2\2\u0223\23\3\2\2\2\u0224\u0225\5Z.\2"+
		"\u0225\u0228\5N(\2\u0226\u0227\7\35\2\2\u0227\u0229\5F$\2\u0228\u0226"+
		"\3\2\2\2\u0228\u0229\3\2\2\2\u0229\25\3\2\2\2\u022a\u022c\5:\36\2\u022b"+
		"\u022d\t\3\2\2\u022c\u022b\3\2\2\2\u022c\u022d\3\2\2\2\u022d\u0230\3\2"+
		"\2\2\u022e\u022f\7u\2\2\u022f\u0231\t\4\2\2\u0230\u022e\3\2\2\2\u0230"+
		"\u0231\3\2\2\2\u0231\27\3\2\2\2\u0232\u0233\7h\2\2\u0233\u0234\7\u008a"+
		"\2\2\u0234\u0235\7\u00d9\2\2\u0235\31\3\2\2\2\u0236\u0237\7h\2\2\u0237"+
		"\u0238\7\u008b\2\2\u0238\u0239\7\u00d9\2\2\u0239\33\3\2\2\2\u023a\u023b"+
		"\7i\2\2\u023b\u023c\7\u00d9\2\2\u023c\35\3\2\2\2\u023d\u023e\5Z.\2\u023e"+
		"\u023f\7\3\2\2\u023f\u0240\5 \21\2\u0240\u0241\7\5\2\2\u0241\37\3\2\2"+
		"\2\u0242\u0243\5\"\22\2\u0243\u0244\7\3\2\2\u0244\u0245\7\u00d1\2\2\u0245"+
		"\u0246\7\5\2\2\u0246!\3\2\2\2\u0247\u0248\t\5\2\2\u0248#\3\2\2\2\u0249"+
		"\u024a\t\6\2\2\u024a%\3\2\2\2\u024b\u0250\5:\36\2\u024c\u024e\7\16\2\2"+
		"\u024d\u024c\3\2\2\2\u024d\u024e\3\2\2\2\u024e\u024f\3\2\2\2\u024f\u0251"+
		"\5Z.\2\u0250\u024d\3\2\2\2\u0250\u0251\3\2\2\2\u0251\u0258\3\2\2\2\u0252"+
		"\u0253\5X-\2\u0253\u0254\7\6\2\2\u0254\u0255\7\u00d1\2\2\u0255\u0258\3"+
		"\2\2\2\u0256\u0258\7\u00d1\2\2\u0257\u024b\3\2\2\2\u0257\u0252\3\2\2\2"+
		"\u0257\u0256\3\2\2\2\u0258\'\3\2\2\2\u0259\u025a\7Z\2\2\u025a\u025b\7"+
		"~\2\2\u025b\u025e\7\u00d5\2\2\u025c\u025d\t\7\2\2\u025d\u025f\5*\26\2"+
		"\u025e\u025c\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u0262\3\2\2\2\u0260\u0261"+
		"\7d\2\2\u0261\u0263\5*\26\2\u0262\u0260\3\2\2\2\u0262\u0263\3\2\2\2\u0263"+
		")\3\2\2\2\u0264\u0265\7\u00ad\2\2\u0265\u0266\7\23\2\2\u0266\u0267\7\u00d5"+
		"\2\2\u0267+\3\2\2\2\u0268\u0269\7\67\2\2\u0269\u026a\7\23\2\2\u026a\u026b"+
		"\7\u00d5\2\2\u026b-\3\2\2\2\u026c\u026d\b\30\1\2\u026d\u026e\5\64\33\2"+
		"\u026e\u0281\3\2\2\2\u026f\u027d\f\3\2\2\u0270\u0271\7%\2\2\u0271\u0272"+
		"\7]\2\2\u0272\u027e\5\64\33\2\u0273\u0274\5\60\31\2\u0274\u0275\7]\2\2"+
		"\u0275\u0276\5.\30\2\u0276\u0277\5\62\32\2\u0277\u027e\3\2\2\2\u0278\u0279"+
		"\7l\2\2\u0279\u027a\5\60\31\2\u027a\u027b\7]\2\2\u027b\u027c\5\64\33\2"+
		"\u027c\u027e\3\2\2\2\u027d\u0270\3\2\2\2\u027d\u0273\3\2\2\2\u027d\u0278"+
		"\3\2\2\2\u027e\u0280\3\2\2\2\u027f\u026f\3\2\2\2\u0280\u0283\3\2\2\2\u0281"+
		"\u027f\3\2\2\2\u0281\u0282\3\2\2\2\u0282/\3\2\2\2\u0283\u0281\3\2\2\2"+
		"\u0284\u0286\7T\2\2\u0285\u0284\3\2\2\2\u0285\u0286\3\2\2\2\u0286\u0294"+
		"\3\2\2\2\u0287\u0289\7`\2\2\u0288\u028a\7}\2\2\u0289\u0288\3\2\2\2\u0289"+
		"\u028a\3\2\2\2\u028a\u0294\3\2\2\2\u028b\u028d\7\u0094\2\2\u028c\u028e"+
		"\7}\2\2\u028d\u028c\3\2\2\2\u028d\u028e\3\2\2\2\u028e\u0294\3\2\2\2\u028f"+
		"\u0291\7G\2\2\u0290\u0292\7}\2\2\u0291\u0290\3\2\2\2\u0291\u0292\3\2\2"+
		"\2\u0292\u0294\3\2\2\2\u0293\u0285\3\2\2\2\u0293\u0287\3\2\2\2\u0293\u028b"+
		"\3\2\2\2\u0293\u028f\3\2\2\2\u0294\61\3\2\2\2\u0295\u0296\7v\2\2\u0296"+
		"\u02a4\5<\37\2\u0297\u0298\7\u00bd\2\2\u0298\u0299\7\3\2\2\u0299\u029e"+
		"\5Z.\2\u029a\u029b\7\4\2\2\u029b\u029d\5Z.\2\u029c\u029a\3\2\2\2\u029d"+
		"\u02a0\3\2\2\2\u029e\u029c\3\2\2\2\u029e\u029f\3\2\2\2\u029f\u02a1\3\2"+
		"\2\2\u02a0\u029e\3\2\2\2\u02a1\u02a2\7\5\2\2\u02a2\u02a4\3\2\2\2\u02a3"+
		"\u0295\3\2\2\2\u02a3\u0297\3\2\2\2\u02a4\63\3\2\2\2\u02a5\u02b0\58\35"+
		"\2\u02a6\u02a8\7\16\2\2\u02a7\u02a6\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8"+
		"\u02a9\3\2\2\2\u02a9\u02ae\5Z.\2\u02aa\u02ab\7\3\2\2\u02ab\u02ac\5\66"+
		"\34\2\u02ac\u02ad\7\5\2\2\u02ad\u02af\3\2\2\2\u02ae\u02aa\3\2\2\2\u02ae"+
		"\u02af\3\2\2\2\u02af\u02b1\3\2\2\2\u02b0\u02a7\3\2\2\2\u02b0\u02b1\3\2"+
		"\2\2\u02b1\65\3\2\2\2\u02b2\u02b7\5Z.\2\u02b3\u02b4\7\4\2\2\u02b4\u02b6"+
		"\5Z.\2\u02b5\u02b3\3\2\2\2\u02b6\u02b9\3\2\2\2\u02b7\u02b5\3\2\2\2\u02b7"+
		"\u02b8\3\2\2\2\u02b8\67\3\2\2\2\u02b9\u02b7\3\2\2\2\u02ba\u02c4\5X-\2"+
		"\u02bb\u02bc\7\3\2\2\u02bc\u02bd\5\b\5\2\u02bd\u02be\7\5\2\2\u02be\u02c4"+
		"\3\2\2\2\u02bf\u02c0\7\3\2\2\u02c0\u02c1\5.\30\2\u02c1\u02c2\7\5\2\2\u02c2"+
		"\u02c4\3\2\2\2\u02c3\u02ba\3\2\2\2\u02c3\u02bb\3\2\2\2\u02c3\u02bf\3\2"+
		"\2\2\u02c49\3\2\2\2\u02c5\u02c6\5<\37\2\u02c6;\3\2\2\2\u02c7\u02c8\b\37"+
		"\1\2\u02c8\u02cc\5> \2\u02c9\u02ca\7r\2\2\u02ca\u02cc\5<\37\5\u02cb\u02c7"+
		"\3\2\2\2\u02cb\u02c9\3\2\2\2\u02cc\u02d5\3\2\2\2\u02cd\u02ce\f\4\2\2\u02ce"+
		"\u02cf\7\13\2\2\u02cf\u02d4\5<\37\5\u02d0\u02d1\f\3\2\2\u02d1\u02d2\7"+
		"z\2\2\u02d2\u02d4\5<\37\4\u02d3\u02cd\3\2\2\2\u02d3\u02d0\3\2\2\2\u02d4"+
		"\u02d7\3\2\2\2\u02d5\u02d3\3\2\2\2\u02d5\u02d6\3\2\2\2\u02d6=\3\2\2\2"+
		"\u02d7\u02d5\3\2\2\2\u02d8\u02da\5B\"\2\u02d9\u02db\5@!\2\u02da\u02d9"+
		"\3\2\2\2\u02da\u02db\3\2\2\2\u02db?\3\2\2\2\u02dc\u02dd\5H%\2\u02dd\u02de"+
		"\5B\"\2\u02de\u031a\3\2\2\2\u02df\u02e0\5H%\2\u02e0\u02e1\5J&\2\u02e1"+
		"\u02e2\7\3\2\2\u02e2\u02e3\5\b\5\2\u02e3\u02e4\7\5\2\2\u02e4\u031a\3\2"+
		"\2\2\u02e5\u02e7\7r\2\2\u02e6\u02e5\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7"+
		"\u02e8\3\2\2\2\u02e8\u02e9\7\22\2\2\u02e9\u02ea\5B\"\2\u02ea\u02eb\7\13"+
		"\2\2\u02eb\u02ec\5B\"\2\u02ec\u031a\3\2\2\2\u02ed\u02ef\7r\2\2\u02ee\u02ed"+
		"\3\2\2\2\u02ee\u02ef\3\2\2\2\u02ef\u02f0\3\2\2\2\u02f0\u02f1\7R\2\2\u02f1"+
		"\u02f2\7\3\2\2\u02f2\u02f7\5:\36\2\u02f3\u02f4\7\4\2\2\u02f4\u02f6\5:"+
		"\36\2\u02f5\u02f3\3\2\2\2\u02f6\u02f9\3\2\2\2\u02f7\u02f5\3\2\2\2\u02f7"+
		"\u02f8\3\2\2\2\u02f8\u02fa\3\2\2\2\u02f9\u02f7\3\2\2\2\u02fa\u02fb\7\5"+
		"\2\2\u02fb\u031a\3\2\2\2\u02fc\u02fe\7r\2\2\u02fd\u02fc\3\2\2\2\u02fd"+
		"\u02fe\3\2\2\2\u02fe\u02ff\3\2\2\2\u02ff\u0300\7R\2\2\u0300\u0301\7\3"+
		"\2\2\u0301\u0302\5\b\5\2\u0302\u0303\7\5\2\2\u0303\u031a\3\2\2\2\u0304"+
		"\u0306\7r\2\2\u0305\u0304\3\2\2\2\u0305\u0306\3\2\2\2\u0306\u0307\3\2"+
		"\2\2\u0307\u0308\7b\2\2\u0308\u030b\5B\"\2\u0309\u030a\7\66\2\2\u030a"+
		"\u030c\5B\"\2\u030b\u0309\3\2\2\2\u030b\u030c\3\2\2\2\u030c\u031a\3\2"+
		"\2\2\u030d\u030f\7[\2\2\u030e\u0310\7r\2\2\u030f\u030e\3\2\2\2\u030f\u0310"+
		"\3\2\2\2\u0310\u0311\3\2\2\2\u0311\u031a\7s\2\2\u0312\u0314\7[\2\2\u0313"+
		"\u0315\7r\2\2\u0314\u0313\3\2\2\2\u0314\u0315\3\2\2\2\u0315\u0316\3\2"+
		"\2\2\u0316\u0317\7\60\2\2\u0317\u0318\7F\2\2\u0318\u031a\5B\"\2\u0319"+
		"\u02dc\3\2\2\2\u0319\u02df\3\2\2\2\u0319\u02e6\3\2\2\2\u0319\u02ee\3\2"+
		"\2\2\u0319\u02fd\3\2\2\2\u0319\u0305\3\2\2\2\u0319\u030d\3\2\2\2\u0319"+
		"\u0312\3\2\2\2\u031aA\3\2\2\2\u031b\u031c\b\"\1\2\u031c\u0320\5D#\2\u031d"+
		"\u031e\t\b\2\2\u031e\u0320\5B\"\6\u031f\u031b\3\2\2\2\u031f\u031d\3\2"+
		"\2\2\u0320\u032c\3\2\2\2\u0321\u0322\f\5\2\2\u0322\u0323\t\t\2\2\u0323"+
		"\u032b\5B\"\6\u0324\u0325\f\4\2\2\u0325\u0326\t\b\2\2\u0326\u032b\5B\""+
		"\5\u0327\u0328\f\3\2\2\u0328\u0329\7\u00d4\2\2\u0329\u032b\5B\"\4\u032a"+
		"\u0321\3\2\2\2\u032a\u0324\3\2\2\2\u032a\u0327\3\2\2\2\u032b\u032e\3\2"+
		"\2\2\u032c\u032a\3\2\2\2\u032c\u032d\3\2\2\2\u032dC\3\2\2\2\u032e\u032c"+
		"\3\2\2\2\u032f\u0330\b#\1\2\u0330\u0363\7s\2\2\u0331\u0332\5Z.\2\u0332"+
		"\u0333\5F$\2\u0333\u0363\3\2\2\2\u0334\u0335\7\u00e0\2\2\u0335\u0363\5"+
		"F$\2\u0336\u0363\5\\/\2\u0337\u0363\5L\'\2\u0338\u0363\5F$\2\u0339\u0363"+
		"\7\u00d7\2\2\u033a\u033b\5X-\2\u033b\u033c\7\3\2\2\u033c\u033d\7\u00d1"+
		"\2\2\u033d\u033f\7\5\2\2\u033e\u0340\5V,\2\u033f\u033e\3\2\2\2\u033f\u0340"+
		"\3\2\2\2\u0340\u0363\3\2\2\2\u0341\u0342\5X-\2\u0342\u034e\7\3\2\2\u0343"+
		"\u0345\5$\23\2\u0344\u0343\3\2\2\2\u0344\u0345\3\2\2\2\u0345\u0346\3\2"+
		"\2\2\u0346\u034b\5:\36\2\u0347\u0348\7\4\2\2\u0348\u034a\5:\36\2\u0349"+
		"\u0347\3\2\2\2\u034a\u034d\3\2\2\2\u034b\u0349\3\2\2\2\u034b\u034c\3\2"+
		"\2\2\u034c\u034f\3\2\2\2\u034d\u034b\3\2\2\2\u034e\u0344\3\2\2\2\u034e"+
		"\u034f\3\2\2\2\u034f\u0350\3\2\2\2\u0350\u0352\7\5\2\2\u0351\u0353\5V"+
		",\2\u0352\u0351\3\2\2\2\u0352\u0353\3\2\2\2\u0353\u0363\3\2\2\2\u0354"+
		"\u0355\7\3\2\2\u0355\u0356\5\b\5\2\u0356\u0357\7\5\2\2\u0357\u0363\3\2"+
		"\2\2\u0358\u0359\7;\2\2\u0359\u035a\7\3\2\2\u035a\u035b\5\b\5\2\u035b"+
		"\u035c\7\5\2\2\u035c\u0363\3\2\2\2\u035d\u0363\5Z.\2\u035e\u035f\7\3\2"+
		"\2\u035f\u0360\5:\36\2\u0360\u0361\7\5\2\2\u0361\u0363\3\2\2\2\u0362\u032f"+
		"\3\2\2\2\u0362\u0331\3\2\2\2\u0362\u0334\3\2\2\2\u0362\u0336\3\2\2\2\u0362"+
		"\u0337\3\2\2\2\u0362\u0338\3\2\2\2\u0362\u0339\3\2\2\2\u0362\u033a\3\2"+
		"\2\2\u0362\u0341\3\2\2\2\u0362\u0354\3\2\2\2\u0362\u0358\3\2\2\2\u0362"+
		"\u035d\3\2\2\2\u0362\u035e\3\2\2\2\u0363\u0369\3\2\2\2\u0364\u0365\f\4"+
		"\2\2\u0365\u0366\7\6\2\2\u0366\u0368\5Z.\2\u0367\u0364\3\2\2\2\u0368\u036b"+
		"\3\2\2\2\u0369\u0367\3\2\2\2\u0369\u036a\3\2\2\2\u036aE\3\2\2\2\u036b"+
		"\u0369\3\2\2\2\u036c\u0373\7\u00d5\2\2\u036d\u0370\7\u00d6\2\2\u036e\u036f"+
		"\7\u00b7\2\2\u036f\u0371\7\u00d5\2\2\u0370\u036e\3\2\2\2\u0370\u0371\3"+
		"\2\2\2\u0371\u0373\3\2\2\2\u0372\u036c\3\2\2\2\u0372\u036d\3\2\2\2\u0373"+
		"G\3\2\2\2\u0374\u0375\t\n\2\2\u0375I\3\2\2\2\u0376\u0377\t\13\2\2\u0377"+
		"K\3\2\2\2\u0378\u0379\t\f\2\2\u0379M\3\2\2\2\u037a\u037b\b(\1\2\u037b"+
		"\u037c\7\r\2\2\u037c\u037d\7\u00cb\2\2\u037d\u037e\5N(\2\u037e\u037f\7"+
		"\u00cd\2\2\u037f\u03a5\3\2\2\2\u0380\u0381\7f\2\2\u0381\u0382\7\u00cb"+
		"\2\2\u0382\u0383\5N(\2\u0383\u0384\7\4\2\2\u0384\u0385\5N(\2\u0385\u0386"+
		"\7\u00cd\2\2\u0386\u03a5\3\2\2\2\u0387\u0388\7\u0097\2\2\u0388\u0389\7"+
		"\3\2\2\u0389\u038a\5Z.\2\u038a\u0391\5N(\2\u038b\u038c\7\4\2\2\u038c\u038d"+
		"\5Z.\2\u038d\u038e\5N(\2\u038e\u0390\3\2\2\2\u038f\u038b\3\2\2\2\u0390"+
		"\u0393\3\2\2\2\u0391\u038f\3\2\2\2\u0391\u0392\3\2\2\2\u0392\u0394\3\2"+
		"\2\2\u0393\u0391\3\2\2\2\u0394\u0395\7\5\2\2\u0395\u03a5\3\2\2\2\u0396"+
		"\u03a2\5R*\2\u0397\u0398\7\3\2\2\u0398\u039d\5P)\2\u0399\u039a\7\4\2\2"+
		"\u039a\u039c\5P)\2\u039b\u0399\3\2\2\2\u039c\u039f\3\2\2\2\u039d\u039b"+
		"\3\2\2\2\u039d\u039e\3\2\2\2\u039e\u03a0\3\2\2\2\u039f\u039d\3\2\2\2\u03a0"+
		"\u03a1\7\5\2\2\u03a1\u03a3\3\2\2\2\u03a2\u0397\3\2\2\2\u03a2\u03a3\3\2"+
		"\2\2\u03a3\u03a5\3\2\2\2\u03a4\u037a\3\2\2\2\u03a4\u0380\3\2\2\2\u03a4"+
		"\u0387\3\2\2\2\u03a4\u0396\3\2\2\2\u03a5\u03aa\3\2\2\2\u03a6\u03a7\f\7"+
		"\2\2\u03a7\u03a9\7\r\2\2\u03a8\u03a6\3\2\2\2\u03a9\u03ac\3\2\2\2\u03aa"+
		"\u03a8\3\2\2\2\u03aa\u03ab\3\2\2\2\u03abO\3\2\2\2\u03ac\u03aa\3\2\2\2"+
		"\u03ad\u03b0\7\u00d8\2\2\u03ae\u03b0\5N(\2\u03af\u03ad\3\2\2\2\u03af\u03ae"+
		"\3\2\2\2\u03b0Q\3\2\2\2\u03b1\u03b6\7\u00de\2\2\u03b2\u03b6\7\u00df\2"+
		"\2\u03b3\u03b6\7\u00e0\2\2\u03b4\u03b6\5Z.\2\u03b5\u03b1\3\2\2\2\u03b5"+
		"\u03b2\3\2\2\2\u03b5\u03b3\3\2\2\2\u03b5\u03b4\3\2\2\2\u03b6S\3\2\2\2"+
		"\u03b7\u03b8\7\u00c2\2\2\u03b8\u03b9\5:\36\2\u03b9\u03ba\7\u00af\2\2\u03ba"+
		"\u03bb\5:\36\2\u03bbU\3\2\2\2\u03bc\u03bd\7A\2\2\u03bd\u03be\7\3\2\2\u03be"+
		"\u03bf\7\u00c3\2\2\u03bf\u03c0\5<\37\2\u03c0\u03c1\7\5\2\2\u03c1W\3\2"+
		"\2\2\u03c2\u03c7\5Z.\2\u03c3\u03c4\7\6\2\2\u03c4\u03c6\5Z.\2\u03c5\u03c3"+
		"\3\2\2\2\u03c6\u03c9\3\2\2\2\u03c7\u03c5\3\2\2\2\u03c7\u03c8\3\2\2\2\u03c8"+
		"Y\3\2\2\2\u03c9\u03c7\3\2\2\2\u03ca\u03cf\7\u00da\2\2\u03cb\u03cf\5^\60"+
		"\2\u03cc\u03cf\7\u00dd\2\2\u03cd\u03cf\7\u00db\2\2\u03ce\u03ca\3\2\2\2"+
		"\u03ce\u03cb\3\2\2\2\u03ce\u03cc\3\2\2\2\u03ce\u03cd\3\2\2\2\u03cf[\3"+
		"\2\2\2\u03d0\u03d3\7\u00d9\2\2\u03d1\u03d3\7\u00d8\2\2\u03d2\u03d0\3\2"+
		"\2\2\u03d2\u03d1\3\2\2\2\u03d3]\3\2\2\2\u03d4\u03d5\t\r\2\2\u03d5_\3\2"+
		"\2\2\u0098ty{\u0089\u008d\u0094\u009d\u00a0\u00a4\u00ad\u00b0\u00b4\u00b7"+
		"\u00bb\u00c2\u00c6\u00ce\u00d1\u00d5\u00de\u00e1\u00e5\u00e7\u00eb\u00f2"+
		"\u00f9\u00fe\u0103\u0107\u010a\u010d\u0110\u0113\u0115\u011a\u011f\u0123"+
		"\u012c\u012f\u0133\u0136\u013a\u0141\u0145\u014b\u0150\u0155\u0159\u015c"+
		"\u015f\u0162\u0164\u0166\u016b\u0170\u0174\u017d\u0180\u0184\u0188\u018f"+
		"\u019a\u019e\u01a1\u01a4\u01a7\u01a9\u01ab\u01b0\u01b5\u01b9\u01c2\u01c5"+
		"\u01c9\u01cc\u01d0\u01d7\u01db\u01e5\u01e9\u01ec\u01ef\u01f2\u01f4\u01f6"+
		"\u01fb\u0200\u0204\u020d\u0210\u0214\u0216\u0222\u0228\u022c\u0230\u024d"+
		"\u0250\u0257\u025e\u0262\u027d\u0281\u0285\u0289\u028d\u0291\u0293\u029e"+
		"\u02a3\u02a7\u02ae\u02b0\u02b7\u02c3\u02cb\u02d3\u02d5\u02da\u02e6\u02ee"+
		"\u02f7\u02fd\u0305\u030b\u030f\u0314\u0319\u031f\u032a\u032c\u033f\u0344"+
		"\u034b\u034e\u0352\u0362\u0369\u0370\u0372\u0391\u039d\u03a2\u03a4\u03aa"+
		"\u03af\u03b5\u03c7\u03ce\u03d2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}