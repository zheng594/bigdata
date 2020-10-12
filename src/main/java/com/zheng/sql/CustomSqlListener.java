// Generated from /Users/zheng/project/bigdata/src/main/java/com/zheng/sql/CustomSql.g4 by ANTLR 4.8
package com.zheng.sql;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link CustomSqlParser}.
 */
public interface CustomSqlListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link CustomSqlParser#init}.
	 * @param ctx the parse tree
	 */
	void enterInit(CustomSqlParser.InitContext ctx);
	/**
	 * Exit a parse tree produced by {@link CustomSqlParser#init}.
	 * @param ctx the parse tree
	 */
	void exitInit(CustomSqlParser.InitContext ctx);
	/**
	 * Enter a parse tree produced by {@link CustomSqlParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(CustomSqlParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CustomSqlParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(CustomSqlParser.ValueContext ctx);
}