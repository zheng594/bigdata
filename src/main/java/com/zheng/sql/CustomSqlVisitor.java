// Generated from /Users/zheng/project/bigdata/src/main/java/com/zheng/sql/CustomSql.g4 by ANTLR 4.8
package com.zheng.sql;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link CustomSqlParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface CustomSqlVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link CustomSqlParser#init}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInit(CustomSqlParser.InitContext ctx);
	/**
	 * Visit a parse tree produced by {@link CustomSqlParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(CustomSqlParser.ValueContext ctx);
}