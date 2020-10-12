// Generated from /Users/zheng/project/bigdata/src/main/java/com/zheng/sql/CustomSql.g4 by ANTLR 4.8
package com.zheng.sql;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

/**
 * This class provides an empty implementation of {@link CustomSqlVisitor},
 * which can be extended to create a visitor which only needs to handle a subset
 * of the available methods.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class CustomSqlBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements CustomSqlVisitor<T> {
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitInit(CustomSqlParser.InitContext ctx) { return visitChildren(ctx); }
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitValue(CustomSqlParser.ValueContext ctx) { return visitChildren(ctx); }
}