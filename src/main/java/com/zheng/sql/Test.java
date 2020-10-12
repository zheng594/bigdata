package com.zheng.sql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.IOException;

/**
 * Created by zheng on 2020/9/8
 */
public class Test {
//    public static void main(String[] args) throws IOException {
//        ANTLRInputStream input = new ANTLRInputStream("{2,{3},4}");
//        //词法解析器，处理input
//        CustomSqlLexer lexer = new CustomSqlLexer(input);
//        //词法符号的缓冲器，存储词法分析器生成的词法符号
//        CommonTokenStream tokens = new CommonTokenStream(lexer);
//        //语法分析器，处理词法符号缓冲区的内容
//        CustomSqlParser parser = new CustomSqlParser(tokens);
//
//        ParseTree tree = parser.init();
//        System.out.println(tree.toStringTree(parser));
//    }

    public static void main(String[] args) throws IOException {
        ANTLRInputStream input = new ANTLRInputStream("{2,4}");
        //词法解析器，处理input
        CustomSqlLexer lexer = new CustomSqlLexer(input);
        //词法符号的缓冲器，存储词法分析器生成的词法符号
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        //语法分析器，处理词法符号缓冲区的内容
        CustomSqlParser parser = new CustomSqlParser(tokens);

        ParseTree tree = parser.init();

        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new ShortToUnicode(),tree);
        System.out.println();
    }
}
