package com.zheng.sql;

/**
 * Created by zheng on 2020/10/10
 */
public class ShortToUnicode extends  CustomSqlBaseListener{
    @Override
    public void enterInit(CustomSqlParser.InitContext ctx) {
        System.out.print('"');
    }

    @Override
    public void exitInit(CustomSqlParser.InitContext ctx) {
        System.out.print('"');
    }

    @Override
    public void enterValue(CustomSqlParser.ValueContext ctx) {
        int value = Integer.valueOf((ctx.INT()).getText());
        System.out.printf("\\u%04x",value);

    }

}
