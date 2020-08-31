var EditObj = function () {
    var editor
    var sessionId;
    return {
        init: function () {
            this.initWebsocket();

            var buildDom = require("ace/lib/dom").buildDom;
            editor = ace.edit("editor");
            editor.setOptions({
                theme: "ace/theme/tomorrow_night_eighties",
                mode: "ace/mode/markdown",
                maxLines: 10,
                minLines: 10,
                autoScrollEditorIntoView: true,
            });

            editor.session.setValue(localStorage.savedValue || "show databases;")

            $('#save').on('click', function () {
                EditObj.save();
            })

            //todo 点击开始执行按钮
            $('#run').on('click', function () {
                $('#result').empty()
                $('#result').append("开始执行<br>")
                EditObj.run(editor.getValue())
            })
        },
        save: function () {
            localStorage.savedValue = editor.getValue();
            editor.session.getUndoManager().markClean();
            alert('保存成功')
        },

        //todo
        //执行sql
        run : function(sql){
            $.ajax({
                url: '/web/run',
                type: 'POST',
                async: true,
                contentType: 'application/json',
                data: JSON.stringify({
                    sql: sql,
                    sessionId:sessionId
                }),
                success: function (res) {}
            })
        },

        //todo
        //初始化websocket
        initWebsocket: function () {
            var socket;
            if (typeof (WebSocket) == "undefined") {
                $('#result').append("您的浏览器不支持WebSocket")
            } else {
                //实现化WebSocket对象
                //ws对应http、wss对应https。
                socket = new WebSocket("ws://localhost:8086/websocket");
                //连接打开事件
                socket.onopen = function () {
                    console.info("连接已打开")
                };
                //收到消息事件
                socket.onmessage = function (event) {
                    var obj = JSON.parse(event.data);
                    sessionId = obj.sessionId;
                    var data = obj.data;
                    if(data){
                        var cols = data.columnNames;

                        var str = "<table border='1'><tr>"
                        for(var i=0;i<cols.length;i++){
                            str += "<td>"+cols[i]+"</td>";
                        }
                        str += "</tr>"

                        var list = data.data;
                        var schemas = data.schemas;
                        for(var i=0;i<list.length;i++){
                            var tmp = list[i];
                            str += "<tr>"
                            for(var j=0;j<schemas.length;j++) {
                                var value = tmp[schemas[j]];
                                str += "<td>"+value+"</td>";
                            }
                            str += "</tr>"
                        }
                        str += "</table>"

                        $('#result').append(str+"<br>")
                    }else{
                        $('#result').append(obj.message+"<br>")
                    }

                };
                //连接关闭事件
                socket.onclose = function () {
                    $('#result').append("连接已关闭，请刷新页面后重新执行")
                    console.log("Socket已关闭");
                };
                //发生了错误事件
                socket.onerror = function () {
                    alert("Socket发生了错误");
                }

                //窗口关闭时，关闭连接
                window.unload = function () {
                    socket.close();
                };
            }
        }
    }


}();


$(document).ready(function () {
    EditObj.init();


});