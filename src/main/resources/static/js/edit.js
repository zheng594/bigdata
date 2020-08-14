var EditObj = function () {
  var editor
  return {
    init: function () {
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

      $('#run').on('click', function () {
        $('#result').text("running......\n")
        EditObj.querySql(editor.getValue())
      })
    },
    save: function () {
      localStorage.savedValue = editor.getValue();
      editor.session.getUndoManager().markClean();
      alert('保存成功')
    },

    querySql: function (sql) {
      $.ajax({
        url: '/querySql',
        type: 'POST',
        async: true,
        contentType: 'application/json',
        data: JSON.stringify({sql: sql}),
        success: function (res) {
          if (res.success) {
            $('#result').append(res.data)
          } else {
            $('#result').append(res.message)
          }
        }
      })
    }


  }


}();


$(document).ready(function () {
  EditObj.init();


});