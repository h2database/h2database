<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!--
Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html><head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <title>Tools</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
    <script type="text/javascript">
//<!--
var current = '';
function go(name) {
    if (name == current) {
        return;
    }
    var tools = document.getElementsByTagName('div');
    for (i = 0; i < tools.length; i++) {
        var div = tools[i];
        if(div.id.substring(0, 4) == 'tool') {
            div.style.display = (div.id == 'tool' + name) ? '' : 'none';
        }
    }
    document.getElementById('commandLine').style.display='';
    document.getElementById('toolName').innerHTML = name;
    document.getElementById('tool').value = name;
    document.getElementById('result').style.display = 'none';
    current = name;
    update();
}
function quote(x) {
    var q = '';
    for (var i=0; i<x.length; i++) {
        var c = x.charAt(i);
        if(c == '"') {
            q += '\\"';
        } else {
            q += c;
        }
    }
    return q;
}
function update() {
    var line = '', args = '';
    for (var i = 0; i < 9; i++) {
        var f = document.getElementById('option' + current + '.' + i);
        if (f != null && f.value.length > 0) {
            var x = quote(f.value);
            if (f.type == 'password') {
            	x = '';
            	for (var j = 0; j < f.value.length; j++) {
	            	x += '*';
	            }
            }
            line += ' -' + f.name + ' "' + x + '"';
            if (args.length > 0) {
                args += ',';
            }
            args += '-' + f.name + ',' + quote(f.value);
        }
    }
    document.getElementById('toolOptions').innerHTML = line;
    document.getElementById('args').value = args;
}
//-->
    </script>
</head>
<body style="margin: 20px">
<form name="tools" method="post" action="tools.do?jsessionid=${sessionId}" id="tools">

<h1>Tools</h1>
<p>
<a href="logout.do?jsessionid=${sessionId}">Logout</a> 
</p>
<hr />
<p>
<a href="javascript:go('Backup')">Backup</a>&nbsp;&nbsp;
<a href="javascript:go('Restore')">Restore</a>&nbsp;&nbsp;
<a href="javascript:go('Recover')">Recover</a>&nbsp;&nbsp;
<a href="javascript:go('DeleteDbFiles')">DeleteDbFiles</a>&nbsp;&nbsp;
<a href="javascript:go('ChangePassword')">ChangePassword</a>
</p><p>
<a href="javascript:go('Script')">Script</a>&nbsp;&nbsp;
<a href="javascript:go('RunScript')">RunScript</a>&nbsp;&nbsp;
<a href="javascript:go('ConvertTraceFile')">ConvertTraceFile</a>&nbsp;&nbsp;
<a href="javascript:go('CreateCluster')">CreateCluster</a>
</p>
<hr />
<div id="toolBackup" style="display: none">
    <h2>Backup</h2>
    <p>Creates a backup of a database.</p>
    <table class="tool">
	    <tr><td>
	    Target file name:&nbsp;</td><td><input id="optionBackup.0" name="file" onkeyup="update()" onchange="update()" value="~/backup.zip" size="50" />
	    </td></tr><tr><td>
	    Source directory:&nbsp;</td><td><input id="optionBackup.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
	    </td></tr><tr><td>
	    Source database name:&nbsp;</td><td><input id="optionBackup.2" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolRestore" name="Restore" style="display: none">
    <h2>Restore</h2>
    <p>Restores a database backup.</p>
    <table class="tool">
	    <tr><td>
	    Source file name:&nbsp;</td><td><input id="optionRestore.0" name="file" onkeyup="update()" onchange="update()" value="~/backup.zip" size="50" />
	    </td></tr><tr><td>
	    Target directory:&nbsp;</td><td><input id="optionRestore.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
	    </td></tr><tr><td>
	    Target database name:&nbsp;</td><td><input id="optionRestore.2" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolRecover" style="display: none">
    <h2>Recover</h2>
    <p>Helps recovering a corrupted database.</p>
    <table class="tool">
	    <tr><td>
	    Directory:&nbsp;</td><td><input id="optionRecover.0" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
	    </td></tr><tr><td>
	    Database name:&nbsp;</td><td><input id="optionRecover.1" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolDeleteDbFiles" style="display: none">
    <h2>DeleteDbFiles</h2>
    <p>Deletes all files belonging to a database.</p>
    <table class="tool">
	    <tr><td>
	    Directory:&nbsp;</td><td><input id="optionDeleteDbFiles.0" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
	    </td></tr><tr><td>
	    Database name:&nbsp;</td><td><input id="optionDeleteDbFiles.1" name="db" onkeyup="update()" onchange="update()" value="delete" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolChangePassword" style="display: none">
    <h2>ChangePassword</h2>
    <p>Allows changing the database file password.</p>
    <table class="tool">
	    <tr><td>
	    Cipher (AES or XTEA):&nbsp;</td><td><input id="optionChangePassword.0" name="cipher" onkeyup="update()" onchange="update()" value="XTEA" />
	    </td></tr><tr><td>
	    Directory:&nbsp;</td><td><input id="optionChangePassword.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
	    </td></tr><tr><td>
	    Database name:&nbsp;</td><td><input id="optionChangePassword.2" name="db" onkeyup="update()" onchange="update()" value="test" size="50" />
	    </td></tr><tr><td>
	    Decryption password:&nbsp;</td><td><input type="password" id="optionChangePassword.3" name="decrypt" onkeyup="update()" onchange="update()" value="" />
	    </td></tr><tr><td>
	    Encryption password:&nbsp;</td><td><input type="password" id="optionChangePassword.4" name="encrypt" onkeyup="update()" onchange="update()" value="" />
	    </td></tr>
    </table>
</div>
<div id="toolScript" style="display: none">
    <h2>Script</h2>
    <p>Allows to convert a database to a SQL script for backup or migration.</p>
    <table class="tool">
	    <tr><td>
	    Source database URL:&nbsp;</td><td><input id="optionScript.0" name="url" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
	    </td></tr><tr><td>
	    User name:&nbsp;</td><td><input id="optionScript.1" name="user" onkeyup="update()" onchange="update()" value="sa" />
	    </td></tr><tr><td>
	    Password:&nbsp;</td><td><input type="password" id="optionScript.2" name="password" onkeyup="update()" onchange="update()" value="" />
	    </td></tr><tr><td>
	    Target script file name:&nbsp;</td><td><input id="optionScript.3" name="script" onkeyup="update()" onchange="update()" value="~/backup.sql" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolRunScript" style="display: none">
    <h2>RunScript</h2>
    <p>Runs a SQL script.</p>
    <table class="tool">
	    <tr><td>
	    Target database URL:&nbsp;</td><td><input id="optionRunScript.0" name="url" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
	    </td></tr><tr><td>
	    User name:&nbsp;</td><td><input id="optionRunScript.1" name="user" onkeyup="update()" onchange="update()" value="sa" />
	    </td></tr><tr><td>
	    Password:&nbsp;</td><td><input type="password" id="optionRunScript.2" name="password" onkeyup="update()" onchange="update()" value="" />
	    </td></tr><tr><td>
	    Source script file name:&nbsp;</td><td><input id="optionRunScript.3" name="script" onkeyup="update()" onchange="update()" value="~/backup.sql" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolConvertTraceFile" style="display: none">
    <h2>ConvertTraceFile</h2>
    <p>Converts a .trace.db file to a Java application and SQL script.</p>
    <table class="tool">
	    <tr><td>
	    Trace file name:&nbsp;</td><td><input id="optionConvertTraceFile.0" name="traceFile" onkeyup="update()" onchange="update()" value="~/test.trace.db" size="50" />
	    </td></tr><tr><td>
	    Script file name:&nbsp;</td><td><input id="optionConvertTraceFile.1" name="script" onkeyup="update()" onchange="update()" value="~/test.sql" size="50" />
	    </td></tr><tr><td>
	    Java directory and class name:&nbsp;</td><td><input id="optionConvertTraceFile.2" name="javaClass" onkeyup="update()" onchange="update()" value="~/Test" size="50" />
	    </td></tr>
    </table>
</div>
<div id="toolCreateCluster" style="display: none">
    <h2>CreateCluster</h2>
    <p>Creates a cluster from a standalone database.</p>
    <table class="tool">
	    <tr><td>
	    Source database URL:&nbsp;</td><td><input id="optionCreateCluster.0" name="urlSource" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
	    </td></tr><tr><td>
	    Target database URL:&nbsp;</td><td><input id="optionCreateCluster.1" name="urlTarget" onkeyup="update()" onchange="update()" value="jdbc:h2:~/copy/test" size="50" />
	    </td></tr><tr><td>
	    User name:&nbsp;</td><td><input id="optionCreateCluster.2" name="user" onkeyup="update()" onchange="update()" value="sa" />
	    </td></tr><tr><td>
	    Password:&nbsp;</td><td><input type="password" id="optionCreateCluster.3" name="password" onkeyup="update()" onchange="update()" value="" />
	    </td></tr><tr><td>
	    Server list:&nbsp;</td><td><input id="optionCreateCluster.4" name="serverlist" onkeyup="update()" onchange="update()" value="server1,server2" size="50" />
	    </td></tr>
    </table>
</div>

<div id="commandLine" style="display: none">
        <input type="submit" class="button" value="Run" />
        <input type="hidden" name="tool" id="tool" value=""/>
        <input type="hidden" name="args" id="args" value=""/>
        <h4>Command line:</h4>
        java -cp h2.jar org.h2.tools.<span id="toolName"></span>
        <span id="toolOptions">${tool}</span>
</div>

<div id="result" style="display: none">
        <h4>Result:</h4>
        <p>${toolResult}</p>
</div>

</form>

<script type="text/javascript">
//<!--
var t = '${tool}';
if (t != '') {
    go(t);
    document.getElementById('result').style.display = '';
}
//-->
</script>
</body></html>