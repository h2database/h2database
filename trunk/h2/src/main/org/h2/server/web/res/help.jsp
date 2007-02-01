<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!-- 
Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
Initial Developer: H2 Group
-->
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <title>${text.a.title}</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
</head>
<body class="result">

<div id="output">

<script type="text/javascript">
<!--
function set(s) {
    top.h2query.document.h2query.sql.value = s;
}
//-->
</script>

<h3>${text.helpImportantCommands}</h3>
<table>
<tr><th>${text.helpIcon}</th><th>${text.helpAction}</th></tr>
<tr>
     <td style="padding:0px"><img src="icon_help.gif" alt="${text.a.help}"/></td>
    <td style="vertical-align: middle;">
        ${text.helpDisplayThis}
    </td>
</tr>
<tr>
     <td style="padding:0px"><img src="icon_history.gif" alt="${text.toolbar.history}"/></td>
    <td style="vertical-align: middle;">
        ${text.helpCommandHistory}
    </td>
</tr>
<tr>
     <td style="padding:0px"><img src="icon_run.gif" alt="${text.toolbar.run}"/></td>
    <td style="vertical-align: middle;">
        ${text.helpExecuteCurrent}
    </td>
</tr>
<tr>
     <td style="padding:0px"><img src="icon_disconnect.gif" alt="${text.toolbar.disconnect}"/></td>
    <td style="vertical-align: middle;">
        ${text.helpDisconnect}
    </td>
</tr>
</table>
<h3>${text.helpSampleSQL}</h3>
<table><tr><th>${text.helpOperations}</th><th>${text.helpStatements}</th></tr>
<tr><td><a href="javascript:set('DROP TABLE IF EXISTS TEST;\rCREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));\rINSERT INTO TEST VALUES(1, \'Hello\');\rINSERT INTO TEST VALUES(2, \'World\');\rSELECT * FROM TEST ORDER BY ID;\rUPDATE TEST SET NAME=\'Hi\' WHERE ID=1;\rDELETE FROM TEST WHERE ID=2;');"> 
    ${text.helpDropTable}<br />
    ${text.helpCreateTable}<br />
    &nbsp;&nbsp;${text.helpWithColumnsIdName}<br />
    ${text.helpAddRow}<br />
    ${text.helpAddAnotherRow}<br />
    ${text.helpQuery}<br />
    ${text.helpUpdate}<br />
    ${text.helpDeleteRow}
</a></td><td>
    DROP TABLE IF EXISTS TEST;<br />
    CREATE TABLE TEST(ID INT PRIMARY KEY,<br />
    &nbsp;&nbsp; NAME VARCHAR(255));<br />
    INSERT INTO TEST VALUES(1, 'Hello');<br />
    INSERT INTO TEST VALUES(2, 'World');<br />
    SELECT * FROM TEST ORDER BY ID;<br />
    UPDATE TEST SET NAME='Hi' WHERE ID=1;<br />
    DELETE FROM TEST WHERE ID=2;
</td></tr>
</table>
<h3>${text.helpAddDrivers}</h3>
<p>
${text.helpAddDriversText}
</p><p>
${text.helpAddDriversOnlyJava}
</p>

</div>

<table id="h2auto" class="autoComp"><tbody></tbody></table>
 
</body></html>