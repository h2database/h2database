<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!-- 
Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
Initial Developer: H2 Group
-->
<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8" /><title>
SQL Grammar
</title><link rel="stylesheet" type="text/css" href="stylesheet.css" />
<script type="text/javascript" src="navigation.js"></script>
</head><body onload="frameMe();">
<table class="content"><tr class="content"><td class="content"><div class="contentDiv">

<h1>SQL Grammar</h1>
<h2>Commands (Data Manipulation)</h2>
<c:forEach var="item" items="commandsDML">
    <a href="#sql${item.id}">${item.topic}</a><br />
</c:forEach>
<h2>Commands (Data Definition)</h2>
<c:forEach var="item" items="commandsDDL">
    <a href="#sql${item.id}">${item.topic}</a><br />
</c:forEach>
<h2>Commands (Other)</h2>
<c:forEach var="item" items="commandsOther">
    <a href="#sql${item.id}">${item.topic}</a><br />
</c:forEach>
<h2>Other Grammar</h2>
<c:forEach var="item" items="otherGrammar">
    <a href="#sql${item.id}">${item.topic}</a><br />
</c:forEach>

<c:forEach var="item" items="commands">
<br />
<a name="sql${item.id}"></a><h3>${item.topic}</h3>
<pre>
${item.syntax}
</pre>
<p>
${item.text}
</p>
<b>Example:</b><br />
${item.example}
<br />
</c:forEach>

<c:forEach var="item" items="otherGrammar">
<br />
<a name="sql${item.id}"></a><h3>${item.topic}</h3>
<pre>
${item.syntax}
</pre>
<p>
${item.text}
</p>
<b>Example:</b><br />
${item.example}
<br />
</c:forEach>

</div></td></tr></table></body></html>