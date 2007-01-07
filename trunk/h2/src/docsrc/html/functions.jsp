<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!-- 
Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
Initial Developer: H2 Group
-->
<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8"><title>
Functions
</title><link rel="stylesheet" type="text/css" href="stylesheet.css">
<script type="text/javascript" src="navigation.js"></script>
</head><body onload="frameMe();">
<table class="content"><tr class="content"><td class="content"><div class="contentDiv">

<h1>Functions</h1>

<h2>Aggregate Functions</h2>
<c:forEach var="item" items="functionsAggregate">
    <a href="#sql${item.id}">${item.topic}</a><br>
</c:forEach>

<h2>Numeric Functions</h2>
<c:forEach var="item" items="functionsNumeric">
    <a href="#sql${item.id}">${item.topic}</a><br>
</c:forEach>

<h2>String Functions</h2>
<c:forEach var="item" items="functionsString">
    <a href="#sql${item.id}">${item.topic}</a><br>
</c:forEach>

<h2>Time and Date Functions</h2>
<c:forEach var="item" items="functionsTimeDate">
    <a href="#sql${item.id}">${item.topic}</a><br>
</c:forEach>

<h2>System Functions</h2>
<c:forEach var="item" items="functionsSystem">
    <a href="#sql${item.id}">${item.topic}</a><br>
</c:forEach>

<c:forEach var="item" items="functionsAll">
<br>
<a name="sql${item.id}"></a><h3>${item.topic}</h3>
<pre>
${item.syntax}
</pre>
<p>
${item.text}
</p>
<b>Example:</b><br>
${item.example}
<br>
</c:forEach>

</div></td></tr></table></body></html>