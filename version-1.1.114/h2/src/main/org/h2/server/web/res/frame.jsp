<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Frameset//EN">
<!--
Copyright 2004-2009 H2 Group.
Multiple-Licensed under the H2 License, Version 1.0,
and under the Eclipse Public License, Version 1.0
(http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <title>${text.a.title}</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
</head>
<frameset cols="*" rows="36,*" frameborder="2" framespacing="4" border="4" >
    <frame noresize="noresize" frameborder="0" marginheight="0" marginwidth="0" src="header.jsp?jsessionid=${sessionId}" name="header" scrolling="no" />
    <frameset cols="200,*" rows="*" frameborder="2" framespacing="4" border="4" >
        <frame frameborder="0" marginheight="0" marginwidth="0" src="tables.do?jsessionid=${sessionId}" name="h2menu" />
        <frameset  rows="180,*" frameborder="2" framespacing="4" border="4" >
            <frame frameborder="0" marginheight="0" marginwidth="0" src="query.jsp?jsessionid=${sessionId}" name="h2query" scrolling="no" />
            <frame frameborder="0" marginheight="0" marginwidth="0" src="help.jsp?jsessionid=${sessionId}" name="h2result" />
        </frameset>
    </frameset>
</frameset>
<noframes>
<body>
    ${text.a.lynxNotSupported}
</body>
</noframes>
</html>
