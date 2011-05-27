<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!--
Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html><head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <title>${text.login.title}</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
    <script type="text/javascript">
        if(self != top) {
            top.location = "login.jsp";
        }
    </script>
</head>
<body style="margin: 20px">
    <form name="login" method="post" action="login.do?jsessionid=${sessionId}" id="login">
    <p>                    <select name="language" size="1"
                        onchange="javascript:document.location='index.do?jsessionid=${sessionId}&amp;language='+login.language.value;"
                    >
                    ${languageCombo}
                    </select>
     &nbsp;&nbsp; <a href="admin.do?jsessionid=${sessionId}">${text.login.goAdmin}</a>
<!--
     &nbsp;&nbsp; <a href="tools.jsp?jsessionid=${sessionId}">${text.login.goTools}</a>
-->
     &nbsp;&nbsp; <a href="help.jsp?jsessionid=${sessionId}">${text.a.help}</a>
     </p>
        <table class="login" cellspacing="0" cellpadding="0">
            <tr class="login">
                <th class="login">${text.login.login}</th>
                <th class="login"></th>
            </tr>
            <tr><td  class="login" colspan="2"></td></tr>
            <tr class="login">
                <td class="login">${text.login.savedSetting}:</td>
                <td class="login">
                    <select name="setting" size="1"
                        style="width:300px"
                        onchange="javascript:document.location='index.do?jsessionid=${sessionId}&amp;setting='+login.setting.value;"
                    >
                    ${settingsList}
                    </select>
                </td>
            </tr>
            <tr class="login">
                <td class="login">${text.login.settingName}:</td>
                <td class="login">
                    <input type="text" name="name" value="${name}" style="width:200px;" />
                    <input type="button" class="button" value="${text.login.save}" onclick="javascript:document.login.action='settingSave.do?jsessionid=${sessionId}';submit()" />
                    <input type="button" class="button" value="${text.login.remove}" onclick="javascript:document.login.action='settingRemove.do?jsessionid=${sessionId}';submit()" />
                </td>
            </tr>
            <tr class="login">
                <td class="login" colspan="2">
                    <hr />
                </td>
            </tr>
            <tr class="login">
                <td class="login">${text.login.driverClass}:</td>
                <td class="login"><input type="text" name="driver" value="${driver}" style="width:300px;" /></td>
            </tr>
            <tr class="login">
                <td class="login">${text.login.jdbcUrl}:</td>
                <td class="login"><input type="text" name="url" value="${url}" style="width:300px;" /></td>
            </tr>
            <tr class="login">
                <td class="login">${text.a.user}:</td>
                <td class="login"><input type="text" name="user" value="${user}" style="width:200px;" /></td>
            </tr>
            <tr class="login">
                <td class="login">${text.a.password}:</td>
                <td class="login"><input type="password" name="password" value="" style="width:200px;" /></td>
            </tr>
            <tr class="login">
                <td class="login"></td>
                <td class="login">
                    <input type="submit" class="button" value="${text.login.connect}" />
                    &nbsp;
                    <input type="button" class="button" value="${text.login.testConnection}" onclick="javascript:document.login.action='test.do?jsessionid=${sessionId}';submit()" />
                    <br />
                    <br />
                </td>
            </tr>
        </table>
        <br />
        <p class="error">${error}</p>
    </form>
</body></html>