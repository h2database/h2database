<!-- 
Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
Initial Developer: H2 Group
-->
<?
$spam2 = ("false" == $spam) ? "" : "SPAM";
$now = date('Y-m-d H:i:s');
$body = "Email: $email
Message:
$text
";
$headers = 'From: newsletter@h2database.com' . "\r\n" .
   'X-Mailer: PHP/' . phpversion();
mail("dbsupport@h2database.com", "[H2 Newsletter] $now $spam2", $body, $headers);
?>
<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8"><title>
H2 Database
</title><link rel="stylesheet" type="text/css" href="../stylesheet.css"></head><body>
<table class="content"><tr class="content"><td class="content"><div class="contentDiv">

<h2>The email was sent successfully</h2>
You have subscribed to the H2 newsletter. 
Your email addresses will only be used in this context.

</div></td></tr></table></body></html>