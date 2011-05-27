<!-- 
Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
Initial Developer: H2 Group
-->
<?
$now = date('Y-m-d H:i:s');
$body = "Email: $email
Message:
$text
";
$headers = 'From: feedback@h2database.com' . "\r\n" .
   'X-Mailer: PHP/' . phpversion();
$headers = 
mail("dbsupport@h2database.com", "[H2 Feedback] $now", $body, $headers);
?>
<html><head><meta http-equiv="Content-Type" content="text/html;charset=utf-8" /><title>
H2 Database
</title><link rel="stylesheet" type="text/css" href="../stylesheet.css" /></head><body>
<table class="content"><tr class="content"><td class="content"><div class="contentDiv">

<h2>The email was sent successfully</h2>
Thank you for your feedback!

</div></td></tr></table></body></html>