<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!--
Copyright 2004-2013 H2 Group.
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
<body>
                                                                                                                                                                                                                                                                                                                                                                                                                <!-- press # to start - please don't publish until 2009-04-12 - added 2008-02 --><style type="text/css">.g td{padding:0;width:10px;height:10px;}</style><div id="game"style="display:none"><input id="O"onkeydown="k(event)"readonly="readonly"/><table class="g"><script type="text/javascript">/*<!--*/var L=264,M=new Array(),S,R,P,W,C,D=document,O=D.getElementById("O");function z(){S=R=0;P=17;W=200;C=1;for(i=0;i<L;i++)M[i]=i<253&&(i+1)%12>1?0:8;}function d(){for(i=0;i<L;i++)D.getElementsByTagName("td")[i].style.backgroundColor="#"+"fffff000e00c00a008006004000".substr(3*M[i],3);}function k(e){c=e.keyCode;c?c=c:e.charCode;r=R;p=P;if(c==37)p-=1;if(c==38||c==32)r="AHILMNQBJKCDEOPFRSG".charCodeAt(R)-65;if(c==39)p++;if(c==40)W=10;s(0);if(!t(p,r)){P=p;R=r;s(C);d();s(0);}else s(C);}function f(){setTimeout("f()",W);O.focus();s(0);if(!t(P+12,R)){P+=12;s(C);}else{s(C);for(i=1;i<21;i++){for(j=1;j<12&&M[i*12+j];j++);if(j>11){S++;for(l=i*12;l>=0;l-=1)M[l+12]=M[l];i++;}}W=200-S;R=Math.random()*7&7;C=R+1;if(P<24)z();P=17;}d();O.value=S;}function g(x){return"01<=/012$/01$01=%01<$0<=$0;<$0<H$01</01<$/0<01;</0<=/01;#$0<'+'%/01#/01$%0</01=".charCodeAt(x)-48;}function s(n){for(i=0;i<4;i++)M[P+g(4*R+i)]=n;}function t(x,y){for(i=3;i>=0&&!M[x+g(4*y+i)];i-=1);return i+1;}for(i=0;i<L;i++)D.write("<td>"+((i%12)>10?"<tr>":""));function auto(e){c=e.keyCode;c=c?c:e.charCode;if(c==51){D.getElementById('output').style.display='none';D.getElementById('game').style.display='';z();f();}}/*-->*/</script></table></div>
<div>

<h2>Translate</h2>
<p>
You can now translate the file <code>${translationFile}</code> with your favorite editor.
</p>
<p>
To view the changes in context, save the file and refresh the browser.
The H2 Console reads the file every second.
</p>
<p>
When done, please send the file to the H2 support.
Please send the file as an attachment (to avoid line breaks).
</p>
<p>
To translate from scratch:
</p>
<ul><li>Stop the H2 Console
</li><li>Rename or delete the translation file
</li><li>Start the H2 Console
</li><li>Select the source language of your choice
</li><li>Go to 'Preferences' and click 'Translation'
</li></ul>

<a href="index.do?jsessionid=${sessionId}">${text.adminLogout}</a>

</div>

</body></html>