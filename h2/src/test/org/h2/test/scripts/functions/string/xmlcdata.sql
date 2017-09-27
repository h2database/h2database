CALL XMLCDATA('<characters>');
> '<![CDATA[<characters>]]>'
> --------------------------
> <![CDATA[<characters>]]>
> rows: 1

CALL XMLCDATA('special text ]]>');
> 'special text ]]&gt;'
> ---------------------
> special text ]]&gt;
> rows: 1

