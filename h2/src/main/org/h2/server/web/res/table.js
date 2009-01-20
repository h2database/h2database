/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

addEvent(window, "load", initSort);

function addEvent(elm, evType, fn, useCapture) {
    // addEvent and removeEvent
    // cross-browser event handling for IE5+,  NS6 and Mozilla
    // By Scott Andrew
    if (elm.addEventListener){
        elm.addEventListener(evType, fn, useCapture);
        return true;
    } else if (elm.attachEvent){
        var r = elm.attachEvent("on"+evType, fn);
        return r;
    } else {
        alert("Handler could not be added");
    }
}

function initSort() {
    if(document.getElementById('editing') != undefined) {
        // don't allow sorting while editing
        return;
    }
    var tables = document.getElementsByTagName("table");
    for (var i=0; i<tables.length; i++) {
        table = tables[i];
        if (table.rows && table.rows.length > 0) {
            var header = table.rows[0];
            for(var j=0;j<header.cells.length;j++) {
                var cell = header.cells[j];
                var text = cell.innerHTML;
                cell.innerHTML = '<a href="#" style="text-decoration: none;" class="sortHeader" onclick="resortTable(this);">'+text+'<span class="sortArrow">&nbsp;&nbsp;</span></a>';
            }
        }
    }
}

function editRow(row, session, write, undo) {
    var table = document.getElementById('editTable');
    var y = row < 0 ? table.rows.length - 1 : row;
    var i;
    for(i=1; i<table.rows.length; i++) {
        var cell = table.rows[i].cells[0];
        if(i == y) {
            var edit = '<img width=16 height=16 src="ico_ok.gif" onclick="editOk('+row+')" onmouseover = "this.className =\'icon_hover\'" onmouseout = "this.className=\'icon\'" class="icon" alt="'+write+'" title="'+write+'" border="1"/>';
            var undo = '<img width=16 height=16 src="ico_undo.gif" onclick="editCancel('+row+')" onmouseover = "this.className =\'icon_hover\'" onmouseout = "this.className=\'icon\'" class="icon" alt="'+undo+'" title="'+undo+'" border="1"/>';
            cell.innerHTML = edit + undo;
        } else {
            cell.innerHTML = '';
        }
    }
    var cells = table.rows[y].cells;
    for(i=1; i<cells.length; i++) {
        var cell = cells[i];
        var text = getInnerText(cell);
        cell.innerHTML = '<input type="text" name="r'+row+'c' + i + '" value="'+text+'" size="' + (text.length+5) + '" onkeydown="return editKeyDown(' + row + ', this, event)" />';
    }
}

function editCancel(row) {
    var editing = document.getElementById('editing');
    editing.row.value = row;
    editing.op.value='3';
    editing.submit();
}

function editOk(row) {
    var editing = document.getElementById('editing');
    editing.row.value = row;
    editing.op.value='1';
    editing.submit();
}

function editKeyDown(row, object, event) {
    var key=event.keyCode? event.keyCode : event.charCode;
    if(key == 46 && event.ctrlKey) {
        // ctrl + delete
        object.value = 'null';
        return false;
    } else if(key == 13) {
        editOk(row);
        return false;
    } else if(key == 27) {
        editCancel(row);
        return false;
    }
}

function getInnerText(el) {
    if (typeof el == "string") return el;
    if (typeof el == "undefined") { return el };
    if (el.innerText) {
        // not needed but it is faster
        return el.innerText;
    }
    var str = "";
    var cs = el.childNodes;
    var l = cs.length;
    for (var i = 0; i < l; i++) {
        switch (cs[i].nodeType) {
        case 1: //ELEMENT_NODE
            str += getInnerText(cs[i]);
            break;
        case 3:    //TEXT_NODE
            str += cs[i].nodeValue;
            break;
        }
    }
    return str;
}

function resortTable(link) {
    // get the span
    var span;
    for (var ci=0;ci<link.childNodes.length;ci++) {
        if (link.childNodes[ci].tagName && link.childNodes[ci].tagName.toLowerCase() == 'span') {
            span = link.childNodes[ci];
        }
    }
    var spantext = getInnerText(span);
    var td = link.parentNode;
    var column = td.cellIndex;
    var table = getParent(td,'TABLE');

    if (table.rows.length <= 1) return;

    // detect sort type
    var sortNumeric = false;
    var x = getInnerText(table.rows[1].cells[column]);
    if (x.match(/^[\d\.]+$/)) {
        sortNumeric = true;
    }
    var newRows = new Array();
    var rows = table.rows;
    for (i=1; i<rows.length; i++) {
        var o = new Object();
        o.data = rows[i];
        o.id = i;
        if(sortNumeric) {
            o.sort = parseFloat(getInnerText(o.data.cells[column]));
            if (isNaN(o.sort)) o.sort = 0;
        } else {
            o.sort = getInnerText(o.data.cells[column]);
        }
        newRows[i-1] = o;
    }
    newRows.sort(sortCallback);
    var arrow;
    if (span.getAttribute("sortDir") == 'down') {
        arrow = '&nbsp;<img src="sort_up.gif" with=7 height=10 border=0/>';
        newRows.reverse();
        span.setAttribute('sortDir','up');
    } else {
        arrow = '&nbsp;<img src="sort_down.gif" with=7 height=10 border=0/>';
        span.setAttribute('sortDir','down');
    }

    // we appendChild rows that already exist to the tbody,
    // so it moves them rather than creating new ones
    var body = table.tBodies[0];
    for (i=0; i<newRows.length; i++) {
        body.appendChild(newRows[i].data);
    }

    // delete any other arrows there may be showing
    var allSpans = document.getElementsByTagName("span");
    for (var i=0;i<allSpans.length;i++) {
        if (allSpans[i].className == 'sortArrow') {
            // in the same table as us?
            if (getParent(allSpans[i],"table") == getParent(link,"table")) {
                allSpans[i].innerHTML = '&nbsp;&nbsp;';
            }
        }
    }
    span.innerHTML = arrow;
}

function getParent(el, pTagName) {
    if (el == null) return null;
    else if (el.nodeType == 1 && el.tagName.toLowerCase() == pTagName.toLowerCase())    {
        // Gecko bug, supposed to be uppercase
        return el;
    } else {
        return getParent(el.parentNode, pTagName);
    }
}

function sortCallback(ra, rb) {
    return (ra.sort==rb.sort) ? (ra.id-rb.id) : (ra.sort<rb.sort ? -1 : 1);
}

