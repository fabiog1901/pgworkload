<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<html><head><title>Python: module simplefaker</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head><body bgcolor="#f0f0f8">

<table width="100%" cellspacing=0 cellpadding=2 border=0 summary="heading">
<tr bgcolor="#7799ee">
<td valign=bottom>&nbsp;<br>
<font color="#ffffff" face="helvetica, arial">&nbsp;<br><big><big><strong>simplefaker</strong></big></big></font></td
><td align=right valign=bottom
><font color="#ffffff" face="helvetica, arial"><a href=".">index</a><br><a href="file:/Users/fabio/projects/pgworkload/src/pgworkload/simplefaker.py">/Users/fabio/projects/pgworkload/src/pgworkload/simplefaker.py</a></font></td></tr></table>
    <p></p>
<p>
<table width="100%" cellspacing=0 cellpadding=2 border=0 summary="section">
<tr bgcolor="#aa55cc">
<td colspan=3 valign=bottom>&nbsp;<br>
<font color="#ffffff" face="helvetica, arial"><big><strong>Modules</strong></big></font></td></tr>
    
<tr><td bgcolor="#aa55cc"><tt>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</tt></td><td>&nbsp;</td>
<td width="100%"><table width="100%" summary="list"><tr><td width="25%" valign=top><a href="csv.html">csv</a><br>
<a href="datetime.html">datetime</a><br>
</td><td width="25%" valign=top><a href="logging.html">logging</a><br>
<a href="multiprocessing.html">multiprocessing</a><br>
</td><td width="25%" valign=top><a href="os.html">os</a><br>
<a href="pandas.html">pandas</a><br>
</td><td width="25%" valign=top><a href="random.html">random</a><br>
<a href="uuid.html">uuid</a><br>
</td></tr></table></td></tr></table><p>
<table width="100%" cellspacing=0 cellpadding=2 border=0 summary="section">
<tr bgcolor="#ee77aa">
<td colspan=3 valign=bottom>&nbsp;<br>
<font color="#ffffff" face="helvetica, arial"><big><strong>Classes</strong></big></font></td></tr>
    
<tr><td bgcolor="#ee77aa"><tt>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</tt></td><td>&nbsp;</td>
<td width="100%"><dl>
<dt><font face="helvetica, arial"><a href="builtins.html#object">builtins.object</a>
</font></dt><dd>
<dl>
<dt><font face="helvetica, arial"><a href="simplefaker.html#SimpleFaker">SimpleFaker</a>
</font></dt></dl>
</dd>
</dl>
 <p>
<table width="100%" cellspacing=0 cellpadding=2 border=0 summary="section">
<tr bgcolor="#ffc8d8">
<td colspan=3 valign=bottom>&nbsp;<br>
<font color="#000000" face="helvetica, arial"><a name="SimpleFaker">class <strong>SimpleFaker</strong></a>(<a href="builtins.html#object">builtins.object</a>)</font></td></tr>
    
<tr bgcolor="#ffc8d8"><td rowspan=2><tt>&nbsp;&nbsp;&nbsp;</tt></td>
<td colspan=2><tt><a href="#SimpleFaker">SimpleFaker</a>(seed:&nbsp;float&nbsp;=&nbsp;None,&nbsp;csv_max_rows:&nbsp;int&nbsp;=&nbsp;100000)<br>
&nbsp;<br>
Pseudo-random&nbsp;data&nbsp;generator&nbsp;based&nbsp;on&nbsp;<br>
the&nbsp;random.Random&nbsp;class.<br>&nbsp;</tt></td></tr>
<tr><td>&nbsp;</td>
<td width="100%">Methods defined here:<br>
<dl><dt><a name="SimpleFaker-__init__"><strong>__init__</strong></a>(self, seed: float = None, csv_max_rows: int = 100000)</dt><dd><tt>Initialize&nbsp;self.&nbsp;&nbsp;See&nbsp;help(type(self))&nbsp;for&nbsp;accurate&nbsp;signature.</tt></dd></dl>

<dl><dt><a name="SimpleFaker-division_with_modulo"><strong>division_with_modulo</strong></a>(self, total: int, divider: int)</dt><dd><tt>Split&nbsp;a&nbsp;number&nbsp;into&nbsp;chunks.<br>
Eg:&nbsp;total=10,&nbsp;divider=3&nbsp;returns&nbsp;[3,3,4]&nbsp;<br>
&nbsp;<br>
Args:<br>
&nbsp;&nbsp;&nbsp;&nbsp;total&nbsp;(int):&nbsp;The&nbsp;number&nbsp;to&nbsp;divide<br>
&nbsp;&nbsp;&nbsp;&nbsp;divider&nbsp;(int):&nbsp;the&nbsp;count&nbsp;of&nbsp;chunks<br>
&nbsp;<br>
Returns:<br>
&nbsp;&nbsp;&nbsp;&nbsp;(list):&nbsp;the&nbsp;list&nbsp;of&nbsp;the&nbsp;individual&nbsp;chunks</tt></dd></dl>

<dl><dt><a name="SimpleFaker-generate"><strong>generate</strong></a>(self, load: dict, exec_threads: int, csv_dir: str, delimiter: str, compression: str)</dt><dd><tt>Generate&nbsp;the&nbsp;CSV&nbsp;datasets<br>
&nbsp;<br>
Args:<br>
&nbsp;&nbsp;&nbsp;&nbsp;load&nbsp;(dict):&nbsp;the&nbsp;data&nbsp;generation&nbsp;definition<br>
&nbsp;&nbsp;&nbsp;&nbsp;exec_threads&nbsp;(int):&nbsp;count&nbsp;of&nbsp;processes&nbsp;for&nbsp;parallel&nbsp;execution<br>
&nbsp;&nbsp;&nbsp;&nbsp;csv_dir&nbsp;(str):&nbsp;destination&nbsp;directory&nbsp;for&nbsp;the&nbsp;CSV&nbsp;files<br>
&nbsp;&nbsp;&nbsp;&nbsp;delimiter&nbsp;(str):&nbsp;field&nbsp;delimiter<br>
&nbsp;&nbsp;&nbsp;&nbsp;compression&nbsp;(str):&nbsp;the&nbsp;compression&nbsp;format&nbsp;(gzip,&nbsp;zip,&nbsp;None..)</tt></dd></dl>

<dl><dt><a name="SimpleFaker-worker"><strong>worker</strong></a>(self, generators: tuple, iterations: int, basename: str, col_names: list, sort_by: list, separator: str, compression: str)</dt><dd><tt>Process&nbsp;worker&nbsp;function&nbsp;to&nbsp;generate&nbsp;the&nbsp;data&nbsp;in&nbsp;a&nbsp;multiprocessing&nbsp;env<br>
&nbsp;<br>
Args:<br>
&nbsp;&nbsp;&nbsp;&nbsp;generators&nbsp;(tuple):&nbsp;the&nbsp;<a href="#SimpleFaker">SimpleFaker</a>&nbsp;data&nbsp;gen&nbsp;objects&nbsp;<br>
&nbsp;&nbsp;&nbsp;&nbsp;iterations&nbsp;(int):&nbsp;count&nbsp;of&nbsp;rows&nbsp;to&nbsp;generate<br>
&nbsp;&nbsp;&nbsp;&nbsp;basename&nbsp;(str):&nbsp;the&nbsp;basename&nbsp;of&nbsp;the&nbsp;output&nbsp;csv&nbsp;file<br>
&nbsp;&nbsp;&nbsp;&nbsp;col_names&nbsp;(list):&nbsp;the&nbsp;csv&nbsp;column&nbsp;names,&nbsp;used&nbsp;for&nbsp;sorting<br>
&nbsp;&nbsp;&nbsp;&nbsp;sort_by&nbsp;(list):&nbsp;the&nbsp;column&nbsp;to&nbsp;sort&nbsp;by<br>
&nbsp;&nbsp;&nbsp;&nbsp;separator&nbsp;(str):&nbsp;the&nbsp;field&nbsp;delimiter&nbsp;in&nbsp;the&nbsp;CSV&nbsp;file<br>
&nbsp;&nbsp;&nbsp;&nbsp;compression&nbsp;(str):&nbsp;the&nbsp;compression&nbsp;format&nbsp;(gzip,&nbsp;zip,&nbsp;None..)</tt></dd></dl>

<hr>
Data descriptors defined here:<br>
<dl><dt><strong>__dict__</strong></dt>
<dd><tt>dictionary&nbsp;for&nbsp;instance&nbsp;variables&nbsp;(if&nbsp;defined)</tt></dd>
</dl>
<dl><dt><strong>__weakref__</strong></dt>
<dd><tt>list&nbsp;of&nbsp;weak&nbsp;references&nbsp;to&nbsp;the&nbsp;object&nbsp;(if&nbsp;defined)</tt></dd>
</dl>
<hr>
Data and other attributes defined here:<br>
<dl><dt><strong>Abc</strong> = &lt;class 'simplefaker.SimpleFaker.Abc'&gt;</dl>

<dl><dt><strong>Bool</strong> = &lt;class 'simplefaker.SimpleFaker.Bool'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;random&nbsp;boolean&nbsp;(0,&nbsp;1)</tt></dl>

<dl><dt><strong>Bytes</strong> = &lt;class 'simplefaker.SimpleFaker.Bytes'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;random&nbsp;byte&nbsp;array</tt></dl>

<dl><dt><strong>Choice</strong> = &lt;class 'simplefaker.SimpleFaker.Choice'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;1&nbsp;item&nbsp;from&nbsp;a&nbsp;list</tt></dl>

<dl><dt><strong>Costant</strong> = &lt;class 'simplefaker.SimpleFaker.Costant'&gt;<dd><tt>Iterator&nbsp;that&nbsp;counts&nbsp;upward&nbsp;forever.</tt></dl>

<dl><dt><strong>Date</strong> = &lt;class 'simplefaker.SimpleFaker.Date'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;Date&nbsp;string</tt></dl>

<dl><dt><strong>Float</strong> = &lt;class 'simplefaker.SimpleFaker.Float'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;random&nbsp;float&nbsp;number</tt></dl>

<dl><dt><strong>Integer</strong> = &lt;class 'simplefaker.SimpleFaker.Integer'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;random&nbsp;integer</tt></dl>

<dl><dt><strong>Json</strong> = &lt;class 'simplefaker.SimpleFaker.Json'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;simple&nbsp;json&nbsp;string</tt></dl>

<dl><dt><strong>Sequence</strong> = &lt;class 'simplefaker.SimpleFaker.Sequence'&gt;<dd><tt>Iterator&nbsp;that&nbsp;counts&nbsp;upward&nbsp;forever.</tt></dl>

<dl><dt><strong>String</strong> = &lt;class 'simplefaker.SimpleFaker.String'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;truly&nbsp;random&nbsp;string&nbsp;of&nbsp;ascii&nbsp;characters</tt></dl>

<dl><dt><strong>Time</strong> = &lt;class 'simplefaker.SimpleFaker.Time'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;Time&nbsp;string</tt></dl>

<dl><dt><strong>Timestamp</strong> = &lt;class 'simplefaker.SimpleFaker.Timestamp'&gt;<dd><tt>Iterator&nbsp;that&nbsp;yields&nbsp;a&nbsp;Timestamp&nbsp;string</tt></dl>

<dl><dt><strong>UUIDv4</strong> = &lt;class 'simplefaker.SimpleFaker.UUIDv4'&gt;<dd><tt>Iterator&nbsp;thar&nbsp;yields&nbsp;a&nbsp;UUIDv4</tt></dl>

</td></tr></table></td></tr></table>
</body></html>