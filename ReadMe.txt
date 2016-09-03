========================================================================
    PstReader application
========================================================================

(c) 2016 Dr. Daniel Azzopardi 

Purpose: 
Strip pst archives of attachments, and submit email messages to a prope-
rly configured Solr instance for indexing.

Uses:
- Solr: http://lucene.apache.org/solr/
- fairport (Terry Mahaffey's GNU port of his earlier pstsdk): 
	https://github.com/terrymah/Fairport
- Boost: http://www.boost.org/

Usage:
Run executible with no arguments for usage instructions. Commands can be
provided in unix (-c) or Windows (/c) form, in any order.

Notes:
Regex filtering for extensions is case insensitive, whilst that for fol-
ders is case sensitive.
Solr needs to be configured with the following fields, all of which are
required:

"string" type: id, pstfile
"text" type: subject, sender, to, filenames, class, body
"long" type: attachments
"tdate" type: created

The relevant portion of schema should look something like (note folding
of subject and body into "text", and pstfile into psts for facetting):

   <field name="_version_" type="long" indexed="true" stored="true"/>
   <field name="id" type="string" indexed="true" stored="true" 
	required="true" multiValued="false" />
   <field name="pstfile" type="string" indexed="true" stored="true"/>  
   <field indexed="true" name="psts" stored="false" type="string"/>
   <copyfield dest="psts" source="pstfile"/>
   <field name="subject" type="text_en" indexed="true" stored="true"/>
   <field name="sender" type="text_en" indexed="true" stored="true"/>
   <field name="created" type="tdate" indexed="true" stored="true"/>
   <field name="to" type="text_en" indexed="true" stored="true"/>
   <field name="attachments" type="long" indexed="true" stored="true"
	/>
   <field name="filenames" type="text_en" indexed="true" stored="true"
	/>
   <field name="class" type="text_en" indexed="true" stored="true"/>
   <field name="body" type="text_en" indexed="true" stored="true"/>
   <field name="text" type="text_en" indexed="true" stored="false" 
	multiValued="true"/>
   <field name="text_rev" type="text_general_rev" indexed="true" 
	stored="false" multiValued="true"/>
 <uniqueKey>id</uniqueKey>
   <copyField source="subject" dest="text"/>
   <copyField source="body" dest="text"/>