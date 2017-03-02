# GroupConcatWithSorting
This is a udaf that helps do a concat on one column ordered by another column.
The delimiter to be used in concat has to be passed in the function.
A sample query looks like below:

 select eid, base64(encode(group_concat_with_sorting(id,name,"\n"), 'utf-8'))  from testorder group by eid;
 
 Here eid is used to group the rows, all the names are concatinated in with new line as the delimiter.
 The order of names depends upon the value of id.
 Here base64 encoding is used so that the new lines may not cause distruption of layout in the hive table.
 Syntax of the function is:
 
 group_concat_with_sorting(column_for_ordering_must_be_integer,column_for_concat,delimiter);
 hive-exec-1.1.0 jar has to be in the build path while creating the udaf jar
 
 Sample command to create the temporary function:
 
 create temporary function group_concat_with_sorting() as 'com.hiveudaf.groupconcat.GroupConcatWithSort';

 
