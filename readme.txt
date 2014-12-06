		**Assignment 3: Record Manager**
****************************************************

					**Team:knariset,asachith,plal1**
					********************************

CONTENTS
*********
1)Instructions to run the code
2)Additional functionalities
3)Description of functions used
4)Additional Test Cases and Additional error checks
5)Implementation

*****************************************************************

1)Instructions to run the code
*******************************

For executing mandatory and additional test cases:

1) In the terminal,navigate to the assignment directory.

2) Type: 
	make -f makefile

**** For random scans of test cases ***
3) ./assign1

**** For multiple scans of test cases and tombstone check***
4) ./assign2

**** For testing test expression ***
5) ./test_expr


*****************************************************************

2)Additional functionalities
**************************
1) We have implemented as an optional extension,we have implemented TOMBSTONE,which is used for marking the address of the deleted record.


*****************************************************************

3)Description of functions used
********************************

The table manager structure pointer is stored to Mgmtdata of the structure provided and use it as we did in Storage_mgr.c and Buffer_mgr.c 

	Function :initRecordManager
	-------------------------
This function is used to Initialize the record manager.Because there is no global data structure, We are doing nothing.


	Function :shutdownRecordManager
	----------------------------
	
This function is used to shutdown the record manager. Because there is nothing initialized, We are doing nothing.

	
	Function : createTable
	-------------------------

1)Open the buffer pool after creating the page file (With same name as the table name passed).
2)Pin the pages.
3)Assign the table manager attributes and the attributes to the relation.
4)After storing,copy the build to page and write it back to disk marking it as dirty.
5)Return the status.

 
	Function : openTable
	---------------------

1)Open the buffer pool.	
2)Assign the page and bm handlers.
3)Deserialize data and unpin the page.
4)Return the status.
The tab_mgr is assigned to mgmtdata.


	Function : closeTable
	---------------------
	
1)Shut down the buffer pool.
2)Free the allocated table and pool pointers.
3)Return the status.


	Function : deleteTable
	--------------------
This function destroys the relative page file and return the return code.


	Function : getNumTuples 
	------------------

This function is used to get the number of tuples.
The mgmtdata is used to get the number of tuples.

**Functions pertaining to handling records in a table**


	Function : insertRecord
	----------------------------

1)Create a table manager and get the reference from mgmtdata.
2)Assign pool and page pointers to table manager
3)Skip the first page as we are storing table info in that. 
4)We need to find out a slot for inserting that record.
5)After finding a slot(if existing),then updating the record
6)If not,then append a block to create a slot and append the record at the end.
4)In both the cases,we need to register that in buffer manager and copy content passed,mark as dirty and unpin it to write it back to disk.
5)Return the status (If failed return RC_RM_INSERT_FAILED).


	Function : deleteRecord
	------------------------

1)Create a table manager and get the reference from mgmtdata.
2)Assign pool and page pointers to table manager
3)Pin the page/record to red.
4)Mark the address of the deleted with a tombstone with a Leading 'zero'.
5)Unpin the page.
6)Return the status (If failed return RC_RM_DELETE_FAILED). 


	Function : updateRecord
	------------------------

1)Create a table manager and get the reference from mgmtdata.
2)Assign pool and page pointers to table manager
3)Get the record size.
4)Move the offset built to the record we need to update.
5)Return the status (If failed return RC_RM_UPDATE_FAILED).


	Function: getRecord
	-----------------------
	
1)Assign the record parameters.
2)Based on RID we need to findout the slot and page of the record.
3)Pull the page tagged from disk and pin it.
4)Move the offset so that we will find out required record (Based on slot and record size)
5)Return the record to rec_data.
6)Unpin it and as it isnt dirty we dont need to write it back to disk.
7)Return status based on return code (If failed return RC_RM_GET_FAILED).


**Functions pertaining to Scanning **
	
	Function : startScan
	------------------------
1)Assign values to the Scanning Manager.
2)Set scanning manager variables to page 1,slot 0(Page 0 is for cover).
3)Refer it back to mgmtdata.
4)Return the status.

	Function : next
	---------------
1)Assign values to the Scanning Manager from earlier sanning managers mgmtdata.
2)As we set scanning manager variables to start of table,we need to increment based on record size and move on.
3)So create an offset first for slot and then for page.
3)Scan all pages and all slot till a matching tuple is found based on the expression passed.
4)If no matched tuples at end then return RC_RM_NO_MORE_TUPLES.
5)Return the status.

	Function : closeScan
	--------------------
1)Assign values to the Scanning Manager from earlier sanning managers mgmtdata.
2)free expression structure and free scanning manager datastrcture and reciding mgmtdata.
3)Return the status.


**Functions pertaining to schemas**

	Function : getRecordSize
	----------------------
This function is used to get the size of a record in a given schema.
1)Based on no of attributes,calculate the size of record
2)Return the size of record 


	Function : createSchema
	--------------------

1)Allocate a new schema.
2)Assign all attributes of the schema passed to createSchema function.
3)Return the reference to schema.

	Function : freeSchema
	--------------------
1)This function is used to free the space occupied by each schema.
2)Free all the schema parameters that were built in createSchema (Attributes,Keys etc.,).
3)Return the status.

	Function : createRecord
	--------------------

1)This function is used to create a record.
2)Allocate memory to the record based on record size.
3)Allocate memory to the record data based on schema size.
4)Return the status.


	Function : freeRecord 
	--------------------
1)This function is used to free the memory that had been allocated to a record.
2)Free the record data.
3)Return the status.


	Function : getAttr 
	------------------
1)This function is used to get an attribute.
2)Get the position of data.
3)Get the value of record->data at attrNum to result.
4)Get the datatype of the corresponding value (Switch used to get the attribute type)
6)Copy the reference of the temporary Value to Value
7)Return the status.


	Function : setAttr
	------------------
1)This function is used to set an attribute.
2)Get the value from the parameter.
3)Go to the record element at attrNum
4)Set or update the value at the position
5)Return the status



4) Additional Test Cases and Additional Error Checks
----------------------------------------------------

	Test cases
	-----------------
We have included additional test cases for executing the following functions.

->
->
->  
	Error Checks
	-----------------
	
We have included the following additional error checks

->  
->
->
->

*****************************************************************

5) Implementation
-----------------

The implementation versions with the descriptions are as follows.

Version 	   Date 	   Description				
----------    --------   ------------
version 1.0   
version 1.1  

version 1.2  
version 1.3  
version 1.4   
version 1.5  
version 1.6  
version 1.7


----------------------------------------------------------------------------------------------------------------------------------
 
