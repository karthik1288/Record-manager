#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "rec_req.h"
#include "buffer_mgr.c"
#include "assumptions.h"

// Design is that I store the table manager structure pointer always to Mgmtdata of the structure provided and use it as I did in Storage_mgr.c 
// Really nothing to implement in init and shutdown
RC initRecordManager (void *mgmtData)
{
	// nothing initialized...So do nothing
	return RC_OK;
}

RC shutdownRecordManager ()
{
	// nothing initialized...So free nothing
	return RC_OK;
}

// When am creating a table am storing page cover to disk and referring whenever needed
RC createTable (char *name, Schema *schema)
{
	RC rc;
	int i=0;
	Table_manager_DS tab_mgr;
	RM_TableData rel;
	// Create a page file
	rc = createPageFile(name);
	// Open a buffer pool after creating a file
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	
	// Initialize created buffer pool	
	rc = initBufferPool(bm, name, 3, RS_FIFO, NULL);
	
	// Pin the pages 
	rc = pinPage(bm,h,i);
	// assign table manager attributes 
	tab_mgr.number_of_Tuples=0;
	tab_mgr.size_of_Schema = Kar_get_Schema_Size(schema->numAttr,schema->keySize);
	
	// assign attributes to relation
	rel.name=name;
	rel.schema=schema;
	rel.mgmtData = &tab_mgr;
	
	// store the build from build from serialize table to 
	char* build=Kar_Serialize_Table_Info(&rel);
	// Copy the build to page and write it back to disk marking it dirty
	memcpy(h->data,build,PAGE_SIZE);
	//copied and marked dirty
	rc = markDirty(bm, h);
    rc = unpinPage(bm, h);
	rc = shutdownBufferPool(bm);
	// Free the build 
	free(build);
	build=NULL;
	// return what expected
	return rc;
}

RC openTable (RM_TableData *rel, char *name)
{
	int i=0;
	RC rc;
	Table_manager_DS* tab_mgr=(Table_manager_DS*)malloc(sizeof(Table_manager_DS));
	// Open buffer pool which is created 
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	
	// 
	rc = initBufferPool(bm, name, 3, RS_FIFO, NULL);

	// Assign tab_mgr back to mgmtdata
	rel->mgmtData=tab_mgr;
	// assign page and bm handlers
	tab_mgr->h=h;
	tab_mgr->bm=bm;
	

	// reading first block thru buffer mgr
	rc = pinPage(bm,h,i);
	// Deserialize data and unpin page
	Kar_DeSerialize_TableInfo(rel,h->data);
	rc = unpinPage(bm, h);
	return rc;
}

RC closeTable (RM_TableData *rel)
{
	// Shut down buffer pool 
	RC rc;
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	rc = shutdownBufferPool(tab_mgr->bm);
	
	// Free page and pool pointers
	free(tab_mgr->h);
	tab_mgr->h=NULL;
	free(tab_mgr->bm);
	tab_mgr->bm=NULL;
	
	// free mgmtdata 
	free(rel->mgmtData);
	rel->mgmtData=NULL;
	
	// Free allocated table
	freeSchema(rel->schema);
	rel->schema=NULL;
	free(rel->name);
	rel->name=NULL;
	
	return rc;
}

RC deleteTable (char *name)
{	
	// Destroy relative page file and return return code
	return destroyPageFile(name);
}

int getNumTuples (RM_TableData *rel)
{
	// Get the mgmtdata to get number of tuples
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	return tab_mgr->number_of_Tuples;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
	RC rc;
	int i,j,page, slot;
	int flag = 0;
	int page_header_size=Kar_get_PageHeader_Size(rel);	
	
	// usual routine
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	BM_BufferPool *bm = tab_mgr->bm;
	BM_PageHandle *h = tab_mgr->h;
	
	// Create the mgmtdata of buffer to fhandle
	BP_Manager * bmp = (BP_Manager *)bm->mgmtData;
	SM_FileHandle* fHandle=&bmp->fHandle;
	
	// Skip first page and read (so dont read 0)
	for(i=1;i<fHandle->totalNumPages;i++)
	{
		fHandle->curPagePos=i;
		rc = pinPage(bm,h,fHandle->curPagePos);
		char* data=h->data;
		// loop for size we got 
		for(j=0;j<page_header_size;j++)
		{
			if(data[j]=='0')
			{
				slot=j;
				flag =1;
				break;
			}
		}
		rc = unpinPage(bm, h);
		if(flag ==1)
		{
			page=i;
			break;
		}
	}
	if(flag == 1)
	{
		// If found need to update record
		record->id.page=page;
		record->id.slot=slot;
		rc = updateRecord(rel,record);
	}
	else
	{
		// Append at the end
		appendEmptyBlock(fHandle);
		
		slot=0;
		fHandle->curPagePos++;
		page=fHandle->curPagePos;
		
		// read and pin it
		rc = pinPage(bm,h,page);
		
		for(i=0;i<page_header_size;i++)
		{
			memcpy(h->data+i,"0",1);
		}
		rc = markDirty(bm, h);
		rc = unpinPage(bm,h);
		// Update record after pinning it
		record->id.page=page;
		record->id.slot=slot;
		rc = updateRecord(rel,record);
	}
	return rc;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
	RC rc;
	char* data;
	// Allocate page and slot of RID
	int page=id.page;
	int slot=id.slot;

	// Usual Routine
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	BM_BufferPool *bm = tab_mgr->bm;
	BM_PageHandle *h = tab_mgr->h;
	
	// pin the page/record  
	rc = pinPage(bm,h,page);
	data=h->data;
	data = data + slot;
	
	// Here 0 indicates a tomb stone
	memcpy(data,"0",1);
	data = data - slot;
	rc = markDirty(bm, h);
	rc = unpinPage(bm, h);
	// return 
	return rc;
}

RC updateRecord (RM_TableData *rel, Record *record)
{
	RC rc;
	int page,slot,move_on=0,rec_size=0,page_header_size=0;
	char *data,*rec_data;
	
	RID id=record->id;
	rec_data=record->data;
	
	page=id.page;
	slot=id.slot;
	
	// Usual routine`
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	BM_BufferPool *bm = tab_mgr->bm;
	BM_PageHandle *h = tab_mgr->h;
	
	rc = pinPage(bm,h,page);
	data=h->data;
	
	// get record size
	rec_size=getRecordSize(rel->schema);
	memcpy(data+slot,"1",1);
	page_header_size = Kar_get_PageHeader_Size(rel);
	
	// Move on from cover
	move_on = move_on + page_header_size;
	move_on = move_on + rec_size*slot;
	data = data + move_on;
	
	// Copy rec_data to data
	memcpy(data,rec_data,rec_size);
	data = data - move_on;
	
	// Unpin and leave
	rc = markDirty(bm, h);
	rc = unpinPage(bm, h);
	
	// return 
	return rc;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	RC rc; 
	int page,slot,move_on=0,rec_size=getRecordSize(rel->schema),page_header_size=Kar_get_PageHeader_Size(rel);
	char *data,*rec_data;
	
	// Usual routine
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	BM_BufferPool *bm = tab_mgr->bm;
	BM_PageHandle *h = tab_mgr->h;

	// assign record parameters
	record->id.page=id.page;
	record->id.slot=id.slot;
	rec_data=record->data;
	
	page=id.page;
	slot=id.slot;
	 
	rc = pinPage(bm,h,page);
	data=h->data;
	move_on = move_on + page_header_size;
	move_on = move_on + rec_size*slot;
	data = data + move_on;
	
	memcpy(rec_data,data,rec_size);
	data = data - move_on;
	// Just getting so no need to write it back
	rc = markDirty(bm, h);
	rc = unpinPage(bm, h);
	// return
	return rc ;
}

// Scanning functions
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	RC rc = RC_OK;
	// now scanning manager in use
	Scanning_Manager_DS* smd=(Scanning_Manager_DS*)malloc(sizeof(Scanning_Manager_DS));
	// assign relation to rel
	scan->rel=rel;
	// assign values to smd data strcture
	smd->page=1;
	smd->slot=0;
	smd->cond=cond;
	// get data back to mgmtdata
	scan->mgmtData=smd;
	
	return rc;
}

RC next (RM_ScanHandle *scan, Record *record)
{
	RC rc = RC_OK;
	int flag = 0,page_header_size=0;
	RID id;
	char *data;
	Value* value;
	// Usual routine
	RM_TableData* rel=scan->rel;
	Table_manager_DS* tab_mgr=(Table_manager_DS*)rel->mgmtData;
	BM_BufferPool *bm = tab_mgr->bm;
	BM_PageHandle *h = tab_mgr->h;
	
	// file handle aslo req here
	BP_Manager * poolMgr = (BP_Manager *)bm->mgmtData;
	SM_FileHandle* fHandle=&poolMgr->fHandle;
	
	// Start a scan from rec 1
	Scanning_Manager_DS* smd=(Scanning_Manager_DS*)scan->mgmtData;
	Expr* cond=smd->cond;
	
	page_header_size = Kar_get_PageHeader_Size(rel);
	
	// Loop with page and slot
	for(;smd->page<fHandle->totalNumPages;smd->page++)
	{
		fHandle->curPagePos=smd->page;
		rc = pinPage(bm,h,fHandle->curPagePos);
		data=h->data;

		for(;smd->slot<page_header_size;smd->slot++)
		{
			if(data[smd->slot]=='1')
			{
				id.page=smd->page;
				id.slot=smd->slot;
				// get the record
				rc = getRecord(rel,id,record);
				// this stores the bool value of the evaluation build of expression
				
				rc = evalExpr(record, rel->schema, cond, &value);
				// bool 
				if(value->v.boolV)
				{
					flag=1;
					smd->slot++;
					break;
				}
			}
		}
		
		rc = unpinPage(bm, h);
		
		if(flag == 1)
			break;
	}

	// If no matched tuples at end
	if(flag == 1)
		rc = RC_OK;
	else
		rc = RC_RM_NO_MORE_TUPLES;
		
	// return
	return rc;
}

RC closeScan (RM_ScanHandle *scan)
{
	RC rc;
	// Scanning manager init
	Scanning_Manager_DS* smd=(Scanning_Manager_DS*)scan->mgmtData;
	Expr* cond=smd->cond;
	
	rc = freeExpr(cond);
	// After closing scan mgmtdata
	
	free(scan->mgmtData);
	scan->mgmtData=NULL;
	
	return rc;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
	int size=0,i;
	for(i=0;i<schema->numAttr;i++)
	{
		if(schema->typeLength[i]!=0)
			size+=schema->typeLength[i];
		else
			size+=sizeof(DT_INT);
	}
	return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	// Allocate new schema
	Schema* schema=(Schema*)malloc(sizeof(Schema));
	
	// Assign all attributes of schema to passed
	schema->keySize=keySize;
	schema->typeLength=typeLength;
	schema->attrNames=attrNames;
	schema->dataTypes=dataTypes;
	schema->numAttr=numAttr;
	schema->keyAttrs=keys;
	
	// return schema
	return schema;
}


RC freeSchema(Schema* schema)
{
	RC rc;
	int numAttrs=schema->numAttr,i;
	// First free all attributes 
	for(i=0;i<numAttrs;i++)
	{
		free(schema->attrNames[i]);
		schema->attrNames[i]=NULL;
	}
	// Free key attributes	
	free(schema->keyAttrs);
	schema->keyAttrs=NULL;
	// Free datatypes
	free(schema->dataTypes);
	schema->dataTypes=NULL;
	// free type length
	free(schema->typeLength);
	schema->typeLength=NULL;
	// Free attr names
	free(schema->attrNames);
	schema->attrNames=NULL;
	// Free schema 
	free(schema);
	schema=NULL;
	
	return rc;
}



RC createRecord (Record **record, Schema *schema)
{
	RC rc = RC_OK; 
	int rec_size;
	// Allocate record 
	(*record)=(Record*)malloc(sizeof(Record));
	rec_size = getRecordSize(schema);
	(*record)->data=(char*)malloc(sizeof(char)*rec_size);
	// return
	return rc;
}

RC freeRecord (Record *record)
{
	RC rc = RC_OK;
	// free record data
	free(record->data);
	record->data=NULL;
	// return
	return rc;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	RC rc = RC_OK;
	int move_on=0,i,attribute_Size=0;
	char* build,*data;
	
	// assign value and record data
	*value=(Value*)malloc(sizeof(Value));
	data=record->data;
	
	// For number of attributes 
	for(i=0;i<=attrNum;i++)
	{
		//get the position of data
		if(i != attrNum)
		{
			// Check for type length
			if(schema->typeLength[i]!=0)
			{
				data = data + schema->typeLength[i];
				move_on = move_on + schema->typeLength[i];
			}
			else
			{
				data = data + sizeof(DT_INT);
				move_on = move_on + sizeof(DT_INT);
			}
		}
		else
			break;
	}
	
	(*value)->dt=schema->dataTypes[i];
	
	if (schema->typeLength[i]==0)
	attribute_Size=sizeof(DT_INT);
	else
	attribute_Size=schema->typeLength[i];
	
	// allocate intermediate result based on attribute size
	build=(char*)malloc(attribute_Size*sizeof(char)+1);
	
	// copy intermediate result
	memcpy(build,data,attribute_Size);
	build[attribute_Size]='\0'; 
	data = data - move_on;
	
	// Based on data_types
	switch(schema->dataTypes[i])
	{
		// INT case
		case DT_INT:
		(*value)->v.intV=atoi(build);
		break;
		// STRING case
		case DT_STRING:
		(*value)->v.stringV=(char*)malloc(strlen(build)*sizeof(char));
		strcpy((*value)->v.stringV,build);
		break;
		// FLOAT case
		case DT_FLOAT:(*value)->v.floatV=atof(build);
		break;
		// BOOL case
		case DT_BOOL:(*value)->v.boolV=atoi(build);
		break;
	}
	
	// Free intermediate result 
	free(build);
	build=NULL;
	// return
	return rc;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	RC rc = RC_OK;
	int i,move_on=0,attribute_Size=0;
	char* build;
	
	// See value of DT
	switch(value->dt)
	{
		// Same as in serialization
		case DT_INT:
			build=(char*)calloc(sizeof(DT_INT), sizeof(char));
			sprintf(build,"%i",value->v.intV);
			break;
		// Same as in serialization
		case DT_STRING:
			build=(char*)calloc(strlen(value->v.stringV), sizeof(char));
			strcpy(build,value->v.stringV);
			break;
		// Same as in serialization
		case DT_FLOAT:
			build=(char*)calloc(sizeof(DT_FLOAT), sizeof(char));
			sprintf(build,"%f",value->v.floatV);
			break;
		// Same as in serialization
		case DT_BOOL:
			build=(char*)calloc(sizeof(DT_BOOL), sizeof(char));
			sprintf(build,"%i",value->v.boolV);
			break;
	}

	// Loop for all attributes
	for(i=0;i<=attrNum;i++)
	{
		// Check for attrnum in for loop
		if(i!=attrNum)
		{
			if(schema->typeLength[i]!=0)
			{
				record->data = record->data + schema->typeLength[i];
				move_on = move_on + schema->typeLength[i];
			}
			else
			{
				record->data = record->data + sizeof(DT_INT);
				move_on = move_on + sizeof(DT_INT);
			}
		}
		else
			break;
	}

	//chunk the wanted attr values and copy to build
	
	if (schema->typeLength[i]==0)
	attribute_Size=sizeof(DT_INT);
	else
	attribute_Size=schema->typeLength[i];
	
	// Memcopy
	memcpy(record->data,build,attribute_Size);
	record->data = record->data - move_on;
	
	// free intermediate result
	free(build);
	build=NULL;
	
	return rc;
}
