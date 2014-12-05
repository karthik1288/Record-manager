#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"


//store management info of a table
typedef struct Table_manager_DS
{
	// Contains pointer to pool and page handle
	BM_BufferPool* bm;
	BM_PageHandle *h;
	// To keep track of table we need to have number of tuples and Each tuple size
	int number_of_Tuples;
	int size_of_Schema;

}Table_manager_DS;

// One data structure to keep track of scanning
typedef struct Scanning_Manager_DS
{
	// to keep track of condition meeting req
	Expr* cond;
	// to keep track of RID things
	int page;
	int slot;
}Scanning_Manager_DS;

//  Get Schema size and header size 
int Kar_get_Schema_Size(int numAttr, int keySize);
int Kar_get_PageHeader_Size(RM_TableData* rel);

//serialize functions
char* Kar_Serialize_Schema(Schema* schema);
char* Kar_Serialize_Table_Info(RM_TableData* rel);
void Kar_DeSerialize_Schema(Schema* schema, char* info);
void Kar_DeSerialize_TableInfo(RM_TableData* rel, char* info);
void free_string(char *S);


