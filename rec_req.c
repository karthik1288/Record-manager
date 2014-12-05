#include "rec_req.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "assumptions.h"

// Returns the page header to be written to disk
int Kar_get_PageHeader_Size(RM_TableData* rel)
{
	// Return the page header size_of_Schema
	// It is (page_size/ record_size+1) >> 
	return PAGE_SIZE/((getRecordSize(rel->schema))+1);
}

// Get size of schema..Based on this we calculate the record size
int Kar_get_Schema_Size(int numAttr, int keySize)
{
	// Total attributes + keys + total_attr_count + at_key*key_size
	int total_attr_count = (Type_of_Attribute+Name_of_Attribute)*numAttr, key_matter= (Position_of_Key*keySize);
	return Number_of_Attributes+Number_of_Keys+ total_attr_count+ key_matter;
}

char* Kar_Serialize_Table_Info(RM_TableData* rel)
{
	int size_tab_meta_data = 0;
	// First thing to do is to get the mgmtdata pointer to a defined strcut
	Table_manager_DS* tmd = (Table_manager_DS*) rel->mgmtData; 
	
	// Get the table info
	size_tab_meta_data = Name_of_Table + Number_of_Tuples;
	// Allocate data to a char pointer of derived size
	char* charinfo=(char*)calloc(size_tab_meta_data,sizeof(char));
	
	// Allocate something to result
	char* res = (char*)calloc(PAGE_SIZE,sizeof(char));
	
	// Serialize table name
	memcpy(charinfo,rel->name,strlen(rel->name));
	charinfo = charinfo + Name_of_Table;

	// Next number of tuples needs to be serialized
	sprintf(charinfo,"%i",getNumTuples(rel));
	charinfo = charinfo + Number_of_Tuples;
	charinfo = charinfo - size_tab_meta_data;

	// Copy char info to result
	memcpy(res,charinfo,size_tab_meta_data);
	
	// add serialized schema to that 
	res = res + size_tab_meta_data;
	memcpy(res,Kar_Serialize_Schema(rel->schema),tmd -> size_of_Schema);
	res = res - size_tab_meta_data;

	free_string (charinfo);
	// return result once freed 
	return res;
}

void Kar_DeSerialize_Schema(Schema* schema, char* info)
{
	int i;
	// Now de-serialize schema when reading  
	// We need to allocate a container
	char* container=(char*)calloc(Number_of_Attributes,sizeof(char));
	char* alloc;
	
	// De-serialize number of attributes
	memcpy(container,info,Number_of_Attributes);
	// get the number of attributes from it
	schema->numAttr = atoi(container);
	// add number of attributes 
	info = info + Number_of_Attributes;
	// Free container
	free_string (container);

	// De-serialize number of keys
	container=(char*)calloc(Number_of_Keys,sizeof(char));
	// Copy keys now
	memcpy(container,info,Number_of_Keys);
	// get the key size
	schema->keySize=atoi(container);
	info = info + Number_of_Keys;
	free_string (container);
	
	// Now serialize attributes
	int numAttr=schema->numAttr;
	schema->attrNames=(char**)malloc(sizeof(char*)*numAttr);
	schema->dataTypes=(DataType*)malloc(sizeof(DataType)*numAttr);
	schema->typeLength=(int*)malloc(sizeof(int)*numAttr);
	schema->keyAttrs=(int*)malloc(sizeof(int)*numAttr);

	for(i=0;i<numAttr;i++)
	{
		schema->attrNames[i]=(char*)malloc(sizeof(char)*Name_of_Attribute);
	}

	// allocate a container`
	container=(char*)calloc(1,sizeof(char));
	
	// based on attribute
	alloc=(char*)calloc(Type_of_Attribute-1,sizeof(char));
	
	for(i=0;i<schema->numAttr;i++)
	{
		memcpy(container,info,1);
		info = info + 1;
		memcpy(alloc,info,Type_of_Attribute-1);
		info = info+Type_of_Attribute-1;
		schema->dataTypes[i]=atoi(container);
		schema->typeLength[i]=atoi(alloc);
	}
	free_string (container);
	free_string (alloc);

	// De-serialize 
	container=(char*)calloc(Name_of_Attribute,sizeof(char));
	for(i=0;i<schema->numAttr;i++)
	{
		memcpy(container,info,Name_of_Attribute);
		info = info + Name_of_Attribute;
		strcpy(schema->attrNames[i],container);
	}
	free_string (container);

	//de-serialize key positions
	container=(char*)calloc(Position_of_Key,sizeof(char));
	for(i=0;i<schema->keySize;i++)
	{
		memcpy(container,info,Position_of_Key);
		info+=Position_of_Key;
		schema->keyAttrs[i]=atoi(container);
	}
	free_string (container);
}

void Kar_DeSerialize_TableInfo(RM_TableData* rel, char* info)
{
	char* container=(char*)calloc(Number_of_Tuples,sizeof(char));
	rel->name=(char*)calloc(Name_of_Table,sizeof(char));
	Table_manager_DS* tmd=(Table_manager_DS*)rel->mgmtData;
	
	// de-serialize table name
	memcpy(rel->name,info,Name_of_Table);
	info = info + Name_of_Table;

	// de-serialize number_of_Tuples
	memcpy(container,info,Number_of_Tuples);
	tmd->number_of_Tuples=atoi(container);
	free_string(container);
	
	info = info + Number_of_Tuples;

	//de-serialize schema
	rel->schema=(Schema*)malloc(sizeof(Schema));
	
	// End the de-serialize schema
	Kar_DeSerialize_Schema(rel->schema,info);
}



char* Kar_Serialize_Schema(Schema* schema)
{
	int i=0,size_of_Schema=Kar_get_Schema_Size(schema->numAttr,schema->keySize);
	char* inf_Schema=(char*)calloc(size_of_Schema,sizeof(char));

	// serialize no of attributes
	sprintf(inf_Schema,"%i",schema->numAttr);
	inf_Schema = inf_Schema + Number_of_Attributes;

	// now serialize of keys
	sprintf(inf_Schema,"%i",schema->keySize);
	inf_Schema = inf_Schema + Number_of_Keys;

	//serialize attribute types, lengths 5 bytes each block, total numAttrs block
	for(i=0;i<schema->numAttr;i++)
	{
		switch(schema->dataTypes[i])
		{
			// CASE INT
			case DT_INT: 
			sprintf(inf_Schema,"%i",DT_INT);
			inf_Schema++;
			sprintf(inf_Schema,"%i",schema->typeLength[i]);
			inf_Schema = inf_Schema + Type_of_Attribute-1;
			break;
			
			// CASE STRING
			case DT_STRING: 
			sprintf(inf_Schema,"%i",DT_STRING);
			inf_Schema++;
			sprintf(inf_Schema,"%i",schema->typeLength[i]);
			inf_Schema = inf_Schema + Type_of_Attribute-1;
			break;
			
			// CASE FLOAT
			case DT_FLOAT: 
			sprintf(inf_Schema,"%i",DT_FLOAT);
			inf_Schema++;
			sprintf(inf_Schema,"%i",schema->typeLength[i]);
			inf_Schema = inf_Schema + Type_of_Attribute-1;
			break;
			
			// CASE BOOL
			case DT_BOOL: 
			sprintf(inf_Schema,"%i",DT_BOOL);
			inf_Schema++;
			sprintf(inf_Schema,"%i",schema->typeLength[i]);
			inf_Schema = inf_Schema + Type_of_Attribute-1;
			break;
		}
	}

	// Serialize attr names
	for(i=0;i<schema->numAttr;i++)
	{
		strcpy(inf_Schema,schema->attrNames[i]);
		inf_Schema = inf_Schema + Name_of_Attribute;
	}

	// serialize key
	for(i=0;i<schema->keySize;i++)
	{
		sprintf(inf_Schema,"%i",schema->typeLength[i]);
		inf_Schema = inf_Schema + Position_of_Key;
	}

	//append the res
	inf_Schema = inf_Schema - size_of_Schema;
	return inf_Schema;
}

// Deallocate a string
void free_string(char *S)
{
	// free string
	free(S);
	S=NULL;
}