#include "stdio.h"
#include "stdlib.h"
#include "dberror.h"
#include "sys/stat.h"
#include "storage_mgr.h"      // Storage manager interfacing
#include "fcntl.h"            // File control
#include "unistd.h"           // For access and file existence

void initStorageManager (void)
{//Initialize any of varibales if required
}

int check_access (char *fn,char type)// To check the access of the file  
{
int amode,rval;
switch ( type ) 
{
case 'e':
// for existence
amode = 00;
break;

case 'r':
// for reading
amode = 04;
break;

case 'w':
// for writing
amode = 02;
break;

case 'x':
// for reading and writing
amode = 06;
break;

default:
// return -1
return -1;
break;
}
return access(fn, amode);
}


RC createPageFile (char *fileName)//To create a page file
{
RC Return_Code;
FILE *File_Pointer; 
int rval,ctr; 
//Check the file for existence
rval = check_access(fileName,'e');
	if( rval != -1)
	{
	//If the file exists 
	printf("\n The file exists already");
	Return_Code = RC_OK;
	}
// Open the file
File_Pointer = fopen (fileName,"wb");
	// Check for return code when file creates
	if (File_Pointer == NULL)
	{
	printf("\n File creation error");
	Return_Code = RC_FILE_HANDLE_NOT_INIT;
	}
	else
	{
	// File opened
	printf("\nFile created and opened to write null characters");
	// Fill with null bytes for the first page (of PAGE_SIZE)
	for(ctr=0 ; ctr<PAGE_SIZE; ctr++)
	fprintf(File_Pointer,"%c",'\0');
	Return_Code = RC_OK;
	}
fclose(File_Pointer);
return Return_Code;
}

// To open a page file
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
RC Return_Code;
FILE *File_Pointer,f1; 
int rval;
struct stat stat_sample;
unsigned long int Page_Blocks=0,FILE_SIZE;
rval = check_access(fileName,'r');
	//Check for existence 
	if (rval != -1)
	{
	printf("\nFile exists and has read access");
	File_Pointer = fopen(fileName, "rb+");
		if(File_Pointer == NULL)
		{
		printf("\nYou have no read access to this file");
		Return_Code = RC_FILE_HANDLE_NOT_INIT;
		goto exit;
		}
		else
		{
		// If opened get the entire file size using sys/stat
		printf("\nFile named %s opened successfully",fileName);
		fseek(File_Pointer,0L,SEEK_END);
		FILE_SIZE = ftell(File_Pointer);
		fseek(File_Pointer,0L,SEEK_SET);
		if (stat(fileName, &stat_sample) == 0)
		{
		printf("\nTotal File Size:%d\n",FILE_SIZE);
		// Then divide by PAGE_SIZE
		Page_Blocks = (FILE_SIZE)/PAGE_SIZE;
			// Assign no of pages to struct element
			if ((FILE_SIZE%PAGE_SIZE) > 0)
			{
			fHandle -> totalNumPages = (Page_Blocks+1);
			printf("\nNo of pages are:%d\n",fHandle->totalNumPages);
			}
			else
			{
			fHandle -> totalNumPages = Page_Blocks ;
			}
		// Initilize the file handler
        	fHandle -> mgmtInfo = File_Pointer;
		fHandle -> fileName = fileName;
        	fHandle -> curPagePos = 0;
        	Return_Code = RC_OK;
		}
		}
	}
	else
	{
	// If the file does not exists then return file not found error 
	printf("\n File doesnt exists");
	Return_Code = RC_FILE_NOT_FOUND;
	}
exit:
return Return_Code;
}

RC closePageFile (SM_FileHandle *fHandle)//To close a page file
{
RC Return_Code;
int rval;
printf("\n Entered Close file method");
//Check for file existence
if (fHandle == NULL)
{
printf("not init");
Return_Code = RC_FILE_HANDLE_NOT_INIT; 
}
//If the file exists,then check mgmtInfo  
else if (fclose (fHandle -> mgmtInfo) == 0)  
{
printf("\nFile named %s closed successfully",fHandle->fileName);
printf("\nTotal pages in a file before close are:%d\n",fHandle->totalNumPages);
Return_Code = RC_OK;
}
else 
{
printf("\nFile closing error");
Return_Code = RC_FILE_HANDLE_NOT_INIT;
}
return Return_Code;
}

RC destroyPageFile (char *fileName)
{
RC Return_Code;
FILE *File_Pointer; 
int rval;

//Check for file existence
rval = check_access(fileName,'e');
//Check fo existence
if (rval != -1)
	{
	printf("\n File exists");
	// if exists ,try deleting
	if(remove(fileName) == 0)
	{
	printf("\nFile named %s deleted", fileName); 
	Return_Code = RC_OK;
	}
	else
	{
	// If error,
	printf("\nFile named %s cant be deleted", fileName); 	 
	Return_Code = RC_FILE_HANDLE_NOT_INIT;
	}
}
else
{
//The file does not exist 
printf("\n File doesnt exists");
Return_Code = RC_FILE_NOT_FOUND;
}
return Return_Code;
}

// Reading blocks from disc 

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer = fHandle->mgmtInfo;
printf ("\nTotal num of pages are:%d\n",fHandle->totalNumPages);
// Check for mgmt_Info  
if ( File_Pointer == NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
	// Check if u are reading an existing page
	if( pageNum >= fHandle-> totalNumPages )
	{
	Return_Code = RC_READ_NON_EXISTING_PAGE;
	printf("\nPage requested wasn't found");
	}
	else
	{
	// Seek to no of pages passed
	fseek ( File_Pointer , (PAGE_SIZE * pageNum) , SEEK_SET);
	printf("\nReading current block...");
	//Read the content of the page 
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	// Set the position of page in a file
	fHandle->curPagePos=pageNum;
	Return_Code = RC_OK ;
	}
fHandle->mgmtInfo = File_Pointer;
}
return Return_Code;
}

// Return current Block position
 
int getBlockPos (SM_FileHandle *fHandle)
{
int Block_Pos = 0;
// Check for file handle
if (fHandle != NULL)
// Return position of page in a file (fHandle -> current position)
Block_Pos=fHandle->curPagePos;
else
printf ("\nFile handle error");
return Block_Pos;
}

//Read first block
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer = fHandle -> mgmtInfo;
// Check for mgmt_Info 
if ( fHandle -> mgmtInfo = NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
// Read the first block
	// Seek to first block 
	fseek ( File_Pointer , 0 , SEEK_SET);
	// Send to first of file
	rewind (File_Pointer);
	//Read the content of the page 

	// fread (page handle,size of page,no of blocks to read,fp)
	printf("\nReading first block...");
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	// Set the position of page in a file
	fHandle->curPagePos=0;
	Return_Code = RC_OK ;
	// Set again for book keeping
	fHandle-> mgmtInfo = File_Pointer;
}
return Return_Code;
}

//Read previous block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer = fHandle->mgmtInfo;
// Check for mgmtInfo 
if ( fHandle -> mgmtInfo = NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
	if (fHandle -> curPagePos == 0)
	{
	printf("\nPage requested doesnt exist as now we are in first block");
	Return_Code = RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
	// Seek to first block 
	fseek (File_Pointer , ( fHandle->curPagePos - 1) , SEEK_SET);
	//Read the content of the page 
	printf("\nReading previous block...");
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	// Set the position of page in a file
	fHandle->curPagePos= fHandle->curPagePos-1 ;
	fHandle ->mgmtInfo = File_Pointer;
	Return_Code = RC_OK ;
	}
}
return Return_Code;
}

//Read Current Block

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
int pagePos;
FILE *File_Pointer;
File_Pointer = fHandle->mgmtInfo;
pagePos = (int) fHandle -> curPagePos;
printf("Current page pos is:%d",pagePos);
// Check for mgmtInfo 
if ( fHandle -> mgmtInfo = NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
	if (fHandle -> curPagePos == fHandle -> totalNumPages)
	{
	printf("\nPage requested doesnt exist as now we are in Last page");
	Return_Code = RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
	// Seek to next block 
	fseek (File_Pointer ,PAGE_SIZE*(pagePos) , SEEK_SET);
	//Read the content of the page
	printf("\nReading current block...");
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	fHandle->mgmtInfo = File_Pointer;
	Return_Code = RC_OK ;
	
	}
}
return Return_Code;
}

//To read next block
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer = fHandle->mgmtInfo;
// Check for mgmtInfo 
if ( File_Pointer == NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
	if (fHandle -> curPagePos == fHandle -> totalNumPages)
	{
	printf("\nPage requested doesnt exist as now we are in last page");
	Return_Code = RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
	// Seek to first block
	fseek (File_Pointer ,PAGE_SIZE*(fHandle->curPagePos + 1 ) , SEEK_SET);
	//Read the content of the page 
	printf("\nReading next block...");
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	// Set the position of page in a file
	fHandle->curPagePos= fHandle->curPagePos + 1 ;
	fHandle->mgmtInfo = File_Pointer;
	Return_Code = RC_OK ;
	}
}
return Return_Code;
}

//To read the last block

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer= fHandle->mgmtInfo;
// Check for mgmtInfo 
if ( File_Pointer == NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
// Read the last block
// Set the current position of page to total no of pages
	// Seek to first block (fp,0,seek_set)
	fseek (File_Pointer , PAGE_SIZE , SEEK_END);
	//Read the content of the page
	printf("\nReading the last block...");
	fread ( memPage, PAGE_SIZE , 1, File_Pointer);
	// Set the position of page in a file
	fHandle->curPagePos= fHandle-> totalNumPages;
	fHandle->mgmtInfo = File_Pointer;
	Return_Code = RC_OK ;
}
return Return_Code;
}


// Writing blocks to a page file 

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
File_Pointer = fHandle->mgmtInfo;
printf ("File is:%s",fHandle->fileName);
// Check for mgmtInfo 
if ( fHandle -> mgmtInfo = NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
		printf("\nPage exists");
		// Seek it to no of pages passed
/*
		if(NULL != File_Pointer){
			printf("\n file ptr is not null");
		}
		printf("\n seek set:");

		int cc = 0,c;
 File_Pointer = fopen(fHandle->fileName,"r");
   while(NULL != File_Pointer)
   {
      c = fgetc(File_Pointer);
cc++;
      if( feof(File_Pointer) )
      {
          break ;
      }
   }
   fclose(File_Pointer);
printf("\n file sz: %d", cc);
*/
		fseek ( File_Pointer ,(PAGE_SIZE * pageNum),SEEK_SET);
		//Write the content to the page 
		printf("\n Writing current block...");
		if (fwrite ( memPage, PAGE_SIZE , 1, File_Pointer) > 0)
		{
		// Set the current position of page in a file
		fHandle->curPagePos=pageNum;
		Return_Code = RC_OK ;
		}
		else
		{
		// If failure then set RC_WRITE_FAILED 3 
		Return_Code = RC_WRITE_FAILED;
		RC_message = "\nNo Write access to file";	
		printf(RC_message);
		}
fHandle->mgmtInfo = File_Pointer;
}
return Return_Code;
}

// To write a current block

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
RC Return_Code;
FILE *File_Pointer;
// Check for mgmtInfo 
if ( File_Pointer ==  NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
	// Check for no of pages
	if(fHandle->curPagePos > (fHandle-> totalNumPages-1) )
	{
	Return_Code = RC_READ_NON_EXISTING_PAGE;
	printf("\nPage requested wasn't found");
	}
	else
	{
		//fp = 0; 
		printf("\nPage exists");
		// Seek it to no of pages passed
		fseek (File_Pointer , 0 , SEEK_CUR);
		//Write the content to the page 
		// fwrite (page handle,PAGE_SIZE,1,fp)
		printf("\nReading current block...");
		if (fwrite ( memPage, PAGE_SIZE , 1, File_Pointer) != 0)
		{
		// Set the current position of page in a file
		Return_Code = RC_OK ;
		fHandle->mgmtInfo = File_Pointer;
		}
		else
		{
		// If failure then set RC_WRITE_FAILED 3 
		Return_Code = RC_WRITE_FAILED;
		RC_message = "\nNo Write access to file";	
		printf(RC_message);
		}
	}
}
return Return_Code;
}

//To append an empty block

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
FILE *File_Pointer;
RC Return_Code;
File_Pointer = fHandle->mgmtInfo;
int ctr,pages = fHandle->totalNumPages;
if (File_Pointer  == NULL)
{
Return_Code = RC_FILE_HANDLE_NOT_INIT;
printf("\nHandle seeking error");
}
else
{
fseek (File_Pointer,(pages)*PAGE_SIZE,SEEK_SET);
printf("\nappending%d\n",pages);
// Fill it with \0 bytes for PAGE_SIZE bytes
for(ctr = 0 ; ctr<PAGE_SIZE; ctr++)
fprintf(File_Pointer,"%c",'\0');
fHandle->totalNumPages++;
fHandle->mgmtInfo = File_Pointer;
Return_Code = RC_OK;
}
return Return_Code;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
RC Return_Code;
int ctr,pageBlocks,hold_size,set;
struct stat stat_instance;
// Check for mgmtInfo 
if (stat(fHandle-> fileName, &stat_instance) == 0)
{
pageBlocks = (stat_instance.st_size) / PAGE_SIZE;
	if ((stat_instance.st_size) % PAGE_SIZE > 0)
	pageBlocks = pageBlocks+1;
	if(pageBlocks > numberOfPages)
	set = 0;
	else 
	set = 1;
	if(set == 1)
	{
	hold_size = (numberOfPages - pageBlocks) *PAGE_SIZE;
	for (ctr = 0 ; ctr < hold_size ; ctr ++)
	fprintf (fHandle->mgmtInfo, "%c", '\0');
	}
	else
	printf ("\nWe already ensured capacity");
Return_Code = RC_OK;
}
else
{
printf ("\nHandle error");
Return_Code = RC_FILE_HANDLE_NOT_INIT;
}
return Return_Code;
}
