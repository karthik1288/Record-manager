#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
// for maxinmum and minimum calculations on replacement
#include <limits.h>
//For multi threading
#include <pthread.h>
// For list functions and variables of nodes and pool
#include "linked_list.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dt.h"

static pthread_mutex_t mutex_initbp=PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_forceflush=PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_shutdown=PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_pin=PTHREAD_MUTEX_INITIALIZER;

// Defined local User-defined functions
void init_other_vals(BP_Manager* bp_manager,const int numPages);
//void free_all(BM_BufferPool *const bm,BP_Manager* bp_manager,page_Frame* frame);

//
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	//local variables
	RC rc = RC_OK;
	int i;
	page_Frame* frame;
	pthread_mutex_lock(&mutex_initbp);
	// assigned passed values to buffer manager pointer
	bm->numPages = numPages;
	bm->strategy = strategy;
	
	// Assign pagefile name
	bm->pageFile = (char*)pageFileName;
	
	// Allocate size to 
	BP_Manager* bp_manager = (BP_Manager*)malloc(sizeof(BP_Manager));
	bm->mgmtData=bp_manager;
	bp_manager->pages_read=0;
	bp_manager->pages_wrote=0;
	bp_manager->max_ind_Flag=-1;

	//open page file and initialize fHandle
	if (openPageFile(bm->pageFile,&bp_manager->fHandle) == RC_OK)
	{
		rc = RC_OK;
	}
	else
	{
		rc = RC_FILE_OPEN_ERROR;
	}

	//init the linked list to be of size numPages
	for(i=0;i < numPages;i++)
	{
		frame=(page_Frame*)malloc(sizeof(page_Frame));
		create_dummy_page_Frame(frame);
		insert_page_Frame(frame,bm);
		rc = RC_OK;
	}

	// init Other Values in bp_manager 
	init_other_vals(bp_manager,numPages);
	pthread_mutex_unlock(&mutex_initbp);
	return rc;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	//local variables
	RC rc = RC_OK;
	int i = 0;
	pthread_mutex_lock(&mutex_shutdown);
    BP_Manager* bp_manager=(BP_Manager*)bm->mgmtData;
	page_Frame* frame=(page_Frame*)bp_manager->head_Node;
    
    //always remember to free memory for frames
	
    while(i < bm->numPages)
    {
    	if(frame->fix_Count!=0)
    	{
    		rc = RC_WRITE_FAILED;
    		return rc;
    	}
    	else
		{
			if (frame->dirty_Flag == true)
			{
    		forceFlushPool(bm);
			}
		}
		frame = frame->next_Node;
		i++;	
    }
	
    
    // Free memory of all variables of bp_manaager and frame 
    frame = (page_Frame*) bp_manager->head_Node;
	i=0;
    // RESET all variables before making anything else
	while(i<bm->numPages)
    {
    	free(frame->page.data);
    	free(frame);
    	frame=frame->next_Node;
    	i++;
    }
	// Free other values
    free(bp_manager->frameContent);
    free(bp_manager->dirty_flag_array);
    free(bp_manager->fix_Count_array);	
    free(bp_manager);
    bp_manager=NULL;
	pthread_mutex_unlock(&mutex_shutdown);
    return rc;
}


RC forceFlushPool(BM_BufferPool *const bm)
{
	//local variables
	RC rc = RC_OK;
	int i;
	
	pthread_mutex_lock(&mutex_forceflush);
    BP_Manager* bp_manager = (BP_Manager*)bm->mgmtData;
	page_Frame* frame = bp_manager->head_Node;
	
	if(bm == NULL)
	{
		rc = RC_POOL_INIT_ERROR;
	}

	while (i < bm->numPages)
	{
		if (frame->dirty_Flag == TRUE && frame->fix_Count == 0)
		{
			writeBlock(frame->page.pageNum, &bp_manager->fHandle, (SM_PageHandle)frame->page.data);
			frame->dirty_Flag = FALSE;
		}
		frame=frame->next_Node;
		i++;
	rc = RC_OK;
	}
	pthread_mutex_unlock(&mutex_forceflush);
	return rc;

}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	//local variables	
	RC rc = RC_OK;
	int i,page_count = bm->numPages;
	
	BP_Manager* bp_manager = (BP_Manager*)bm->mgmtData;
	page_Frame* head_Node = bp_manager->head_Node;
	PageNumber* frame_content_array = bp_manager->frameContent;
		
	if(bm==NULL)
	{
		rc = RC_POOL_INIT_ERROR;
		return NULL;
	}
	else
	{
		if(frame_content_array != NULL)
		{
			for(i=0;i< page_count;i++)
			{
				frame_content_array[i] = head_Node->page.pageNum;
				head_Node = head_Node->next_Node;
			}
		}
		return  frame_content_array;
	}
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
	//local variables
	RC rc = RC_OK;
	int i,page_count = bm->numPages;
	
	BP_Manager* bp_manager = (BP_Manager*)bm->mgmtData;
	page_Frame* head_Node = bp_manager->head_Node;
	bool* dirty_flags_array = bp_manager->dirty_flag_array;
	
	if(bm==NULL)
	{
		rc = RC_POOL_INIT_ERROR;
		return NULL;
	}
	else
	{
		if(dirty_flags_array!=NULL)
		{
			for(i=0;i<page_count;i++)
			{
				dirty_flags_array[i]=head_Node->dirty_Flag;
				head_Node=head_Node->next_Node;
			}
		}
		return dirty_flags_array;
	}
}

int *getFixCounts (BM_BufferPool *const bm)
{
	// local variables
	RC rc = RC_OK;
	int i,page_count = bm->numPages;
	
	BP_Manager* bp_manager = (BP_Manager*)bm->mgmtData;
	page_Frame* head_Node = bp_manager->head_Node;
	int* fix_Counts_array = bp_manager->fix_Count_array;
	
	if(bm==NULL)
	{
		rc = RC_POOL_INIT_ERROR;
		return NULL;
	}
	else
	{
		if(fix_Counts_array!=NULL)
		{
			for(i=0;i<page_count;i++)
			{
				fix_Counts_array[i]=head_Node->fix_Count;
				head_Node=head_Node->next_Node;
			}
		}
		return fix_Counts_array;
	}
}

int getNumReadIO (BM_BufferPool *const bm)
{
	// local pool manager object
	BP_Manager* bp_manager=(BP_Manager*)bm->mgmtData;
	return bp_manager->pages_read;
}

int getNumWriteIO (BM_BufferPool *const bm)
{
	// local pool manager object
	BP_Manager* bp_manager=(BP_Manager*)bm->mgmtData;
	return bp_manager->pages_wrote;
}


// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//local variables
	RC rc = RC_OK;
	int pageno=page->pageNum;
	
	page_Frame* frame_array=((BP_Manager*)bm->mgmtData)->head_Node;
	
	if(bm == NULL)
	{
	return RC_POOL_INIT_ERROR;
	}
	else
	{
		while(frame_array!=NULL)
		{
			if(frame_array->page.pageNum == pageno)
			{
				frame_array->dirty_Flag=TRUE;
				break;
			}
			frame_array = frame_array->next_Node;
		}
		rc = RC_OK;
	}
	return rc;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//local variables
	RC rc = RC_OK;
	int pageNum=page->pageNum;
		
	page_Frame* frame_array=((BP_Manager*)bm->mgmtData)->head_Node;
		
	if(bm == NULL)
	{
	return RC_POOL_INIT_ERROR;
	}
	else
	{
		while(frame_array!=NULL)
		{	
			printf ("Cleared");
			if(frame_array->page.pageNum==pageNum)
			{
				frame_array->fix_Count--;
				
				if (frame_array->dirty_Flag) 
				{
					printf("\nNew node assigned\n");
					frame_array->page.data=page->data;
					forcePage(bm, page);
				}
				
				if(frame_array->fix_Count <= 0)
				{
				//return RC_CANNOT_UNPIN_APAGE;
				break;
				}
			}
			frame_array = frame_array->next_Node;
		}
		rc = RC_OK;;
	}
return rc;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//local variables
	RC rc = RC_OK;
	
	BP_Manager* bp_manager = (BP_Manager*)bm->mgmtData;
	
	if(bm == NULL)
	{
	return RC_POOL_INIT_ERROR;
	}
	else
	{
	writeBlock(page->pageNum, &bp_manager->fHandle, (SM_PageHandle)page->data);
	printf("\nDonw with writing\n");
	bp_manager->pages_wrote++;
	rc = RC_OK;
	}
	return rc;
}


RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum)
{
	//local variables
	RC rc = RC_OK;
	int i = 0;
	pthread_mutex_lock(&mutex_pin);
	BP_Manager * bp_manager = (BP_Manager *)bm->mgmtData;
	page_Frame * frame = bp_manager->head_Node;
	
	//To find a page is alrready in pool
	while (i < bm->numPages) 
	{
		if (frame->page.pageNum == pageNum) 
		{
			page->pageNum = pageNum;
			page->data = frame->page.data;
			
			// increase fix count
			frame->fix_Count++;
			// IF FIFO DO nothing
			if (bm->strategy == RS_FIFO)
			{
			}
			else if (bm->strategy == RS_LRU) 
			{
				frame->replace_Flag = get_max_replaceable_Flag(bm) + 1;
				bp_manager->max_ind_Flag = frame->replace_Flag;
			}
			else
			{
			rc = RC_INVALID_STRATEGY;
			}
			// return if found
			return rc;
		}
		frame = frame->next_Node;
		i++;
	}

	// if not found check for minimum possible frame 
	frame=get_min_replaceable_page_Frame(bm);
	
	// Get the page from file if doesnt exists
	readBlock(pageNum,&bp_manager->fHandle,(SM_PageHandle)frame->page.data);
	
	// increment needed and exit
	bp_manager->pages_read++;
	frame->fix_Count++;
	
	// assign page frames
	frame->page.pageNum = pageNum;
	page->pageNum=pageNum;
	page->data=frame->page.data;
	
	// replace flag setter
	frame->replace_Flag = get_max_replaceable_Flag(bm) + 1;
	bp_manager->max_ind_Flag = frame->replace_Flag;
	
	rc = RC_OK;
	pthread_mutex_unlock(&mutex_pin);
	return rc;
}



void init_other_vals(BP_Manager* bp_manager,const int numPages)
{
	bp_manager->frameContent = (PageNumber*)malloc(sizeof(PageNumber)*numPages);
	bp_manager->dirty_flag_array = (bool*)malloc(sizeof(bool)*numPages);
	bp_manager->fix_Count_array = (int*)malloc(sizeof(int)*numPages);
}


/*void free_all(BM_BufferPool *const bm,BP_Manager* bp_manager,page_Frame* frame)
{
	int i=0;
	// RESET all variables before making anything else
    frame = (page_Frame*) bp_manager->head_Node;
	
	while(i<bm->numPages)
    {
    	free(frame->page.data);
    	free(frame);
    	frame=frame->next_Node;
    	i++;
    }
        //free (frame); 
	// Free other values
    free(bp_manager->frameContent);
    free(bp_manager->dirty_flag_array);
    free(bp_manager->fix_Count_array);
}*/
