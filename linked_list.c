#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
// for maxinmum and minimum calculations on replacement
#include <limits.h>

#include "linked_list.h"
#include "buffer_mgr.h"
#include "dberror.h"

// LOcal functions
void init_page(page_Frame* frame);

//initialize a frame/node of a pool with all default values
void create_dummy_page_Frame(page_Frame* frame)
{
	// Dirty must be false
	frame->dirty_Flag=FALSE;
	// fix counts is zero
	frame->fix_Count=0;
	// default page details
	init_page(frame);
	// replaceable flag
	frame->replace_Flag = -1;
	// Set next node pointer to null
	frame->next_Node=NULL;
}

//insert a frame to the head_Node of the frames in the buffer pool
void insert_page_Frame(page_Frame* pf,BM_BufferPool *const bm)
 {
	RC rc = RC_OK;
	// local pool manager object
	BP_Manager* bp_manager = (BP_Manager*) bm->mgmtData;
		if (bm==NULL)
		{
		rc = RC_POOL_INIT_ERROR;
		}
		else 
		{
			if (pf==NULL)
			{
			rc = RC_NULL_FRAME_RETURN;
			}
			else
			{
			page_Frame* head_Node = bp_manager->head_Node;
				if(head_Node!=NULL)
				{
				bp_manager->head_Node = pf;
				pf->next_Node = head_Node;
				}
				else
				{
				bp_manager->head_Node=pf;
				}
			}
		}
 }


// TO get the maximum replaceable frame
int get_max_replaceable_Flag(BM_BufferPool *const bm) 
{
	// local pool manager object
	BP_Manager* bp_manager = (BP_Manager*) bm->mgmtData;
	return (bp_manager)->max_ind_Flag;
}

// To get the frame that can be replaced by the algorithm 
page_Frame* get_min_replaceable_page_Frame(BM_BufferPool *const bm)
{
	// local pool manager object
	BP_Manager* bp_manager=(BP_Manager*)bm->mgmtData;
	page_Frame* frame=bp_manager->head_Node;
	page_Frame* min_replaceable_page_Frame;
	int i,page_count;
	int min_Flag=INT_MAX;
	page_count = bm->numPages;

	// Loop till end of pool
	for(i=0;i< page_count;i++)
	{
		// Find a frame minimal replaceable and return it
		if((frame->replace_Flag<min_Flag) && (frame->fix_Count==0))
		{
			//Assign once you get them
			min_replaceable_page_Frame = frame;
			min_Flag = frame->replace_Flag;
		}
		frame=frame->next_Node;
	}
	
	return min_replaceable_page_Frame;
}


/* Function to initialize a page*/
void init_page(page_Frame* frame)
{
	// Page number is also -1 at first
	frame->page.pageNum=-1;
	// initialize a frame of PAGE_SIZE
	frame->page.data = (char *) malloc(sizeof(char*)*PAGE_SIZE);
	// returns a pointer to the memory area page.data
	memset(frame->page.data, 0, sizeof(char)*PAGE_SIZE);
}	
