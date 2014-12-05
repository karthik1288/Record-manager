#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

// A node 's structure 
typedef struct page_Frame
{
	// to find whether is dirty
    bool dirty_Flag;
	// Fix count i.e no of clients calling a node
    int fix_Count;
	// Page handle and content
    BM_PageHandle page;
	// for any algorithm to work on initially a replacement flag needs to be initialized
	int replace_Flag;
	// Pointer to next node
    struct page_Frame* next_Node;
}page_Frame;

// strcture of a buffer pool manager
typedef struct BP_Manager
{
	// head node of a list
	page_Frame* head_Node;
	// File Handle
	SM_FileHandle fHandle;
	// Content of a frame
	PageNumber *frameContent;
	// Holds all the dirty flags of each node
    bool *dirty_flag_array;
	// holds 
	int *fix_Count_array;
	// for statistical functions
	int pages_read;
	int pages_wrote;
	// for the function get_max_replaceable_Flag 
	int max_ind_Flag;
}BP_Manager;

// to create a node
void create_dummy_page_Frame(page_Frame* frame);

// To insert a node
void insert_page_Frame(page_Frame* pf,BM_BufferPool *const bm);

// get the page that can be replaced
int get_max_replaceable_Flag(BM_BufferPool *const bm);

// get the page that can replaceable after applying an algorithm
page_Frame* get_min_replaceable_page_Frame(BM_BufferPool *const bm);
