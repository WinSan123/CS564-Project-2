/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

// START STUDENT ASSIGNED METHODS

void BufMgr::advanceClock() {
  // advance clock pointer
  clockHand++;

  if (clockHand >= numBufs)
    // Reset clockHand to first buffer
    clockHand = 0;
}

/**
* Allocate a free frame.
*
* @param frame   Frame reference, frame ID of allocated frame returned
* via this variable
* @throws BufferExceededException If no such buffer is found which can be
* allocated
*/
void BufMgr::allocBuf(FrameId &frame) {
  // Allocate a free frame (just return the next available frame)

  // param frame is the variable which the return value should be in
  // throws BufferExceededException if no such buffer is found which can be
  // allocated

  // The buffer that the clock is starting on. Used together with
  // candidateSeen to determine if should throw BufferExceededException
  FrameId startingFrame = clockHand;

  // Whether a candidate has been seen since last rotation of clock hand
  bool candidateSeen = false;

  while (true) {
    // Advance the clock hand
    advanceClock();

    if ((clockHand == startingFrame) && (!candidateSeen))
      throw BufferExceededException();

    BufDesc *currFrameDesc = &bufDescTable.at(clockHand);

    // Valid set?
    if (!currFrameDesc->valid) {
      // No: use frame and call "Set()" on the frame ("Set()" is called
      //   by caller of this method to set the file and pageNum for the
      //   frame)
      frame = clockHand;
      return;
    }

    // refbit set?
    if (currFrameDesc->refbit) {
      // Yes: clear refbit
      currFrameDesc->refbit = false;

      // This frame can be used next full rotation of clock so no
      // exception must be thrown if full revolution done once
      candidateSeen = true;

      // Advance Clock Pointer (continue to next iter of while loop)
      continue;
    }

    // page pinned?
    if (currFrameDesc->pinCnt > 0) {
      // Yes: Advance Clock Pointer (continue to next iter of while loop)
      continue;
    }

    // dirty bit set?
    if (currFrameDesc->dirty) {
      // Yes: Flush page to disk
      Page page = bufPool.at(currFrameDesc->frameNo);
      currFrameDesc->file.writePage(page);
      currFrameDesc->dirty = false;
    }

    // If the buffer frame has a valid page in it, remove the appropriate
    //   entry from the hash table
    hashTable.remove(currFrameDesc->file, currFrameDesc->pageNo);

    // Use Frame (caller needs to call "Set()" on the BufDesc for this frame)
    frame = clockHand;
    return;
  }
}

/**
* Reads the given page from the file into a frame and returns the pointer to
* page. If the requested page is already present in the buffer pool pointer
* to that frame is returned otherwise a new frame is allocated from the
* buffer pool for reading the page.
*
* @param file   	File object
* @param PageNo  Page number in the file to be read
* @param page  	Reference to page pointer. Used to fetch the Page object
* in which requested page from file is read in.
*/
void BufMgr::readPage(File &file, const PageId pageNo, Page *&page) {
  FrameId frameNo;  // will be frame number
  try {
    // lookup sets frameNo if successful
    hashTable.lookup(file, pageNo, frameNo);

    // Since the page is already in the table do what is necessary and
    // return

    // Set page (this is the return value)
    page = &bufPool.at(frameNo);

    BufDesc *bufDesc = &bufDescTable.at(frameNo);

    // do what is necessary
    bufDesc->refbit = true;
    bufDesc->pinCnt++;

    return;
  } catch (HashNotFoundException &e) {
    // page not currently in hashTable

    // allocate a buffer frame
    // FrameID of allocated frame set to frameNo
    allocBuf(frameNo);

    // read the page from disk into the buffer pool frame
    bufPool[frameNo] = file.readPage(pageNo);
    page = &bufPool.at(frameNo);

    // insert page into hashtable
    hashTable.insert(file, pageNo, frameNo);

    // invoke Set() on the frame to set it up properly
    BufDesc *frameDesc = &bufDescTable.at(frameNo);
    frameDesc->Set(file, pageNo);

    return;
  }
}

/**
* Unpin a page from memory since it is no longer required for it to remain in
* memory.
*
* @param file   	File object
* @param PageNo  Page number
* @param dirty		True if the page to be unpinned needs to be
* marked dirty
* @throws  PageNotPinnedException If the page is not already pinned
*/
void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty) {
  // decrements the pinCnt of the frame containing (file, PageNo) and if dirty
  // == true sets the dirty bit throws PageNotePinned if pntCnt is already zero
  // does nothing if page is not found in table

  FrameId frameNo;  // will be frame number
  try {
    // lookup sets frameNo if successful
    hashTable.lookup(file, pageNo, frameNo);

    BufDesc *bufDesc = &bufDescTable.at(frameNo);

    if (bufDesc->pinCnt == 0) {
        throw PageNotPinnedException(file.filename(), pageNo, frameNo);
    }

    bufDesc->pinCnt--;
    bufDesc->dirty = dirty;
  } catch (HashNotFoundException &e) {
    // does nothing if page is not found in table
  }
}

/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
*
* @param file   	File object
* @param PageNo  Page number. The number assigned to the page in the file is
* returned via this reference.
* @param page  	Reference to page pointer. The newly allocated in-memory
* Page object is returned via this reference.
*/
void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page) {
  // allocate empty page in specificed file by invoking file.allocatePage()
  // newly allocated page gets returned
  // allocBuf is called to get a buffer pool frame
  // entry is inserted into table
  // Set() is called to set up frame
  // Set() returns page num of new page and pointer to buffer frame

  // Get new Frame
  FrameId frameNo;  // will be frame number
  allocBuf(frameNo);

  Page newPage = file.allocatePage();

  // Returns both page number of the newly allocated page to the caller via the
  //    pageNo parameter and a pointer to the BUFFER FRAME allocated for the
  //    page via the page parameter
  pageNo = newPage.page_number();
  page = &bufPool.at(frameNo);

  // insert page into hashtable
  printf("%d %d\n", pageNo, frameNo);
  hashTable.insert(file, pageNo, frameNo);

  // invoke Set() on the frame to set it up properly
  BufDesc *frameDesc = &bufDescTable.at(frameNo);
  frameDesc->Set(file, pageNo);
}

/**
 * @brief Writes all dirty pages of file to the disk
 * All frames must have a pincount of 0 before it can be wrtiten
 * If not, error will be thrown
 *
 * @param file File object
 * @throws PagePinnedException if page's pincount is not equal to 0 in the
 * buffer pool
 * @throws BadBufferException if page is invalid
 */
void BufMgr::flushFile(File &file) {
  // iterate through bufTable to find pages corresponding to our
  // File parameter
  for (FrameId i = 0; i < numBufs; ++i) {
    if (bufDescTable[i].file == file) {
      PageId pageNo = bufDescTable[i].pageNo;

      // check if page is pinned
      if (bufDescTable[i].pinCnt != 0) {
        // outputs file name, page number, and frame number
        throw PagePinnedException(file.filename(), pageNo, i);
      }

      // check if page is valid
      if (bufDescTable[i].valid == false) {
        // outputs frame number, if dirty , if valid, and if page referenced
        // recently
        throw BadBufferException(i, bufDescTable[i].dirty,
                                 bufDescTable[i].valid, bufDescTable[i].refbit);
      }

      // check if page's dirty bit is set
      if (bufDescTable[i].dirty == true) {
        // assign current page a var
        Page currPage = bufPool[i];

        // increment accesses to bufStats
        bufStats.accesses++;

        // write page to disk
        bufDescTable[i].file.writePage(currPage);
        // increment disk writes
        bufStats.diskwrites++;

        // reset dirty bit
        bufDescTable[i].dirty = false;

        // remove page from hash table
        hashTable.remove(file, pageNo);

        // invoke clear method of bufDesc for page frame
        bufDescTable[i].clear();
      }
    }
  }
}

/**
 * @brief delete page from file as well as from the buffer pool if present
 *
 * @param file File object
 * @param PageNo Page number
 * @throws HashNotFoundException if page to be disposed is not in the buffer
 * pool
 */
void BufMgr::disposePage(File &file, const PageId PageNo) {
  try {
    FrameId frameNo;
    hashTable.lookup(file, PageNo, frameNo);

    // free frame
    bufDescTable[frameNo].clear();
    // corresponding entry is removed
    hashTable.remove(file, PageNo);
  } catch (HashNotFoundException &e) {
    // when page to be deleted is not in the buffer pool
    // we do nothing
  }

  // delete page from file
  file.deletePage(PageNo);
}

// END STUDENT ASSIGNED METHODS

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
