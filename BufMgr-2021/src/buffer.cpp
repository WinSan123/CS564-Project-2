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

    if (clockHand == startingFrame) {
      if (!candidateSeen) {
        throw BufferExceededException();
      } else {
        // Reset it so that if there is no candidateSeen this time around it
        // will throw error
        candidateSeen = false;
      }
    }

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

    try {
      // If the buffer frame has a valid page in it, remove the appropriate
      //   entry from the hash table
      hashTable.remove(currFrameDesc->file, currFrameDesc->pageNo);
    } catch (HashNotFoundException &e) {
    }

    // Use Frame (caller needs to call "Set()" on the BufDesc for this frame)
    frame = clockHand;
    return;
  }
}

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
    bufPool.at(frameNo) = file.readPage(pageNo);
    page = &bufPool.at(frameNo);

    // insert page into hashtable
    hashTable.insert(file, pageNo, frameNo);

    // invoke Set() on the frame to set it up properly
    BufDesc *frameDesc = &bufDescTable.at(frameNo);
    frameDesc->Set(file, pageNo);

    return;
  }
}

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
  hashTable.insert(file, pageNo, frameNo);

  // invoke Set() on the frame to set it up properly
  BufDesc *frameDesc = &bufDescTable.at(frameNo);
  frameDesc->Set(file, pageNo);
}

void BufMgr::flushFile(File &file) {
  // iterate through bufTable to find pages corresponding to our
  // File parameter
  for (FrameId i = 0; i < numBufs; ++i) {
    BufDesc *bufDesc = &bufDescTable.at(i);

    if (bufDesc->file == file) {
      PageId pageNo = bufDesc->pageNo;

      // check if page is pinned
      if (bufDesc->pinCnt != 0) {
        // throw file name, page number, and frame number
        throw PagePinnedException(file.filename(), pageNo, i);
      }

      // check if page is valid
      if (bufDesc->valid == false) {
        // throw framenumber, dirty, refbit
        throw BadBufferException(i, bufDesc->dirty, bufDesc->valid,
                                 bufDesc->refbit);
      }

      // check if page's dirty bit is set
      if (bufDesc->dirty == true) {
        // assign current page a var
        Page currPage = bufPool.at(i);

        // write page to disk
        bufDesc->file.writePage(currPage);

        // reset dirty bit
        bufDesc->dirty = false;

        // remove page from hash table
        hashTable.remove(file, pageNo);

        // invoke clear method of bufDesc for page frame
        bufDesc->clear();
      }
    }
  }
}

void BufMgr::disposePage(File &file, const PageId PageNo) {
  FrameId frameNo;
  hashTable.lookup(file, PageNo, frameNo);

  // free frame
  bufDescTable.at(frameNo).clear();

  // corresponding entry is removed
  hashTable.remove(file, PageNo);

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
