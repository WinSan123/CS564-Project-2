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

// START STUDENT HELPER METHODS

// END STUDENT HELPER METHODS

// START STUDENT ASSIGNED METHODS

void BufMgr::advanceClock() {
    // advance clock pointer
    this.clockHand++;

    if (this.clockHand >= this.numBufs)
        // Reset clockHand to first buffer
        this.clockHand = 0;
}

void BufMgr::allocBuf(FrameId& frame) {
    // Allocate a free frame
    // param frame is the variable which the return value should be in
    // throws BufferExceededException If no such buffer is found which can be
    // allocated
    //
    // CALLER OF THIS METHOD: MAKE SURE TO CHECK IF THE FRAME IS DIRTY
    // (this.bufDescTable.at(frame).dirty;) and then flush the page if it
    // is before using it

    // The buffer that the clock is starting on. Used together with
    // candidateSeen to determine if should throw BufferExceededException
    FrameId startingFrame = this.clockHand;

    // Whether a candidate has been seen since last rotation of clock hand
    bool candidateSeen = false;

    while (true) {
        // Advance the clock hand
        this.advanceClock();

        if ((this.clockHand == startingFrame) && (!candidateSeen))
            throw badgerdb::BufferExceededException();

        BufDesc currFrame = this.bufDescTable.at(this.clockHand);

        // Valid set?
        if (!currBufDesc.valid) {
            // No: use frame and call "Set()" on the frame ("Set()" is called
            //   by caller of this method to set the file and pageNum for the
            //   frame)
            frame = this.clockHand;
            return;
        }

        // refbit set?
        if (currFrame.refbit) {
            // Yes: clear refbit
            currFrame.refbit = false;

            // This frame can be used next full rotation of clock so no
            // exception must be thrown if full revolution done once
            candidateSeen = true;

            // Advance Clock Pointer (continue to next iter of while loop)
            continue;
        }

        // page pinned?
        if (currFrame.pinCnt > 0) {
            // Yes: Advance Clock Pointer (continue to next iter of while loop)
            continue;
        }

        // Use Frame (it was set to @param frame)
        frame = this.clockHand;
        return;
    }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
    PageId pageNo = NULL;

    currBuff = this.hashTable.lookup(file, pageNo, pageNo);
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
}

void BufMgr::flushFile(File& file) {
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
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
