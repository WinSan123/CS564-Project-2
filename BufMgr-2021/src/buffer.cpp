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

namespace badgerdb
{

    constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

    //----------------------------------------
    // Constructor of the class BufMgr
    //----------------------------------------

    BufMgr::BufMgr(std::uint32_t bufs)
        : numBufs(bufs),
          hashTable(HASHTABLE_SZ(bufs)),
          bufDescTable(bufs),
          bufPool(bufs)
    {
        for (FrameId i = 0; i < bufs; i++)
        {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        clockHand = bufs - 1;
    }

    // START STUDENT HELPER METHODS

    // END STUDENT HELPER METHODS

    // START STUDENT ASSIGNED METHODS

    void BufMgr::advanceClock()
    {
        // advance clock pointer
        clockHand++;

        if (clockHand >= numBufs)
            // Reset clockHand to first buffer
            clockHand = 0;
    }

    void BufMgr::allocBuf(FrameId &frame)
    {
        // Allocate a free frame (just return the next available frame)

        // param frame is the variable which the return value should be in
        // throws BufferExceededException If no such buffer is found which can be
        // allocated

        // The buffer that the clock is starting on. Used together with
        // candidateSeen to determine if should throw BufferExceededException
        FrameId startingFrame = clockHand;

        // Whether a candidate has been seen since last rotation of clock hand
        bool candidateSeen = false;

        while (true)
        {
            // Advance the clock hand
            advanceClock();

            if ((clockHand == startingFrame) && (!candidateSeen))
                throw BufferExceededException();

            BufDesc currFrameDesc = bufDescTable.at(clockHand);

            // Valid set?
            if (!currFrameDesc.valid)
            {
                // No: use frame and call "Set()" on the frame ("Set()" is called
                //   by caller of this method to set the file and pageNum for the
                //   frame)
                frame = clockHand;
                return;
            }

            // refbit set?
            if (currFrameDesc.refbit)
            {
                // Yes: clear refbit
                currFrameDesc.refbit = false;

                // This frame can be used next full rotation of clock so no
                // exception must be thrown if full revolution done once
                candidateSeen = true;

                // Advance Clock Pointer (continue to next iter of while loop)
                continue;
            }

            // page pinned?
            if (currFrameDesc.pinCnt > 0)
            {
                // Yes: Advance Clock Pointer (continue to next iter of while loop)
                continue;
            }

            // dirty bit set?
            if (currFrameDesc.dirty)
            {
                // Yes: Flush page to disk
                Page page = bufPool.at(currFrameDesc.frameNo);
                currFrameDesc.file.writePage(page);
                currFrameDesc.dirty = false;
            }

            // If the buffer frame has a valid page in it, remove the appropriate
            //   entry from the hash table
            hashTable.remove(currFrameDesc.file, currFrameDesc.pageNo);

            // Use Frame (caller needs to call "Set()" on the BufDesc for this frame)
            frame = clockHand;
            return;
        }
    }

    void BufMgr::readPage(File &file, const PageId pageNo, Page *&page)
    {

        FrameId frameNo; // will be frame number
        try
        {
            // lookup sets frameNo if successful
            hashTable.lookup(file, pageNo, frameNo);

            // Since the page is already in the table do what is necessary and
            //   return

            // Set page (this is the return value)
            page = bufPool.at(frameNo);

            BufDesc bufDesc = bufDescTable.at(frameNo);

            // do what is necessary
            bufDesc.refbit = true;
            bufDesc.pinCnt++;

            return;
        }
        catch (HashNotFoundException *e)
        {
            // page not currently in hashTable

            // allocate a buffer frame
            // FrameID of allocated frame set to frameNo
            allocBuf(frameNo);

            // read the pag efrom disk into the buffer pool frame
            page = file.readPage(pageNo);
            bufPool.assign(frameNo, page);

            // insert page into hashtable
            hashTable.insert(file, pageNo, frameNo);

            // invoke Set() on the frame to set it up properly
            BufDesc frameDesc = bufDescTable.at(frameNo);
            frameDesc.Set(file, pageNo);

            return;
        }
    }

    void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty)
    {
    }

    void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page)
    {
    }

    /**
     * @brief Writes all dirty pages of file to the disk
     * All frames must have a pincount of 0 before it can be wrtiten
     * If not, error will be thrown       
     * 
     * @param file File object
     * @throws PagePinnedException if page's pincount is not equal to 0 in the buffer pool
     * @throws BadBufferException if page is invalid
     */
    void BufMgr::flushFile(File &file)
    {
        // iterate through bufTable to find pages corresponding to our
        // File parameter
        for (FrameId i = 0; i < numBufs; ++i)
        {
            if (bufDescTable[i].file == file)
            {
                PageId pageNo = bufDescTable[i].pageNo;

                // check if page is pinned
                if (bufDescTable[i].pinCnt != 0)
                {
                    // outputs file name, page number, and frame number
                    throw PagePinnedException(file.filename(), pageNo, i);
                }

                // check if page is valid
                if (bufDescTable[i].valid == false)
                {
                    // outputs frame number, if dirty , if valid, and if page referenced recently
                    throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
                }

                // check if page's dirty bit is set
                if (bufDescTable[i].dirty == true)
                {
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
     * @throws HashNotFoundException if page to be disposed is not in the buffer pool
     */
    void BufMgr::disposePage(File &file, const PageId PageNo)
    {
        try
        {
            FrameId frameNo;
            hashTable.lookup(file, PageNo, frameNo);

            //free frame
            bufDescTable[frameNo].clear();
            // corresponding entry is removed
            hashTable.remove(file, PageNo);
        }
        catch (HashNotFoundException e)
        {
            // when page to be deleted is not in the buffer pool
            // we do nothing
        }

        // delete page from file
        file.deletePage(PageNo);
    }

    // END STUDENT ASSIGNED METHODS

    void BufMgr::printSelf(void)
    {
        int validFrames = 0;

        for (FrameId i = 0; i < numBufs; i++)
        {
            std::cout << "FrameNo:" << i << " ";
            bufDescTable[i].Print();

            if (bufDescTable[i].valid)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

} // namespace badgerdb
