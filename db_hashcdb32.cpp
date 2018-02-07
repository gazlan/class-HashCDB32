/* ******************************************************************** **
** @@ HashCDB32
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__
#endif

/* ******************************************************************** **
** uses precompiled headers
** ******************************************************************** */

#include "stdafx.h"

#include <errno.h>

#include "file.h"
#include "hash_murmur3.h"
#include "db_hashcdb32.h"

/* ******************************************************************** **
** @@ internal defines
** ******************************************************************** */

#define DEFAULT_MURMUR_HASH_SEED       (5381)

/* ******************************************************************** **
** @@ internal prototypes
** ******************************************************************** */

struct CDB32_HEADER
{
   char     _pszText[3];
   BYTE     _byVersion;
   DWORD    _dwHash;
   DWORD    _dwHashSeed;
   DWORD    _dwRecTotal;
};

/* ******************************************************************** **
** @@ external global variables
** ******************************************************************** */

extern DWORD      dwKeepError;

/* ******************************************************************** **
** @@ static global variables
** ******************************************************************** */
            
/* ******************************************************************** **
** @@ real code
** ******************************************************************** */

/* ******************************************************************** **
** @@ HashCDB32::HashCDB32()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : Constructor
** ******************************************************************** */

HashCDB32::HashCDB32()
{
   _pMaker = new CDB32_MAKER;

   memset(_pMaker,0,sizeof(CDB32_MAKER));

   _pFinder = new CDB32_FINDER;

   memset(_pFinder,0,sizeof(CDB32_FINDER));

   _dwEnumSize = 0;
}

/* ******************************************************************** **
** @@ HashCDB32::~HashCDB32()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : Destructor
** ******************************************************************** */

HashCDB32::~HashCDB32()
{
   if (_pMaker)
   {
      for (int ii = 0; ii < CDB_PRIMARY_HASH_SIZE; ++ii)
      {
         if (_pMaker->_pRecList[ii])
         {
            CDB32_RECORD_LIST*   pNextList = _pMaker->_pRecList[ii]->_pNext;

            while (pNextList)
            {
               CDB32_RECORD_LIST*      pTemp = pNextList;

               pNextList = pTemp->_pNext;
                                 
               delete pTemp;
               pTemp = NULL;
            }

            delete _pMaker->_pRecList[ii];
            _pMaker->_pRecList[ii] = NULL;
         }
      }

      delete _pMaker;
      _pMaker = NULL;
   }

   if (_pFinder)
   {
      delete _pFinder;
      _pFinder = NULL;
   }
}

/* ******************************************************************** **
** @@ HashCDB32::SetBuffer()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::SetBuffer(const BYTE* const pBuf,DWORD dwSize)
{
   ASSERT(pBuf && dwSize);

   if (IsBadReadPtr(pBuf,dwSize))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   _pMemView    = pBuf;
   _dwFileSize  = dwSize;
   _dwValuePos  = 0;
   _dwValueSize = 0;
   _dwKeyPos    = 0;
   _dwKeySize   = 0;

   DWORD    dwDataEnd = *(DWORD*)pBuf;

   if (dwDataEnd < CDB_TOC_SIZE)
   {
      dwDataEnd = CDB_TOC_SIZE;
   }
   else if (dwDataEnd >= dwSize)
   {
      dwDataEnd = dwSize;
   }

   _dwEOD = dwDataEnd;

   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::IRead()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::IRead(void* pBuf,DWORD dwSize,DWORD dwRecPos)
{
   const void* const    pData = IGetChunkPtr(dwSize,dwRecPos);
  
   if (!pData)
   {
      // Error !
      ASSERT(0);
      return false;
   }
  
   memcpy(pBuf,pData,dwSize);
  
   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::ReadValue()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::ReadValue(void* pBuf,DWORD dwSize)
{
   if (IsBadWritePtr(pBuf,dwSize))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   return IRead(pBuf,GetValueSize(),GetValuePos());
}

/* ******************************************************************** **
** @@ HashCDB32::ReadKey()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::ReadKey(void* pBuf,DWORD dwSize)
{
   if (IsBadWritePtr(pBuf,dwSize))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   return IRead(pBuf,GetKeySize(),GetKeyPos());
}

/* ******************************************************************** **
** @@ HashCDB32::IGetChunkPtr()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

const void* const HashCDB32::IGetChunkPtr(DWORD dwChunkSize,DWORD dwChunkPos)
{
   if ((dwChunkPos > _dwFileSize) || ((_dwFileSize - dwChunkPos) < dwChunkSize))
   {
      // Error !
      ASSERT(0);
      dwKeepError = EINVAL;
      return NULL;
   }

   return _pMemView + dwChunkPos;
}

/* ******************************************************************** **
** @@ HashCDB32::GetValuePtr()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

const void* const HashCDB32::GetValuePtr()
{
   return IGetChunkPtr(GetValueSize(),GetValuePos());
}

/* ******************************************************************** **
** @@ HashCDB32::GetKeyPtr()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

const void* const HashCDB32::GetKeyPtr()
{
   return IGetChunkPtr(GetKeySize(),GetKeyPos());
}

/* ******************************************************************** **
** @@ HashCDB32::Point()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::Point(const void* const pKey,DWORD dwKeySize)
{
   if (dwKeySize >= _dwEOD) 
   {
      // Error !
      // Key size too large
      ASSERT(0);
      return false;
   }

   DWORD    dwHash = IHash(pKey,dwKeySize);

   // Find (dwRecPos,n) hash table to use
   // First CDB_TOC_SIZE bytes (TOC) are always available
   // (dwHash % PRIMARY_HASH_SIZE) * CDB_HASH_RECORD_SIZE
   // Index in TOC (PRIMARY_HASH_SIZE * CDB_HASH_RECORD_SIZE)
   const BYTE*    pHashTab = _pMemView + ((dwHash << 3) & 0x07FF); // 2047

   if (IsBadReadPtr(pHashTab + sizeof(DWORD),sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwHashTabSize = *(DWORD*)(pHashTab + sizeof(DWORD));
 
   if (!dwHashTabSize)         
   {
      // Error !
      // Empty table
      ASSERT(0);
      return false;
   }        
 
   DWORD    dwLookupSize = dwHashTabSize << 3;  // Bytes of pHashTabStart to lookup

   if (IsBadReadPtr(pHashTab,sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwRecPos = *(DWORD*)pHashTab; // pHashTabStart position
 
   // Overflow of dwLookupSize?
   // Is pHashTabStart inside data section?
   // pHashTabStart start within file?
   // Entire pHashTabStart within file?
   if ((dwHashTabSize > (_dwFileSize >> 3)) || (dwRecPos < _dwEOD) || (dwRecPos > _dwFileSize) || (dwLookupSize > (_dwFileSize - dwRecPos)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   const BYTE*    pHashTabStart = _pMemView + dwRecPos;
   const BYTE*    pHashTabEnd   = pHashTabStart + dwLookupSize;   // After end of pHashTabStart
 
   // HashTab starting position: rest of dwHash modulo htsize, CDB_HASH_RECORD_SIZE bytes per item
   pHashTab = pHashTabStart + (((dwHash >> 8) % dwHashTabSize) << 3);

   while (true)
   {
      if (IsBadReadPtr(pHashTab + sizeof(DWORD),sizeof(DWORD)))
      {
         // Error !
         ASSERT(0);
         return false;
      }

      dwRecPos = *(DWORD*)(pHashTab + sizeof(DWORD));   // Record position
 
      if (!dwRecPos)
      {
         // Error !
         ASSERT(0);
         return false;
      }
 
      if (IsBadReadPtr(pHashTab,sizeof(DWORD)))
      {
         // Error !
         ASSERT(0);
         return false;
      }

      if (*(DWORD*)pHashTab == dwHash)
      {
         if (dwRecPos > (_dwEOD - CDB_HASH_RECORD_SIZE)) // key+val lengths
         {
            // Error !
            ASSERT(0);
            return false;
         }
 
         if (IsBadReadPtr(_pMemView + dwRecPos,sizeof(DWORD)))
         {
            // Error !
            ASSERT(0);
            return false;
         }

         if (*(DWORD*)(_pMemView + dwRecPos) == dwKeySize)
         {
            if ((_dwEOD - dwKeySize) < (dwRecPos + CDB_HASH_RECORD_SIZE))
            {
               // Error !
               ASSERT(0);
               return false;
            }
 
            if (!memcmp(pKey,_pMemView + dwRecPos + CDB_HASH_RECORD_SIZE,dwKeySize))
            {
               if (IsBadReadPtr(_pMemView + dwRecPos + sizeof(DWORD),sizeof(DWORD)))
               {
                  // Error !
                  ASSERT(0);
                  return false;
               }

               dwHashTabSize = *(DWORD*)(_pMemView + dwRecPos + sizeof(DWORD));

               dwRecPos += CDB_HASH_RECORD_SIZE;
               
               if ((_dwEOD < dwHashTabSize) || ((_dwEOD - dwHashTabSize) < (dwRecPos + dwKeySize)))
               {
                  // Error !
                  ASSERT(0);
                  return false;
               }
 
               // Found
               _dwKeyPos    = dwRecPos;
               _dwKeySize   = dwKeySize;
               _dwValuePos  = dwRecPos + dwKeySize;
               _dwValueSize = dwHashTabSize;
 
               return true;
            }
         }
      }
 
      dwLookupSize -= CDB_HASH_RECORD_SIZE;
 
      if (!dwLookupSize)
      {
         // Error !
         ASSERT(0);
         return false;
      }
 
      if ((pHashTab += CDB_HASH_RECORD_SIZE) >= pHashTabEnd)
      {
         pHashTab = pHashTabStart;
      }
   }

   return false;
}

/* ******************************************************************** **
** @@ HashCDB32::GetValuePos()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

DWORD HashCDB32::GetValuePos()
{
   return _dwValuePos;
}

/* ******************************************************************** **
** @@ HashCDB32::GetValueSize()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

DWORD HashCDB32::GetValueSize()
{
   return _dwValueSize;
}

/* ******************************************************************** **
** @@ HashCDB32::GetKeyPos()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

DWORD HashCDB32::GetKeyPos()
{
   return _dwKeyPos;
}

/* ******************************************************************** **
** @@ HashCDB32::GetKeySize()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

DWORD HashCDB32::GetKeySize()
{
   return _dwKeySize;
}

/* ******************************************************************** **
** @@ HashCDB32::InitKeyFetchCursor()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */
/*
bool HashCDB32::InitKeyFetchCursor(const void* const pKey,DWORD dwKeySize)
{
   _pFinder->_pKey      = pKey;
   _pFinder->_dwKeySize = dwKeySize;
   _pFinder->_dwHash    = Hash(pKey,dwKeySize);

   _pFinder->_pHashTab = _pMemView + ((_pFinder->_dwHash << 3) & 0x07FF); // 2047

   if (IsBadReadPtr(_pFinder->_pHashTab + sizeof(DWORD),sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwRes = *(DWORD*)(_pFinder->_pHashTab + sizeof(DWORD));

   _pFinder->_dwLookupSize = dwRes << 3;

   if (!dwRes)
   {
      // Error !
      ASSERT(0);
      return false;
   }

   if (IsBadReadPtr(_pFinder->_pHashTab,sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwRecPos = *(DWORD*)(_pFinder->_pHashTab);

   if ((dwRes > (_dwFileSize >> 3)) || (dwRecPos < _dwEOD) || (dwRecPos > _dwFileSize) || (_pFinder->_dwLookupSize > (_dwFileSize - dwRecPos)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   // Found
   _pFinder->_pHashTabStart = _pMemView + dwRecPos;
   _pFinder->_pHashTabEnd   = _pFinder->_pHashTabStart + _pFinder->_dwLookupSize;
   _pFinder->_pHashTab      = _pFinder->_pHashTabStart + (((_pFinder->_dwHash >> 8) % dwRes) << 3);

   return true;
}
*/
/* ******************************************************************** **
** @@ HashCDB32::KeyFetch()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */
/*
bool HashCDB32::KeyFetch()
{
   DWORD    dwKeySize = _pFinder->_dwKeySize;

   while (_pFinder->_dwLookupSize)
   {
      if (IsBadReadPtr(_pFinder->_pHashTab + sizeof(DWORD),sizeof(DWORD)))
      {
         // Error !
         ASSERT(0);
         return false;
      }

      DWORD    dwRecPos = *(DWORD*)(_pFinder->_pHashTab + sizeof(DWORD));

      if (!dwRecPos)
      {
         // Error !
         ASSERT(0);
         return false;
      }

      if (IsBadReadPtr(_pFinder->_pHashTab,sizeof(DWORD)))
      {
         // Error !
         ASSERT(0);
         return false;
      }

      DWORD    dwRes = *(DWORD*)(_pFinder->_pHashTab) == _pFinder->_dwHash;

      _pFinder->_pHashTab += CDB_HASH_RECORD_SIZE;

      if (_pFinder->_pHashTab >= _pFinder->_pHashTabEnd)
      {
         _pFinder->_pHashTab = _pFinder->_pHashTabStart;
      }

      _pFinder->_dwLookupSize -= CDB_HASH_RECORD_SIZE;

      if (dwRes)
      {
         if (dwRecPos > (_dwFileSize - CDB_HASH_RECORD_SIZE))
         {
            // Error !
            ASSERT(0);
            return false;
         }

         if (IsBadReadPtr(_pMemView + dwRecPos,sizeof(DWORD)))
         {
            // Error !
            ASSERT(0);
            return false;
         }

         if (*(DWORD*)(_pMemView + dwRecPos) == dwKeySize)
         {
            if ((_dwFileSize - dwKeySize) < (dwRecPos + CDB_HASH_RECORD_SIZE))
            {
               // Error !
               ASSERT(0);
               return false;
            }

            if (!memcmp(_pFinder->_pKey,_pMemView + dwRecPos + CDB_HASH_RECORD_SIZE,dwKeySize))
            {
               if (IsBadReadPtr(_pMemView + dwRecPos + sizeof(DWORD),sizeof(DWORD)))
               {
                  // Error !
                  ASSERT(0);
                  return false;
               }

               dwRes = *(DWORD*)(_pMemView + dwRecPos + sizeof(DWORD));

               dwRecPos += CDB_HASH_RECORD_SIZE;

               if ((_dwFileSize < dwRes) || ((_dwFileSize - dwRes) < (dwRecPos + dwKeySize)))
               {
                  // Error !
                  ASSERT(0);
                  return false;
               }

               // Found
               _dwKeyPos    = dwRecPos;
               _dwKeySize   = dwKeySize;
               _dwValuePos  = dwRecPos + dwKeySize;
               _dwValueSize = dwRes;

               return true;
            }
         }
      }
   }

   // Error !
   ASSERT(0);
   return false;
}
*/
/* ******************************************************************** **
** @@ HashCDB32::IRecFetch()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::IRecFetch()
{
   DWORD          dwEOD    = _dwEOD;
   const BYTE*    pMemView = _pMemView;

   DWORD    dwRecPos = _dwEnumSize;

   if (dwRecPos > (dwEOD - CDB_HASH_RECORD_SIZE))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   if (IsBadReadPtr(pMemView + dwRecPos,sizeof(DWORD) * 2))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwKeySize   = *(DWORD*)(pMemView + dwRecPos);
   DWORD    dwValueSize = *(DWORD*)(pMemView + dwRecPos + sizeof(DWORD));

   dwRecPos += CDB_HASH_RECORD_SIZE;
   
   if (((dwEOD - dwKeySize) < dwRecPos) || ((dwEOD - dwValueSize) < (dwRecPos + dwKeySize)))
   {
      // Error !
      ASSERT(0);
      return false;
   }
   
   _dwKeyPos    = dwRecPos;
   _dwKeySize   = dwKeySize;
   _dwValuePos  = dwRecPos + dwKeySize;
   _dwValueSize = dwValueSize;

   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::Create()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

void HashCDB32::Create(HANDLE hFile)
{
   ASSERT(_pMaker);
   ASSERT(hFile != INVALID_HANDLE_VALUE);

   if (!_pMaker || (hFile == INVALID_HANDLE_VALUE))
   {
      // Error !
      ASSERT(0);
      return;
   }

   memset(_pMaker,0,sizeof(CDB32_MAKER));

   _pMaker->_hFile        = hFile;
   _pMaker->_dwDataPos    = sizeof(CDB32_HEADER) + CDB_TOC_SIZE;
//   _pMaker->_pWriteBufPos = _pMaker->_pWriteBuf  + CDB_TOC_SIZE;

   if (SetFilePointerCUR(_pMaker->_hFile,_pMaker->_dwDataPos,SEEK_SET) != _pMaker->_dwDataPos)
   {
      // Error !
      ASSERT(0);
      return;
   }
}

/* ******************************************************************** **
** @@ HashCDB32::IAppend()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

bool HashCDB32::IAppend(DWORD dwHash,const void* const pKey,DWORD dwKeySize,const void* const pValue,DWORD dwValueSize)
{  
   BYTE        pRecBuf[CDB_HASH_RECORD_SIZE];

   memset(pRecBuf,0,CDB_HASH_RECORD_SIZE);

   if ((dwKeySize > (0xFFFFFFFF - (_pMaker->_dwDataPos + CDB_HASH_RECORD_SIZE))) || (dwValueSize > (0xFFFFFFFF - (_pMaker->_dwDataPos + dwKeySize + CDB_HASH_RECORD_SIZE))))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    dwIdx = dwHash & 0xFF;

   CDB32_RECORD_LIST*   pRL = _pMaker->_pRecList[dwIdx];

   if (!pRL || (pRL->_dwCnt >= (sizeof(pRL->_pArr) / sizeof(pRL->_pArr[0]))))
   {
      pRL = new CDB32_RECORD_LIST;
     
      memset(pRL,0,sizeof(CDB32_RECORD_LIST));

      pRL->_dwCnt = 0;
      pRL->_pNext = _pMaker->_pRecList[dwIdx];

      _pMaker->_pRecList[dwIdx] = pRL;
   }

   dwIdx = pRL->_dwCnt++;

   pRL->_pArr[dwIdx]._dwHash   = dwHash;
   pRL->_pArr[dwIdx]._dwRecPos = _pMaker->_dwDataPos;
   
   memcpy(pRecBuf,                &dwKeySize,  sizeof(DWORD));
   memcpy(pRecBuf + sizeof(DWORD),&dwValueSize,sizeof(DWORD));

   if (!IWrite((BYTE*)pRecBuf,CDB_HASH_RECORD_SIZE) || !IWrite((BYTE*)pKey,dwKeySize) || !IWrite((BYTE*)pValue,dwValueSize))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   ++_pMaker->_dwRecCnt;

   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::Append()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

bool HashCDB32::Append(const void* const pKey,DWORD dwKeySize,const void* const pValue,DWORD dwValueSize)
{
   return IAppend(IHash(pKey,dwKeySize),pKey,dwKeySize,pValue,dwValueSize);
}

/* ******************************************************************** **
** @@ HashCDB32::Finalize()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

bool HashCDB32::Finalize()
{
   if (((0xFFFFFFFF - _pMaker->_dwDataPos) >> 3) < _pMaker->_dwRecCnt)
   {
      // Error !
      ASSERT(0);
      return false;
   }

   DWORD    pHashCnt[CDB_PRIMARY_HASH_SIZE];

   memset(pHashCnt,0,sizeof(DWORD) * CDB_PRIMARY_HASH_SIZE);

   DWORD    pHashPos[CDB_PRIMARY_HASH_SIZE];

   memset(pHashPos,0,sizeof(DWORD) * CDB_PRIMARY_HASH_SIZE);

   CDB32_RECORD_LIST*     pRecList = NULL;

   // Count pHashTabStart sizes and reorder RecLists
   DWORD    dwHashSize = 0;
  
   for (DWORD tt = 0; tt < CDB_PRIMARY_HASH_SIZE; ++tt)
   {
      CDB32_RECORD_LIST*     pRL_Temp = NULL;

      DWORD    ii = 0;
      
      pRecList = _pMaker->_pRecList[tt];
  
      while (pRecList)
      {
         CDB32_RECORD_LIST*     pRL_Next = pRecList->_pNext;
        
         pRecList->_pNext = pRL_Temp;
         
         pRL_Temp = pRecList;

         ii += pRecList->_dwCnt;
         
         pRecList = pRL_Next;
      }
  
      _pMaker->_pRecList[tt] = pRL_Temp;
  
      if (dwHashSize < (pHashCnt[tt] = ii << 1))
      {
         dwHashSize = pHashCnt[tt];
      }
   }

   // Allocate memory to hold max pHashTab
   CDB32_RECORD*    pHashTab = new CDB32_RECORD[dwHashSize + 2];

   memset(pHashTab,0,sizeof(CDB32_RECORD) * (dwHashSize + 2));

   CDB32_RECORD*    pHashTabOrg = pHashTab;
     
   BYTE*    pBuf = (BYTE*)pHashTab;

   pHashTab += 2;

   // Build hash tables
   for (tt = 0; tt < CDB_PRIMARY_HASH_SIZE; ++tt)
   {
      pHashPos[tt] = _pMaker->_dwDataPos;
  
      DWORD    dwLen = pHashCnt[tt];

      if (!dwLen)
      {
         continue;
      }
  
      for (DWORD ii = 0; ii < dwLen; ++ii)
      {
         pHashTab[ii]._dwHash   = 0;
         pHashTab[ii]._dwRecPos = 0;
      }
  
      for (pRecList = _pMaker->_pRecList[tt]; pRecList; pRecList = pRecList->_pNext)
      {
         for (ii = 0; ii < pRecList->_dwCnt; ++ii)
         {
            DWORD    dwHIdx = (pRecList->_pArr[ii]._dwHash >> 8) % dwLen;
  
            while (pHashTab[dwHIdx]._dwRecPos)
            {
               if (++dwHIdx == dwLen)
               {
                  dwHIdx = 0;
               }
            }
  
            pHashTab[dwHIdx] = pRecList->_pArr[ii];
         }
      }
  
      for (ii = 0; ii < dwLen; ++ii)
      {
         memcpy(pBuf + (ii << 3),&pHashTab[ii]._dwHash,sizeof(DWORD));
         memcpy(pBuf + (ii << 3) + sizeof(DWORD),&pHashTab[ii]._dwRecPos,sizeof(DWORD));
      }

      if (!IWrite(pBuf,dwLen << 3))
      {
         // Error !
         ASSERT(0);
         return false;
      }
   }

   delete pHashTabOrg;
   pHashTabOrg = NULL;

   pBuf = _pMaker->_pWriteBuf;
  
   for (tt = 0; tt < CDB_PRIMARY_HASH_SIZE; ++tt)
   {
      memcpy(pBuf + (tt << 3),&pHashPos[tt],sizeof(DWORD));

      memcpy(pBuf + (tt << 3) + sizeof(DWORD),&pHashCnt[tt],sizeof(DWORD));
   }
  
   CDB32_HEADER      _Header;

   memset(&_Header,0,sizeof(CDB32_HEADER));

   memcpy(_Header._pszText,"CDB",3);

   _Header._byVersion = 0;

   _Header._dwHash = MurmurHash3_x86_32(&_Header,sizeof(DWORD),MM3_DEFAULT_HASH_SEED);

   _Header._dwHashSeed = DEFAULT_MURMUR_HASH_SEED;

   _Header._dwRecTotal = _pMaker->_dwRecCnt;

   if (SetFilePointerBOF(_pMaker->_hFile) || !WriteBuffer(_pMaker->_hFile,&_Header,sizeof(CDB32_HEADER)))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   if (!WriteBuffer(_pMaker->_hFile,pBuf,CDB_TOC_SIZE))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::IHash()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

DWORD HashCDB32::IHash(const void* const pBuf,DWORD dwSize)
{
   return MurmurHash3_x86_32(pBuf,dwSize,DEFAULT_MURMUR_HASH_SEED);
}

/* ******************************************************************** **
** @@ HashCDB32::IWrite()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

bool HashCDB32::IWrite(const BYTE* const pBuf,DWORD dwSize)
{
   if (!WriteBuffer(_pMaker->_hFile,pBuf,dwSize))
   {
      // Error !
      ASSERT(0);
      return false;
   }

   _pMaker->_dwDataPos += dwSize;

   return true;
}

/* ******************************************************************** **
** @@ HashCDB32::IMatch()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  : 0 = not found, 1 = error, or record length
** ******************************************************************** */

DWORD HashCDB32::IMatch(DWORD dwPos,const char* const pKey,DWORD dwKeySize)
{
   char*    pPattern = (char*)pKey;
    
   if (SetFilePointerCUR(_pMaker->_hFile,sizeof(CDB32_HEADER) + dwPos,SEEK_SET) != dwPos)
   {
      return 1;
   }
   
   if (!ReadBuffer(_pMaker->_hFile,_pMaker->_pWriteBuf,CDB_HASH_RECORD_SIZE))
   {
      return 1;
   }
   
   if (IsBadReadPtr(_pMaker->_pWriteBuf,sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return 0;
   }

   if (*(DWORD*)(_pMaker->_pWriteBuf) != dwKeySize)
   {
      // Error !
      ASSERT(0);
      return 0;
   }

   // record length; check its validity
   if (IsBadReadPtr(_pMaker->_pWriteBuf + sizeof(DWORD),sizeof(DWORD)))
   {
      // Error !
      ASSERT(0);
      return 0;
   }

   DWORD    rlen = *(DWORD*)(_pMaker->_pWriteBuf + sizeof(DWORD));
   
   if (rlen > (_pMaker->_dwDataPos - dwPos - dwKeySize - CDB_HASH_RECORD_SIZE))
   {
      // Error !
      ASSERT(0);
      dwKeepError = EINVAL;  // someone changed our file?
      return 1;
   } 
   
   rlen += dwKeySize + CDB_HASH_RECORD_SIZE;

   while (dwKeySize)
   {
      int   len = dwKeySize > sizeof(_pMaker->_pWriteBuf) ? sizeof(_pMaker->_pWriteBuf) : dwKeySize;
   
      if (!ReadBuffer(_pMaker->_hFile,_pMaker->_pWriteBuf,len))
      {
         return 1;
      }
   
      if (memcmp(_pMaker->_pWriteBuf,pPattern,len))
      {
         return 0;
      }
   
      pPattern  += len;
      dwKeySize -= len;
   }

   return rlen;
}

/* ******************************************************************** **
** @@ HashCDB32::ISearch()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

int HashCDB32::ISearch(const void* const pKey,DWORD dwKeySize,DWORD dwHash)
{
   bool  bSeeked = false;

   int   ret    = 0;
   
   for (CDB32_RECORD_LIST* pRecList = _pMaker->_pRecList[dwHash & 0xFF]; pRecList; pRecList = pRecList->_pNext)
   {
      CDB32_RECORD*    pRecStart = pRecList->_pArr;

      // Cycle
      for (CDB32_RECORD*  pRec = pRecStart + pRecList->_dwCnt; --pRec >= pRecStart; )
      {
         if (pRec->_dwHash != dwHash)
         {
            continue;
         }
   
         bSeeked = true;
   
         DWORD    dwRes = IMatch(pRec->_dwRecPos,(char*)pKey,dwKeySize);
   
         if (!dwRes)
         {
            continue;
         }
   
         if (dwRes == 1)
         {
            return -1;
         }
   
         ret = 1;
   
         goto FINISH;
      }
   }
   
FINISH:
   
   if (bSeeked && (SetFilePointerCUR(_pMaker->_hFile,sizeof(CDB32_HEADER) + _pMaker->_dwDataPos,SEEK_SET) != (sizeof(CDB32_HEADER) + _pMaker->_dwDataPos)))
   {
      return -1;
   }

   return ret;
}

/* ******************************************************************** **
** @@ HashCDB32::First()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::First()
{  
   _dwEnumSize = CDB_TOC_SIZE;

   return IRecFetch();
}

/* ******************************************************************** **
** @@ HashCDB32::Next()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : 
** ******************************************************************** */

bool HashCDB32::Next()
{
   _dwEnumSize += (CDB_HASH_RECORD_SIZE + _dwKeySize + _dwValueSize);

   return IRecFetch();
}

/* ******************************************************************** **
** End of File
** ******************************************************************** */
