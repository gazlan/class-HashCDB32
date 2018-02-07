/* ******************************************************************** **
** @@ HashCDB32
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#ifndef _HASH_CDB32_HPP_
#define _HASH_CDB32_HPP_

#if _MSC_VER > 1000
#pragma once
#endif

/* ******************************************************************** **
** @@ internal defines
** ******************************************************************** */

#define CDB_HASH_RECORD_SIZE              (sizeof(DWORD) * 2) 
#define CDB_PRIMARY_HASH_SIZE             (256) 
#define CDB_TOC_SIZE                      (CDB_PRIMARY_HASH_SIZE * CDB_HASH_RECORD_SIZE)

/* ******************************************************************** **
** @@ struct CDB32_RECORD
** @  Copyrt : 
** @  Author : 
** @  Update :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#pragma pack(push,1)
struct CDB32_RECORD
{              
   DWORD       _dwHash;
   DWORD       _dwRecPos;
};
#pragma pack(pop)

/* ******************************************************************** **
** @@ struct CDB32_RECORD_LIST
** @  Copyrt : 
** @  Author : 
** @  Update :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#pragma pack(push,1)
struct CDB32_RECORD_LIST
{                          
   CDB32_RECORD_LIST*      _pNext;
   DWORD                   _dwCnt;
   CDB32_RECORD            _pArr[254];
};
#pragma pack(pop)

/* ******************************************************************** **
** @@ struct CDB32_MAKER
** @  Copyrt : 
** @  Author : 
** @  Update :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#pragma pack(push,1)
struct CDB32_MAKER
{
   HANDLE                  _hFile;
   // private 
   DWORD                   _dwDataPos;    
   DWORD                   _dwRecCnt;
   BYTE                    _pWriteBuf[CDB_PRIMARY_HASH_SIZE << 3];
   CDB32_RECORD_LIST*      _pRecList[CDB_PRIMARY_HASH_SIZE];   // List of arrays of record infos
};
#pragma pack(pop)

/* ******************************************************************** **
** @@ struct CDB32_FINDER
** @  Copyrt : 
** @  Author : 
** @  Update :
** @  Update :
** @  Notes  :
** ******************************************************************** */

#pragma pack(push,1)
struct CDB32_FINDER
{
   DWORD                      _dwHash;
   const BYTE*                _pHashTab;
   const BYTE*                _pHashTabStart;
   const BYTE*                _pHashTabEnd;
   DWORD                      _dwLookupSize;
   const void*                _pKey;
   DWORD                      _dwKeySize;
};
#pragma pack(pop)

/* ******************************************************************** **
** @@ class HashCDB32
** @  Copyrt : 
** @  Author : 
** @  Update :
** @  Update :
** @  Notes  :
** ******************************************************************** */

class HashCDB32
{
   private:

      CDB32_MAKER*         _pMaker;
      CDB32_FINDER*        _pFinder;
      DWORD                _dwEnumSize;
      const BYTE*          _pMemView;     // Map'ed file memory
      DWORD                _dwFileSize;
      DWORD                _dwEOD;        // End of data ptr
      DWORD                _dwRecPos;
      DWORD                _dwValueSize;  // Found data
      DWORD                _dwValuePos;
      DWORD                _dwKeySize;    // Found key
      DWORD                _dwKeyPos;

   public:

       HashCDB32();
      ~HashCDB32();
      
      // Create new
      void     Create(HANDLE hFile);
      bool     Append(const void* const pKey,DWORD dwKeySize,const void* const pValue,DWORD dwValueSize);
      bool     Finalize();
            
      // Open exist
      bool     SetBuffer(const BYTE* const pBuf,DWORD dwSize);

      // Access            
      __inline const void* const GetValuePtr();
      __inline const void* const GetKeyPtr();

      bool     ReadKey  (void* pBuf,DWORD dwSize);
      bool     ReadValue(void* pBuf,DWORD dwSize);

      // Search
      bool     First();
      bool     Next();
      bool     Point(const void* const pKey,DWORD dwKeySize);
                
//      bool     InitKeyFetchCursor(const void* const pKey,DWORD dwKeySize);
//      bool     KeyFetch();
//      void     SetRecFetchCursor();

      DWORD    GetKeyPos   ();
      DWORD    GetKeySize  ();

      DWORD    GetValuePos ();
      DWORD    GetValueSize();

   private:
                  
      DWORD    IHash(const void* const pBuf,DWORD dwSize);
      bool     IRecFetch();
      bool     IRead(void* pBuf,DWORD dwSize,DWORD dwPos);
      bool     IAppend(DWORD dwHash,const void* const pKey,DWORD dwKeySize,const void* const pValue,DWORD dwValueSize);
      bool     IWrite(const BYTE* const pBuf,DWORD dwSize);
      DWORD    IMatch(DWORD dwPos,const char* const pKey,DWORD dwKeySize);
                        
      int      ISearch(const void* const pKey,DWORD dwKeySize,DWORD dwHash);

      const void* const    IGetChunkPtr(DWORD dwChunkSize,DWORD dwChunkPos);
};

/* ******************************************************************** **
** @@ internal prototypes
** ******************************************************************** */

/* ******************************************************************** **
** @@ external global variables
** ******************************************************************** */

/* ******************************************************************** **
** @@ static global variables
** ******************************************************************** */

/* ******************************************************************** **
** @@ Global Function Prototypes
** ******************************************************************** */

#endif

/* ******************************************************************** **
** End of File
** ******************************************************************** */
