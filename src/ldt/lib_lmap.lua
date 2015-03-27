-- Large Map (LMAP) Operations Library
-- ======================================================================
-- Copyright [2015] Aerospike, Inc.. Portions may be licensed
-- to Aerospike, Inc. under one or more contributor license agreements.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- ======================================================================
--
-- Track the data and iteration of the last update.
local MOD="lib_lmap_2014_12_14.B"; 

-- This variable holds the version of the code. It should match the
-- stored version (the version of the code that stored the ldtCtrl object).
-- If there's a mismatch, then some sort of upgrade is needed.
-- This number is currently an integer because that is all that we can
-- store persistently.  Ideally, we would store (Major.Minor), but that
-- will have to wait until later when the ability to store real numbers
-- is eventually added.
local G_LDT_VERSION = 3;

-- ======================================================================
-- || GLOBAL PRINT and GLOBAL DEBUG ||
-- ======================================================================
-- Use these flags to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- (*) "F" is used for general debug prints
-- (*) "E" is used for ENTER/EXIT prints
-- (*) "B" is used for BANNER prints
-- (*) DEBUG is used for larger structure content dumps.
-- ======================================================================
local GP;     -- Global Print Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=false; -- Set B (Banners) to true to turn ON Banner Print
local D=false; -- Set D (Detail) to get more Detailed Debug Output.
local DEBUG=false; -- turn on for more elaborate state dumps.

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LMAP Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LMAP library module:
--
-- (*) Status = lmap.put( topRec, ldtBinName, newName, newValue, createSpec) 
-- (*) Status = lmap.put_all( topRec, ldtBinName, nameValueMap, createSpec)
-- (*) Map    = lmap.get( topRec, ldtBinName, searchName )
-- (*) Map    = lmap.get_all( topRec, ldtBinName, nameList )
-- (*) Map    = lmap.exist( topRec, ldtBinName, searchName )
-- (*) Map    = lmap.scan( topRec, ldtBinName, filterModule, filter, fargs )
-- (*) List   = lmap.nameList( topRec, ldtBinName )
-- (*) Status = lmap.remove( topRec, ldtBinName, searchName )
-- (*) Status = lmap.destroy( topRec, ldtBinName )
-- (*) Number = lmap.size( topRec, ldtBinName )
-- (*) Map    = lmap.config( topRec, ldtBinName )
-- (*) Status = lmap.set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Number = lmap.get_capacity( topRec, ldtBinName )
-- (*) Number = lmap.ldt_exists(topRec, ldtBinName)
-- ======================================================================
-- Deprecated:
-- (*) Status = lmap.create( topRec, ldtBinName, createSpec) 
-- ======================================================================
--
-- Large Map Design/Architecture
--
-- The Large Map follows typical Map function, which is to say that it
-- contains a (potentially large) collection of name/value pairs.  These
-- name/value pairs are held in sub-record storage containers, which keeps
-- the amount of data stored in the main (top) record relatively small.
--
-- The Large Map design uses a single Bin (user-named LDT Bin) to hold
-- an LDT control structure that holds a Hash Directory.  The Hash directory
-- contains sub-record references (digests).  To locate a value, we hash
-- the name, follow the hash(name) modulo HashDirSize to a Hash Directory
-- Cell, and then search that sub-record for the name.
-- Each Subrecord contains two data lists, one for names and one for values.
-- ======================================================================
-- >> Please refer to ldt/doc_lmap.md for architecture and design notes.
-- ======================================================================

-- ======================================================================
-- Aerospike Database Server Functions:
-- ======================================================================
-- Aerospike Record Functions:
-- status = aerospike:create( topRec )
-- status = aerospike:update( topRec )
-- status = aerospike:remove( rec ) (not currently used)
--
-- Aerospike SubRecord Functions:
-- newRec = aerospike:create_subrec( topRec )
-- rec    = aerospike:open_subrec( topRec, digestString )
-- status = aerospike:update_subrec( childRec )
-- status = aerospike:close_subrec( childRec )
-- status = aerospike:remove_subrec( subRec ) 
--
-- Record Functions:
-- digest = record.digest( childRec )
-- status = record.set_type( topRec, recType )
-- status = record.set_flags( topRec, binName, binFlags )
-- ======================================================================
--
-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt/ldt_errors');

-- Import our third party Hash Function:
local CRC32 = require('ldt/CRC32');

-- We have recently moved a number of COMMON functions into the "ldt_common"
-- module, namely the subrec routines and some list management routines.
-- We will likely move some other functions in there as they become common.
local ldt_common = require('ldt/ldt_common');

local Map = getmetatable( map() );
local List = getmetatable( list() );
-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
-- This flavor of LDT (LMAP)
local LDT_TYPE = "LMAP";

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE = 'T';
local AS_FALSE = 'F';

-- Our Hash Tables can operate in two modes:
-- (*) Static (a fixed size Hash Table)
-- (*) Dynamic (A variable size Hash Table that uses Linear Hash Algorithm)
--     (Dynamic hashing is not yet operational, but will be soon).
local HS_STATIC  = 'S';
local HS_DYNAMIC = 'D';

-- ======================= << DEFAULT VALUES >> ========================
local DEFAULT = {

  -- Currently, the default for the Hash Table Management is STATIC.  When
  -- it is ready (fully tested), we will enable DYNAMIC mode that uses Linear
  -- Hashing and has more graceful directory growth (and shrinkage).
  HASH_STATE = HS_STATIC;

  -- Initial Size of of a STATIC Hash Table.   Once we start to use Linear
  -- Hashing (dynamic growth) we'll set the initial size to be small.
  -- The Hash Directory has a default starting size that can be overwritten.
  HASH_MODULO = 512;

  -- The Hash Directory has a "number of bits" (hash depth) that it uses to
  -- to calculate calculate the current hash value.
  HASH_DEPTH = 9; -- goes with HASH_MODULO, above.
  --
  -- Switch from a single list to distributed lists after this amount, i.e.
  -- convert the compact list to a hash directory of cells.
  THRESHOLD = 10;

  -- Switch from a SMALL list in the cell anchor to a full Sub-Rec.
  -- We've set this to ZERO as a default, because any significant
  -- size object can cause use to exceed the TopRecord Size.
  BINLIST_THRESHOLD = 0;
};

local MAGIC="MAGIC";     -- the magic value for Testing LDT integrity

-- The LDT Control Structure is a LIST of Objects:
-- (*) A Common Property Map
-- (*) An LDT-Specific Map
-- (*) A Storage Format Value (or Object).
local LDT_PROP_MAP  = 1;
local LDT_CTRL_MAP  = 2;
local LDT_SF_VAL    = 3;

-- The Storage Format value will (currently) be one of two values,
-- either (1) for the initial LDT design (MSG_PACK) or (2) a JSON encoding
-- that converts to/from JSON strings and Lua Tables.
local SF_MSGPACK    = 1;
local SF_JSON       = 2;

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT = 'C'; -- Using "single cell list" (compact) mode
local SS_REGULAR = 'R'; -- Using "Regular Storage" (regular) mode

-- Result Returns (successful values).  Errors are a different category.
local RESULT_OK = 0;
local RESULT_OVERWRITE = 1;

-- In order to tell the Server what's happening with LDT (and maybe other
-- calls), we call "set_context()" with various flags.  The server then
-- uses this to measure LDT call behavior.
local UDF_CONTEXT_LDT = 1;

-- Key Compare Function for Complex Objects
-- By default, a complex object will have a "KEY" field, which the
-- key_compare() function will use to compare.  If the user passes in
-- something else, then we'll use THAT to perform the compare, which
-- MUST return -1, 0 or 1 for A < B, A == B, A > B.
-- UNLESS we are using a simple true/false equals compare.
-- ========================================================================
-- Actually -- the default will be EQUALS.  The >=< functions will be used
-- in the Ordered LIST implementation, not in the simple list implementation.
-- ========================================================================
local KC_DEFAULT="keyCompareEqual"; -- Key Compare used only in complex mode
local KH_DEFAULT="keyHash";         -- Key Hash used only in complex mode

-- Enhancements for LMAP begin here 

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
-- come back to bite me.
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (CDIR NOT used in this context)
-- (2) As a TYPE in our own propMap[PM.RecType] field: CDIR *IS* used here.
local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
local RT_ESR = 4; -- 0x4: Existence Sub Record

---- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values. (There's no secret message hidden in these values).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- Trying to keep a consistent mapping across all LDT's : lstacks, lmap, lset 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common across all LDTs and managed by the Record Property Map
local RPM = {
  LdtCount             = 'C',  -- Number of LDTs in this rec
  VInfo                = 'V',  -- Partition Version Info
  Magic                = 'Z',  -- Special Sauce
  SelfDigest           = 'D'   -- Digest of this record
};

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common for all LDT's
local PM = {
  ItemCount             = 'I', -- (Top): Count of all items in LDT
  Version               = 'V', -- (Top): Code Version
  SubRecCount           = 'S', -- (Top): # of sub-recs in the LDT
  LdtType               = 'T', -- (Top): Type: stack, set, map, list
  BinName               = 'B', -- (Top): LDT Bin Name
  Magic                 = 'Z', -- (All): Special Sauce
  CreateTime			= 'C',
  RecType               = 'R', -- (All): Type of Rec:Top,Ldr,Esr,CDir
  EsrDigest             = 'E', -- (All): Digest of ESR
  ParentDigest          = 'P', -- (Subrec): Digest of TopRec
  SelfDigest            = 'D'  -- (Subrec): Digest of THIS Record
};

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LDT Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- These fields are common across all LDTs:
-- Fields Common (LC) to ALL LDTs (managed by the LDT COMMON routines)
local LC = {
  UserModule             = 'P', -- User's Lua file for overrides
  StoreMode              = 'M', -- List Mode or Binary Mode
  StoreLimit             = 'L', -- Used for Eviction (eventually)
};

-- LDT Fields Specific (LS) to lmap 
local LS = {
  HashType               = 'h', -- Hash Type: HS_STATIC or HS_DYNAMIC.
  HashDepth              = 'd', -- # of Hash Bits to use in Dynamic Hashing
  LdrEntryCountMax       = 'e', -- Max # of items in an LDR
  LdrByteEntrySize       = 's',
  LdrByteCountMax        = 'b',
  StoreState             = 'S',-- "Compact List" or "Regular Hash"
  BinaryStoreSize        = 'B', 
  TotalCount             = 'N',-- Total insert count (not counting dels)
  HashDirSize            = 'O',-- Show current Hash Dir Size
  HashDirMark            = 'm',-- Show where we are in the linear hash
  Threshold              = 'H',-- Convert from simple list to hash table
  CompactNameList        = 'n',--Simple Compact List -- before "dir mode"
  CompactValueList       = 'v',--Simple Compact List -- before "dir mode"
  OverWrite              = 'o',-- Allow Overwrite of a Value for a given
                                     -- name.  If false (AS_FALSE), then we
                                     -- throw a UNIQUE error.
  HashDirectory          = 'W',-- The Directory of Hash Entries
  HashCellMaxList        = 'X' -- Max List size in a Cell anchor
};

-- The LDR (SubRecord) Control Map holds MINIMAL information:
-- It currently holds ONLY the Byte Count -- for when Binary Storage is
-- in effect.  Otherwise, it is mostly ignored.
local LDR_ByteEntryCount       = 'C';

-- ------------------------------------------------------------------------
-- Maintain the LDT letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
-- -----------------------------------------------------------------------
---- >>> Be Mindful of the LDT Common Fields that ALL LDTs must share <<<
-- -----------------------------------------------------------------------
-- A:                        a:                        0:
-- B:LS.BinaryStoreSize      b:LS.LdrByteCountMax       1:
-- C:                        c:                        2:
-- D:                        d:LS.HashDepth            3:
-- E:                        e:LS.LdrEntryCountMax      4:
-- G:                        g:                        6:
-- H:LS.Threshold            h:LS.HashType              7:
-- I:                        i:                        8:
-- J:                        j:                        9:
-- K:                        k:LC.KeyType         
-- L:LC.StoreLimit           l:
-- M:LC.StoreMode            m:LS.HashDirMark
-- N:LS.TotalCount           n:LS.CompactNameList
-- O:                        o:LS.OverWrite
-- P:                        p:
-- Q:                        q:
-- R:                        r:
-- S:LS.StoreState           s:LS.LdrByteEntrySize
-- V:                        v:LS.CompactValueList
-- W:LS.HashDirectory         w:                     
-- X:                        x:                    
-- Y:                        y:
-- Z:                        z:
-- -----------------------------------------------------------------------
-- Cell Anchors are used in both LSET and LMAP.  They use the same Hash
-- Directory structure and Hash Cell structure, except that LSET uses a
-- SINGLE value list and LMAP uses two lists (Name, Value).
-- -----------------------------------------------------------------------
-- Cell Anchor Fields:  A cell anchor is a map object that sits in each
-- cell of the hash directory.   Since we don't have the freedom of keeping
-- NULL array entries (as one might in C), we have to keep an active object
-- in the Hash Directory list, otherwise, a NULL (nil) entry would actually
-- crash in message pack (or, somewhere).
--
-- A Hash Cell can be in one of FOUR states:
-- (1) C_EMPTY: just the CellState has a value.
-- (2) C_LIST: a small list of objects is anchored to this cell.
-- (3) C_DIGEST: A SINGLE DIGEST value points to a single sub-record.
-- (4) C_TREE: A Tree Root points to a set of Sub-Records
-- -----------------------------------------------------------------------
-- Here are the fields used in a Hash Cell Anchor
local C_CellState      = 'S'; -- Hold the Cell State
local C_CellNameList   = 'N'; -- Pt to a LIST of Name objects
local C_CellValueList  = 'V'; -- Pt to a LIST of Value objects
local C_CellDigest     = 'D'; -- Pt to a single digest value
local C_CellTree       = 'T'; -- Pt to a LIST of digests (Radix Tree)
local C_CellItemCount  = 'C'; -- Cell Item count, once we're in Sub-Rec Mode

-- Here are the various constants used with Hash Cells
-- Hash Cell States:
local C_STATE_EMPTY   = 'E'; -- This cell is empty.
local C_STATE_LIST    = 'L'; 
local C_STATE_DIGEST  = 'D';
local C_STATE_TREE    = 'T';

-- NOTE that we may choose to mark a Hash Cell Entry as "EMPTY" directly
-- and save the space and MSG_PACK cost of converting a map that holds ONLY
-- an "E". This will add one more check to the logic (check type/value)
-- but it will probably be worth it overall to avoid packing empty maps.

-- Other Hash Cell values/objects
-- (TBD)

-- -----------------------------------------------------------------------
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++====================++
-- || INTERNAL BIN NAMES || -- Local, but global to this module
-- ++====================++
-- The Top Rec LDT bin is named by the user -- so there's no hardcoded name
-- for each used LDT bin.
--
-- In the main record, there is one special hardcoded bin -- that holds
-- some shared information for all LDTs.
-- Note the 14 character limit on Aerospike Bin Names.
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local REC_LDT_CTRL_BIN  = "LDTCONTROLBIN"; -- Single bin for all LDT in rec

-- There are TWO different types of (Child) sub-records that are associated
-- with an LMAP LDT:
-- (1) LDR (LDT Data Record) -- used in both the Warm and Cold Lists
-- (2) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- Note the 14 character limit on Aerospike Bin Names.
--                         123456789ABCDE
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_NLIST_BIN     = "LdrNListBin";  
local LDR_VLIST_BIN     = "LdrVListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin.
local SUBREC_PROP_BIN="SR_PROP_BIN";
--
-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)


-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================
-- We have several different situations where we need to look up a user
-- defined function:
-- (*) Predicate Filter (perform additional predicate tests on an object)
--
-- These functions are passed in by name (UDF name, Module Name), so we
-- must check the existence/validity of the module and UDF each time we
-- want to use them.  Furthermore, we want to centralize the UDF checking
-- into one place -- so on entry to those LDT functions that might employ
-- these UDFs (e.g. insert, filter), we'll set up either READ UDFs or
-- WRITE UDFs and then the inner routines can call them if they are
-- non-nil.
-- ======================================================================
local G_Filter = nil;
local G_FunctionArgs = nil;

-- Special Function -- if supplied by the user in the "createModule", then
-- we call that UDF to adjust the LDT configuration settings.
local G_SETTINGS = "adjust_settings";

-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- -----------------------------------------------------------------------
-- resetPtrs()
-- -----------------------------------------------------------------------
-- Reset the UDF Ptrs to nil.
-- -----------------------------------------------------------------------
local function resetUdfPtrs()
  G_Filter = nil;
  G_FunctionArgs = nil;
end -- resetPtrs()

-- ======================================================================
-- propMapSummary( resultMap, propMap )
-- ======================================================================
-- Add the propMap properties to the supplied resultMap.
-- ======================================================================
local function propMapSummary( resultMap, propMap )

  -- Fields common for all LDT's
  resultMap.PropItemCount        = propMap[PM.ItemCount];
  resultMap.PropVersion          = propMap[PM.Version];
  resultMap.PropSubRecCount      = propMap[PM.SubRecCount];
  resultMap.PropLdtType          = propMap[PM.LdtType];
  resultMap.PropBinName          = propMap[PM.BinName];
  resultMap.PropMagic            = propMap[PM.Magic];
  resultMap.PropCreateTime       = propMap[PM.CreateTime];
  resultMap.PropEsrDigest        = propMap[PM.EsrDigest];
  resultMap.PropRecType          = propMap[PM.RecType];
  resultMap.PropParentDigest     = propMap[PM.ParentDigest];
  resultMap.PropSelfDigest       = propMap[PM.SelfDigest];
end -- function propMapSummary()
  
-- ======================================================================
-- ldtMapSummary( resultMap, ldtMap )
-- ======================================================================
-- Add the LDT Map properties to the supplied resultMap.
-- ======================================================================
local function ldtMapSummary( resultMap, ldtMap )

  -- General LMAP Parms:
  resultMap.StoreMode            = ldtMap[LC.StoreMode];
  resultMap.StoreState           = ldtMap[LS.StoreState];
  resultMap.KeyType              = ldtMap[LC.KeyType];
  resultMap.BinaryStoreSize      = ldtMap[LS.BinaryStoreSize];
  resultMap.KeyType              = ldtMap[LC.KeyType];
  resultMap.TotalCount	         = ldtMap[LS.TotalCount];		
  resultMap.HashDirSize          = ldtMap[LS.HashDirSize];
  resultMap.Threshold		     = ldtMap[LS.Threshold];
  resultMap.HashCellMaxList      = ldtMap[LS.HashCellMaxList];
  
  -- LDT Data Record Settings:
  resultMap.LdrEntryCountMax     = ldtMap[LS.LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[LS.LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[LS.LdrByteCountMax];

end -- function ldtMapSummary


-- ======================================================================
-- ldtDebugDump()
-- ======================================================================
-- To aid in debugging, dump the entire contents of the ldtCtrl object
-- for LMAP.  Note that this must be done in several prints, as the
-- information is too big for a single print (it gets truncated).
-- ======================================================================
local function ldtDebugDump( ldtCtrl )
  local meth = "ldtDebugDump()";

  -- Print MOST of the "TopRecord" contents of this LMAP object.
  local resultMap                = map();
  resultMap.SUMMARY              = "LMAP Summary";

  trace("\n\n <><>  BEGIN <><><> [ LDT LMAP SUMMARY ] <><><><><><><><><> \n");

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    trace("<<<%s>>>", tostring(resultMap));
    return 0;
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  
  if( propMap[PM.Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN MAP--No Magic";
    trace("<<<%s>>>", tostring(resultMap));
    return 0;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );
  trace("\n<<<%s>>>\n", tostring(resultMap));
  resultMap = nil;

  -- Reset for each section, otherwise the result would be too much for
  -- the info call to process, and the information would be truncated.
  local resultMap2 = map();
  resultMap2.SUMMARY              = "LMAP-SPECIFIC Values";

  -- Load the LMAP-specific properties
  ldtMapSummary( resultMap2, ldtMap );
  trace("\n<<<%s>>>\n", tostring(resultMap2));
  resultMap2 = nil;

  -- Print the Hash Directory
  local resultMap3 = map();
  resultMap3.SUMMARY              = "LMAP Hash Directory";
  resultMap3.HashDirectory        = ldtMap[LS.HashDirectory];
  trace("\n<<<%s>>>\n", tostring(resultMap3));

  trace("\n\n <><><> END  <><><> [ LDT LMAP SUMMARY ] <><><><><><><><><> \n");
end -- function ldtDebugDump()

-- ======================================================================
-- local function ldtSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtCtrl 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function ldtSummary( ldtCtrl )
  local meth = "ldtSummary()";

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();
  resultMap.SUMMARY              = "LMAP Summary";

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    return resultMap;
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  if( propMap == nil or ldtMap == nil ) then
    warn("[ERROR]<%s:%s>: EMPTY PropMap or LDT Map", MOD, meth);
    resultMap.ERROR = "EMPTY PropMap or LDT Map";
    return resultMap;
  end
  
  if( propMap[PM.Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN MAP--No Magic";
    return resultMap;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );

  -- Load the LMAP-specific properties
  ldtMapSummary( resultMap, ldtMap );

  return resultMap;
end -- ldtSummary()

-- ======================================================================
-- Make it easier to use ldtSummary(): Have a String version.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
    return tostring( ldtSummary( ldtCtrl ) );
end

-- ======================================================================
-- local function ldtMapSummary( ldtMap )
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap 
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- Note that for THIS purpose -- the summary map has the full long field
-- names in it -- so that we can more easily read the values.
-- ======================================================================
local function ldtMapDump( ldtMap )
  local meth = "ldtMapDump()";

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();
  resultMap.SUMMARY              = "CTRL MAP Summary";

  if ( ldtMap == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT MAP!!", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT MAP.";
    return resultMap;
  end

  ldtMapSummary( resultMap, ldtMap );

  return resultMap;
end -- ldtMapDump()

-- ======================================================================
-- Make it easier to use ldtMapSummary(): Have a String version.
-- ======================================================================
local function ldtMapSummaryString( ldtMap )
    return tostring( ldtMapDump( ldtMap ) );
end

-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two main Record Types used in the LDT Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LDT bin
-- (*) ldtBinName: the LDT Data Record that holds user Data
-- (*) compact_mode_flag : decides LMAP storage mode : SS_COMPACT or SS_REGULAR
--
-- <+> Naming Conventions:
--   + All Field names (e.g. LC.StoreMode) begin with Upper Case
--   + All variable names (e.g. ldtMap) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[ldtBinName] or ldrRec[LDR_CTRL_BIN]);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ======================================================================
-- initializeLdtCtrl: (LMAP)
-- ======================================================================
-- Set up the LMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LDT BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LMAP
-- behavior.  Thus this function represents the "type" LMAP -- all
-- LMAP control fields are defined here.
-- The LMap is obtained using the user's LMap Bin Name.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the LDT
-- Return: The initialized ldtCtrl structure.
-- ======================================================================
-- Additional Notes:
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLdtCtrl( topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  
  -- Create 2 maps : The generic property map 
  -- and lmap specific property map. Create one
  -- list : the actual LDR list for lmap. 
  -- Note: All Field Names start with UPPER CASE.
  local ldtMap = map();
  local propMap = map(); 
  local ldtCtrl = list(); 
  
  GP=E and trace("[ENTER]<%s:%s>::ldtBinName(%s)",
      MOD, meth, tostring(ldtBinName));

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM.ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM.SubRecCount] = 0;
  propMap[PM.Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM.LdtType]    = LDT_TYPE; -- Validate the ldt type
  propMap[PM.Magic]      = MAGIC; -- Special Validation
  propMap[PM.BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM.RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM.EsrDigest]  = 0; -- not set yet.
  propMap[PM.SelfDigest] = record.digest( topRec );
  propMap[PM.RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM.CreateTime] = aerospike:get_current_time();
  
  ldtMap[LC.StoreLimit]  = 0; -- No storage Limit at start

  -- LMAP Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[LS.LdrByteEntrySize] =   0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  =   0; -- Max # of Data Chunk Bytes (binary mode)

  ldtMap[LS.StoreState]       = SS_COMPACT; -- Start out in "single list" mode
  ldtMap[LS.HashType]         = HS_STATIC; -- HS_STATIC or HS_DYNAMIC.
  -- ldtMap[LS.BinaryStoreSize]; -- Set if/when we use binary mode.
  ldtMap[LS.TotalCount]       = 0; -- Count of both valid and deleted elements
  ldtMap[LS.HashDirSize]      = DEFAULT.HASH_MODULO; -- Hash Dir Size
  -- Set this when we're ready to use dynamic hashing.
  -- ldtMap[LS.HashDepth]        = DEFAULT.HASH_DEPTH; -- # of hash bits to use
  
  -- Rehash after this many have been inserted
  ldtMap[LS.Threshold]       = DEFAULT.THRESHOLD;
  -- name-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[LS.CompactNameList]  = list();
  -- value-entries of name-value pair in lmap to be held in compact mode 
  ldtMap[LS.CompactValueList] = list();

  -- We allow or do NOT allow overwrites of values for a given name.
  ldtMap[LS.OverWrite] = AS_TRUE; -- Start out flexible.

  -- Just like we have a COMPACT LIST for the entire Hash Table, we also
  -- have small lists that we'll keep in each Hash Cell -- to keep us from
  -- allocating a subrecord for just one or two items.  As soon as we pass
  -- the threshold (e.g. 4), then we'll convert to a sub-record.
  ldtMap[LS.HashCellMaxList] = DEFAULT.BINLIST_THRESHOLD;
 
  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  ldt_common.setLdtRecordType( topRec );

  -- Put our new maps in a list, in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  -- Once this list of 2 maps is created, we need to assign it to topRec
  topRec[ldtBinName]            = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]<%s:%s> : LMAP Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth);
  return ldtCtrl;
  
end -- initializeLdtCtrl()

-- ======================================================================
-- initializeLMapRegular()
-- ======================================================================
-- Set up the ldtCtrl map for REGULAR use (sub-records).
-- ======================================================================
local function initializeLMapRegular( ldtMap )
  local meth = "initializeLMapRegular()";
  
  GP=E and trace("[ENTER]: <%s:%s>:: Regular Mode", MOD, meth );
  
  ldtMap[LS.StoreState]  = SS_REGULAR; -- Now Regular, was compact.
  	  
  -- Setup and Allocate everything for the Hash Directory.
  local hashDirSize = ldtMap[LS.HashDirSize];
  local newDirList = list.new(hashDirSize); -- Our new Hash Directory

  -- NOTE: Rather than create an EMPTY cellAnchor (which is a map), we now
  -- also allow a simple C_STATE_EMPTY value to show that the hash cell
  -- is empty.  As soon as we insert something into it, the cellAnchor will
  -- become a map with a state and a value.
  for i = 1, hashDirSize do
    newDirList[i] = C_STATE_EMPTY;
  end

  ldtMap[LS.HashDirectory]        = newDirList;
  
  GP=D and trace("[DETAIL]<%s:%s> LMAP Summary after Init(%s)",
       MOD, meth, ldtMapSummaryString(ldtMap));

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  
end -- function initializeLMapRegular

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( ldtBinName )
  local meth = "validateBinName()";
  GP=E and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
      MOD, meth, tostring(ldtBinName));

  if ldtBinName == nil  then
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( ldtBinName ) ~= "string"  then
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( ldtBinName ) > 14 then
    error( ldte.ERR_BIN_NAME_TOO_LONG );
  end
end -- validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) mustExist: if true, complain if the ldtBin  isn't perfect.
-- Result:
--   If mustExist == true, and things Ok, return ldtCtrl.
-- ======================================================================
local function validateRecBinAndMap( topRec, ldtBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=E and trace("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( ldtBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  ldt_common.validateBinName( ldtBinName );

  local ldtCtrl;
  local propMap;

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid Bin
  -- (*) Must have a valid Map in the bin.
  --
  -- Otherwise, If "mustExist" is false, then basically we're just going
  -- to check that our bin includes MAGIC, if it is non-nil.
  -- TODO : Flag is true for get, config, size, delete etc 
  -- Those functions must be added b4 we validate this if section 

  if mustExist then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) ) then
      debug("[ERROR EXIT]:<%s:%s>:Missing Top Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
     
    -- Control Bin Must Exist, in this case, ldtCtrl is what we check.
    if ( not  topRec[ldtBinName] ) then
      debug("[ERROR EXIT]<%s:%s> LDT BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ldtBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end
    -- This will "error out" if anything is wrong.
    ldtCtrl, propMap = ldt_common.validateLdtBin(topRec,ldtBinName,LDT_TYPE);

    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if ( topRec and topRec[ldtBinName] ) then
      ldtCtrl, propMap = ldt_common.validateLdtBin(topRec,ldtBinName,LDT_TYPE);
    end -- if worth checking
  end -- else for must exist

  -- Finally -- let's check the version of our code against the version
  -- in the data.  If there's a mismatch, then kick out with an error.
  -- Although, we check this in the "must exist" case, or if there's 
  -- a valid propMap to look into.
  if ( mustExist or propMap ) then
    local dataVersion = propMap[PM.Version];
    if ( not dataVersion or type(dataVersion) ~= "number" ) then
      dataVersion = 0; -- Basically signals corruption
    end

    if( G_LDT_VERSION > dataVersion ) then
      warn("[ERROR EXIT]<%s:%s> Code Version (%d) <> Data Version(%d)",
        MOD, meth, G_LDT_VERSION, dataVersion );
      warn("[Please reload data:: Automatic Data Upgrade not yet available");
      error( ldte.ERR_VERSION_MISMATCH );
    end
  end -- final version check

  GP=E and trace("[EXIT]<%s:%s> OK", MOD, meth);
  return ldtCtrl; -- Save the caller the effort of extracting the map.
end -- validateRecBinAndMap()

-- ======================================================================
-- || validateValue()
-- ======================================================================
-- In the calling function, we've landed on the name we were looking for.
-- ======================================================================
local function validateValue( storedValue )
  local meth = "validateValue()";

  GP=E and trace("[ENTER]<%s:%s> validateValue(%s)",
                 MOD, meth, tostring( storedValue ) );
                 
  local liveObject;
  liveObject = storedValue;
  -- If we have a filter, apply that.
  local resultFiltered;
  if( G_Filter ~= nil ) then
    resultFiltered = G_Filter( liveObject, G_FunctionArgs );
  else
    resultFiltered = liveObject;
  end
  return resultFiltered; -- nil or not, we just return it.
end -- validateValue()

-- =======================================================================
-- searchList()
-- =======================================================================
-- Search a list for an item.  Given that this is LMAP, we're searching
-- for a "Name", which is a SIMPLE type and is our searchKey.
--
-- Parms:
-- (*) nameList: the list of NAMES (Name/Value) from the record
-- (*) searchKey: the "name"  we're searching for
-- Return the position if found, else return ZERO.
-- =======================================================================
local function searchList( nameList, searchKey )
  local meth = "searchList()";
  GP=E and trace("[ENTER]: <%s:%s> Looking for searchKey(%s) in List(%s)",
     MOD, meth, tostring(searchKey), tostring(nameList));
                 
  local position = 0; 

  -- Nothing to search if the list is null or empty
  if( nameList == nil or list.size( nameList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyList", MOD, meth );
    return 0;
  end

  -- Search the list for the item (searchKey) return the position if found.
  -- Note that searchKey may be the entire object, or it may be a subset.
  local listSize = list.size(nameList);
  local item;
  local dbKey;
  for i = 1, listSize, 1 do
    item = nameList[i];
    GP=F and trace("[COMPARE]<%s:%s> index(%d) SV(%s) and ListVal(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(item));
    -- a value that does not exist, will have a nil nameList item
    -- so we'll skip this if-loop for it completely                  
    if item ~= nil and item == searchKey then
      position = i;
      break;
    end -- end if not null and equals
  end -- end for each item in the list

  GP=E and trace("[EXIT]<%s:%s> Result: Position(%d)", MOD, meth, position );
  return position;
end -- searchList()

-- =======================================================================
-- scanLMapList()
-- =======================================================================
-- Scan the Name/Value lists, touching every item and applying the (global)
-- filter to each one (if applicable).  For every value that passes the
-- filter, add the name/value to the resultMap.
--
-- (*) nameList: the list of NAMES (Name/Value) from the LMAP
-- (*) valueList: the list of VALUES (Name/Value) from the LMAP
-- (*) resultMap: The caller's ResultMap (where we'll put the results)
-- Result:
-- OK: return 0: Fill in "resultMap".
-- ERROR: LDT Error code to caller
-- =======================================================================
local function scanLMapList(nameList, valueList, resultMap )
  local meth = "scanLMapList()";
  GP=E and trace("[ENTER]: <%s:%s> ScanList: Names(%s) Values(%s)",
      MOD, meth, tostring(nameList), tostring(valueList));

  if( resultMap == nil ) then
    warn("[ERROR]<%s:%s> NULL RESULT MAP", MOD, meth );
    error(ldte.ERR_INTERNAL);
  end
                 
  -- Nothing to search if the list is null or empty.  Assume that ValueList
  -- is in the same shape as the NameList.
  if( nameList == nil or list.size( nameList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyNameList", MOD, meth );
    return 0;
  end

  if( valueList == nil or list.size( valueList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyValueList", MOD, meth );
    return 0;
  end

  -- Search the list.
  local listSize = list.size(nameList);
  local name;
  local resultValue;
  for i = 1, listSize, 1 do
    resultValue = validateValue( valueList[i] );
    if( resultValue ~= nil ) then
      resultMap[nameList[i]] = resultValue;
    end
  end -- end for each item in the list

  GP=DEBUG and trace("[DEBUG]<%s:%s> ResultMap(%s)", MOD, meth,
    tostring(resultMap));

  GP=E and trace("[EXIT]<%s:%s> ResultMap Size(%d)", MOD, meth,
    map.size( resultMap ));
end -- scanLMapList()

-- ========================================================================
-- compute_settings()
-- ========================================================================
-- This function takes in the user's settings and sets the appropriate
-- values in the LDT mechanism.
--
-- All parameters must be numbers.  
-- ldtMap        :: The main control Map of the LDT
-- configMap     :: A Map of Config Settings.
--
-- The values we expect to see in the configMap will be one or more of
-- the following values.  For any value NOT seen in the map, we will use
-- the published default value.
--
-- MaxObjectSize :: the maximum object size (in bytes).
-- MaxKeySize    :: the maximum Key size (in bytes).
-- MaxObjectCount:: the maximum LDT Collection size (number of data objects).
-- WriteBlockSize:: The namespace Write Block Size (in bytes)
-- PageSize      :: Targetted Page Size (8kb to 1mb)
-- RecordOverHead:: Amount of "other" space used in this record.
-- ========================================================================
-- ========================================================================
local function compute_settings(ldtMap, configMap )
 
  local meth = "compute_settings()";

  -- Perform some validation of the user's Config Parameters
  -- Notice that this is done only once at the initial create.
  local rc = ldt_common.validateConfigParms(ldtMap, configMap);
  if rc ~= 0 then
    warn("[ERROR]<%s:%s> Unable to Set Configuration due to errors",
      MOD, meth);
    error(ldte.ERR_INPUT_PARM);
  end

  -- Now that all of the values have been validated, we can use them
  -- safely without worry.  No more checking needed.
  local maxObjectSize   = configMap.MaxObjectSize;
  local maxKeySize      = configMap.MaxKeySize;
  local maxObjectCount  = configMap.MaxObjectCount;
  local pageSize        = configMap.TargetPageSize;
  local writeBlockSize  = configMap.WriteBlockSize;
  local recordOverHead  = configMap.RecordOverHead;
  local userPageSize    = configMap.PageSize;
  local maxObjectCount  = 10000;

  if (configMap.maxObjectCount ~= nil) then
    maxObjectCount  = configMap.maxObjectCount;
  end

  -- These are the values that we have to set.
  local storeState;          -- Start Compact or Regular
  local hashDirSize;         -- Dependent on LDT Capacity and Obj Count
  local threshold;           -- Convert to HashTable
  local cellListThreshold;   -- Convert List to SubRec.
  local ldrListMax;          -- # of elements to store in a data sub-rec

  -- Set up our various OVER HEAD values
  -- We know that LMAP Ctrl occupies up to 400 bytes;
  local ldtOverHead = 400;
  recordOverHead = recordOverHead + ldtOverHead;
  local hashCellOverHead = 40;
  local ldrOverHead = 200;

  -- Set up our various limits or ceilings
  local topRecByteLimit = pageSize - recordOverHead;
  local dataRecByteLimit = pageSize - ldrOverHead;

  -- Figure out the settings for our Compact List Threshold.
  -- By default, if they can fit, we'd like to have as many as 100
  -- elements in our Compact List.  However, for Large Objects, we'll 
  -- have to settle for far fewer, or zero.
  -- In general, we would hope for at least a hot list of size 4
  -- (4 items, with a transfer size of 2) however, if we have really
  -- large objects AND/OR we have multiple LDTs in this record, then
  -- we cannot hog TopRecord space for the Hot List.
  local compactListCountCeiling = 100;

  -- We'd like the Compact List to fit in under the ceiling amount,
  -- or be 1/2 of the target page size, whichever is less.
  local pageAvailableBytes = math.floor(topRecByteLimit / 2);
  local compactListByteCeiling = 50000;
  local compactListTargetBytes =
    (pageAvailableBytes < compactListByteCeiling) and
    (pageAvailableBytes) or compactListByteCeiling;

  -- COMPACT LIST CALCULATION
  -- If not even a object can fit in the Compact List,
  -- then we need to skip the Compact List altogether and have
  -- items be written directly to the Hash Table (the LDR pages).
  if (maxObjectSize  > compactListTargetBytes) then
    -- Don't bother with a Compact List.  Objects are too big.
    threshold = 0;
    storeState = SS_REGULAR;
    debug("[NOTICE]<%s:%s> Max ObjectSize(%d) is too large for CompactList",
      MOD, meth, maxObjectSize);
  else
    threshold = compactListTargetBytes / maxObjectSize;
    if threshold > compactListCountCeiling then
      threshold = compactListCountCeiling;
    end
    storeState = SS_COMPACT;
  end

  -- HASH DIRECTORY SIZE CALCULATION
  -- When we're in "STATIC MODE" (which is what's used up to Version 3),
  -- we need to set the size of the hash table according to what we need
  -- with regard to total storage.  The theoretical storage limit for
  -- the hash directory is:
  -- All available space in the TopRec, divided by Cell Overhead
  -- (MaxPageSize - recordOverHead) / hashCellOverHead
  local hashDirMin = 128; -- go no smaller than this.
  local hashDirMax = (pageSize - recordOverHead) / hashCellOverHead;
  local ldrItemCapacity = math.floor(dataRecByteLimit / maxObjectSize);
  local hashDirSize = maxObjectCount / ldrItemCapacity;
  if hashDirSize > hashDirMax then
    hashDirSize = hashDirMax;
  end
  if hashDirSize < hashDirMin then
    hashDirSize = hashDirMin;
  end

  -- BIN LIST THRESHOLD CALCULATION
  -- Note:  We want to stash things in a Hash Cell Bin List only when
  -- we know that the list of objects will not allow the overall TopRec
  -- memory footprint to overflow the space allowed for the TopRec.
  if maxObjectSize < hashCellOverHead then
    cellListThreshold = math.floor(hashCellOverHead / maxObjectSize);
  else
    cellListThreshold = 0;
  end

  -- LDT Data Record (LDR) List Limit
  ldrListMax = math.floor(dataRecByteLimit / maxObjectSize);

  if (ldrListMax * maxObjectSize) < (userPageSize - ldrOverHead) then
    ldrListMax = math.floor((userPageSize - ldrOverHead) / maxObjectSize);
  end

  -- Apply our computed values to the LDT Map.
  ldtMap[LS.StoreState]       = storeState;
  ldtMap[LS.Modulo]           = hashDirSize;
  ldtMap[LS.Threshold]        = threshold;
  ldtMap[LS.BinListThreshold] = cellListThreshold;
  ldtMap[LS.LdrEntryCountMax] = ldrListMax;

  return 0;

end 



-- ======================================================================
-- setupLdtBin()
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, firstName, firstValue, createSpec)
  local meth = "setupLdtBin()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)",MOD,meth,tostring(ldtBinName));

  local ldtCtrl = initializeLdtCtrl( topRec, ldtBinName );
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  
  -- Remember that record.set_type() for the TopRec
  -- is handled in initializeLdtCtrl()
  
  -- Set the type of this record to LDT (it might already be set)
  -- No Longer needed.  The Set Type is handled in initializeLdtCtrl()
  -- record.set_type( topRec, RT_LDT ); -- LDT Type Rec
  
  -- If the user has passed in settings that override the defaults
  -- (the createSpec), then process that now.
  local configMap = {};

  if (createSpec ~= nil) then
    local createSpecType = type(createSpec);
    if (createSpecType == "string") then
      ldt_common.processModule(ldtMap, configMap, createSpec); -- Use Module
    elseif (getmetatable(createSpec) == Map) then
      configMap = createSpec; -- Use Passed in Map
    else 
      error(ldte.ERR_INPUT_CREATESPEC);
    end
  elseif firstValue ~= nil then
    -- Use First value
    configMap.MaxObjectSize = ldt_common.getValSize(firstName);
    configMap.MaxObjectCount = 100000;
    configMap.MaxKeySize = ldt_common.getValSize(firstName);
  end
  compute_settings(ldtMap, configMap);

  -- Set up our Bin according to the initial State: Compact List or Hash Dir.
  if ldtMap[LS.StoreState] == SS_REGULAR then
    initializeLMapRegular( ldtMap );
  end
  
  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
                 MOD, meth , tostring(ldtMap));

  -- Sets the topRec control bin attribute to point to the 2 item list
  -- we created from InitializeLSetMap() : 
  -- Item 1 :  the property map & Item 2 : the ldtMap
  topRec[ldtBinName] = ldtCtrl; -- store in the record
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- NOTE: The Caller will write out the LDT bin.  Return the ldtCtrl for
  -- convenience (so the caller doesn't need to extract it from topRec).
  return ldtCtrl;
end -- setupLdtBin( topRec, ldtBinName ) 

-- ======================================================================
-- local  CRC32 = require('CRC32'); Do this above, in the "global" area
-- ======================================================================
-- Return the hash of "value", with modulo.
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- ======================================================================
local function stringHash( value, modulo )
  local meth = "stringHash()";
  GP=E and trace("[ENTER]<%s:%s> val(%s) Mod = %s", MOD, meth,
    tostring(value), tostring(modulo));

  local result = 0;
  if value ~= nil and type(value) == "string" then
    result = CRC32.Hash( value ) % modulo;
  end
  GP=E and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result));
  return result;
end -- stringHash()

-- ======================================================================
-- Return the hash of "value", with modulo
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- NOTE: Use a better Hash Function.
-- ======================================================================
local function numberHash( value, modulo )
  local meth = "numberHash()";
  GP=E and trace("[ENTER]<%s:%s> val(%s) Mod = %s", MOD, meth,
    tostring(value), tostring(modulo));

  local result = 0;
  if value ~= nil and type(value) == "number" then
    result = CRC32.Hash( value ) % modulo;
  end
  GP=E and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result));
  return result;
end -- numberHash()

-- ======================================================================
-- computeHashCell()
-- Find the right Hash Cell for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single "Compact cell".
-- Second -- use the right hash function (depending on the type).
-- Third.  Our Lists/Arrays are based on 1 (ONE), rather than 0 (ZERO), so
-- handle that HERE -- add ONE to our result.
-- ======================================================================
local function computeHashCell( searchKey, ldtMap )
  local meth = "computeHashCell()";
  GP=E and trace("[ENTER]: <%s:%s> val(%s) type = %s Map(%s) ", MOD, meth,
    tostring(searchKey), type(searchKey), tostring(ldtMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  local cellNumber  = 0;
  local key = 0; 
  -- We compute a hash ONLY for regular mode.  Compact mode always returns 0.
  if ldtMap[LS.StoreState] == SS_REGULAR then
    local keyType = type(searchKey);
    local modulo = ldtMap[LS.HashDirSize];
    if keyType == "number" or keyType == "string" then
      cellNumber = (CRC32.Hash( searchKey ) % modulo) + 1;
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type %s (should be number, string or map)",
           MOD, meth, type(searchKey) );
      error( ldte.ERR_INTERNAL );
    end
  end
  
  GP=E and trace("[EXIT]<%s:%s> Val(%s) CellNumber(%d)", MOD, meth,
    tostring(searchKey), cellNumber );

  return cellNumber;
end -- computeHashCell()

-- ======================================================================
-- ldrSubRecSummary()
-- ======================================================================
-- Print out interesting stats about this LDR Sub-Record
-- ======================================================================
local function  ldrSubRecSummary( subRec ) 
  local meth = "ldrSubRecSummary()";
  GP=E and trace("[ENTER]<%s:%s>", MOD, meth );

  if( subRec  == nil ) then
    return "NULL Data Chunk (LDR) RECORD";
  end;
  if( subRec[LDR_CTRL_BIN]  == nil ) then
    return "NULL LDR CTRL BIN";
  end;
  if( subRec[SUBREC_PROP_BIN]  == nil ) then
    return "NULL LDR PROPERTY BIN";
  end;

  local resultMap = map();
  local subRecCtrlMap = subRec[LDR_CTRL_BIN];
  local subRecPropMap = subRec[SUBREC_PROP_BIN];

  resultMap.SUMMARY = "LDR SUMMARY";
  resultMap.SelfDigest   = subRecPropMap[PM.SelfDigest];
  resultMap.ParentDigest   = subRecPropMap[PM.ParentDigest];
  resultMap.CtrlLDRByteCnt = subRecCtrlMap[LDR_ByteEntryCount];

  resultMap.LDR_NameList = subRec[LDR_NLIST_BIN];
  resultMap.NameListSize = list.size( resultMap.LDR_NameList );
  resultMap.LDR_ValueList = subRec[LDR_VLIST_BIN];
  resultMap.ValueListSize = list.size( resultMap.LDR_ValueList );

  GP=E and trace("[EXIT]<%s:%s>", MOD, meth );

  return tostring( resultMap );
end -- ldrSubRecSummary()

-- ======================================================================
-- cellAnchorEmpty(cellAnchor)
-- Check this cell Anchor to see if it is in an empty state
-- We have two ways of expressing "empty", so we must check them both.
-- Return:
-- true: if in an EMPTY state
-- false: if not empty
-- ======================================================================
local function cellAnchorEmpty( cellAnchor )
  GP=E and trace("[ENTER]<%s:%s>%s",MOD,"cellAnchorEmpty",tostring(cellAnchor));

  return
    (not cellAnchor ) or
    type(cellAnchor) == "string" and cellAnchor == C_STATE_EMPTY or
    type(cellAnchor) == "userdata" and cellAnchor[C_CellState] == C_STATE_EMPTY
end

-- ======================================================================
-- ldtInitPropMap( propMap, subDigest, topDigest, rtFlag, ldtMap )
-- ======================================================================
-- Set up the LDR Property Map (one PM per LDT).  This function will move
-- into the ldt_common module.
-- Parms:
-- (*) propMap: 
-- (*) esrDigest:
-- (*) subDigest:
-- (*) topDigest:
-- (*) rtFlag:
-- (*) topPropMap;
-- ======================================================================
local function
ldtInitPropMap( propMap, esrDigest, selfDigest, topDigest, rtFlag, topPropMap )
  local meth = "ldtInitPropMap()";
  GP=E and trace("[ENTER]: <%s:%s>", MOD, meth );

  -- Remember the ESR in the Top Record
  topPropMap[PM.EsrDigest] = esrDigest;

  -- Initialize the PropertyMap in the new ESR
  propMap[PM.EsrDigest]    = esrDigest;
  propMap[PM.RecType  ]    = rtFlag;
  propMap[PM.Magic]        = MAGIC;
  propMap[PM.ParentDigest] = topDigest;
  propMap[PM.SelfDigest]   = selfDigest;

end -- ldtInitPropMap()

-- ======================================================================
-- createLMapSubRec()
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the LdtMap (the warm dir list), and return it.
-- In this function, we create a LDR sub-rec and init two structures: 
-- a. The property-map for the new LDR sub-rec chunk
-- b. The ctrl-map for the new LDR sub-rec chunk record
-- a & b are done in initializeSubRec()
-- Once that is done in the called-function, we then make a call to create 
-- an ESR and init that struct as well in createAndInitESR(). 
-- From the above function, we call setLdtRecordType() to do some 
-- byte-level magic on the ESR property-map structure. 
-- Return:
-- A=subRec, B=subRecDigest
-- ======================================================================
-- Here are the fields in an LDR Record:
-- (*) ldrRec[LDR_PROP_BIN]: The propery Map (defined here)
-- (*) ldrRec[LDR_CTRL_BIN]: The control Map (defined here)
-- (*) ldrRec[LDR_NLIST_BIN]: The Name Entry List (when in list mode)
-- (*) ldrRec[LDR_VLIST_BIN]: The Value Entry List (when in list mode)
-- (*) ldrRec[LDR_BNRY_BIN]: The Packed Data Bytes (when in Binary mode)
-- ======================================================================
local function createLMapSubRec( src, topRec, ldtCtrl )
  local meth = "createLMapSubRec()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

    -- Set up the TOP REC prop and ctrl maps
    local propMap    = ldtCtrl[LDT_PROP_MAP];
    local ldtMap     = ldtCtrl[LDT_CTRL_MAP];
    local ldtBinName = propMap[PM.BinName];
  
  -- Create the Aerospike Sub-Record, initialize the bins: Ctrl, List
  -- Notes: 
  -- (1) All Field Names start with UPPER CASE.
  -- (2) Remember to add the ldrSubRec to the SRC (done in createSubRec())
  -- (3) createSubRec() jumps out on any error -- don't need to test here.
  -- (4) createSubRec() updates PM counts, etc
  local newSubRec = ldt_common.createSubRec(src, topRec, ldtCtrl, RT_SUB );
  local subRecPropMap = newSubRec[SUBREC_PROP_BIN];

  -- The common createSubRec() function creates the Sub-Record and sets up
  -- the property bin.  It's our job to set up the LDT-Specific bins
  -- for a Warm List Sub-Record.
  
  local subRecCtrlMap = map();
  local subRecDigest = record.digest( newSubRec );

  --  Use Top level LMAP entry for mode and max values
  subRecCtrlMap[LDR_ByteEntryCount]  = 0;  -- A count of Byte Entries
  
  -- Assign Prop, Control info and List info to the LDR bins
  -- Note that SubRec Prop Bin was set in createSubRec().
  newSubRec[LDR_CTRL_BIN] = subRecCtrlMap;
  newSubRec[LDR_NLIST_BIN] = list();
  newSubRec[LDR_VLIST_BIN] = list();

  -- Add our new Sub-Rec (the digest) to the DigestList
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
   GP=F and trace("[DEBUG]<%s:%s> Add New SubRec(%s) Dig(%s) to HashDir(%s)",
    MOD, meth, tostring(newSubRec), tostring(subRecDigest),
    tostring(ldtMap[LS.HashDirectory]));

  GP=F and trace("[DEBUG]<%s:%s>Post CHunkAppend:NewChunk(%s) LMap(%s): ",
    MOD, meth, tostring(subRecDigest), tostring(ldtMap));
   
  -- Increment the Digest Count
  -- gets inceremented once per LDR entry add. 
  local subRecCount = propMap[PM.SubRecCount];
  propMap[PM.SubRecCount] = (subRecCount + 1);

  -- Mark this Sub-Rec as dirty -- so that it doesn't get reclaimed until
  -- the end of the overall Lua call.
  ldt_common.updateSubRec( src, newSubRec );

  GP=E and trace("[EXIT]<%s:%s> SR PropMap(%s) Name-list: %s value-list: %s ",
    MOD, meth, tostring( subRecPropMap ), tostring(newSubRec[LDR_NLIST_BIN]),
    tostring(newSubRec[LDR_VLIST_BIN]));
  
  return newSubRec, subRecDigest;
end --  createLMapSubRec()

-- =======================================================================
-- regularScan()
-- =======================================================================
-- Search the entire Hash Directory for an item.
-- Parms:
-- (*) src: SubRec Context
-- (*) topRec: The main AS Record (needed for open subrec)
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) resultMap:
-- Results:
-- OK: Results are in resultMape
-- ERROR: LDT Error to client
-- =======================================================================
local function regularScan( src, topRec, ldtCtrl, resultMap )
  local meth = "regularScan()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- For each cell in the Hash Directory, extract that Cell and scan
  -- its contents.  The contents of a cell may be:
  -- (*) EMPTY
  -- (*) A Pair of Short Name/Value lists
  -- (*) A SubRec digest
  -- (*) A Radix Tree of multiple SubRecords
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local hashDirectory = ldtMap[LS.HashDirectory]; 
  local cellAnchor;

  local hashDirSize = list.size( hashDirectory );

  for i = 1, hashDirSize,  1 do
    cellAnchor = hashDirectory[i];
    if( not cellAnchorEmpty( cellAnchor )) then
      GP=DEBUG and trace("[DEBUG]<%s:%s>\nHash Cell :: Index(%d) Cell(%s)",
        MOD, meth, i, tostring(cellAnchor));

      -- If not empty, then the cell anchor must be either in an empty
      -- state, or it has a Sub-Record.  Later, it might have a Radix tree
      -- of multiple Sub-Records.
      if( cellAnchor[C_CellState] == C_STATE_LIST ) then
        -- The small list is inside of the cell anchor.  Get the lists.
        scanLMapList( cellAnchor[C_CellNameList], cellAnchor[C_CellValueList],
          resultMap );
      elseif( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
        -- We have a sub-rec -- open it
        local digest = cellAnchor[C_CellDigest];
        if( digest == nil ) then
          warn("[ERROR]: <%s:%s>: nil Digest value",  MOD, meth );
          error( ldte.ERR_SUBREC_OPEN );
        end

        local digestString = tostring(digest);
        local subRec = ldt_common.openSubRec( src, topRec, digestString );
        if( subRec == nil ) then
          warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",  MOD, meth,
            digestString );
          error( ldte.ERR_SUBREC_OPEN );
        end
        scanLMapList( subRec[LDR_NLIST_BIN], subRec[LDR_VLIST_BIN], resultMap );
        ldt_common.closeSubRec( src, subRec, false);
      else
        -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        -- When we do a Radix Tree, we will STILL end up with a SubRecord
        -- but it will come from a Tree.  We just need to manage the SubRec
        -- correctly.
        warn("[ERROR]<%s:%s> Not yet ready to handle Radix Trees in Hash Cell",
          MOD, meth );
        error( ldte.ERR_INTERNAL );
        -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      end
    end
  end -- for each Hash Dir Cell

  GP=DEBUG and trace("[DEBUG]<%s:%s> ResultMap(%s)", MOD, meth,
    tostring(resultMap));

  GP=E and trace("[EXIT]<%s:%s> MapSize(%d)", MOD, meth, map.size(resultMap) );
  return 0;
end -- function regularScan()

-- ======================================================================
-- compactInsert( ldtCtrl, newName, newValue );
-- ======================================================================
-- Search the compact list, and insert if not found.
-- Parms:
-- (*) ldtCtrl: The main LDT Structure
-- (*) newName: Name to be inserted
-- (*) newValue: Value to be inserted
-- ======================================================================
local function compactInsert( ldtCtrl, newName, newValue )
  local meth = "compactInsert()";
  GP=E and trace("[ENTER]<%s:%s>Insert Name(%s) Value(%s)",
    MOD, meth, tostring(newName), tostring(newValue));
  
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc = RESULT_OK;
  
  -- NOTE: We're expecting the lists to be built, and it's an error if
  -- they are not there.
  local nameList = ldtMap[LS.CompactNameList]; 
  local valueList = ldtMap[LS.CompactValueList]; 

  if nameList == nil or valueList == nil then
    warn("[ERROR]:<%s:%s> Name/Value is nil: name(%s) value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
    error( ldte.ERR_INTERNAL );
  end

  -- Prepare the value for storage (either overwrite or append).
  local storeValue = newValue;

  -- Look for the right place to put it.
  local position = searchList( nameList, newName );
  -- If we find it, then we will either OVERWRITE or generate an error.
  -- If we overwrite, then we MUST NOT INCREMENT THE COUNT.
  if( position > 0 ) then
    if( ldtMap[LS.OverWrite] == AS_FALSE) then
      trace("[UNIQUE VIOLATION]:<%s:%s> Name(%s) Value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
      error( ldte.ERR_UNIQUE_KEY );
    else
      -- For the same name, we are going to write a new value at position P.
      rc = RESULT_OVERWRITE;
      valueList[position] = storeValue;
    end
  else
    -- Store the name in the name list.
    list.append( nameList, newName );
    list.append( valueList, storeValue );
  end

  GP=E and trace("[EXIT]<%s:%s>Name(%s) Value(%s) N List(%s) V List(%s) rc(%d)",
     MOD, meth, tostring(newName), tostring(newValue), 
     tostring(nameList), tostring(valueList), rc );
  
  return rc;
end -- compactInsert()

-- ======================================================================
-- Hash Directory Management
-- ======================================================================
-- Using the Linear Hash Algorithm, we will incrementally expand the
-- hash directory.  With Linear Hash, two things happen.   There is the
-- physical directory change, and then there is the logical address change.
-- Logically, the hash directory DOUBLES each time it gets reallocated,
-- however, physically this does not have to happen.  Physically, we just
-- add one more cell to the end.
--
--  +====+
--  |Cell|
--  | 1  |
--  +====+
--  +====+====+
--  |Cell|Cell|
--  | 1  | 2  |
--  +====+====+
--  +====+====+====+====+
--  |Cell|Cell|Cell|Cell|   
--  | 1  | 2  | 3  | 4  |   
--  +====+====+====+====+
--  +====+====+====+====+====+====+====+====+
--  |Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell|   
--  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  |   
--  +====+====+====+====+====+====+====+====+
--
--  +====+====+====+====+====+====+====+====+....+....+
--  |Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell:Cell:
--  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  |  9 : 10 :
--  +====+====+====+====+====+====+====+====+....+....+
--                                       ^ 
--                                       |
--                                      Mark
--
--  +====+====+====+====+====+====+====+====+====+....+
--  |Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell|Cell:
--  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  |  9 | 10 :
--  +====+====+====+====+====+====+====+====+====+....+
--    ^                                        ^ 
--    |                                        |
--  Split(cells 1 and 9)                      Mark
-- ======================================================================
-- ======================================================================
-- Hash Cell Management
-- ======================================================================

-- ======================================================================
-- lmapListInsert()
-- ======================================================================
-- Insert into the two lists for LMAP, assumign the search goes ok.
-- Parms: 
-- (*) ldtCtrl
-- (*) nameList
-- (*) valueList
-- (*) newName
-- (*) newValue
-- (*) check:
-- Return:
-- 0: all is well
-- 1: Value overwritten: Do NOT update the count.
-- ======================================================================
local function
lmapListInsert( ldtCtrl, nameList, valueList, newName, newValue, check )
  local meth = "lmapListInsert()";
  GP=E and trace("[ENTER]<%s:%s> NList(%s) MVlist(%s) Name(%s) Value(%s)",
    MOD, meth, tostring(nameList), tostring(valueList),
    tostring(newName), tostring(newValue));

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc = RESULT_OK;

  -- We're setting the value up here, even though there's a small chance that
  -- we have an illegal overwrite case, because otherwise we'd have to do
  -- this in two places.
  local storeValue = newValue;
  local position = 0;
  if( check == 1 ) then
    position = searchList( nameList, newName );
  end
  -- If we find it, then see if we are going to overwrite or flag an error.
  if( position > 0 ) then
    if( ldtMap[LS.OverWrite] == AS_FALSE) then
      info("[UNIQUE VIOLATION]:<%s:%s> Name(%s) Value(%s)",
                 MOD, meth, tostring(newName), tostring(newValue));
      error( ldte.ERR_UNIQUE_KEY );
    else
      rc = RESULT_OVERWRITE; -- we will overwrite.  Same Name, New Value.
      valueList[position] = storeValue;
    end
  else
    -- Add the new name/value to the existing lists. (rc is still 0).
    list.append( nameList, newName );
    list.append( valueList, storeValue );
  end

  GP=E and trace("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmapListInsert()

-- ======================================================================
-- hashCellConvert()
-- ======================================================================
-- Convert the hash cell LIST into a single sub-record, change the cell
-- anchor state (to DIGEST) and then move the list data into the Sub-Rec.
-- Parms:
-- (*) src:
-- (*) topRec:
-- (*) ldtCtrl:
-- (*) cellAnchor:
-- Returns:
-- 0: New Value Written, all ok
-- 1: Existing value OVERWRITTEN, ok but count stays the same
-- other: Error Code
-- ======================================================================
local function hashCellConvert( src, topRec, ldtCtrl, cellAnchor )
  local meth = "hashCellConvert()";
  GP=E and trace("[ENTER]<%s:%s> HCell(%s)", MOD, meth, tostring(cellAnchor));

  -- Validate that the state of this Hash Cell Anchor really is "LIST",
  -- because otherwise something bad would be done.
  if( cellAnchor[C_CellState] ~= C_STATE_LIST ) then
    warn("[ERROR]<%s:%s> Bad Hash Cell Anchor State(%s). Should be LIST.",
      MOD, meth, tostring(cellAnchor[C_CellState]));
    error( ldte.ERR_INTERNAL );
  end

  -- Create a new Sub-Rec, store the digest and store the list data. 
  -- Note that we don't need to check values or counts, because we already
  -- know that we're good.   We are assuming that no single value is
  -- so ungodly large that we can get in trouble with moving a small list
  -- into a Sub-Rec.  If that DOES get us into trouble, then we have to
  -- figure out better INTERNAL support for checking sizes of Lua objects.
  local subRec, subRecDigest = createLMapSubRec( src, topRec, ldtCtrl );

  if( subRec == nil ) then
    warn("[ERROR]<%s:%s>: SubRec Create Error",  MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  else
    GP=F and trace("[DEBUG]<%s:%s>: SubRec Create SUCCESS(%s) Dig(%s)",
        MOD, meth, ldrSubRecSummary( subRec ), tostring(subRecDigest));
  end

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  local nameList = cellAnchor[C_CellNameList];
  local valueList = cellAnchor[C_CellValueList];

  -- Just convert -- no INSERT here.
  -- rc = lmapListInsert( ldtCtrl, nameList, valueList, newName, newValue, 1 );

  subRec[LDR_NLIST_BIN] = nameList;
  subRec[LDR_VLIST_BIN] = valueList;

  -- Set the state the hash cell to "DIGEST" and then NULL out the list
  -- values (which are now in the sub-rec).
  cellAnchor[C_CellState] = C_STATE_DIGEST;
  cellAnchor[C_CellDigest] = subRecDigest;

  -- Remove the Name/Value lists from the Cell Anchor.
  map.remove(cellAnchor, C_CellNameList);
  map.remove(cellAnchor, C_CellValueList);

  ldt_common.updateSubRec( src, subRec );

  GP=F and trace("[DEBUG]<%s:%s> Cell(%s) LDR Summary(%s)", MOD, meth,
    tostring(cellAnchor), ldrSubRecSummary( subRec ));

  GP=E and trace("[EXIT]<%s:%s> Conversion Successful:", MOD, meth );
end -- function hashCellConvert()

-- ======================================================================
-- hashCellConvertInsert()
-- ======================================================================
-- We no longer use this function -- just do REGULAR convert (above)
-- without an insert.
-- ======================================================================
-- Convert the hash cell LIST into a single sub-record, change the cell
-- anchor state (to DIGEST) and then move the list data into the Sub-Rec.
-- Parms:
-- (*) src:
-- (*) topRec:
-- (*) ldtCtrl:
-- (*) cellAnchor:
-- (*) newName:
-- (*) newValue:
-- Returns:
-- 0: New Value Written, all ok
-- 1: Existing value OVERWRITTEN, ok but count stays the same
-- other: Error Code
-- ======================================================================
local function
hashCellConvertInsert(src, topRec, ldtCtrl, cellAnchor, newName, newValue)
  local meth = "hashCellConvertInsert()";
  GP=E and trace("[ENTER]<%s:%s> HCell(%s) newName(%s) newValue(%s)", MOD, meth,
    tostring(cellAnchor), tostring(newName), tostring(newValue));

  -- Validate that the state of this Hash Cell Anchor really is "LIST",
  -- because otherwise something bad would be done.
  if( cellAnchor[C_CellState] ~= C_STATE_LIST ) then
    warn("[ERROR]<%s:%s> Bad Hash Cell Anchor State(%s). Should be LIST.",
      MOD, meth, tostring(cellAnchor[C_CellState]));
    error( ldte.ERR_INTERNAL );
  end

  -- Create a new Sub-Rec, store the digest and store the list data. 
  -- Note that we don't need to check values or counts, because we already
  -- know that we're good.   We are assuming that no single value is
  -- so ungodly large that we can get in trouble with moving a small list
  -- into a Sub-Rec.  If that DOES get us into trouble, then we have to
  -- figure out better INTERNAL support for checking sizes of Lua objects.
  local subRec, subRecDigest = createLMapSubRec( src, topRec, ldtCtrl );
  -- local subRecDigest = record.digest( subRec );

  if( subRec == nil ) then
    warn("[ERROR]<%s:%s>: SubRec Create Error",  MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  else
    GP=F and trace("[DEBUG]<%s:%s>: SubRec Create SUCCESS(%s) Dig(%s)",
        MOD, meth, ldrSubRecSummary( subRec ), tostring(subRecDigest));
  end

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc;

  local nameList = cellAnchor[C_CellNameList];
  local valueList = cellAnchor[C_CellValueList];

  rc = lmapListInsert( ldtCtrl, nameList, valueList, newName, newValue, 1 );

  subRec[LDR_NLIST_BIN] = nameList;
  subRec[LDR_VLIST_BIN] = valueList;

  -- Set the state the hash cell to "DIGEST" and then NULL out the list
  -- values (which are now in the sub-rec).
  cellAnchor[C_CellState] = C_STATE_DIGEST;
  cellAnchor[C_CellDigest] = subRecDigest;

  -- Remove the Name/Value lists from the Cell Anchor.
  map.remove(cellAnchor, C_CellNameList);
  map.remove(cellAnchor, C_CellValueList);

  ldt_common.updateSubRec( src, subRec );

  GP=F and trace("[DEBUG]<%s:%s> Cell(%s) LDR Summary(%s)", MOD, meth,
    tostring(cellAnchor), ldrSubRecSummary( subRec ));

  GP=E and trace("[EXIT]<%s:%s> Conversion Successful: rc(%d)", MOD, meth, rc);
  return rc;
end -- function hashCellConvertInsert()

-- ======================================================================
-- hashCellSubRecInsert()
-- ======================================================================
-- Insert the value into the sub-record.  If this insert would trigger
-- an overflow, then split the sub-record into two.
-- This might be an existing radix tree with sub-recs, or it might be
-- a single sub-rec that needs to split and introduce a tree.
-- Parms:
-- (*) src
-- (*) topRec
-- (*) ldtCtrl
-- (*) cellAnchor
-- (*) newName
-- (*) newValue
-- Return:
-- 0: if all goes well
-- 1: if we inserted, but overwrote, so do NOT update the counts.
-- other: Error Code
-- ======================================================================
local function
hashCellSubRecInsert(src, topRec, ldtCtrl, cellAnchor, newName, newValue)
  local meth = "hashCellSubRecInsert()";
  GP=E and trace("[ENTER]<%s:%s> CellAnchor(%s) newName(%s) newValue(%s)",
    MOD, meth, tostring(cellAnchor), tostring(newName), tostring(newValue));

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc;

  -- LMAP Version 1:  Just a pure Sub-Rec insert, no trees just yet.
  local digest = cellAnchor[C_CellDigest];
  local digestString = tostring(digest);
  -- local subRec = openSubrec( src, topRec, digestString );
  -- NOTE: openSubRec() does its own error checking. No more needed here.
  local subRec = ldt_common.openSubRec( src, topRec, digestString );

  -- ATTENTION!!!  Here is the place where we will eventually do the check
  -- for single Sub-Rec overflow and turn the single sub-rec into a Radix
  -- tree of multiple sub-records.
  trace("[REMEMBER]<%s:%s> HERE: we'll check for SubRec spill into Radix tree",
      MOD, meth );

  local nameList  = subRec[LDR_NLIST_BIN];
  local valueList = subRec[LDR_VLIST_BIN];
  if( not nameList ) or ( not valueList ) then
    warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  rc = lmapListInsert( ldtCtrl, nameList, valueList, newName, newValue, 1 );

  -- Now, we have to re-assign the lists back into the bin values.
  subRec[LDR_NLIST_BIN] = nameList;
  subRec[LDR_VLIST_BIN] = valueList;
  ldt_common.updateSubRec( src, subRec );

  GP=F and trace("[DEBUG]<%s:%s> Post LDR Insert Summary(%s)",
    MOD, meth, ldrSubRecSummary( subRec ));

  GP=E and trace("[EXIT]<%s:%s> SubRecInsert Successful: rc(%d)",MOD,meth,rc);
  return rc;
end -- function hashCellSubRecInsert()

-- ======================================================================
-- newCellAnchor()
-- ======================================================================
-- Perform an insert into a NEW Cell Anchor.
-- A Cell Anchor starts in the following state:
-- (*) A local List (something small)
-- Parms:
-- (*) newName: Name to be inserted
-- (*) newValue: Value to be inserted
-- RETURN:
--  New cellAnchor.
-- ======================================================================
local function newCellAnchor( newName, newValue )
  local meth = "newCellAnchor()";
  GP=E and trace("[ENTER]:<%s:%s> Name(%s) Value(%s) ", MOD, meth,
    tostring(newName), tostring(newValue));

  local cellAnchor = map();
  if newName and newValue then
    cellAnchor[C_CellState] = C_STATE_LIST;
    local nameList = list();
    local valueList = list();
    list.append( nameList, newName );
    list.append( valueList, newValue );
    cellAnchor[C_CellNameList] = nameList;
    cellAnchor[C_CellValueList] = valueList;
  else
    -- Note that we also allow a Hash Cell to have the value
    -- "C_STATE_EMPTY" directly, without being in a map. 
    cellAnchor[C_CellState] = C_STATE_EMPTY;
  end
  -- Recall that we don't start using C_CellItemCount until we have
  -- at least one Sub-Record in this Hash Cell.

  GP=E and trace("[EXIT]:<%s:%s> NewCell(%s) ",MOD,meth,tostring(cellAnchor));
  return cellAnchor;
end -- newCellAnchor()

-- ======================================================================
-- hashDirInsert()
-- ======================================================================
-- Perform a "Regular" Hash Directory Insert.
-- First, Take the new value and locate the Hash Directory Cell.
-- Then, look inside the Cell:  There will be a Cell Anchor that might
-- hold a LIST, a single SubRec Pointer (digest) or a Radix Tree of multiple
-- Digests.
-- Locate the correct spot (either immediate list or sub-record).
-- Insert the sub-record into the spot.
-- Additional Complications:
-- (*) We've hit an overflow situation:
--   (1) We must either convert the list into a Sub-Record
--   (2) We must convert a single Sub-Record into a Radix Tree
--   (3) We must split the Sub-Record of a tree into two Sub-Records.
-- Step ONE: Use a Single Sub-Record.  Note the state so that we can
-- gracefully extend into use of a Radix Tree.
-- Parms:
-- (*) src: SubRec Context Block
-- (*) topRec: The main Aerospike Record
-- (*) ldtCtrl: The LDT Control Structure
-- (*) newName: The new Name to be added
-- (*) newValue: The new Value to be added
-- (*) check: When ==1, check the list for exist.  When ==0, no check.
-- Return:
-- ======================================================================
local function hashDirInsert( src, topRec, ldtCtrl, newName, newValue, check)
  local meth = "hashDirInsert()";
  GP=E and trace("[ENTER]<%s:%s> Name(%s) Value(%s)",
   MOD, meth, tostring(newName), tostring(newValue));
                 
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local ldtBinName =  propMap[PM.BinName];
  local rc = 0; -- start out OK.

  -- Remember that our Hash Dir goes from 1..N, rather than 0..(N-1)
  local cellNumber = computeHashCell( newName, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  GP=F and trace("[DEBUG]<%s:%s> CellNum(%s) CellAnchor(%s)", MOD, meth,
    tostring(cellNumber), tostring(cellAnchor));

  -- ----------------------------------------------------------------------
  -- We have three main cases:
  -- (1) Empty Hash Cell.  We allocate a list and append it. Done.
  -- (2) List Hash Cell.
  --     - Search List, if not found, append.
  --     - Check for overflow:
  --       - If Overflow, convert to Sub-Record
  -- (3) It's a sub-rec cell (or a tree cell).
  --     - Locate appropriate Sub-Record.
  --     - Insert into Sub-Record.
  -- ----------------------------------------------------------------------
  if ( cellAnchorEmpty( cellAnchor ) ) then
    -- Easy :: hash cell anchor insert.
    cellAnchor = newCellAnchor( newName, newValue );
    hashDirectory[cellNumber] = cellAnchor;
    -- If we don't allow hash Cell Lists, then convert this cell anchor
    -- to use a SUB-RECORD.
    if ( ldtMap[LS.HashCellMaxList] == 0) then
      hashCellConvert( src, topRec, ldtCtrl, cellAnchor )
    end
  elseif ( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- We have a list. if check==1, then we will search the list before we
    -- insert. If we find it, AND we overwrite, then we return "1" to signal
    -- that our caller should NOT update the overall count.
    -- So -- do the insert, and then see if we should convert to Sub-Rec.
    local nameList  = cellAnchor[C_CellNameList];
    local valueList = cellAnchor[C_CellValueList];
    GP=F and trace("[DEBUG]<%s:%s> Lists BEFORE: NL(%s) VL(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));

    rc = lmapListInsert(ldtCtrl,nameList,valueList,newName,newValue,check);

    GP=F and trace("[DEBUG]<%s:%s> Lists AFTER: NL(%s) VL(%s)", MOD, meth,
      tostring(nameList), tostring(valueList));

    if ( list.size(nameList) > ldtMap[LS.HashCellMaxList] ) then
      hashCellConvert( src, topRec, ldtCtrl, cellAnchor )
    end

    GP=F and trace("[DEBUG]<%s:%s>Anchor AFTER Possible Convert: Anchor(%s)",
      MOD, meth, tostring(cellAnchor));
  else
    -- It's a sub-record insert, with a possible tree overflow
    rc = hashCellSubRecInsert(src,topRec,ldtCtrl,cellAnchor,newName,newValue);
  end

  -- All done -- Save our work.
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- NOTE: Caller will update stats (e.g. ItemCount).
  GP=E and trace("[EXIT]<%s:%s> SubRecInsert Successful: N(%s) V(%s) rc(%d)",
    MOD, meth, tostring(newName), tostring(newValue), rc);
  return rc;
end -- function hashDirInsert()

-- ======================================================================
-- compactDelete()
-- ======================================================================
-- Delete an item from the compact list.
-- For the compact list, it's a simple list delete (if we find it).
-- (*) ldtMap: The LMAP-Specific config map
-- (*) searchName: the name of the name/value pair to be deleted
-- (*) resultMap: the map carrying the name/value pair result.
-- ======================================================================
local function compactDelete( ldtMap, searchName, resultMap )
  local meth = "compactDelete()";

  GP=E and trace("[ENTER]<%s:%s> Name(%s)", MOD, meth, tostring(searchName));

  local nameList = ldtMap[LS.CompactNameList];
  local valueList = ldtMap[LS.CompactValueList];

  local position = searchList( nameList, searchName );
  if( position == 0 ) then
    -- Didn't find it -- report an error.
    debug("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok -- found the name, so let's delete the value.
  -- listDelete() will generate a new list, so we store that back into
  -- the ldtMap.
  resultMap[searchName] = validateValue( valueList[position] );
  ldtMap[LS.CompactNameList]  = ldt_common.listDelete( nameList, position );
  ldtMap[LS.CompactValueList] = ldt_common.listDelete( valueList, position );

  GP=E and trace("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- compactDelete()

-- ======================================================================
-- regularDelete()
-- ======================================================================
-- Remove a map entry from a SubRec (regular storage mode).
-- Params:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec: The Aerospike record holding the LDT
-- (*) ldtCtrl: The main LDT control structure
-- (*) searchName: the name of the name/value pair to be deleted
-- (*) resultMap: the map carrying the name/value pair result.
-- ======================================================================
local function regularDelete( src, topRec, ldtCtrl, searchName, resultMap )
  local meth = "regularDelete()";
  GP=E and trace("[ENTER]<%s:%s> Name(%s)", MOD, meth, tostring(searchName));

  local rc = 0; -- start out OK.
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  -- Compute the subRec address that holds the searchName
  local cellNumber = computeHashCell( searchName, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];
  local subRec;
  local nameList;
  local valueList;

  -- If no sub-record, then not found.
  if ( cellAnchorEmpty( cellAnchor )) then
    debug("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- If not empty, then the cell anchor must be either in an empty
  -- state, or it has a Sub-Record.  Later, it might have a Radix tree
  -- of multiple Sub-Records.
  if( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- The small list is inside of the cell anchor.  Get the lists.
    nameList  = cellAnchor[C_CellNameList];
    valueList = cellAnchor[C_CellValueList];
  elseif( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
    -- If the cell state is NOT empty and NOT a list, it must be a subrec.
    -- We have a sub-rec -- open it
    local digest = cellAnchor[C_CellDigest];
    if( digest == nil ) then
      warn("[ERROR]: <%s:%s>: nil Digest value",  MOD, meth );
      error( ldte.ERR_SUBREC_OPEN );
    end

    local digestString = tostring(digest);
    -- local subRec = openSubrec( src, topRec, digestString );
    -- NOTE: openSubRec() does its own error checking. No more needed here.
    subRec = ldt_common.openSubRec( src, topRec, digestString );

    nameList  = subRec[LDR_NLIST_BIN];
    valueList = subRec[LDR_VLIST_BIN];
    if ( ( not nameList ) or ( not valueList ) ) then
      warn("[ERROR]<%s:%s> Empty List: NameList(%s) ValueList(%s)", MOD, meth,
        tostring(nameList), tostring(valueList));
      error( ldte.ERR_INTERNAL );
    end
  else
    -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    -- When we do a Radix Tree, we will STILL end up with a SubRecord
    -- but it will come from a Tree.  We just need to manage the SubRec
    -- correctly.
    warn("[ERROR]<%s:%s> Not yet ready to handle Radix Trees in Hash Cell",
      MOD, meth );
    error( ldte.ERR_INTERNAL );
    -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  end

  local position = searchList( nameList, searchName );
  if( position == 0 ) then
    -- Didn't find it -- report an error. First, Close the subRec(Not Dirty)
    ldt_common.closeSubRec( src, subRec, false);
    debug("[NOT FOUND]<%s:%s> searchName(%s)", MOD, meth, tostring(searchName));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok -- found the name, so let's delete the value.
  -- listDelete() will generate a new list, so we store that back into
  -- where we got the list:
  -- (*) The Cell Anchor List
  -- (*) The Sub-Record.
  resultMap[searchName] = validateValue( valueList[position] );
  if( cellAnchor[C_CellState] == C_STATE_LIST ) then
    cellAnchor[C_CellNameList] =  ldt_common.listDelete( nameList,position );
    cellAnchor[C_CellValueList] = ldt_common.listDelete( valueList,position );
  else
    subRec[LDR_NLIST_BIN] = ldt_common.listDelete( nameList, position );
    subRec[LDR_VLIST_BIN] = ldt_common.listDelete( valueList, position );
    ldt_common.updateSubRec( src, subRec );
  end

  GP=E and trace("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- function regularDelete()

-- ======================================================================
-- regularSearch()
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- ======================================================================
local function regularSearch(src,topRec,propMap,ldtMap,searchName,resultMap)
  local meth = "regularSearch()";
  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  local rc = 0; -- start out OK.

  local cellNumber = computeHashCell( searchName, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  if ( cellAnchorEmpty( cellAnchor )) then
  -- if ( cellAnchor[C_CellState] == C_STATE_EMPTY ) then
    debug("[NOT FOUND]<%s:%s> Value not found for name(%s)",
      MOD, meth, tostring( searchName ) );
    error( ldte.ERR_NOT_FOUND );
  end

  local nameList;
  local valueList;
  if ( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- Get the cellAnchor lists
    nameList = cellAnchor[C_CellNameList];
    valueList = cellAnchor[C_CellValueList];
  elseif ( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
    -- Get the lists from the single Sub-Rec
    local digest = cellAnchor[C_CellDigest];
    local digestString = tostring(digest);
    -- local subRec = openSubrec( src, topRec, digestString );
    -- NOTE: openSubRec() does its own error checking. No more needed here.
    local subRec = ldt_common.openSubRec( src, topRec, digestString );
    nameList  = subRec[LDR_NLIST_BIN];
    valueList = subRec[LDR_VLIST_BIN];
  else
    -- Get the lists from the correct Sub-Rec in the Radix Tree.
    -- Radix tree support not yet implemented
    warn("[INTERNAL ERROR]<%s:%s> Search name(%s), RADIX Tree Not Ready",
      MOD, meth, tostring( searchName ));
    error( ldte.ERR_INTERNAL );
  end

  -- We've got a namelist to search.
  if( nameList == nil ) then
    warn("[ERROR]<%s:%s> empty Subrec NameList", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local resultObject = nil;
  local position = searchList( nameList, searchName );
  local resultFiltered = nil;
  if( position > 0 ) then
    -- ok -- found the name, so let's process the value.
    resultObject = validateValue( valueList[position] );
  end

  if( resultObject == nil ) then
    debug("[WARNING]<%s:%s> Value not found for name(%s)",
      MOD, meth, tostring( searchName ) );
    error( ldte.ERR_NOT_FOUND );
  end
  resultMap[searchName] = resultObject;
  
  -- NOTE: We could close all sub-recs here, but it really doesn't matter
  -- for a single search.
  -- ALSO -- resultMap is returned via parameter, so does not need to be
  -- returned here as a function result.

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
                   MOD, meth, tostring(resultMap));
end -- function regularSearch()

-- ======================================================================
-- convertCompactToHashDir( topRec, ldtCtrl, newName, newValue )
-- ======================================================================
-- Convert the current "Compact List" to a regular Sub-Record Hash List.
--
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (ldtMap[LS.Threshold]), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
--
-- Note that we have to keep "store threshold" LOWER than the maximum number
-- of sub-records that we can have Open and Dirty at the same time.
-- 
-- We perform the following operations:
-- a. Copy the existing list into a temp-list
-- b. Add lmap related control-fields to ldtMap 
-- c. Build the sub-rec structure needed to add a list of digests
-- (fixed-size warm-list).  Note that an ESR will be created when the
-- first Sub-Record is created.
-- d. Insert records and shove into sub-recs appropriately
--
-- NOTE: We're going to use a "fast insert" into the Hash Directory, because
-- we know that we don't need to "Re-Search" the individual hash lists (or
-- Sub-Records), we can just append to existing lists.   The Searching takes
-- a fair amount of time and we can bypass that.
-- 
-- Parms:
-- (*) src
-- (*) topRec
-- (*) ldtCtrl
-- Return:
-- 0: if all goes well
-- 1: If we overwrote a value
-- ERR: if anything went wrong
-- ======================================================================
local function convertCompactToHashDir( src, topRec, ldtCtrl )
  local meth = "convertCompactToHashDir()";
  GP=E and trace("[ENTER]:<%s:%s> ", MOD, meth);

  GP=B and trace("[REHASH!!]:<%s:%s> Convert to SubRec", MOD, meth );

  -- Get the Name and Value Lists, then iterate thru them, performing
  -- "FAST INSERT" into the Hash Table (no searching needed).
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName =  propMap[PM.BinName];

  local nameList = ldtMap[LS.CompactNameList]; 
  local valueList = ldtMap[LS.CompactValueList]; 

  if nameList == nil or valueList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(ldtBinName));
    error( ldte.ERR_INSERT );
  end

  -- Create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  initializeLMapRegular( ldtMap );

  -- Note that "Fast Insert" does not update stats -- because we know that
  -- it is a special case of converting the Compact List.
  -- There are two cases:
  -- (1) We're allowed to insert values into the Bin Lists
  -- (2) We must go straight to Sub-Recs (no bin lists allowed).
  listSize = list.size(nameList);
  for i = 1, listSize, 1 do
    -- Use a regular insert -- since now we must be flexible enough to
    -- handle ZERO BinList Limits.
    hashDirInsert( src, topRec, ldtCtrl, nameList[i], valueList[i], 0);
  end

  -- We no longer need the Compact Lists.  Remove the entries.
  map.remove(ldtMap, LS.CompactNameList);
  map.remove(ldtMap, LS.CompactValueList);

  GP=E and trace("[EXIT]: <%s:%s> rc(0)", MOD, meth);
  return 0;
end -- convertCompactToHashDir()

-- ======================================================================
-- localPut()
-- ======================================================================
-- Insert a new element into the map.  The checking has already been done.
-- Also, the caller will do all of the control map writing/saving.
-- Parms
-- (*) src: The Sub-Record Context (tracks open sub-rec pages)
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) newName: Name to be inserted into the Large Map
-- (*) newValue: Value to be inserted into the Large Map
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- Return:
-- 0: All Ok -- wrote New Value.
-- 1: Ok -- but OVERWROTE value (replace option is ON)
-- other: Error Code
-- ======================================================================
local function localPut( src, topRec, ldtCtrl, newName, newValue )
  local meth = "localPut()";
  GP=E and trace("[ENTER]<%s:%s> newName(%s) newValue(%s)",
     MOD, meth, tostring(newName), tostring(newValue) );
                 
  GP=F and trace("[DEBUG]<%s:%s> SRC(%s)", MOD, meth, tostring(src));

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local rc = 0;

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single list into the real sub-record organization.
  -- After the rehash (conversion), we'll drop into the regular LMAP insert.
  local totalCount = ldtMap[LS.TotalCount];

  if ( ldtMap[LS.StoreState] == SS_COMPACT ) then
    -- Do the insert into CompactList, THEN see if we should rehash the
    -- list into the Hash Directory.
    rc = compactInsert( ldtCtrl, newName, newValue );
    -- Now, if we're over "Threshold", convert to Sub-Rec organization.
    if ( totalCount + 1 > ldtMap[LS.Threshold] ) then
      GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH AFTER INSERT", MOD, meth);
      convertCompactToHashDir(src, topRec, ldtCtrl);
    end
  else
    rc = hashDirInsert( src, topRec, ldtCtrl, newName, newValue, 1); 
  end

  --  NOTE: We do NOT update counts here -- our caller(s) will take
  --  care of that.  Also the caller will update the Top-Record.
  
  GP=E and trace("[EXIT]<%s:%s> : Done. RC(%d)", MOD, meth, rc );
  return rc;
end -- function localPut()

-- ======================================================================
-- localGet()
-- ======================================================================
-- Get an element from the map.  If found, return ZERO and the result
-- will be in the resultMap.  
-- ======================================================================
-- Parms:
-- (*) src:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) propMap:
-- (*) ldtMap:
-- (*) searchname:
-- (*) resultMap:
-- RETURN:
-- Success: Item is added to the resultMap
-- ======================================================================
local function localGet( src, topRec, propMap, ldtMap, searchName, resultMap )

  local meth = "lmap.get()";
  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  local rc = 0; -- start out OK.
  
  -- Process these two options differently.  Either we're in COMPACT MODE,
  -- which means have two simple lists connected to the LDT BIN, or we're
  -- in REGULAR_MODE, which means we're going to open up a SubRecord and
  -- read the lists in there.
  if ldtMap[LS.StoreState] == SS_COMPACT then 
    local nameList = ldtMap[LS.CompactNameList];
    local position = searchList( nameList, searchName );
    local resultObject = nil;
    if( position > 0 ) then
      local valueList = ldtMap[LS.CompactValueList];
      resultObject = validateValue( valueList[position] );
      resultMap[nameList[position]] = resultObject;
    end
    if( resultObject == nil ) then
      debug("[NOT FOUND]<%s:%s> name(%s) not found",
        MOD, meth, tostring(searchName));
      rc = -1;
    end
  else
    -- Search the SubRecord.
    rc = regularSearch( src, topRec, propMap, ldtMap, searchName, resultMap );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
     MOD, meth, tostring(resultMap));

  return rc;
end -- function localGet()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Map (LMAP) Library Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- (*) lmap.put(topRec, ldtBinName, newName, newValue, createSpec, src) 
-- (*) lmap.put_all(topRec, ldtBinName, nameValueMap, createSpec, src)
-- (*) lmap.get(topRec, ldtBinName, searchName, userMod, filter, fargs, src)
-- (*) lmap.get_all( topRec, ldtBinName, nameList )
-- (*) lmap.exist(topRec, ldtBinName, searchName, src)
-- (*) lmap.scan(topRec, ldtBinName, filterModule, filter, fargs, src)
-- (*) lmap.keyList(topRec, ldtBinName)
-- (*) lmap.remove(topRec, ldtBinName, searchName, src)
-- (*) lmap.destroy(topRec, ldtBinName, src)
-- (*) lmap.size(topRec, ldtBinName)
-- (*) lmap.config(topRec, ldtBinName)
-- (*) lmap.set_capacity(topRec, ldtBinName, new_capacity)
-- (*) lmap.get_capacity(topRec, ldtBinName)
-- ======================================================================
-- The following functions are deprecated:
-- (*) create( topRec, ldtBinName, createSpec )
--
-- The following functions are for development use:
-- (*) lmap.dump()
-- ======================================================================
-- We define a table of functions that are visible to both INTERNAL UDF
-- calls and to the EXTERNAL LDT functions.  We define this table, "lmap",
-- which contains the functions that will be visible to the module.
local lmap = {}
-- ======================================================================

-- ======================================================================
-- lmap.create() -- Setup a new LDT Bin in the record.
-- ======================================================================
-- Create/Initialize a Map structure in a bin, using a single LMAP
-- bin, using User's name, but Aerospike TYPE (AS_LMAP)
--
-- The LMAP starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record -- in the first lmap bin. 
-- Once the first lmap list goes over its item-count limit, we switch to 
-- standard mode and the entries get collated into a single LDR. We then
-- generate a digest for this LDR, hash this digest over N bins of a digest
-- list. 
-- Please refer to lmap_design.lua for details. 
-- 
-- Parameters: 
-- (1) topRec: the user-level record holding the LMAP Bin
-- (2) ldtBinName: The name of the LMAP Bin
-- (3) createSpec: The map or module containing the adjust_settings() function
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.create( topRec, ldtBinName, createSpec )
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP CREATE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

                 
  local meth = "lmap.create()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring(createSpec) );
  
  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if the Bin Name is bad.
  validateBinName( ldtBinName );
  local rc = 0;

  if createSpec == nil then
    GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL createSpec",
      MOD, meth, tostring(ldtBinName));
  else
    GP=E and trace("[ENTER2]: <%s:%s> ldtBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( createSpec ));
  end

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LDT field to an
  -- existing record.
  if( topRec[ldtBinName] ~= nil ) then
  warn("[ERROR EXIT]: <%s:%s> LDT BIN (%s) Already Exists",
  MOD, meth, ldtBinName );
  error( ldte.ERR_BIN_ALREADY_EXISTS );
  end
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.

  -- Set up a new LDT Bin
  local ldtCtrl = setupLdtBin( topRec, ldtBinName, nil, nil, createSpec );

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- end lmap.create()

-- ======================================================================
-- || lmap.put() -- Insert a Name/Value pair into the LMAP
-- ======================================================================
-- Put a value into the MAP.
-- Take the value, perform a hash and a modulo function to determine which
-- hash cell is used, then add to the list for that cell.
--
-- We will cache all data in the Cell ZERO until we reach a certain number N
-- (e.g. 100), and then at N+1 we will create all of the remaining cells in
-- the hash directory and redistribute the numbers, then insert the next
-- (e.g. 101th) value.  That way we save the initial storage cost of small,
-- inactive or dead users.
-- ==> The CtrlMap will show which state we are in:
-- (*) StoreState=SS_COMPACT: We are in SINGLE LIST state (no hash, cell 0)
-- (*) StoreState=SS_REGULAR: We hash, mod N, then insert into THAT cell.
--
-- Please refer to doc_lmap.md for further notes. 
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) newName: Name to be inserted into the Large Map
-- (*) newValue: Value to be inserted into the Large Map
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lmap.put( topRec, ldtBinName, newName, newValue, createSpec, src )
  GP=B and trace("\n\n >>>>>>>>> API[ LMAP PUT ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.put()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) name(%s) value(%s) module(%s)",
    MOD, meth, tostring(ldtBinName), tostring(newName),tostring(newValue),
    tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO] <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, newName, newValue, createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local rc = 0;

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read/Write Functions (Filter)
  G_Filter = ldt_common.setReadFunctions(nil, nil );
  
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  rc = localPut( src, topRec, ldtCtrl, newName, newValue );
  -- Update the counts.  If there were any errors, the code would have
  -- jumped out of the Lua code entirely.  So, if we're here, the insert
  -- was successful.
  -- However, it is ALSO the case that if we allow OVERWRITE, then we don't
  -- update the count.  If rc == 1, then we overwrote the value, so we
  -- DO NOT UPDATE the count.
  if ( rc == 0 ) then
    local itemCount = propMap[PM.ItemCount];
    local totalCount = ldtMap[LS.TotalCount];
    propMap[PM.ItemCount] = itemCount + 1; -- number of valid items goes up
    ldtMap[LS.TotalCount] = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]<%s:%s> Successful PUT: New IC(%d) TC(%d)",
         MOD, meth, itemCount + 1, totalCount + 1);
    -- Close ALL of the subrecs that might have been opened
    rc = ldt_common.closeAllSubRecs( src );
    if( rc < 0 ) then
      warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
      error( ldte.ERR_SUBREC_CLOSE );
    end
  elseif ( rc == 1 ) then
    GP=F and trace("[DEBUG]<%s:%s> OVERWRITE: Did NOT update Count(%d)",
         MOD, meth, propMap[PM.ItemCount]);
  else
    warn("[ERROR]<%s:%s> UNEXPECTED return(%s) from localPut()",
         MOD, meth, tostring(rc));
  end
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  -- Look at the results after EACH insert.
  -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  -- Must make SURE that this is never done in production, as it is a very
  -- costly operation.
  -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  if ( DEBUG == true ) then
    info("EXPENSIVE LMAP VALIDATION TURNED ON!!");
    local startSize = propMap[PM.ItemCount];

    trace("\n\n>>>>>>>>>>>>>>> VALIDATE PUT: Count Size(%d) <<<<<<<<<<<<\n",
        startSize);
    local endSize = lmap.dump( topRec, ldtBinName, src );
    trace("\n\n>>>>>>>>>>>>>>>>>> DONE VALIDATE Dump Size(%d)<<<<<<<<<<<<\n",
      endSize);
    if( startSize ~= endSize ) then
      warn("[INTERNAL ERROR]: StartSize(%d) <> EndSize(%d)",
        startSize, endSize );
    end
  end -- if DEBUG
   
  GP=E and trace("[EXIT]<%s:%s> : Done.  RC(%s)", MOD, meth, tostring(rc) );
  return rc;
end -- function lmap.put()

-- ======================================================================
-- put_all() -- Insert multiple name/value pairs into the LMAP
-- ======================================================================
-- This now uses localPut() to write, so now we can return a VECTOR of
-- errors in the event of any one insert going badly.
-- In fact, if the ERROR is not severe (e.g. a duplicate value error),
-- then we should keep on going.
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) nameValMap: The Map containing all of the new Name/Value pairs
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lmap.put_all( topRec, ldtBinName, nameValMap, createSpec, src )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP PUT ALL] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.put_all()";
  local rc = 0;
   
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) Name/Val MAP(%s) module(%s)", MOD,
    meth, tostring(ldtBinName), tostring(nameValMap), tostring(createSpec) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO] <%s:%s> LMAP CONTROL BIN does not Exist:Creating",
         MOD, meth );

    for name, value in map.pairs( nameValMap ) do
      -- set up a new LDT bin
      setupLdtBin( topRec, ldtBinName, name, value, createSpec );
      break;
    end
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read/Write Functions (Filter)
  G_Filter = ldt_common.setReadFunctions( nil, nil );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local itemCount = propMap[PM.ItemCount];
  local totalCount = ldtMap[LS.TotalCount];
  local newCount = 0;
  for name, value in map.pairs( nameValMap ) do
    if name and value then

      GP=F and trace("[DEBUG]<%s:%s> Arg: Name(%s) Val(%s) TYPE(%s)",
        MOD, meth, tostring( name ), tostring( value ), type(value));
      rc = localPut( src, topRec, ldtCtrl, name, value );
      -- We need to drop out of here if there's an error, but we have to do it
      -- carefully because all previous PUTS must have succeeded.  So, we
      -- should really return a VECTOR of return status!!!!
      -- Although -- Subrecs do not get written until the end, so if we do
      -- jump out with an error, then (in theory), no Sub-Rec gets updated.
      -- TODO: Return a VECTOR of error status and jump out with that vector
      -- on error!!
      if( rc == 0 ) then
        newCount = newCount + 1;
        propMap[PM.ItemCount] = itemCount + newCount; -- number of valid items goes up
        ldtMap[LS.TotalCount] = totalCount + newCount; -- Total number of items goes up
        GP=F and trace("[DEBUG]<%s:%s> lmap insertion for N(%s) V(%s) RC(%d)",
          MOD, meth, tostring(name), tostring(value), rc );
      elseif ( rc == 1 ) then
        GP=F and trace("[DEBUG]<%s:%s> OVERWRITE: Did NOT update Count(%d)",
             MOD, meth, propMap[PM.ItemCount]);
      else
        GP=F and trace("[ERROR]<%s:%s> lmap insertion for N(%s) V(%s) RC(%d)",
          MOD, meth, tostring(name), tostring(value), rc );
      end
    else
      debug("[NOTICE]<%s:%s> Nil Value in Name(%s) Value(%s) Pair.", MOD, meth,
        tostring(name), tostring(value));
    end
  end -- for each new value in the map

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  -- With recent changes, we know that the record is now already created
  -- so all we need to do is perform the update (no create needed).
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
   
  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmap.put_all()

-- ======================================================================
-- lmap.get() -- Return a map containing the searched-for name/value pair.
-- ======================================================================
-- Return the MAP of the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) searchname:
-- (*) filterModule:
-- (*) filter:
-- (*) fargs:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function
lmap.get(topRec, ldtBinName, searchName, filterModule, filter, fargs, src)
  GP=B and info("\n\n >>>>>>>>> API[ LMAP GET] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.get()";
  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local resultMap = map(); -- add results to this list.
  local rc = 0; -- start out OK.
  
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end
  
  -- Set up the Read Functions (Filter)
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;

--  --
--  -- Process these two options differently.  Either we're in COMPACT MODE,
--  -- which means have two simple lists connected to the LDT BIN, or we're
-- -- in REGULAR_MODE, which means we're going to open up a SubRecord and
--  -- read the lists in there.
--  if ldtMap[LS.StoreState] == SS_COMPACT then 
--    local nameList = ldtMap[LS.CompactNameList];
--    local position = searchList( nameList, searchName );
--    local resultObject = nil;
--    if( position > 0 ) then
--      local valueList = ldtMap[LS.CompactValueList];
--      resultObject = validateValue( valueList[position] );
--    end
--    if( resultObject == nil ) then
--      debug("[NOT FOUND]<%s:%s> name(%s) not found",
--        MOD, meth, tostring(searchName));
--      error( ldte.ERR_NOT_FOUND );
--    end
--    resultMap[nameList[position]] = resultObject;
--  else
--    -- Search the SubRecord.
--    regularSearch( src, topRec, propMap, ldtMap, searchName, resultMap );
--  end
--

  rc = localGet( src, topRec, propMap, ldtMap, searchName, resultMap );

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
     MOD, meth, tostring(resultMap));

  return resultMap;
end -- function lmap.get()

-- ======================================================================
-- lmap.get_all() -- Return a map containing all of the the searched-for
-- name/value pairs.
-- ======================================================================
-- Return the MAP of the name/value pairs for all names that exist in 
-- the large Map.
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) nameList:
-- (*) filterModule:
-- (*) filter:
-- (*) fargs:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- RETURN:
-- Success: Map containing the name/values found.
-- Error: Error Code and Message.
-- ======================================================================
function
lmap.get_all(topRec, ldtBinName, nameList, filterModule, filter, fargs, src)
  GP=B and info("\n\n >>>>>>>>> API[ LMAP GET_ALL] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.get()";
  GP=E and trace("[ENTER]<%s:%s> Search for Values: NameList(%s)",
                 MOD, meth, tostring( nameList ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local rc = 0; -- start out OK.


  if nameList == nil or #nameList == 0 then
    return map.new(1);
  end

  local resultMap = map.new(#nameList); -- add results to this list.
  
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end
  
  -- Set up the Read Functions (Filter)
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;

  local listSize = #nameList;

  for i = 1, listSize do
    rc = localGet( src, topRec, propMap, ldtMap, nameList[i], resultMap );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
     MOD, meth, tostring(resultMap));

  return resultMap;
end -- function lmap.get()


-- ======================================================================
-- lmap.exists() -- Return 1 if the name exists.
-- ======================================================================
-- Locate the name/value pair if the name exists in the map.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- If the item exists, return 1, otherwise return 0.
-- Parms:
-- (*) topRec: the Server record that holds the Large Map Instance
-- (*) ldtBinName: The name of the bin for the Large Map
-- (*) searchname:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lmap.exists(topRec, ldtBinName, searchName, src )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP EXISTS] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.exists()";
  GP=E and trace("[ENTER]<%s:%s> Search for Value(%s)",
                 MOD, meth, tostring( searchName ) );
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local resultMap = {}; -- add results to this list.
  local rc = 0; -- start out OK.
  
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end
  
  -- Set up the Read Functions (Filter)
  G_Filter = ldt_common.setReadFunctions( nil, nil );
  G_FunctionArgs = nil;

  -- Process these two options differently.  Either we're in COMPACT MODE,
  -- which means have two simple lists connected to the LDT BIN, or we're
  -- in REGULAR_MODE, which means we're going to open up a SubRecord and
  -- read the lists in there.
  if ldtMap[LS.StoreState] == SS_COMPACT then 
    local nameList = ldtMap[LS.CompactNameList];
    local position = searchList( nameList, searchName );
    local resultObject = nil;
    if( position > 0 ) then
      local valueList = ldtMap[LS.CompactValueList];
      resultObject = validateValue( valueList[position] );
    end
    if( resultObject == nil ) then
      debug("[NOT FOUND]<%s:%s> name(%s) not found",
        MOD, meth, tostring(searchName));
      error( ldte.ERR_NOT_FOUND );
    end
    resultMap[nameList[position]] = resultObject;
  else
    -- Search the SubRecord.
    regularSearch( src, topRec, propMap, ldtMap, searchName, resultMap );
  end

  local exists = 0;
  if resultMap[searchName] ~= nil then
    exists = 1;
  end

  GP=E and trace("[EXIT]: <%s:%s>: Exits Returns (%d)", MOD, meth, exists);
  return exists;
end -- function lmap.exists()

-- ========================================================================
-- lmap.scan() -- Return a map containing ALL name/value pairs.
-- ========================================================================
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) filterModule:
-- (*) filter:
-- (*) fargs:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ========================================================================
function lmap.scan(topRec, ldtBinName, filterModule, filter, fargs, src)
  GP=B and info("\n\n >>>>>>>>> API[ LMAP SCAN ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.scan()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) UMod(%s) Filter(%s) Fargs(%s)",
   MOD, meth, tostring(ldtBinName), tostring(filterModule), tostring(filter),
   tostring(fargs));
                 
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local itemCount = propMap[PM.ItemCount];
  if (itemCount == nil or itemCount == 0) then
    return map.new(1);
  end
  local resultMap = map.new(itemCount);
  local rc = 0; -- start out OK.

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Set up the Read Functions (Filter)
  G_Filter = ldt_common.setReadFunctions(filterModule, filter );
  G_FunctionArgs = fargs;

  if ldtMap[LS.StoreState] == SS_COMPACT then 
    -- Scan the Compact Name/Value Lists
    rc = scanLMapList( ldtMap[LS.CompactNameList], ldtMap[LS.CompactValueList],
      resultMap );
  else -- regular searchAll
    -- Search all of the Sub-Records in the Hash Directory.  Actually,
    -- this is more complex, because each Hash Cell may be EMPTY, hold a
    -- small LIST, or may be a Sub-Record.
    rc = regularScan(src, topRec, ldtCtrl, resultMap ); 
  end

  GP=E and trace("[EXIT]: <%s:%s>: Scan Returns Size(%d)",
                   MOD, meth, map.size(resultMap));
  	  
  GP=DEBUG and ldt_common.dumpMap(resultMap, "LMap Scan Results:Dumped to Log");

  return resultMap;
end -- function lmap.scan()

-- ======================================================================
-- lmap.remove() -- Remove the name/value pair matching <searchName>
-- ======================================================================
-- Delete a value from the MAP.
-- Find the value (bin and structure), then remove it.
--
-- If StoreState is compact, then we know to look in the compact list.
-- Otherwise, we'll search the right list for this hash value.
--
-- Please refer to lmap_design.lua for further notes. 
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- (*) createSpec: When in "Create Mode", use this Create Spec
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function
lmap.remove( topRec, ldtBinName, searchName, filterModule, filter, fargs, src )
  GP=B and trace("\n\n  >>>>>>>> API[ REMOVE ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.remove()";
   
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) name(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchName));

  GP=E and trace("[DEBUG]<%s:%s> filterModule(%s) filter(%s) fargs(%s)",
    MOD, meth, tostring(filterModule), tostring(filter),tostring(fargs));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Set up the Read Functions (filter)
  G_Filter = ldt_common.setReadFunctions(filterModule, filter);
  G_FunctionArgs = fargs;
  
  -- For the compact list, it's a simple list delete (if we find it).
  -- For the subRec list, it's a more complicated search and delete.
  local resultMap = {};
  if ldtMap[LS.StoreState] == SS_COMPACT then
    rc = compactDelete( ldtMap, searchName, resultMap );
  else
    -- It's "regular".  Find the right LDR (subRec) and search it.
    rc = regularDelete( src, topRec, ldtCtrl, searchName, resultMap );
  end

  -- Update the counts.  If there were any errors, the code would have
  -- jumped out of the Lua code entirely.  So, if we're here, the delete
  -- was successful.
  local itemCount = propMap[PM.ItemCount];
  local totalCount = ldtMap[LS.TotalCount];
  propMap[PM.ItemCount] = itemCount - 1; -- number of valid items goes down
  ldtMap[LS.TotalCount] = totalCount - 1; -- Total number of items goes up

  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, update the record
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if not rc then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
   
  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lmap.remove()

-- ========================================================================
-- lmap.destroy() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the Ldt Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NOTE: This could eventually be moved to COMMON, and be "ldt.destroy()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
function lmap.destroy( topRec, ldtBinName, src )
  GP=B and info("\n\n  >>>>>>>> API[ LMAP DESTROY ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.destroy()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
 
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  ldt_common.destroy( src, topRec, ldtBinName, ldtCtrl );

  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- function lmap.destroy()

-- ========================================================================
-- lmap.size() -- return the number of elements (item count) in the LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = size is greater or equal to 0.
--   res = -1: Some sort of error
-- ========================================================================
function lmap.size( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP SIZE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "size()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local itemCount = propMap[PM.ItemCount];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=E and trace("[EXIT]: <%s:%s> : SIZE(%d)", MOD, meth, itemCount );
  return itemCount;
end -- function lmap.size()

-- ========================================================================
-- lmap.config() -- return the config settings
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = Map of config settings
--   res = -1: Some sort of error
-- ========================================================================
function lmap.config( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>> API[ LMAP CONFIG ] <<<<<<<<<< \n");
  
  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.config()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  local config = ldtSummary(ldtCtrl); 

  GP=E and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config) );
  return config;
end -- function lmap.config();

-- ========================================================================
-- lmap.get_capacity() -- return the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.get_capacity( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>>>> API[ LMAP GET CAPACITY ] <<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.get_capacity()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and Ldt control map from the Ldt bin list.
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local capacity = ldtMap[LC.StoreLimit];
  if( capacity == nil ) then
    capacity = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function lmap.get_capacity()

-- ========================================================================
-- set_capacity() -- set the current capacity setting for this LDT
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lmap.set_capacity( topRec, ldtBinName, capacity )
  GP=B and info("\n\n >>>>>>>>>>> API[ LMAP SET CAPACITY ] <<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.set_capacity()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and Ldt control map from the Ldt bin list.
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  if( capacity ~= nil and type(capacity) == "number" and capacity >= 0 ) then
    ldtMap[LC.StoreLimit] = capacity;
  else
    warn("[ERROR]<%s:%s> Bad Capacity Value(%s)",MOD,meth,tostring(capacity));
    error( ldte.ERR_INTERNAL );
  end

  -- All done, store the record
  -- Update the Top Record with the new control info
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end

  GP=E and trace("[EXIT]: <%s:%s> : new size(%d)", MOD, meth, capacity );
  return 0;
end -- function lmap.set_capacity()

-- ========================================================================
-- lmap.ldt_exists() --
-- ========================================================================
-- return 1 if there is an lmap object here, otherwise 0
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   True:  (LMAP exists in this bin) return 1
--   False: (LMAP does NOT exist in this bin) return 0
-- ========================================================================
function lmap.ldt_exists( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>>>> API[ LMAP EXISTS ] <<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lmap.ldt_exists()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  if ldt_common.ldt_exists(topRec, ldtBinName, LDT_TYPE ) then
    GP=F and trace("[EXIT]<%s:%s> Exists", MOD, meth);
    return 1
  else
    GP=F and trace("[EXIT]<%s:%s> Does NOT Exist", MOD, meth);
    return 0
  end
end -- function lmap.ldt_exists()

-- ========================================================================
-- ========================================================================
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
--
-- Dump the full contents of the Large Map, with Separate Hash Groups
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Parms:
-- (1) topRec: the user-level record holding the Ldt Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) src: Sub-Rec Context - Needed for repeated calls from caller
-- Return a COUNT of the number of items dumped to the log.
-- ========================================================================
function lmap.dump( topRec, ldtBinName, src )
  GP=F and trace("\n\n  >>>>>>>>>>>> API[ LMAP DUMP ] <<<<<<<<<<<<<<<< \n");
  --
  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "dump()";
  GP=E and trace("[ENTER]<%s:%s> BIN(%s)", MOD, meth, tostring(ldtBinName) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- First, dump the control information.
  ldtDebugDump( ldtCtrl );

  -- local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  local cellAnchor;
  local count = 0;
  local rc = 0;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local resultMap = map();
  local label = "EMPTY";

  if ldtMap[LS.StoreState] == SS_COMPACT then 
    -- Scan the Compact Name/Value Lists
    rc = scanLMapList( ldtMap[LS.CompactNameList], ldtMap[LS.CompactValueList],
      resultMap );
    ldt_common.dumpMap( resultMap, "CompactList");
    count = map.size( resultMap );
  else -- "Regular Hash Dir"
    -- Search all of the Sub-Records in the Hash Directory.  Actually,
    -- this is more complex, because each Hash Cell may be EMPTY, hold a
    -- small LIST, or may be a Sub-Record.
    local hashDirectory = ldtMap[LS.HashDirectory]; 

    -- For each Hash Cell, Dump the contents
    for i = 1, list.size( hashDirectory ), 1 do
      label = "EMPTY:"; -- the default.  If not filled in somewhere else.
      resultMap = map();
      cellAnchor = hashDirectory[i];
      if ( not cellAnchorEmpty( cellAnchor ) ) then
      -- TODO: Move this code into a common "cellAnchor" Scan.
        GP=DEBUG and trace("[DEBUG]<%s:%s> Hash Cell :: Index(%d) Cell(%s)",
          MOD, meth, i, tostring(cellAnchor));

        -- If not empty, then the cell anchor must be either in a LIST
        -- state, or it has a Sub-Record.  Later, it might have a Radix tree
        -- of multiple Sub-Records.
        if( cellAnchor[C_CellState] == C_STATE_LIST ) then
          label = "LIST:";
          -- The small list is inside of the cell anchor.  Get the lists.
          scanLMapList( cellAnchor[C_CellNameList], cellAnchor[C_CellValueList],
            resultMap );
        elseif( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
          -- We have a sub-rec -- open it
          local digest = cellAnchor[C_CellDigest];
          if( digest == nil ) then
            warn("[ERROR]: <%s:%s>: nil Digest value",  MOD, meth );
            error( ldte.ERR_SUBREC_OPEN );
          end

          label = "SUB-REC:";

          local digestString = tostring(digest);
          local subRec = ldt_common.openSubRec( src, topRec, digestString );
          if( subRec == nil ) then
            warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",
              MOD, meth, digestString );
            error( ldte.ERR_SUBREC_OPEN );
          end
          scanLMapList(subRec[LDR_NLIST_BIN], subRec[LDR_VLIST_BIN], resultMap);
          ldt_common.closeSubRec( src, subRec, false);
        else
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          -- When we do a Radix Tree, we will STILL end up with a SubRecord
          -- but it will come from a Tree.  We just need to manage the SubRec
          -- correctly.
          warn("[ERROR]<%s:%s> Not ready to handle Radix Trees in Hash Cell",
            MOD, meth );
          error( ldte.ERR_INTERNAL );
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        end
      end -- Cell not empty
      -- Print out the map for EACH Hash Cell.
      ldt_common.dumpMap( resultMap, label .. "Cell:" .. tostring(i));
      count = count + map.size( resultMap );
    end -- for each Hash Dir Cell
  end -- regular Hash Dir

  GP=E and trace("[EXIT]<%s:%s> <><><> DONE <><><>", MOD, meth );
  return count; 
end -- function lmap.dump();

-- ======================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return lmap;
-- ========================================================================
--   _     ___  ___  ___  ______ 
--  | |    |  \/  | / _ \ | ___ \
--  | |    | .  . |/ /_\ \| |_/ /
--  | |    | |\/| ||  _  ||  __/ 
--  | |____| |  | || | | || |    
--  \_____/\_|  |_/\_| |_/\_|    (LIB)
--                               
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
