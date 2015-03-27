-- AS Large Set (LSET) Operations
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
-- Track the date and iteration of the last update.
local MOD="lib_lset_2014_12_14.B"; 

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
-- <<  LSET Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSET module:
--
-- (*) Status = lset.add( topRec, ldtBinName, newValue, createSpec, src)
-- (*) Status = lset.add_all( topRec, ldtBinName, valueList, createSpec, src)
-- (*) Object = lset.get( topRec, ldtBinName, searchValue, src) 
-- (*) Number = lset.exists( topRec, ldtBinName, searchValue, src) 
-- (*) List   = lset.scan( topRec, ldtBinName, filterModule, filter, fargs, src)
-- (*) Status = lset.remove( topRec, ldtBinName, searchValue, src) 
-- (*) Object = lset.take( topRec, ldtBinName, searchValue, src) 
-- (*) Status = lset.destroy( topRec, ldtBinName, src)
-- (*) Number = lset.size( topRec, ldtBinName )
-- (*) Map    = lset.get_config( topRec, ldtBinName )
-- (*) Status = lset.set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = lset.get_capacity( topRec, ldtBinName )
-- (*) Number = lset.ldt_exists(topRec, ldtBinName)
-- ======================================================================

-- Large Set Design/Architecture
--
-- Large Set includes two different implementations in the same module.
-- The implementation used is determined by the setting "SetTypeStore"
-- in the LDT control structure.  There are the following choices.
-- (1) "TopRecord" SetTypeStore, which holds all data in the top record.
--     This is appropriate for small to medium lists only, as the total
--     storage capacity is limited to the max size of a record, which 
--     defaults to 128kb, but can be upped to 2mb (or even more?)
-- (2) "SubRecord" SetTypeStore, which holds data in sub-records.  With
--     the sub-record type, Large Sets can be virtually any size, although
--     the digest directory in the TopRecord can potentially grow large 
--     for VERY large sets.
--
-- The LDT bin value in a top record, known as "ldtCtrl" (LDT Control),
-- is a list of two maps.  The first map is the property map, and is the
-- same for every LDT.  It is done this way so that the LDT code in
-- the Aerospike Server can read any LDT property using the same mechanism.
-- ======================================================================
-- >> Please refer to ldt/doc_lset.md for architecture and design notes.
-- ======================================================================
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSET Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are two different implementations of Large Set:
-- (1) TopRecord Mode (the first one to be implemented)
-- (2) SubRecord Mode (the one that is better used in production)
--
-- ++=================++
-- || TOP RECORD MODE ||
-- ++=================++
-- In a user record, the bin holding the Large SET control information
-- is named with the user's name, however, there is a restriction that there
-- can be only ONE "TopRecord Mode" LSET instance in a record.  This
-- restriction exists because of the way the numbered bins are used in the
-- record.  The Numbered LSETS bins each contain a list of entries. They
-- are prefixed with "LSetBin_" and numbered from 0 to N, where N is the
-- modulo value that is set on create (otherwise, default to 31).
--
-- When an LDT instance is created in a record, a hidden "LDT Properties" bin
-- is created in the record.  There is only ONE LDT Prop bin per record, no
-- matter how many LDT instances are in the record.
--
-- In the bin named by the user for the LSET LDT instance, (e.g. MyURLs)
-- there is an "LDT Control" structure, which is a list of two maps.
-- The first map, the Property Map, holds the same information for every
-- LDT (stack, list, map, set).  The second map holds LSET-specific 
-- information.
--
-- (Standard Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User | LDT |LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2| Prop|CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |     |           |                   
--                      V     V     V           V                   
--                   +=====++===+ +===+       +===+                  
--                   |P Map||val| |val|       |val|
--                   +-----+|val| |val|       |val|
--                   |C Map||...| |...|       |...|
--                   +=====+|val| |val|       |val|
--                          +===+ +===+       +===+ 
--
-- The Large Set distributes searches over N lists.  Searches are done
-- with linear scan in one of the bin lists.  The set values are hashed
-- and then the specific bin is picked "hash(val) Modulo N".  The N bins
-- are organized by name using the method:  prefix "LsetBin_" and the
-- modulo N number.
--
-- The modulo number is always a prime number -- to minimize the amount
-- of "collisions" that are often found in power of two modulo numbers.
-- Common choices are 17, 31 and 61.
--
-- The initial state of the LSET is "Compact Mode", which means that ONLY
-- ONE LIST IS USED -- in Bin 0.  Once there are a "Threshold Number" of
-- entries, the Bin 0 entries are rehashed into the full set of bin lists.
-- Note that a more general implementation could keep growing, using one
-- of the standard "Linear Hashing-style" growth patterns.
--
-- (Compact Mode)
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User | LDT |LSET |LSET |LSET |. . .|LSET |                |
-- |Bin 1|Bin 2| Prop|CTRL |Bin 0|Bin 1|     |Bin N|                |
-- +-----+-----+-----+-----+----------------------------------------+
--                      |     |   
--                      V     V  
--                   +=====++===+
--                   |P Map||val|
--                   +-----+|val|
--                   |C Map||...|
--                   +=====+|val|
--                          +===+
-- ++=================++
-- || SUB RECORD MODE ||
-- ++=================++
--
-- (Standard Mode)
-- +-----+-----+-----+-----+-----+
-- |User |User | LDT |LSET |User |
-- |Bin 1|Bin 2| Prop|CTRL |Bin N|
-- +-----+-----+-----+-----+-----+
--                      |    
--                      V    
--                   +======+
--                   |P Map |
--                   +------+
--                   |C Map |
--                   +------+
--                   |@|@||@| Hash Cell Anchors (Control + Digest List)
--                   +|=|=+|+
--                    | |  |                       SubRec N
--                    | |  +--------------------=>+--------+
--                    | |              SubRec 2   |Entry 1 |
--                    | +------------=>+--------+ |Entry 2 |
--                    |      SubRec 1  |Entry 1 | |   o    |
--                    +---=>+--------+ |Entry 2 | |   o    |
--                          |Entry 1 | |   o    | |   o    |
--                          |Entry 2 | |   o    | |Entry n |
--                          |   o    | |   o    | +--------+
--                          |   o    | |Entry n |
--                          |   o    | +--------+
--                          |Entry n | "LDR" (LDT Data Record) Pages
--                          +--------+ hold the actual data entries
--
-- The Hash Directory actually has a bit more structure than just a 
-- digest list. The Hash Directory is a list of "Cell Anchors" that
-- contain several pieces of information.
-- A Cell Anchor maintains a count of the subrecords attached to a
-- a hash directory entry.  Furthermore, if there are more than one
-- subrecord associated with a hash cell entry, then we basically use
-- the hash value to further distribute the values across multiple
-- sub-records.
--
--  +-------------+
-- [[ Cell Anchor ]]
--  +-------------+
-- Cell Item Count: Total number of items in this hash cell
-- Cell SubRec Count: Number of sub-records associated with this hash cell
-- Cell SubRec Depth: Depth (modulo) of the rehash (1, 2, 4, 8 ...)
-- Cell Digest Map: The association of a hash depth value to a digest
-- Cell Item List: If in "bin compact mode", the list of elements.
--
-- ======================================================================
-- Aerospike Server Functions:
-- The following functions are used to manipulate TopRecords and
-- SubRecords.
-- ======================================================================
-- Aerospike Record Functions:
-- status = aerospike:create( topRec )
-- status = aerospike:update( topRec )
-- status = aerospike:remove( rec ) (not currently used)
--
--
-- Aerospike SubRecord Functions:
-- newRec = aerospike:create_subrec( topRec )
-- rec    = aerospike:open_subrec( topRec, childRecDigest)
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
-- ++==================++
-- || External Modules ||
-- ++==================++
-- set up our "outside" links.
-- We use this to get our Hash Functions
local  CRC32 = require('ldt/CRC32');

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt/ldt_errors');

-- We have recently moved a number of COMMON functions into the "ldt_common"
-- module, namely the subrec routines and some list management routines.
-- We will likely move some other functions in there as they become common.
local ldt_common = require('ldt/ldt_common');


local Map = getmetatable( map() );
local List = getmetatable( list() );
-- ++=======================================++
-- || GLOBAL VALUES -- Local to this module ||
-- ++=======================================++
-- This flavor of LDT (only LSET defined here)
local LDT_TYPE   = "LSET";

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE= 'T';
local AS_FALSE= 'F';

-- =======================================================================
-- NOTE: It is important that the next values stay consistent
-- with the same variables in the ldt/settings_lset.lua file.
-- ===========================================================<Begin>=====
-- In this early version of SET, we distribute values among lists that we
-- keep in the top record.  This is the default modulo value for that list
-- distribution.
local DEFAULT_MODULO = 512;

-- Switch from a single list to distributed lists after this amount
local DEFAULT_THRESHOLD = 10;

-- Switch from a SMALL list in the cell anchor to a full Sub-Rec.
-- We've set this to ZERO as a default, because any significant
-- size object can cause use to exceed the TopRecord Size.
local DEFAULT_BINLIST_THRESHOLD = 0;

-- Define the default value for the "Unique Identifier" function.
-- User can override the function name, if they so choose.
local UI_FUNCTION_DEFAULT = "unique_identifier";

-- ======================================================================
-- Default Capacity -- should be high, but not too high.  If the user wants
-- to go REALLY high, they should specify that.  If we don't pick a value,
-- then the user will hit a storage limit error without warning.
-- ======================================================================
local DEFAULT_JUMBO_CAPACITY   =    500;
local DEFAULT_LARGE_CAPACITY   =   5000;
local DEFAULT_MEDIUM_CAPACITY  =  50000;
local DEFAULT_SMALL_CAPACITY   = 500000;
-- ===========================================================<End>=======

-- Use this to test for CtrlMap Integrity.  Every map should have one.
local MAGIC="MAGIC";     -- the magic value for Testing LSET integrity

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
local SS_COMPACT = 'C'; -- Using "single bin" (compact) mode
local SS_REGULAR = 'R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
local KT_ATOMIC  = 'A'; -- the set value is just atomic (number or string)
local KT_COMPLEX = 'C'; -- the set value is complex. Use Function to get key.

-- Hash Value (HV) Constants
local HV_EMPTY = 'E'; -- Marks an Entry Hash Directory Entry.

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)
--
-- HashType (HT) values
local HT_STATIC  = 'S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC = 'D'; -- Use a DYNAMIC set of bins for hash lists

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records
local ST_HYBRID = 'H'; -- Store values (lists) Hybrid Style
-- NOTE: Hybrid style means that we'll use sub-records, but for any hash
-- value that is less than "SUBRECORD_THRESHOLD", we'll store the value(s)
-- in the top record.  It is likely that very short lists will waste a lot
-- of sub-record storage. Although, storage in the top record also costs
-- in terms of the read/write of the top record.

-- Key Compare Function for Complex Objects
-- By default, a complex object will have a "key" field, which the
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

-- AS LSET Bin Names
-- We now use the User's name for the LDT Bin, even in the TopRec case.
-- However, we do pick special bin name prefix when we use either TopRec
-- or SubRec bins.
local LSET_DATA_BIN_PREFIX   = "LSetBin_";

-- Enhancements for LSET begin here 

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
local RT_CDIR= 3; -- xxx: Cold Dir Subrec::Not used for set_type() 
local RT_ESR = 4; -- 0x4: Existence Sub Record

-- Errors used in LDT Land, but errors returned to the user are taken
-- from the common error module: ldt_errors.lua
local ERR_OK            =  0; -- HEY HEY!!  Success
local ERR_GENERAL       = -1; -- General Error
local ERR_NOT_FOUND     = -2; -- Search Error

-- In order to tell the Server what's happening with LDT (and maybe other
-- calls, we call "set_context()" with various flags.  The server then
-- uses this to measure LDT call behavior.
local UDF_CONTEXT_LDT = 1;

-- -----------------------------------------------------------------------
---- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values. (There's no secret message hidden in these values).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields common across lset, lstack & lmap 
local RPM = {
  LdtCount             = 'C',  -- Number of LDTs in this rec
  VInfo                = 'V',  -- Partition Version Info
  Magic                = 'Z',  -- Special Sauce
  SelfDigest           = 'D'   -- Digest of this record
};

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Property Map (PM) Fields for all LDT's
local PM = {
  ItemCount             = 'I', -- (Top): Count of all items in LDT
  Version               = 'V', -- (Top): Code Version
  SubRecCount           = 'S', -- (Top): # of subRecs in the LDT
  LdtType               = 'T', -- (Top): Type: stack, set, map, list
  BinName               = 'B', -- (Top): LDT Bin Name
  Magic                 = 'Z', -- (All): Special Sauce
  CreateTime            = 'C', -- (Top): LDT Create Time
  RecType               = 'R', -- (All): Type of Rec:Top,Ldr,Esr,CDir
  EsrDigest             = 'E', -- (All): Digest of ESR
  ParentDigest          = 'P', -- (Subrec): Digest of TopRec
  SelfDigest            = 'D'  -- (Subrec): Digest of THIS Record
};

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LDT Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields Common (LC) to ALL LDTs (managed by the LDT COMMON routines)
local LC = {
  UserModule             = 'P', -- User's Lua file for overrides
  StoreLimit             = 'L', -- Used for Eviction (eventually)
};

-- Fields Specific (LS) to lset & lmap 
local LS = {
  HashType               = 'h', -- Hash Type (static or dynamic)
  HashDepth              = 'd', -- # of hash Bits to use in Dynamic Hashing
  LdrEntryCountMax       = 'e', -- Max size of the LDR List
  LdrByteEntrySize       = 's', -- Size of a Fixed Byte Object
  LdrByteCountMax        = 'b', -- Max Size of the LDR in bytes
  StoreState             = 'S', -- Store State (Compact or List)
  SetTypeStore           = 'T', -- Type of the Set Store (Rec/SubRec)
  BinaryStoreSize        = 'B', -- Size of Object when in Binary form
  TotalCount             = 'C', -- Total number of slots used
  Modulo 				 = 'm', -- Modulo used for Hash Function
  Threshold              = 'H', -- Threshold: Compact->Regular state
  CompactList            = 'c', -- Compact List (when in Compact Mode)
  HashDirectory          = 'D', -- Directory of Hash Cells
  HashCellMaxList        = 'X'  -- Threshold for converting from a
                                -- Hash Cell binlist to sub-record.
};

-- ------------------------------------------------------------------------
-- Maintain the LSET letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
-- ------------------------------------------------------------------------
---- >>> Be Mindful of the LDT Common Fields that ALL LDTs must share <<<
-- ------------------------------------------------------------------------
-- A:                        a:                       0:
-- B:LS.BinaryStoreSize      b:LS.LdrByteCountMax     1:
-- C:LS.TotalCount           c:LS.CompactList         2:
-- D:LS.HashDirectory        d:                       3:
-- E:                        e:LS.LdrEntryCountMax    4:
-- G:                        g:                       6:
-- H:LS.Threshold            h:LS.HashType            7:
-- I:                        i:                       8:
-- J:                        j:                       9:
-- K:                        k:LC.KeyType
-- L:LC.StoreLimit           l:
-- M:LC.StoreMode            m:LS.Modulo
-- N:                        n:
-- O:                        o:
-- P:                        p:
-- Q:                        q:
-- R:                        r:                     
-- S:LS.StoreState           s:LS.LdrByteEntrySize   
-- V:                        v:
-- W:                        w:                     
-- X:LS.HashCellMaxList      x:                     
-- Y:                        y:
-- Z:                        z:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ------------------------------------------------------------------------
-- For the Sub-Record version of Large Set, we store the values for a
-- particular hash cell in one or more sub-records.  For a given modulo
-- (directory size) value, we allocate a small object that holds the anchor
-- for one or more sub-records that hold values.
-- ------------------------------------------------------------------------
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
-- Similar to LMAP, but we don't use "NameList" in LSET, just "ValueList"
-- local C_CellNameList   = 'N'; -- Pt to a LIST of objects (not for LSET)
local C_CellValueList  = 'V'; -- Pt to a LIST of objects
local C_CellDigest     = 'D'; -- Pt to a single digest value
local C_CellTree       = 'T'; -- Pt to a LIST of digests
local C_CellItemCount  = 'C'; -- Cell item count, once we're in Sub-Rec Mode

-- Here are the various constants used with Hash Cells
local C_STATE_EMPTY   = 'E'; -- 
local C_STATE_LIST    = 'L';
local C_STATE_DIGEST  = 'D';
local C_STATE_TREE    = 'T';

-- NOTE that we may choose to mark a Hash Cell Entry as "EMPTY" directly
-- and save the space and MSG_PACK cost of converting a map that holds ONLY
-- an "E". This will add one more check to the logic (check type/value)
-- but it will probably be worth it overall to avoid packing empty maps.
-- -----------------------------------------------------------------------
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT Data Record (LDR) Control Map Fields (Recall that each Map ALSO has
-- the PM (general property map) fields.
local LDR_ByteEntryCount       = 'C'; -- Count of bytes used (in binary mode)
local LDR_NextSubRecDigest     = 'N'; -- Digest of Next Subrec in the chain

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
-- with an LSET LDT:
-- (1) LDR (LDT Data Record) -- used to hold data from the Hash Cells
-- (2) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN   = "SR_PROP_BIN";
--
-- The LDT Data Records (LDRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local LDR_CTRL_BIN      = "LdrControlBin";  
local LDR_LIST_BIN      = "LdrListBin";  
local LDR_BNRY_BIN      = "LdrBinaryBin";

-- Enhancements for LSET end here 

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

-- Special Function -- if supplied by the user in the "Create Module",
-- then we call that UDF to adjust the LDT configuration settings.
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

-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 

-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- createSearchPath: Create and initialize a search path structure so
-- that we can fill it in during our hash chain search.
-- Parms:
-- (*) ldtMap: topRec map that holds all of the control values
-- ======================================================================
local function createSearchPath( ldtMap )
  local sp        = map();
  sp.FoundLevel   = 0;      -- No valid level until we find something
  sp.LevelCount   = 0;      -- The number of levels we looked at.
  sp.RecList      = list(); -- Track all open nodes in the path
  sp.DigestList   = list(); -- The mechanism to open each level
  sp.PositionList = list(); -- Remember where the key was
  sp.HasRoom      = list(); -- Remember where there is room.

  return sp;
end -- createSearchPath()

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
-- Add the ldtMap properties to the supplied resultMap.
-- ======================================================================
local function ldtMapSummary( resultMap, ldtMap )
  
    -- LDT Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = ldtMap[LS.LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[LS.LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[LS.LdrByteCountMax];
  
  -- General LDT Parms:
  resultMap.StoreMode            = ldtMap[LC.StoreMode];
  resultMap.StoreState           = ldtMap[LS.StoreState];
  resultMap.SetTypeStore         = ldtMap[LS.SetTypeStore];
  resultMap.StoreLimit           = ldtMap[LC.StoreLimit];
  resultMap.BinaryStoreSize      = ldtMap[LS.BinaryStoreSize];
  resultMap.KeyType              = ldtMap[LC.KeyType];
  resultMap.TotalCount			 = ldtMap[LS.TotalCount];		
  resultMap.Modulo 				 = ldtMap[LS.Modulo];
  resultMap.Threshold			 = ldtMap[LS.Threshold];
  resultMap.HashCellMaxList      = ldtMap[LS.HashCellMaxList];

end -- function ldtMapSummary

-- ======================================================================
-- Return the STRING version of ldtMapSummary()
-- ======================================================================
local function ldtMapSummaryString( ldtMap )
  local resultMap = map();
  ldtMapSummary( resultMap, ldtMap )
  return tostring(resultMap);
end

-- ======================================================================
-- ldtDebugDump()
-- ======================================================================
-- To aid in debugging, dump the entire contents of the ldtCtrl object
-- for LSET.  Note that this must be done in several prints, as the
-- information is too big for a single print (it gets truncated).
-- ======================================================================
local function ldtDebugDump( ldtCtrl )
  local meth = "ldtDebugDump()";

  -- Print MOST of the "TopRecord" contents of this LSET object.
  local resultMap                = map();
  resultMap.SUMMARY              = "LSET Summary";

  info("\n\n <><><><><><><><><> [ LDT LSET SUMMARY ] <><><><><><><><><> \n");
  --
  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  if( propMap[PM.Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN MAP--No Magic";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );
  info("\n<<<%s>>>\n", tostring(resultMap));
  resultMap = nil;

  -- Reset for each section, otherwise the result would be too much for
  -- the info call to process, and the information would be truncated.
  local resultMap2 = map();
  resultMap2.SUMMARY              = "LSET-SPECIFIC Values";

  -- Load the LSET-specific properties
  ldtMapSummary( resultMap2, ldtMap );
  info("\n<<<%s>>>\n", tostring(resultMap2));
  resultMap2 = nil;

  -- Print the Hash Directory
  local resultMap3 = map();
  resultMap3.SUMMARY              = "LSET Hash Directory";
  resultMap3.HashDirectory        = ldtMap[LS.HashDirectory];
  info("\n<<<%s>>>\n", tostring(resultMap3));

end -- function ldtDebugDump()

-- ======================================================================
-- local function ldtSummary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtCtrl )
  local meth = "ldtSummary()";

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    return "EMPTY LDT BIN VALUE";
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  
  if( propMap[PM.Magic] ~= MAGIC ) then
    return "BROKEN MAP--No Magic";
  end;

  -- Return a map to the caller, with descriptive field names
  local resultMap                = map();
  resultMap.SUMMARY              = "LSET Summary";

  -- Load up the COMMON properties
  propMapSummary( resultMap, propMap );

  -- Load up the LSET specific properties:
  ldtMapSummary( resultMap, ldtMap );

  return resultMap;
end -- ldtSummary()

-- ======================================================================
-- local function ldtSummaryString( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the ldtMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
   GP=F and trace("Calling ldtSummaryString "); 
  return tostring( ldtSummary( ldtCtrl ));
end -- ldtSummaryString()

-- ======================================================================
-- initializeLdtCtrl:
-- ======================================================================
-- Set up the LSetMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSetBIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSet
-- behavior.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) namespace: The Namespace of the record (topRec)
-- (*) set: The Set of the record (topRec)
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized ldtMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLdtCtrl(topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  GP=E and trace("[ENTER]: <%s:%s>::Bin(%s)",MOD, meth, tostring(ldtBinName));
  
  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific LDT Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local ldtMap = map();
  local ldtCtrl = list();

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM.ItemCount]  = 0; -- A count of all items in the stack
  propMap[PM.SubRecCount] = 0; -- A count of all Sub-Records in the LDT
  propMap[PM.Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM.LdtType]    = LDT_TYPE; -- Validate the ldt type
  propMap[PM.Magic]      = MAGIC; -- Special Validation
  propMap[PM.BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM.RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM.EsrDigest]  = 0; -- not set yet.
  propMap[PM.CreateTime] = aerospike:get_current_time();

  -- Specific LSET Parms: Held in ldtMap
  ldtMap[LC.StoreLimit]  = DEFAULT_MEDIUM_CAPACITY; -- Storage Limit

  -- LDT Data Record Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax]= 100;  -- Max # of Data Chunk items (List Mode)
  ldtMap[LS.LdrByteEntrySize]=   0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax] =   0; -- Max # of Data Chunk Bytes (binary mode)

  ldtMap[LS.StoreState]       = SS_COMPACT; 

  -- ===================================================
  -- NOTE: We will leave the DEFAULT LSET implementation as SUBRECORD,
  -- as the TopRecord version was interesting but does not perform well
  -- for large sizes.
  -- ===================================================
--ldtMap[LS.SetTypeStore]     = ST_RECORD;    -- Not used much anymore
  ldtMap[LS.SetTypeStore]     = ST_SUBRECORD; -- Default: Use Subrecords

  ldtMap[LS.HashType]         = HT_STATIC; -- Static or Dynamic
  -- ldtMap[LS.BinaryStoreSize] -- Set Later, if/when needed for Binary

  -- Complex will work for both atomic/complex.
  ldtMap[LC.KeyType]          = KT_COMPLEX; -- Most things will be complex
  ldtMap[LS.TotalCount]       = 0; -- Count of both valid and deleted elements
  ldtMap[LS.Modulo]           = DEFAULT_MODULO;
  -- Threshold for converting to a real hash table from the Compact List
  ldtMap[LS.Threshold]       =  DEFAULT_THRESHOLD; -- Rehash after this size
  -- Threshold for converting from a list in the Cell Anchor to a real
  -- Sub-Record.
  ldtMap[LS.HashCellMaxList]  = DEFAULT_BINLIST_THRESHOLD;

  -- Put our new maps in a list, in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  topRec[ldtBinName]            = ldtCtrl;
  record.set_flags( topRec, ldtBinName, BF_LDT_BIN ); -- must set every time

  GP=F and trace("[DEBUG]: <%s:%s> : LSET Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  ldt_common.setLdtRecordType( topRec );

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtCtrl;

end -- initializeLdtCtrl()

-- ======================================================================
-- We use the "CRC32" package for hashing the value in order to distribute
-- the value to the appropriate "sub lists".
-- ======================================================================
-- local  CRC32 = require('ldt/CRC32'); Do this above, in the "global" area
-- ======================================================================
-- Return the hash of "value", with modulo.
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- ======================================================================
local function old_stringHash( value, modulo )
  if value ~= nil and type(value) == "string" then
    return CRC32.Hash( value ) % modulo;
  else
    return 0;
  end
end -- stringHash

-- ======================================================================
-- Return the hash of "value", with modulo
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- NOTE: Use a better Hash Function.
-- ======================================================================
local function old_numberHash( value, modulo )
  local meth = "numberHash()";
  local result = 0;
  if value ~= nil and type(value) == "number" then
    -- math.randomseed( value ); return math.random( modulo );
    result = CRC32.Hash( value ) % modulo;
  end
  GP=E and trace("[EXIT]:<%s:%s>HashResult(%s)", MOD, meth, tostring(result))
  return result
end -- numberHash

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
-- Get (create) a unique bin name given the current counter.
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- ======================================================================
local function getBinName( number )
  local binPrefix = LSET_DATA_BIN_PREFIX;
  return binPrefix .. tostring( number );
end

-- ======================================================================
-- setupNewBin: Initialize a new bin -- (the thing that holds a list
-- of user values).  If this is the FIRST bin (zero), then it is really
-- the CompactList, although that looks like a regular list.
-- Parms:
-- (*) topRec
-- (*) Bin Number
-- Return: New Bin Name
-- ======================================================================
local function setupNewBin( topRec, binNum )
  local meth = "setupNewBin()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%d) ", MOD, meth, binNum );

  local binName = getBinName( binNum );
  -- create the first LSetBin_n LDT bin
  topRec[binName] = list(); -- Create a new list for this new bin

  -- This bin must now be considered HIDDEN:
  GP=E and trace("[DEBUG]: <%s:%s> Setting BinName(%s) as HIDDEN",
                 MOD, meth, binName );
  record.set_flags(topRec, binName, BF_LDT_HIDDEN ); -- special bin

  GP=E and trace("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)",
                 MOD, meth, binNum, binName );

  return binName;
end -- setupNewBin

-- ======================================================================
-- Produce a COMPARABLE value (our overloaded term here is "key") from
-- the user's value.
-- The value is either simple (atomic) or an object (complex).  Complex
-- objects either have a key function defined, or we produce a comparable
-- "keyValue" from "value" by performing a tostring() operation.
--
-- NOTE: According to Chris (yes, everybody hates Chris), the tostring()
-- method will ALWAYS create the same string for complex objects that
-- have the same value.  We've noticed that tostring() does not always
-- show maps with fields in the same order, but in theory two objects (maps)
-- with the same content will have the same tostring() value.
-- Parms:
-- (*) ldtMap: The basic LDT Control structure
-- (*) value: The value from which we extract a compare-able "keyValue".
-- Return a comparable value:
-- ==> The original value, if it is an atomic type
-- ==> A Unique Identifier subset (that is atomic)
-- ==> The entire object, in string form.
-- ======================================================================
local function getKeyValue( ldtMap, value )
  local meth = "getKeyValue()";
  GP=E and trace("[ENTER]<%s:%s> value(%s) KeyType(%s)",
    MOD, meth, tostring(value), tostring(ldtMap[LC.KeyType]) );

  if( value == nil ) then 
    GP=E and trace("[Early EXIT]<%s:%s> Value is nil", MOD, meth );
    return nil;
  end

  GP=E and trace("[DEBUG]<%s:%s> Value type(%s)", MOD, meth,
    tostring( type(value)));

  local keyValue;
  -- This test looks bad.  Let's just base our decision on the value
  -- that is in front of us.
  -- if( ldtMap[LC.KeyType] == KT_ATOMIC or type(value) ~= "userdata" ) then
  if( type(value) == "number" or type(value) == "string" ) then
    keyValue = value;
  else
    -- Now, we assume type is "userdata".
    keyValue = tostring( value );
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(keyValue) );
  return keyValue;
end -- getKeyValue();

-- ======================================================================
-- computeHashCell()
-- ======================================================================
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" StoreState.
-- In compact mode, we ALWAYS look in the single bin (TopRec mode).
-- Second -- use the right hash function (depending on the type).
-- NOTE that we should be passed in ONLY KEYS, not objects, so we don't
-- need to do  "Key Extract" here, regardless of whether we're doing
-- ATOMIC or COMPLEX Object values.  Also, KeyExtract basically turns objects
-- into strings.
-- ======================================================================
local function computeHashCell( searchKey, ldtMap )
  local meth = "computeHashCell()";
  GP=E and trace("[ENTER]: <%s:%s> val(%s) Map(%s) ",
                 MOD, meth, tostring(searchKey), tostring(ldtMap) );

  -- Check StoreState:  If we're in single bin mode, it's easy. Everything
  -- goes to Bin ZERO.
  -- Otherwise, Hash the KEY value, assuming it's either a number or a string.
  local cellNumber  = 0; -- Default, if COMPACT mode
  if ldtMap[LS.StoreState] == SS_REGULAR then
    if not searchKey then
      warn("[INTERNAL ERROR]<%s:%s> null search key", MOD, meth );
      error( ldte.ERR_INTERNAL );
    end
    -- There are really only TWO primitive types that we can handle,
    -- and that is NUMBER and STRING.  Anything else is just wrong!!
    -- Also, note that cell number goes from 1::Mod, not 0::Mod-1.
    local keyType = type(searchKey);
    local modulo  = ldtMap[LS.Modulo];
    if keyType == "number" or keyType == "string" then
      cellNumber = (CRC32.Hash( searchKey ) % modulo) + 1;
    else
      warn("[INTERNAL ERROR]<%s:%s>Hash(%s) requires type number or string!",
        MOD, meth, type(searchKey) );
      error( ldte.ERR_INTERNAL );
    end
  end

  GP=E and trace("[EXIT]: <%s:%s> searchKey(%s) Hash Cell(%d) ",
                 MOD, meth, tostring(searchKey), cellNumber );

  return cellNumber;
end -- computeHashCell()

-- =======================================================================
-- subRecSummary()
-- =======================================================================
-- Show the basic parts of the sub-record contents.  Make sure that this
-- isn't nil or equal to ZERO before extracing fields.
-- Return a STRING of the Summary Map that shows the value.
-- =======================================================================
local function subRecSummary( subrec )
  local resultMap = map();
  if( subrec == nil ) then
    resultMap.SubRecSummary = "NIL SUBREC";
  elseif( type(subrec) == "number" and subrec == 0 ) then
    resultMap.SubRecSummary = "NOT a SUBREC (ZERO)";
  else
    resultMap.SubRecSummary = "Regular SubRec";

    local propMap  = subrec[SUBREC_PROP_BIN];
    local ctrlMap  = subrec[REC_LDT_CTRL_BIN];
    local valueList  = subrec[LDR_LIST_BIN];

    -- General Properties (the Properties Bin)
    resultMap.SUMMARY           = "NODE Summary";
    resultMap.PropMagic         = propMap[PM.Magic];
    resultMap.PropCreateTime    = propMap[PM.CreateTime];
    resultMap.PropEsrDigest     = propMap[PM.EsrDigest];
    resultMap.PropRecordType    = propMap[PM.RecType];
    resultMap.PropParentDigest  = propMap[PM.ParentDigest];
    
    resultMap.ControlMap = ctrlMap;
    resultMap.ValueList = valueList;
  end
  
  return tostring( resultMap );
  
end -- subRecSummary()

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
end --cellAnchorEmpty()

-- ======================================================================
--  validateValue()
-- ======================================================================
-- In the calling function, we've landed on the name we were looking for.
-- ======================================================================
local function validateValue( storedValue )
  local meth = "validateValue()";
  GP=D and trace("[ENTER]<%s:%s> Value(%s)", MOD, meth, tostring(storedValue));

--  HOWEVER
--  This won't work correctly because we need to return the results of
--  G_Filter() even if it is nul. 
--  local resultFiltered = 
--    ( G_Filter and G_Filter( liveObject, G_FunctionArgs ) ) or liveObject;
--
--  return resultFiltered;
                 
  local liveObject = storedValue;
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
-- scanLSetList(): Scan the LSET value list.
-- =======================================================================
-- Scan the Value list, touching every item and applying the (global)
-- filter to each one (if applicable).  For every value that passes the
-- filter, add the value to the resultList.
--
-- (*) valueList: the list of VALUES in LSET
-- (*) resultList: The caller's ResultList (where we'll put the results)
-- Result:
-- OK: return 0: Fill in "resultList".
-- ERROR: LDT Error code to caller
-- =======================================================================
local function scanLSetList( valueList, resultList )
  local meth = "scanLSetList()";
  GP=E and trace("[ENTER]: <%s:%s> ScanList: Values(%s)[%d] resultList(%s)",
    MOD, meth, tostring(valueList), #valueList, tostring(resultList));

  if( resultList == nil ) then
    warn("[ERROR]<%s:%s> NULL RESULT LIST", MOD, meth );
    error(ldte.ERR_INTERNAL);
  end

  -- Nothing to search if the list is null or empty.  If nothing there,
  -- just return.
  if not valueList or #valueList == 0 then
    GP=F and trace("[NOTE]<%s:%s> EmptyValueList", MOD, meth );
    return 0;
  end

  -- Search the list.
  local listSize = list.size(valueList);
  local resultValue;
  for i = 1, listSize, 1 do
    resultValue = validateValue( valueList[i] );
    if resultValue  then
      list.append( resultList, resultValue);
    end
  end -- end for each item in the list

  GP=DEBUG and trace("[DEBUG]<%s:%s> ResultList(%s)", MOD, meth,
    tostring(resultList));

  GP=E and trace("[EXIT]<%s:%s> ResultList Size(%d)", MOD, meth,
    list.size( resultList ));
end -- scanLSetList()


-- =======================================================================
-- cellAnchorDump()
-- =======================================================================
-- Dump the contents of a hash cell.
-- The cellAnchor is either "EMPTY" or it has a valid Cell Anchor structure.
-- First, check type (string) and value (HC_EMPTY) to see if nothing is
-- here.  Notice that this means that we have to init the hashDir correctly
-- when we create it.
-- Return: A resultMap of the type and contents of the Hash Cell
-- =======================================================================
local function cellAnchorDump( src, topRec, cellAnchor, cellNumber )
  local meth = "cellAnchorDump()";
  GP=E and trace("[ENTER]<%s:%s> CellNum(%d) CellAnchor((%s)", MOD, meth,
    cellNumber, tostring(cellAnchor));

  local resultList = list();
  local resultMap = map();
  resultMap.CellNumber = cellNumber;

  if( cellAnchorEmpty( cellAnchor) ) then
    list.append(resultList, "EMPTY CELL ANCHOR" );
  else
    GP=D and trace("[DEBUG]<%s:%s>\nHash Cell :: Index(%d) Cell(%s)",
      MOD, meth, cellNumber, tostring(cellAnchor));

    -- If not empty, then the cell anchor must be either in 
    -- (*) A LIST STATE,
    -- (*) A DIGEST STATE 
    -- (*) A RADIX TREE STATE.
    if( cellAnchor[C_CellState] == C_STATE_LIST ) then
      -- The small list is inside of the cell anchor.  Get the lists.
      scanLSetList( cellAnchor[C_CellValueList], resultList );
      resultMap.CellType = "LIST";
      resultMap.Contents = resultList;
    elseif( cellAnchor[C_CellState] == C_STATE_DIGEST ) then
      -- We have a sub-rec -- open it
      resultMap.CellType = "DIGEST";
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
      scanLSetList( subRec[LDR_LIST_BIN], resultList );
      ldt_common.closeSubRec( src, subRec, false);
      resultMap.Contents = resultList;
    else
      resultMap.CellType = "RADIX TREE";
      -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      -- When we do a Radix Tree, we will STILL end up with a SubRecord
      -- but it will come from a Tree.  We just need to manage the SubRec
      -- correctly.
      warn("[ERROR]<%s:%s> Not yet ready to handle Radix Trees in Hash Cell",
        MOD, meth );
      error( ldte.ERR_INTERNAL );
      -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    end -- radix tree
  end -- non-empty cellAnchor

  GP=D and trace("[DEBUG]<%s:%s>ResultMap(%s)",MOD,meth,tostring(resultMap));
  GP=E and trace("[EXIT]<%s:%s>",MOD,meth);

  return resultMap;
end -- cellAnchorDump()

-- =======================================================================
-- searchList()
-- =======================================================================
-- Search a list for an item.  Each object (atomic or complex) is translated
-- into a "searchKey".  That can be a hash, a tostring or any other result
-- of a "uniqueIdentifier()" function.
--
-- (*) ldtMap: Main LDT Control Structure
-- (*) valueList: the list of values from the record
-- (*) searchKey: the "translated value"  we're searching for
-- Return the position if found, else return ZERO.
-- =======================================================================
local function searchList(ldtMap, valueList, searchKey )
  local meth = "searchList()";
  GP=E and trace("[ENTER]: <%s:%s> Looking for searchKey(%s) in List(%s)",
     MOD, meth, tostring(searchKey), tostring(valueList));
                 
  local position = 0; 

  -- Nothing to search if the list is null or empty
  if( valueList == nil or list.size( valueList ) == 0 ) then
    GP=F and trace("[DEBUG]<%s:%s> EmptyList", MOD, meth );
    return 0;
  end

  -- TODO: Change this to use VALIDATE VALUE

  -- Search the list for the item (searchKey) return the position if found.
  -- Note that searchKey may be the entire object, or it may be a subset.
  local listSize = list.size(valueList);
  local item;
  local dbKey;
  local modValue;
  for i = 1, listSize, 1 do
    item = valueList[i];
    GP=F and trace("[COMPARE]<%s:%s> index(%d) SV(%s) and ListVal(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(item));
    -- a value that does not exist, will have a nil valueList item
    -- so we'll skip this if-loop for it completely                  
    if item ~= nil then
      modValue = item;
      -- Get a "compare" version of the object.  This is either a summary
      -- piece, or just a "tostring()" of the entire object.
      dbKey = getKeyValue( ldtMap, modValue );
      GP=F and trace("[ACTUAL COMPARE]<%s:%s> index(%d) SV(%s) and dbKey(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(dbKey));
      if(dbKey ~= nil and type(searchKey) == type(dbKey) and searchKey == dbKey)
      then
        position = i;
        GP=F and trace("[FOUND!!]<%s:%s> index(%d) SV(%s) and dbKey(%s)",
                   MOD, meth, i, tostring(searchKey), tostring(dbKey));
        break;
      end
    end -- end if not null and not empty
  end -- end for each item in the list

  GP=E and trace("[EXIT]<%s:%s> Result: Position(%d)", MOD, meth, position );
  return position;
end -- searchList()

-- ======================================================================
-- lsetListInsert()
-- ======================================================================
-- Search the LSET List, and if not present, append to the list.
-- Parms: 
-- (*) ldtCtrl
-- (*) valueList
-- (*) searchKey: The potentially simple version of the value
-- (*) newValue
-- (*) check: 1==Search before insert, 0==no search, just insert
-- Return:
-- 0: all is well
-- Other: Error
-- NOTE: When we allow overwrite, then when we overwrite, we'll return 1
-- ======================================================================
local function lsetListInsert( ldtCtrl, valueList, searchKey, newValue, check)
  local meth = "lsetListInsert()";
  GP=E and trace("[ENTER]<%s:%s> Vlist(%s) Key(%s) Value(%s)",
    MOD, meth, tostring(valueList), tostring(searchKey), tostring(newValue));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc = 0;

  -- We're setting the value up here, even though there's a small chance that
  -- we have an illegal overwrite case, because otherwise we'd have to do
  -- this in two places.
  local storeValue = newValue;
  local position = 0;
  if check == 1 then
    position = searchList( ldtMap, valueList, searchKey );
  end

  -- If we find it, it's an error (later we might have the overwrite option)
  if( position > 0 ) then
    info("[UNIQUE VIOLATION]<%s:%s> Value(%s)", MOD, meth, tostring(newValue));
    error( ldte.ERR_UNIQUE_KEY );
  else
    -- Add the new name/value to the existing lists. (rc is still 0).
    list.append( valueList, storeValue );
  end
  GP=D and trace("[DEBUG] ValueList(%s)", tostring(valueList));
  GP=E and trace("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- function lsetListInsert()

-- =======================================================================
-- topRecScan()
-- =======================================================================
-- Scan a List, append all the items in the list to result if they pass
-- the filter.
-- Parms:
-- (*) topRec:
-- (*) resultList: List holding search result
-- (*) ldtCtrl: The main LDT control structure
-- Return: resultlist 
-- =======================================================================
local function topRecScan( topRec, ldtCtrl, resultList )
  local meth = "topRecScan()";
  GP=E and trace("[ENTER]: <%s:%s> Scan all TopRec elements", MOD, meth );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local listCount = 0;
  local liveObject = nil; 
  local resultFiltered = nil;

  -- Loop through all the modulo n lset-record bins 
  local distrib = ldtMap[LS.Modulo];
  GP=F and trace(" Number of LSet bins to parse: %d ", distrib)
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    GP=F and trace(" Parsing through :%s ", tostring(binName))
	if topRec[binName] ~= nil then
      local objList = topRec[binName];
      if( objList ~= nil ) then
        for i = 1, list.size( objList ), 1 do
          if objList[i] ~= nil then
            liveObject = objList[i]; 
            -- APPLY FILTER HERE, if we have one.  If no filter, then pass
            -- the value thru.
            if G_Filter ~= nil then
              resultFiltered = G_Filter( liveObject, G_FunctionArgs );
            else
              resultFiltered = liveObject;
            end
            list.append( resultList, resultFiltered );
          end -- end if not null and not empty
  		end -- end for each item in the list
      end -- if bin list not nil
    end -- end of topRec null check 
  end -- end for distrib list for-loop 

  GP=E and trace("[EXIT]: <%s:%s> Appending %d elements to ResultList ",
                 MOD, meth, list.size(resultList));

  -- Return list passed back via "resultList".
  return 0; 
end -- topRecScan


-- ======================================================================
-- subRecScan()
-- ======================================================================
-- Perform the "regular" scan -- a scan of the hash directory that will
-- mostly contain pointers to Sub-Records.
-- Parms:
-- (*) topRec:
-- (*) resultList: List holding search result
-- (*) ldtCtrl: The main LDT control structure
-- Return: resultlist 
-- ======================================================================
local function subRecScan( src, topRec, ldtCtrl, resultList )
  local meth = "subRecScan()";
  GP=E and trace("[ENTER]: <%s:%s> Scan all SubRec elements", MOD, meth );

  GP=F and trace("[DEBUG] <%s:%s> LDT Summary(%s)", MOD, meth,
    ldtSummaryString( ldtCtrl ));

  -- For each cell in the Hash Directory, extract that Cell and scan
  -- its contents.  The contents of a cell may be:
  -- (*) EMPTY
  -- (*) A Pair of Short Name/Value lists
  -- (*) A SubRec digest
  -- (*) A Radix Tree of multiple SubRecords
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local hashDirectory = ldtMap[LS.HashDirectory];
  local compactList;

  -- This LSET is either in COMPACT MODE or it's in HASH DIR mode.
  -- If Compact -- do that first and return.
  if ldtMap[LS.StoreState] == SS_COMPACT then
    compactList = ldtMap[LS.CompactList];
    scanLSetList( compactList, resultList );
  else
    -- Ok, it's a "Regular Scan".  Go thru the Hash Table, open up each
    -- cell Anchor and process it.
    local cellAnchor;
    local hashDirSize = list.size( hashDirectory );

    for i = 1, hashDirSize,  1 do
      cellAnchor = hashDirectory[i];
      if  not cellAnchorEmpty( cellAnchor ) then

        GP=D and trace("[DEBUG]<%s:%s>\nHash Cell :: Index(%d) Cell(%s)",
          MOD, meth, i, tostring(cellAnchor));

        -- If not empty, then the cell anchor must be either a LIST, or it
        -- is some sort of Sub-Record (either a Digest or a LIST of Digests).
        -- For now it can be only a single Digest, but later it might be
        -- a LIST (which is our representation of a Radix Tree).
        if( cellAnchor[C_CellState] == C_STATE_LIST ) then
          -- The small list is inside of the cell anchor.  Get the lists.
          scanLSetList( cellAnchor[C_CellValueList], resultList );
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
          scanLSetList( subRec[LDR_LIST_BIN], resultList );
          ldt_common.closeSubRec( src, subRec, false);
        else
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          -- When we do a Radix Tree, we will STILL end up with a SubRecord
          -- but it will come from a Tree.  We just need to manage the SubRec
          -- correctly.
          warn("[ERROR]<%s:%s> Not yet ready to handle Radix Trees in HashCell",
            MOD, meth );
          error( ldte.ERR_INTERNAL );
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        end
      end
    end -- for each Hash Dir Cell
  end -- else Regular Scan

  GP=D and trace("[RESULTS]<%s:%s> ResultList(%s)", 
    MOD, meth, tostring(resultList));
  GP=E and trace("[EXIT]: <%s:%s> Appended %d elements to ResultList ",
                 MOD, meth, #resultList );
  return 0; 
end -- subRecScan

-- ======================================================================
-- localTopRecInsert()
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtCtrl: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- RETURN:
--  0: ok
-- -1: Unique Value violation
-- ======================================================================
local function localTopRecInsert( topRec, ldtCtrl, newValue, stats )
  local meth = "localTopRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s>value(%s) stats(%s) ldtCtrl(%s)",
    MOD, meth, tostring(newValue), tostring(stats), ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[LDT_PROP_MAP];  
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];
  
  -- We'll get the KEY and use that to feed to the hash function, which will
  -- tell us what bin we're in.
  local searchKey = getKeyValue( ldtMap, newValue );
  local binNumber = computeHashCell( searchKey, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  
  -- We're doing "Lazy Insert", so if a bin is not there, then we have not
  -- had any values for that bin (yet).  Allocate the list now.
  if binList == nil then
    GP=F and trace("[DEBUG]:<%s:%s> Creating List for binName(%s)",
                 MOD, meth, tostring( binName ) );
    binList = list();
  else
    -- Look for the value, and insert if it is not there.
    local position = searchList( ldtMap, binList, searchKey );
    if( position > 0 ) then
      info("[ERROR]<%s:%s> Attempt to insert duplicate value(%s)",
        MOD, meth, tostring( newValue ));
      error(ldte.ERR_UNIQUE_KEY);
    end
  end
  local storeValue = newValue;
  list.append( binList, storeValue );

  topRec[binName] = binList; 
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

  -- Update stats if appropriate.
  if( stats == 1 ) then -- Update Stats if success
    local itemCount = propMap[PM.ItemCount];
    local totalCount = ldtMap[LS.TotalCount];
    
    propMap[PM.ItemCount] = itemCount + 1; -- number of valid items goes up
    ldtMap[LS.TotalCount] = totalCount + 1; -- Total number of items goes up
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec,ldtBinName,BF_LDT_BIN);--Must set every time

    GP=F and trace("[STATUS]<%s:%s>Updating Stats TC(%d) IC(%d) Val(%s)",
      MOD, meth, ldtMap[LS.TotalCount], propMap[PM.ItemCount], 
        tostring( newValue ));
  else
    GP=F and trace("[STATUS]<%s:%s>NOT updating stats(%d)",MOD,meth,stats);
  end

  GP=E and trace("[EXIT]<%s:%s>Insert Results: RC(0) Value(%s) binList(%s)",
    MOD, meth, tostring( newValue ), tostring(binList));

  return 0;
end -- localTopRecInsert()

-- ======================================================================
-- topRecRehashSet()
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (ldtMap[LS.Threshold]), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) ldtCtrl
-- ======================================================================
local function topRecRehashSet( topRec, ldtCtrl )
  local meth = "topRecRehashSet()";
  GP=E and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[LDT_PROP_MAP];  
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  local singleBinName = getBinName( 0 );
  local singleBinList = topRec[singleBinName];
  if singleBinList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         MOD, meth, tostring(singleBinName));
    error( ldte.ERR_INSERT );
  end
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  ldtMap[LS.StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = ldtMap[LS.Modulo];
  for i = 0, (distrib - 1), 1 do
    -- assign a new list to topRec[binName]
    setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localTopRecInsert(topRec,ldtCtrl,listCopy[i],0); -- do NOT update counts.
  end

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- topRecRehashSet()

-- ======================================================================
-- subRecSearch()
-- ======================================================================
-- Search the contents of the Sub-Record Oriented Hash Directory. If the
-- object that corresponds to "searchKey" is found, return it.  Otherwise,
-- return nil.
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec: Top Record (main aerospike record)
-- (*) ldtCtrl: Main LDT Control Structure.
-- (*) searchKey: The value (or subvalue) that we're searching for.
-- Return: 
-- ==> Found: Return Object
-- ==> Not Found or other Error: Return Nil
-- Marker:  6/30/2014
-- ======================================================================
local function subRecSearch( src, topRec, ldtCtrl, searchKey )
  local meth = "subRecSearch()";
  
  GP=E and trace("[ENTER]<%s:%s> SearchVal(%s)", MOD,meth,tostring(searchKey));
    
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local position = 0;

  local resultObject;
  local valueList;
  -- Process these two options differently.  Either we're in COMPACT MODE,
  -- which means have a simple value list connected to the LDT BIN, or we're
  -- in REGULAR_MODE, which means we're going to open up a SubRecord and
  -- read the value list in there.
  if ldtMap[LS.StoreState] == SS_COMPACT then
    valueList = ldtMap[LS.CompactList];
    position = searchList( ldtMap, valueList, searchKey );
    if( position > 0 ) then
      resultObject = validateValue( valueList[position] );
    end
  else
    -- Regular "Sub-Record" Mode.  Search the Hash Table.
    position = 0;
    local subRec = 0;
    local cellNumber = computeHashCell( searchKey, ldtMap );
    local hashDirectory = ldtMap[LS.HashDirectory];
    local cellAnchor = hashDirectory[cellNumber];

    if ( cellAnchorEmpty( cellAnchor ) ) then
      return nil;
    end

    -- If not empty, then the cell anchor must be either in a a LIST 
    -- state, or it has a Sub-Record.  Later, it might have a Radix tree
    -- of multiple Sub-Records.
    if( cellAnchor[C_CellState] == C_STATE_LIST ) then
      -- Get the list and search it.
      valueList = cellAnchor[C_CellValueList];
      if( valueList ~= nil ) then
        -- Search the list.  Return position if found.
        position = searchList( ldtMap, valueList, searchKey );
        if( position > 0 ) then
          resultObject = validateValue( valueList[position] );
        end
      end
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
        warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",
          MOD, meth, digestString );
        error( ldte.ERR_SUBREC_OPEN );
      end
      valueList = subRec[LDR_LIST_BIN];
      position = searchList( ldtMap, valueList, searchKey );
      if( position > 0 ) then
        resultObject = validateValue( valueList[position] );
      end
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
  end -- Regular Sub-Rec Mode

  GP=E and trace("[EXIT]:<%s:%s> resultObject(%s)", MOD, meth,
    tostring(resultObject));

  return resultObject;
end -- subRecSearch()

-- ======================================================================
-- SaveSubRecSearch()
-- ======================================================================
-- Search the contents of the Sub-Record Oriented Hash Directory. If the
-- object that corresponds to "searchKey" is found, return it.  Otherwise,
-- return nil.
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec: Top Record (main aerospike record)
-- (*) ldtCtrl: Main LDT Control Structure.
-- (*) searchKey: The value (or subvalue) that we're searching for.
-- Return: 
-- Successful operation:
-- ==> Found in Subrec:  {position, subRecPtr}
-- ==> Found in ValueList:  {position, 0}
-- ==> NOT Found:  {0, subRecPtr}
-- Extreme Error -- longjump out
-- ======================================================================
local function SaveSubRecSearch( src, topRec, ldtCtrl, searchKey )
  local meth = "SaveSubRecSearch()";
  
  GP=E and trace("[ENTER]<%s:%s> SearchVal(%s)", MOD,meth,tostring(searchKey));
    
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local valueList;
  local position = 0;
  local subRec = 0;

  local cellNumber = computeHashCell( searchKey, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  if ( cellAnchorEmpty( cellAnchor ) ) then
    return 0, 0;
  end

  -- If not empty, then the cell anchor must be either in a a LIST 
  -- state, or it has a Sub-Record.  Later, it might have a Radix tree
  -- of multiple Sub-Records.
  if( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- Get the list and search it.
    valueList = cellAnchor[C_CellValueList];
    if( valueList ~= nil ) then
      -- Search the list.  Return position if found.
      position = searchList( ldtMap, valueList, searchKey );
    end
    return position, 0;
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
      warn("[ERROR]: <%s:%s>: subRec nil or empty: Digest(%s)",
        MOD, meth, digestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
    valueList = subRec[LDR_LIST_BIN];
    position = searchList( ldtMap, valueList, searchKey );
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

  GP=E and trace("[EXIT]:<%s:%s> Position(%d) subRec(%s)", MOD, meth,
    position, tostring(subRec));

  return position, subRec;
end -- SaveSubRecSearch()

-- ======================================================================
-- topRecSearch
-- ======================================================================
-- In Top Record Mode,
-- Find an element (i.e. search), and optionally apply a filter.
-- Return the element if found, return an error (NOT FOUND) otherwise
-- Parms:
-- (*) topRec: Top Record -- needed to access numbered bins
-- (*) ldtCtrl: the main LDT Control structure
-- (*) searchKey: This is the value we're looking for.
-- NOTE: We've now switched to using a different mode for the filter
-- We set that up once on entry into the search function (using the 
-- new lookup mechanism involving user modules).
-- Return: The Found Object, or Error (not found)
-- ======================================================================
local function topRecSearch( topRec, ldtCtrl, searchKey )
  local meth = "topRecSearch()";
  GP=E and trace("[ENTER]: <%s:%s> Search Key(%s)",
                 MOD, meth, tostring( searchKey ) );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Find the appropriate bin for the Search value
  local binNumber = computeHashCell( searchKey, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local liveObject = nil;
  local resultFiltered = nil;
  local position = 0;

  -- We bother to search only if there's a real list.
  if binList ~= nil and list.size( binList ) > 0 then
    position = searchList( ldtMap, binList, searchKey );
    if( position > 0 ) then
      -- Apply the filter to see if this item qualifies
      local liveObject = binList[position];

      -- APPLY FILTER HERE, if we have one.
      if G_Filter ~= nil then
        resultFiltered = G_Filter( liveObject, G_FunctionArgs );
      else
        resultFiltered = liveObject;
      end
    end -- if search found something (pos > 0)
  end -- if there's a list

  if( resultFiltered == nil ) then
    debug("[WARNING]<%s:%s> Value not found: Value(%s)",
      MOD, meth, tostring( searchKey ) );
    error( ldte.ERR_NOT_FOUND );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Success: SearchKey(%s) Result(%s)",
     MOD, meth, tostring(searchKey), tostring( resultFiltered ));
  return resultFiltered;
end -- function topRecSearch()

-- ======================================================================
-- newCellAnchor()
-- ======================================================================
-- Perform an insert into a NEW Cell Anchor.
-- A Cell Anchor starts in the following state:
-- (*) A local List (something small)
-- Parms:
-- (*) newValue: Value to be inserted
-- RETURN:
--  New cellAnchor.
-- ======================================================================
local function newCellAnchor( newValue )
  local meth = "newCellAnchor()";
  GP=E and trace("[ENTER]:<%s:%s> Value(%s) ", MOD, meth, tostring(newValue));
  
  local cellAnchor = map();
  if newValue then
    cellAnchor[C_CellState] = C_STATE_LIST;
    local newList = list();
    list.append( newList, newValue );
    cellAnchor[C_CellValueList] = newList;
  else
    cellAnchor[C_CellState] = C_STATE_EMPTY;
  end
  -- Recall that we don't start using C_CellItemCount until we have
  -- at least one Sub-Record in this Hash Cell.
  
  GP=E and trace("[EXIT]:<%s:%s> NewCell(%s) ",MOD,meth,tostring(cellAnchor));
  return cellAnchor;
end -- newCellAnchor()

-- ======================================================================
-- createLSetSubRec() Create a new LSET Sub-Rec and initialize it.
-- ======================================================================
-- Create and initialize the Sub-Rec for Hash.
-- All LDT sub-records have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
--    >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
-- local SUBREC_PROP_BIN   = "SR_PROP_BIN";
--
-- The LDT Data Records (LDRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit)    12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
-- local LDR_CTRL_BIN      = "LdrControlBin";  
-- local LDR_LIST_BIN      = "LdrListBin";  
-- local LDR_BNRY_BIN      = "LdrBinaryBin";
-- ======================================================================
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtCtrl: Main LDT Control Structure
-- Contents of a Node Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) LDR_CTRL_BIN:    Main Node Control structure
-- (3) LDR_LIST_BIN:    Value List goes here
-- (4) LDR_BNRY_BIN:    Packed Binary Array (if used) goes here
-- ======================================================================
local function createLSetSubRec( src, topRec, ldtCtrl )
  local meth = "createLSetSubRec()";
  GP=E and trace("[ENTER]<%s:%s> ", MOD, meth );

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Create the Aerospike Sub-Record, initialize the Bins (Ctrl, List).
  -- The ldt_common.createSubRec() handles the record type and the SRC.
  -- It also kicks out with an error if something goes wrong.
  local subRec = ldt_common.createSubRec( src, topRec, ldtCtrl, RT_SUB );
  local ldrPropMap = subRec[SUBREC_PROP_BIN];
  local ldrCtrlMap = map();

  -- Store the new maps in the record.
  -- subRec[SUBREC_PROP_BIN] = ldrPropMap;
  subRec[LDR_CTRL_BIN]    = ldrCtrlMap;
  subRec[LDR_LIST_BIN] = list(); -- Holds the Items
  -- subRec[LDR_BNRY_BIN] = nil; -- not used (yet)

  -- Note, If we had BINARY MODE working for inner nodes, we would initialize
  -- the Key BYTE ARRAY here.

  GP=E and trace("[EXIT]<%s:%s> SubRec Summary(%s)",
    MOD, meth, subRecSummary(subRec));
  return subRec;
end -- createLSetSubRec()

-- ======================================================================
-- hashCellConvert()
-- ======================================================================
-- Convert the hash cell LIST into a single sub-record, change the cell
-- anchor state (to DIGEST) and then move the list data into the Sub-Rec.
-- Note that this is the only place that creates Sub-Records.
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

  -- Create a LSET new Sub-Rec, store the digest and store the list data. 
  -- Note that we don't need to check values or counts, because we already
  -- know that we're good -- Uniqueness has already been checked.
  -- Also, We are assuming that no single value is
  -- so ungodly large that we can get in trouble with moving a small list
  -- into a Sub-Rec.  If that DOES get us into trouble, then we have to
  -- figure out better INTERNAL support for checking sizes of Lua objects.
  local subRec = createLSetSubRec( src, topRec, ldtCtrl );
  local subRecDigest = record.digest( subRec );

  if not subRec then
    warn("[ERROR]<%s:%s>: SubRec Create Error",  MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  else
    GP=F and trace("[NOTICE]<%s:%s>: SubRec Create SUCCESS(%s) Dig(%s)",
        MOD, meth, subRecSummary(subRec), tostring(subRecDigest));
  end

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  local valueList = cellAnchor[C_CellValueList];

  -- Just convert -- no INSERT here.
  subRec[LDR_LIST_BIN] = valueList;

  -- Set the state the hash cell to "DIGEST" and then NULL out the list
  -- values (which are now in the sub-rec).
  cellAnchor[C_CellState] = C_STATE_DIGEST;
  cellAnchor[C_CellDigest] = subRecDigest;

  -- Remove the map entry now that we're done with it.
  map.remove(cellAnchor, C_CellValueList);

  ldt_common.updateSubRec( src, subRec );

  GP=F and trace("[DEBUG]<%s:%s> Cell(%s) LDR Summary(%s)", MOD, meth,
    tostring(cellAnchor), subRecSummary( subRec ));

  GP=E and trace("[EXIT]<%s:%s> Conversion Successful:", MOD, meth );
end -- function hashCellConvert()

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
-- (*) searchKey: The potentially primitive version of the value
-- (*) newValue
-- Return:
-- 0: if all goes well
-- 1: if we inserted, but overwrote, so do NOT update the counts.
-- other: Error Code
-- ======================================================================
local function
hashCellSubRecInsert(src, topRec, ldtCtrl, cellAnchor, searchKey, newValue)
  local meth = "hashCellSubRecInsert()";
  GP=E and trace("[ENTER]<%s:%s> CellAnchor(%s) searchKey(%s) newValue(%s)",
    MOD, meth, tostring(cellAnchor), tostring(searchKey), tostring(newValue));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc;

  -- LSET Version 1:  Just a pure Sub-Rec insert, no trees just yet.
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

  local valueList = subRec[LDR_LIST_BIN];
  if( not valueList ) then
    warn("[ERROR]<%s:%s> Empty List: ValueList(%s)", MOD, meth,
      tostring(valueList));
    error( ldte.ERR_INTERNAL );
  end

  rc = lsetListInsert( ldtCtrl, valueList, searchKey, newValue, 1);

  -- Now, we have to re-assign the lists back into the bin values.
  subRec[LDR_LIST_BIN] = valueList;
  ldt_common.updateSubRec( src, subRec );

  GP=F and trace("[DEBUG]<%s:%s> Post LDR Insert Summary(%s)",
    MOD, meth, subRecSummary( subRec ));

  GP=E and trace("[EXIT]<%s:%s> SubRecInsert Successful: rc(%d)",MOD,meth,rc);
  return rc;
end -- function hashCellSubRecInsert()

-- ======================================================================
-- subRecCompactInsert()
-- ======================================================================
-- Insert into the Compact List (for Sub-Rec Mode).
-- ======================================================================
local function subRecCompactInsert( ldtMap, newValue )
  local meth = "subRecCompactInsert()";
  GP=E and trace("[ENTER]:<%s:%s>newValue(%s)", MOD, meth, tostring(newValue));

  -- In the event that we have a search key (a subset of the entire value
  -- that we're going to search on), we extract that here. If not then "key"
  -- is just the same as "newValue".
  local searchKey = getKeyValue( ldtMap, newValue );

  -- A CompactList insert means we just add to the list that is held in
  -- the ldtMap.  A check has already been made that we're not over the
  -- threshold (COUNT >= ldtMap[LS.Threshold]), so we're safe to insert,
  -- provided that we don't find the item in the initial search.
  local compactList = ldtMap[LS.CompactList];
  if( compactList == nil ) then -- Not a likely case, but who knows?
      compactList = list();
    list.append( compactList, newValue );
    ldtMap[LS.CompactList] = compactList;
    GP=F and trace("[DEBUG]<%s:%s>NEW CompactList Insert: Obj(%s) List(%s)",
      MOD, meth, tostring(newValue), tostring(compactList));
  else
    -- NOTE: In the future we may provide an OVERWRITE option that alows the
    -- new item to replace the existing one.
    local position = searchList( ldtMap, compactList, searchKey );
    if( position > 0 ) then
      warn("[UNIQUE ERROR]<%s:%s> Key(%s) value(%s) found in list(%s)", MOD,
      meth, tostring(searchKey), tostring(newValue), tostring(compactList));
      error( ldte.ERR_UNIQUE_KEY );
    end
    list.append( compactList, newValue );
    GP=F and trace("[DEBUG]<%s:%s>CmpctList Insert: Pos(%d) Obj(%s) List(%s)",
      MOD, meth, position, tostring(newValue), tostring(compactList));
  end
end --subRecCompactInsert()

-- ======================================================================
-- hashDirInsert() (Sub-Rec Mode)
-- ======================================================================
-- Perform the main work of insert.  We locate the "cellAnchor" in the
-- HashDirectory, then either create or insert into the appropriate subrec.
--
-- Note that and COMPACT LIST business has already been handled by the caller,
-- so here we are ONLY dealing with Sub-Recs.
-- Parms:
-- (*) src: Sub Rec Context
-- (*) topRec: The top DB Record:
-- (*) ldtCtrl: The LSet control map
-- (*) newValue: Value to be inserted
-- (*) check: 1==Search, 0==No Search, just insert
-- RETURN:
--  0: ok
-- -1: Unique Value violation
-- ======================================================================
local function hashDirInsert( src, topRec, ldtCtrl, newValue, check)
  local meth = "hashDirInsert()";
  GP=E and trace("[ENTER]:<%s:%s>Insert(%s) check(%d)",
    MOD, meth, tostring(newValue), check);

  local propMap = ldtCtrl[LDT_PROP_MAP];  
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];
  local rc = 0;

  -- Sub-Rec Mode: Regular Hash Directory Insert.
  -- We'll get the searchKey and use that to feed to the hash function, which
  -- will tell us what bin we're in.
  local searchKey = getKeyValue( ldtMap, newValue );
    -- Remember that our Hash Dir goes from 1..N, rather than 0..(N-1)
  local cellNumber = computeHashCell( searchKey, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];

  -- --------------------------------------------------------------------
  -- We have three main cases:
  -- (1) Empty Hash Cell.  We allocate a list and append it. Done.
  -- (2) List Hash Cell.
  --     - Search List, if not found, append.
  --     - Check for overflow:
  --       - If Overflow, convert to Sub-Record
  -- (3) It's a sub-rec cell (or a tree cell).
  --     - Locate appropriate Sub-Record.
  --     - Insert into Sub-Record.
  -- --------------------------------------------------------------------
  if ( cellAnchorEmpty( cellAnchor ) ) then
    -- Case (1): Easy :: New Hash Cell and Insert
    cellAnchor = newCellAnchor( newValue );
    hashDirectory[cellNumber] = cellAnchor;

    -- If we don't allow hash Cell Lists, then convert this cell anchor
    -- to use a SUB-RECORD.
    if ( ldtMap[LS.HashCellMaxList] == 0) then
      hashCellConvert( src, topRec, ldtCtrl, cellAnchor )
    end
    
    GP=D and trace("[DEBUG]<%s:%s>  New Cell Anchor(%s)", MOD, meth,
      tostring( cellAnchor ));

  elseif ( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- Case (2): List Insert, Check for Overflow and Sub-Rec conversion
    local valueList = cellAnchor[C_CellValueList];
    GP=D and trace("[DEBUG]<%s:%s> Lists BEFORE: VL(%s)", MOD, meth,
      tostring(valueList));

    lsetListInsert( ldtCtrl, valueList, searchKey, newValue, check );

    GP=D and trace("[DEBUG]<%s:%s> Lists AFTER: VL(%s)", MOD, meth,
      tostring(valueList));

    local listSize = list.size( valueList );
    if ( listSize > ldtMap[LS.HashCellMaxList] ) then
      hashCellConvert( src, topRec, ldtCtrl, cellAnchor );
    end

    GP=D and trace("[DEBUG]<%s:%s>Anchor AFTER Possible Convert: Anchor(%s)",
      MOD, meth, tostring(cellAnchor));

  else
    GP=D and trace("[DEBUG]<%s:%s>Starting Regular SubRec Insert", MOD, meth);
    -- Harder.  Convert List into Subrec and insert.
    -- It's a sub-record insert, with a possible tree overflow
    rc = hashCellSubRecInsert( src, topRec, ldtCtrl, cellAnchor,
                               searchKey, newValue );
  end

  -- All done -- Save our work.
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- NOTE: Caller will update stats (e.g. ItemCount).
  GP=E and trace("[EXIT]<%s:%s>Insert Results: RC(%d) Value(%s) ",
    MOD, meth, rc, tostring( newValue ));

  return rc;
end -- hashDirInsert()

-- ======================================================================
-- initializeLSetRegular()
-- ======================================================================
-- Set up the ldtCtrl map for REGULAR use (sub-records).
-- ======================================================================
local function initializeLSetRegular( ldtMap )
  local meth = "initializeLSetRegular()";
  
  GP=E and trace("[ENTER]: <%s:%s>:: Regular Mode", MOD, meth );
  
  ldtMap[LS.StoreState]  = SS_REGULAR; -- Now Regular, was compact.
  	  
  -- Setup and Allocate everything for the Hash Directory.
  local hashDirSize = ldtMap[LS.Modulo];
  local newDirList = list.new(hashDirSize); -- Our new Hash Directory

  -- NOTE: Rather than create an EMPTY cellAnchor (which is a map), we now
  -- also allow a simple C_STATE_EMPTY value to show that the hash cell
  -- is empty.  As soon as we insert something into it, the cellAnchor will
  -- become a map with a state and a value.
  for i = 1, hashDirSize do
    newDirList[i] = C_STATE_EMPTY;
  end

  ldtMap[LS.HashDirectory]        = newDirList;
  
  GP=F and trace("[DEBUG]<%s:%s> LSET Summary after Init(%s)",
       MOD, meth, ldtMapSummaryString(ldtMap));

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  
end -- function initializeLSetRegular

-- ======================================================================
-- subRecConvert()
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (LS.Threshold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from the compact list, null out the
-- compact list and then reinsert them into the regular hash directory.
-- Parms:
-- (*) src: Sub-Rec Context
-- (*) topRec: The Database Record
-- (*) ldtCtrl: The LDT Bin Control Structure
-- ======================================================================
local function subRecConvert( src, topRec, ldtCtrl )
  local meth = "subRecConvert()";
  GP=E and trace("[ENTER]:<%s:%s>\n\n!!!! SUBREC REHASH !!!! ", MOD, meth );

  local propMap = ldtCtrl[LDT_PROP_MAP];  
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- Get a copy of the compact List.  If this doesn't work as expected, then
  -- we will have to get a real "list.take()" copy.
  local compactList = ldtMap[LS.CompactList];
  -- local listCopy = list.take( compactList, #compactList );
  if compactList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty list", MOD, meth );
    error( ldte.ERR_INSERT );
  end

  -- Create and initialize the control-map parameters needed for the switch to 
  -- SS_REGULAR mode : add digest-list parameters 
  initializeLSetRegular( ldtMap );
  
  -- Insert each element of the compact list into the Hash dir, but we leave
  -- "checking" turned off because there's no need to search.  We just append.
  for i = 1, #compactList do
    hashDirInsert( src, topRec, ldtCtrl, compactList[i], 0)
  end
  
  -- We no longer need the Compact Lists.
  map.remove(ldtMap, LS.CompactList);

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- subRecConvert()

-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( binName )
  local meth = "validateBinName()";
  GP=E and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
  MOD, meth, tostring(binName));

  if binName == nil  then
    warn("[ERROR EXIT]:<%s:%s> Null Bin Name", MOD, meth );
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( binName ) ~= "string"  then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Not a String", MOD, meth );
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif string.len( binName ) > 14 then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Too Long", MOD, meth );
    error( ldte.ERR_BIN_NAME_TOO_LONG );
  end
  GP=E and trace("[EXIT]:<%s:%s> Ok", MOD, meth );
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
-- recordOverHead:: Amount of "other" space used in this record.
-- ========================================================================
local function compute_settings(ldtMap, configMap )
  local meth="compute_settings()"
  -- Perform some validation of the user's Config Parameters
  -- Notice that this is done only once at the initial create.
  local rc = ldt_common.validateConfigParms(ldtMap, configMap);
  if rc ~= 0 then
    warn("[ERROR]<%s:%s> Unable to Set Configuration due to errors",
      MOD, meth);
    return -1;
  end

  -- Now that all of the values have been validated, we can use them
  -- safely without worry.  No more checking needed.
  local maxObjectSize   = configMap.MaxObjectSize;
  local maxKeySize      = configMap.MaxKeySize;
  local maxObjectCount  = configMap.MaxObjectCount;
  local pageSize        = configMap.TargetPageSize;
  local writeBlockSize  = configMap.WriteBlockSize;
  local recordOverHead  = configMap.RecordOverHead;

  -- These are the values that we have to set.
  local storeState;          -- Start Compact or Regular
  local hashType = HT_STATIC;-- Static or Dynamic 
  local hashDirSize;         -- Dependent on LDT Capacity and Obj Count
  local threshold;           -- Convert to HashTable
  local cellListThreshold;   -- Convert List to SubRec.
  local ldrListMax;          -- # of elements to store in a data sub-rec

  -- Set up our various OVER HEAD values
  -- We know that LMAP/LSET Ctrl occupies up to 400 bytes;
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

  -- Apply our computed values to the LDT Map.
  ldtMap[LS.StoreState]       = storeState;
  ldtMap[LS.HashType]         = hashType;
  ldtMap[LS.Modulo]           = hashDirSize;
  ldtMap[LS.Threshold]        = threshold;
  ldtMap[LS.BinListThreshold] = cellListThreshold;
  ldtMap[LS.LdrEntryCountMax] = ldrListMax;
  return 0;
end 

-- ======================================================================
-- setupLdtBin()
-- ======================================================================
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- Return:
--   The newly created ldtCtrl Map
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, firstValue, createSpec ) 
  local meth = "setupLdtBin()";
  GP=E and trace("[ENTER]<%s:%s> binName(%s)",MOD,meth,tostring(ldtBinName));

  local ldtCtrl = initializeLdtCtrl( topRec, ldtBinName );
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 
  
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
    local key  = getKeyValue(ldtMap, firstValue);
    configMap.MaxObjectSize = ldt_common.getValSize(firstValue);
    configMap.MaxObjectCount = 100000;
    configMap.MaxKeySize = ldt_common.getValSize(key);
  end
  compute_settings(ldtMap, configMap);

  -- Set up our Bin according to the initial state.  The default init
  -- has already setup for Compact List, but if we're starting in regular
  -- mode, we have to init the Hash Table (when in SubRec mode).
  -- initializeLdtCtrl always sets ldtMap[LS.StoreState] to SS_COMPACT.
  -- When in TopRec mode, there is only one bin.
  -- When in SubRec mode, there's only one hash cell.
  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Setup the compact list in sub-rec mode.  This will eventually 
    -- rehash into a full size hash directory.
    if ldtMap[LS.StoreState] == SS_REGULAR then
      initializeLSetRegular( ldtMap );
    end
  else
    -- Setup the compact list in topRec mode
    -- This one will assign the actual record-list to topRec[binName]
    setupNewBin( topRec, 0 );
  end

  
  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
                 MOD, meth , tostring(ldtMap));

  -- Sets the topRec control bin attribute to point to the two item list
  -- we created from InitializeLSetMap() : 
  -- Item 1 : the property map 
  -- Item 2 : the ldtMap
  topRec[ldtBinName] = ldtCtrl; -- store in the record
  record.set_flags( topRec, ldtBinName, BF_LDT_BIN );

  -- NOTE: The Caller will write out the LDT bin.
  return ldtCtrl;
end -- setupLdtBin()

-- ======================================================================
-- topRecInsert()
-- ======================================================================
-- Insert a value into the set.
-- Take the value, perform a hash and a modulo function to determine which
-- bin list is used, then add to the list.
--
-- We will use predetermined BIN names for this initial prototype
-- 'LSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of data
-- Notice that this means that THERE CAN BE ONLY ONE AS Set object per record.
-- In the final version, this will change -- there will be multiple 
-- AS Set bins per record.  We will switch to a modified bin naming scheme.
--
-- NOTE: Design, V2.  We will cache all data in the FIRST BIN until we
-- reach a certain number N (e.g. 100), and then at N+1 we will create
-- all of the remaining bins in the record and redistribute the numbers, 
-- then insert the 101th value.  That way we save the initial storage
-- cost of small, inactive or dead users.
-- ==> The CtrlMap will show which state we are in:
-- (*) StoreState=SS_COMPACT: We are in SINGLE BIN state (no hash)
-- (*) StoreState=SS_REGULAR: We hash, mod N, then insert (append) into THAT bin.
--
-- +========================================================================+=~
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... | ~
-- +========================================================================+=~
--    ~=+===========================================+
--    ~ | Set Bin 1 | Set Bin 2 | o o o | Set Bin N |
--    ~=+===========================================+
--            V           V                   V
--        +=======+   +=======+           +=======+
--        |V List |   |V List |           |V List |
--        +=======+   +=======+           +=======+
--
-- Parms:
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtCtrl: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- ======================================================================
local function topRecInsert( topRec, ldtCtrl, newValue )
  local meth = "topRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s) NewValue(%s)",
                 MOD, meth, tostring(ldtCtrl), tostring( newValue ) );

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local totalCount = ldtMap[LS.TotalCount];
  local itemCount = propMap[PM.ItemCount];
  
  GP=F and trace("[DEBUG]<%s:%s>Store State(%s) Total Count(%d) ItemCount(%d)",
    MOD, meth, tostring(ldtMap[LS.StoreState]), totalCount, itemCount );

  if ldtMap[LS.StoreState] == SS_COMPACT and
    totalCount >= ldtMap[LS.Threshold]
  then
    GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH BEFORE INSERT", MOD, meth);
    topRecRehashSet( topRec, ldtCtrl );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  -- localTopRecInsert() will jump out with its own error call if something bad
  -- happens so no return code (or checking) needed here.
  localTopRecInsert( topRec, ldtCtrl, newValue, 1 );

  -- NOTE: the update of the TOP RECORD has already
  -- been taken care of in localTopRecInsert, so we don't need to do it here.
  --
  -- All done, store the record
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );
  if rc and  rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function topRecInsert()

-- ======================================================================
-- subRecInsert()
-- ======================================================================
-- Insert a value into the set, using the SubRec design.
-- Take the value, perform a hash and a modulo function to determine which
-- directory cell is used, open the appropriate subRec, then add to the list.
-- This is the main (outer) function for performing Inserts (adds) into the
-- Sub-Rec oriented LSET code.
--
-- Since this is the top level function, here is where we decide on whether
-- we'll do:
-- (*) a COMPACT LIST insert
-- (*) a Conversion to Sub-Rec insert
-- (*) a Regular Sub-Rec insert
--
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) ldtCtrl: The name of the bin for the AS Large Set
-- (*) newValue: Value to be inserted into the Large Set
-- ======================================================================
local function subRecInsert( src, topRec, ldtCtrl, newValue )
  local meth = "subRecInsert()";
  
  GP=E and trace("[ENTER]:<%s:%s> LDT CTRL(%s) NewValue(%s)",
                 MOD, meth, ldtSummaryString(ldtCtrl), tostring( newValue ));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash the compact list into the full directory structure.
  local totalCount = ldtMap[LS.TotalCount];
  local itemCount = propMap[PM.ItemCount];
  
  GP=F and trace("[DEBUG]<%s:%s>Store State(%s) Total Count(%d) ItemCount(%d)",
    MOD, meth, tostring(ldtMap[LS.StoreState]), totalCount, itemCount );

  if ldtMap[LS.StoreState] == SS_COMPACT then
    -- Do the insert into the CompactList, THEN see if we should rehash
    -- (convert) the list.
    subRecCompactInsert( ldtMap, newValue );
    if ( totalCount + 1  >= ldtMap[LS.Threshold] ) then
      GP=F and trace("[DEBUG]<%s:%s> CALLING REHASH AFTER INSERT", MOD, meth);
      subRecConvert( src, topRec, ldtCtrl );
    end
  else
    -- Call our local multi-purpose insert() to do the job.
    -- hashDirInsert() will jump out with its own error call if something
    -- bad happens so no return code (or checking) needed here.
    hashDirInsert( src, topRec, ldtCtrl, newValue, 1);
  end

  -- Update stats
  local itemCount = propMap[PM.ItemCount];
  local totalCount = ldtMap[LS.TotalCount];
    
  propMap[PM.ItemCount] = itemCount + 1; -- number of valid items goes up
  ldtMap[LS.TotalCount] = totalCount + 1; -- Total number of items goes up
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec,ldtBinName,BF_LDT_BIN);--Must set every time

  GP=F and trace("[STATUS]<%s:%s>Updating Stats TC(%d) IC(%d)", MOD, meth,
    ldtMap[LS.TotalCount], propMap[PM.ItemCount] );

  -- Store it again here -- for now.  Remove later, when we're sure.  
  topRec[ldtBinName] = ldtCtrl;
  -- Also -- in Lua -- all data (like the maps and lists) are inked by
  -- reference -- so they do not need to be "re-updated".  However, the
  -- record itself, must have the object re-assigned to the BIN.
  -- Also -- must ALWAYS reset the bin flag, every time.
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  
  -- All done, store the record
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );
  if rc and  rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return 0;
end -- function subRecInsert()

-- ======================================================================
-- compactDelete()
-- ======================================================================
-- Delete an item from the compact list.
-- For the compact list, it's a simple list delete (if we find it).
-- (*) ldtMap: The LDT-Specific control structure
-- (*) searchKey: the Searchable portion of the Object 
-- (*) resultList: the list carrying the result back to the caller.
-- ======================================================================
local function compactDelete( ldtMap, searchKey, resultList )
  local meth = "compactDelete()";

  GP=E and trace("[ENTER]<%s:%s> searchKey(%s)", MOD,meth,tostring(searchKey));

  local valueList = ldtMap[LS.CompactList];

  local position = searchList( ldtMap, valueList, searchKey );
  if( position == 0 ) then
    -- Didn't find it -- report an error.
    debug("[NOT FOUND]<%s:%s> searchKey(%s)", MOD, meth, tostring(searchKey));
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok -- found the searchKey, so let's delete the value.
  -- listDelete() will generate a new list, so we store that back into
  -- the ldtMap.
  local resultValue = validateValue( valueList[position] );
  if ( not resultValue ) then
    -- The object did NOT pass the filter criteria, so then NOT FOUND.
    debug("[NOT QUALIFIED]<%s:%s> Object for Key(%s) did not satisfy filter",
      MOD, meth, tostring(searchKey));
    error( ldte.ERR_NOT_FOUND );
  end

  if resultList then
    list.append( resultList, resultValue );
  end

  ldtMap[LS.CompactList]  = ldt_common.listDelete( valueList, position );

  GP=E and trace("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- compactDelete()

-- ======================================================================
-- regularDelete()
-- ======================================================================
-- This is the Sub-Rec-Specific delete -- Basically, the "regular" delete
-- that will be used most of the time.
-- Find the element in the Hash Table, locate the cell Anchor.
-- The Cell Anchor will either point to a simple List or Sub-Record(s).
--
-- Return the element if found and if the resultList is non-nil
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec:
-- (*) ldtCtrl
-- (*) searchKey: the form of object (key/whole) we hash and compare to.
-- (*) resultList;  when not NIL, return the deleted value here.
-- ======================================================================
local function regularDelete(src, topRec, ldtCtrl, searchKey, resultList)
  local meth = "regularDelete()";
  GP=E and trace("[ENTER]: <%s:%s> SearchKey(%s)",
                 MOD, meth, tostring( searchKey ) );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  -- Compute the subRec address that holds the deleteValue searchKey
  local cellNumber = computeHashCell( searchKey, ldtMap );
  local hashDirectory = ldtMap[LS.HashDirectory];
  local cellAnchor = hashDirectory[cellNumber];
  local subRec;
  local valueList;

  -- If no sub-record, then not found.
  if( cellAnchorEmpty( cellAnchor ) ) then 
    debug("[NOT FOUND]<%s:%s>searchKey(%s)", MOD, meth, tostring(searchKey));
    error( ldte.ERR_NOT_FOUND );
  end

  if DEBUG == true then
    trace("[DUMP]<%s:%s>Cell Anchor::Number(%d) DUMP:", MOD, meth, cellNumber);
    local resultMap = cellAnchorDump( src, topRec, cellAnchor, cellNumber );
    trace("\n\n[DUMP]: <<< %s >>>\n", tostring(resultMap));
  end

  -- The cell anchor must be either in a LIST state or a Sub-Record State.
  -- Later, it could also be a Radix Tree of Sub-Records.
  if( cellAnchor[C_CellState] == C_STATE_LIST ) then
    -- The small list is inside of the cell anchor.  Get the lists.
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
    -- NOTE: openSubRec() does its own error checking. No more needed here.
    subRec = ldt_common.openSubRec( src, topRec, digestString );

    valueList = subRec[LDR_LIST_BIN];
    if ( not valueList ) then
      warn("[ERROR]<%s:%s> Empty Value List: ", MOD, meth );
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

  local position = searchList( ldtMap, valueList, searchKey );
  if( position == 0 ) then
    -- Didn't find it: report an error.  First, Close the subRec (not dirty)
    ldt_common.closeSubRec( src, subRec, false);
    debug("[NOT FOUND]<%s:%s> searchKey(%s)", MOD, meth, tostring(searchKey));
    error( ldte.ERR_NOT_FOUND );
  end

  -- NOTE: Switch searchList() to ALSO return the object so that we have
  -- the option of saving it if we want it.  
  --
  -- ok -- found it, so let's delete the value.
  --
  --
  -- listDelete() will generate a new list, so we store that back into
  -- where we got the list:
  -- (*) The Cell Anchor List
  -- (*) The Sub-Record.
  if resultList then
    list.append( resultList, validateValue( valueList[position] ) );
  end

  if( cellAnchor[C_CellState] == C_STATE_LIST ) then
    cellAnchor[C_CellValueList] = ldt_common.listDelete( valueList, position );
  else
    subRec[LDR_LIST_BIN] = ldt_common.listDelete( valueList, position );
    ldt_common.updateSubRec( src, subRec );
  end

  GP=E and trace("[EXIT]<%s:%s> FOUND: Pos(%d)", MOD, meth, position );
  return 0;
end -- function regularDelete()

-- ======================================================================
-- subRecDelete()
-- ======================================================================
-- Delete an item from the LSET -- when in Sub-Record Mode.
-- This is the top level function that either finds and removes the
-- item from the Compact List (if present) or looks up the item in the
-- Hash Directory and removes it from there.
-- Return the element if found and if returnVal is true.
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec:
-- (*) ldtCtrl
-- (*) searchKey:
-- (*) resultList;  when not NIL, return the deleted value here.
-- ======================================================================
local function subRecDelete(src, topRec, ldtCtrl, searchKey, resultList)
  local meth = "subRecDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( searchKey ) );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  local compactList;
  if ldtMap[LS.StoreState] == SS_COMPACT then
    compactDelete( ldtMap, searchKey, resultList );
  else
    regularDelete( src, topRec, ldtCtrl, searchKey, resultList );
  end

  GP=E and trace("[EXIT]<%s:%s> FOUND searchKey(%s)",
    MOD, meth, tostring(searchKey));
  return 0;
end -- function subRecDelete()

-- ======================================================================
-- topRecDelete()
-- ======================================================================
-- Top Record Mode
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtCtrl
-- (*) searchKey:
-- (*) returnList: When not nil, return the value here.
-- ======================================================================
local function topRecDelete( topRec, ldtCtrl, searchKey, returnList)
  local meth = "topRecDelete()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( searchKey ) );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- Find the appropriate bin for the Search value
  local binNumber = computeHashCell( searchKey, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  local liveObject = nil;
  local resultFiltered = nil;
  local position = 0;

  -- We bother to search only if there's a real list.
  if binList ~= nil and list.size( binList ) > 0 then
    position = searchList( ldtMap, binList, searchKey );
    if( position > 0 ) then
      -- Apply the filter to see if this item qualifies
      local liveObject = binList[position];

      -- APPLY FILTER HERE, if we have one.
      if G_Filter ~= nil then
        resultFiltered = G_Filter( liveObject, G_FunctionArgs );
      else
        resultFiltered = liveObject;
      end
    end -- if search found something (pos > 0)
  end -- if there's a list

  if( position == 0 or resultFiltered == nil ) then
    debug("[WARNING]<%s:%s> Value not found: SearchKey(%s)",
      MOD, meth, tostring(searchKey) );
    error( ldte.ERR_NOT_FOUND );
  end

  -- ok, we got the value.  Remove it and update the record.  Also,
  -- update the stats.
  -- OK -- we can't simply NULL out the entry -- because that breaks the bin
  -- when we try to store.  So -- we'll instead replace this current entry
  -- with the END entry -- and then we'll COPY (take) the list ... until
  -- we have the ability to truncate a list in place.
  local listSize = list.size( binList );
  if( position < listSize ) then
    binList[position] = binList[listSize];
  end
  local newBinList = list.take( binList, listSize - 1 );

  -- NOTE: The MAIN record LDT bin, holding ldtCtrl, is of type BF_LDT_BIN,
  -- but the LSET named bins are of type BF_LDT_HIDDEN.
  topRec[binName] = newBinList;
  record.set_flags(topRec, binName, BF_LDT_HIDDEN );--Must set every time

  -- The caller will update the stats and update the main ldt bin.

  GP=E and trace("[EXIT]<%s:%s>: Success: DeleteValue(%s) Key(%s) binList(%s)",
    MOD, meth, tostring( resultFiltered ), tostring(searchKey),
    tostring(binList));
  if returnList then
    list.append( returnList, resultFiltered );
  end

  return 0;
end -- function topRecDelete()

-- ========================================================================
-- subRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function subRecDump( src, topRec, ldtCtrl )
  local meth = "subRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  local resultMap = map();

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local retValue = nil;

  -- Dump the full LSET contents.
  -- If in COMPACT mode, then easy -- show the compact List.
  -- Otherwise, loop thru the Hash Directory and dump each Hash Cell.
  if ldtMap[LS.StoreState] == SS_COMPACT then
    resultMap.StorageState = "CompactList";
    local compactList = ldtMap[LS.CompactList];
    resultMap[0] = compactList;
  else
    resultMap.StorageState = "HashDirectory";

    -- Loop through the Hash Directory, get each cellAnchor, and show the
    -- cellAnchorSummary.
    --
    local tempList;
    local binList;
    local hashDirectory = ldtMap[LS.HashDirectory];
    local distrib = ldtMap[LS.Modulo];
    for j = 0, (distrib - 1), 1 do
      local cellAnchor = hashDirectory[j];
      resultMap[j] = cellAnchorDump( src, topRec, cellAnchor, j);
    end -- for each cell
  end

  GP=E and trace("[EXIT]<%s:%s>ResultMap(%s)",MOD,meth,tostring(resultMap));
  return resultMap;

end -- subRecDump();

-- ========================================================================
-- topRecDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function topRecDump( topRec, ldtCtrl )
  local meth = "TopRecDump()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)", MOD, meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  local resultList = list(); -- list of BIN LISTS
  local listCount = 0;
  local retValue = nil;

  -- Loop through all the modulo n lset-record bins 
  local distrib = ldtMap[LS.Modulo];

  GP=F and trace(" Number of LSet bins to parse: %d ", distrib)

  local tempList;
  local binList;
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    tempList = topRec[binName];
    binList = list();
    list.append( binList, binName );
    if( tempList == nil or list.size( tempList ) == 0 ) then
      list.append( binList, "EMPTY LIST")
    else
      binList = ldt_common.listAppendList( binList, tempList );
    end
    trace("[DEBUG]<%s:%s> BIN(%s) TList(%s) B List(%s)", MOD, meth, binName,
      tostring(tempList), tostring(binList));
    list.append( resultList, binList );
  end -- end for distrib list for-loop 

  GP=E and trace("[EXIT]<%s:%s>ResultList(%s)",MOD,meth,tostring(resultList));

end -- topRecDump();


-- ========================================================================
-- localDump()
-- ========================================================================
-- Dump the full contents of the Large Set, with Separate Hash Groups
-- shown in the result.
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
local function localDump( src, topRec, ldtBinName )
  local meth = "localDump()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s)", MOD, meth,tostring(ldtBinName));

  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  local resultMap;

  G_Filter = ldt_common.setReadFunctions( nil, nil );

  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy
    resultMap = subRecDump( src, topRec, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    resultMap = topRecDump( topRec, ldtCtrl );
  end

  return resultMap; 
end -- localDump();

-- ========================================================================
-- topRecDestroy() -- Remove the LDT entirely from the TopRec LSET record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function topRecDestroy( topRec, ldtCtrl )
  local meth = "topRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));

  -- The caller has already dealt with the Common/Hidden LDT Prop Bin.
  -- All we need to do here is deal with the Numbered bins.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- Address the TopRecord version here.
  -- Loop through all the modulo n lset-record bins 
  -- Go thru and remove (mark nil) all of the LSET LIST bins.
  local distrib = ldtMap[LS.Modulo];
  for j = 0, (distrib - 1), 1 do
	local binName = getBinName( j );
    -- Remove this bin -- assuming it is not already nil.  Setting a 
    -- non-existent bin to nil seems to piss off the lower layers. 
    if( topRec[binName] ~= nil ) then
        topRec[binName] = nil; -- Remove the bin from the record.
    end
  end -- end for distrib list for-loop 

  -- Mark the enitre control-info structure nil.
  topRec[ldtBinName] = nil; -- Remove the LDT bin from the record.
  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;

end -- topRecDestroy()

-- ========================================================================
-- subRecDestroy() -- Remove the LDT (and subrecs) entirely from the record.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  The Parent (caller) has already dealt 
-- with the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) src: Sub-Rec Context - Needed for repeated calls from caller
-- (2) topRec: the user-level record holding the LDT Bin
-- (3) ldtBinName: The LDT Bin
-- (4) ldtCtrl: The LDT Control Structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
local function subRecDestroy( src, topRec, ldtBinName, ldtCtrl )
  local meth = "LSET subRecDestroy()";

  GP=E and trace("[ENTER]: <%s:%s> LDT CTRL(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl));

  ldt_common.destroy( src, topRec, ldtBinName, ldtCtrl );
  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- subRecDestroy()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Set (LSET) Library Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- NOTE: Requirements/Restrictions (this version).
-- (1) One Set Per Record if using "TopRecord" Mode
-- ======================================================================
-- (*) Status = add( topRec, ldtBinName, newValue, createSpec, src)
-- (*) Status = add_all( topRec, ldtBinName, valueList, createSpec, src)
-- (*) Object = get( topRec, ldtBinName, searchValue, src) 
-- (*) Number  = exists( topRec, ldtBinName, searchValue, src) 
-- (*) List   = scan( topRec, ldtBinName, filterModule, filter, fargs, src)
-- (*) Status = remove( topRec, ldtBinName, searchValue, src) 
-- (*) Status = destroy( topRec, ldtBinName, src)
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- ======================================================================
-- The following functions are deprecated:
-- (*) Status = create( topRec, ldtBinName, createSpec )
-- ======================================================================
-- We define a table of functions that are visible to both INTERNAL UDF
-- calls and to the EXTERNAL LDT functions.  We define this table, "lset",
-- which contains the functions that will be visible to the module.
local lset = {};
-- ======================================================================

-- ======================================================================
-- lset.create() -- Create an LSET Object in the record bin.
-- ======================================================================
-- There are two different implementations of LSET.  One stores ALL data
-- in the top record, and the other uses the traditional LDT sub-record
-- style.  A configuration setting determines which style is used.
-- Please see the file: doc_lset.md (Held in the  same directory as this
-- lua file) for additional information on the two types of LSET.
--
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) createSpec: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- Result:
--   rc = 0: Ok, LDT created
--   rc < 0: Error.
-- ======================================================================
function lset.create( topRec, ldtBinName, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LSET CREATE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.create()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring(createSpec) );

  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if ldtBinName is bad.
  validateBinName( ldtBinName );
  local rc = 0;

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error.  We don't check for topRec already existing,
  -- because that is NOT an error.  We may be adding an LDT field to an
  -- existing record.
  if( topRec[ldtBinName] ~= nil ) then
    warn("[ERROR EXIT]: <%s:%s> LDT BIN (%s) Already Exists",
                   MOD, meth, ldtBinName );
    error( ldte.ERR_BIN_ALREADY_EXISTS );
  end
  
  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );

  -- We need a new LDT bin -- set it up.
  local ldtCtrl = setupLdtBin( topRec, ldtBinName, nil, createSpec );

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- lset.create()

-- ======================================================================
-- lset.add()
-- ======================================================================
-- Perform the local insert, for whichever mode (toprec, subrec) is
-- called for.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) newValue: The new Object to be placed into the set.
-- (*) createSpec: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- ======================================================================
-- TODO: Add a common core "local insert" that can be used by both this
-- function and the lset.all_all() function.
-- ======================================================================
function lset.add( topRec, ldtBinName, newValue, createSpec, src, commit)
  -- commit by default
  commit = commit or true
  GP=B and info("\n\n  >>>>>>>>>>>>> API[ LSET ADD ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.add()";
  GP=E and trace("[ENTER]:<%s:%s> LSetBin(%s) NewValue(%s) createSpec(%s)",
                 MOD, meth, tostring(ldtBinName), tostring( newValue ),
                 tostring( createSpec ));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- NOTE: Can't get ldtCtrl from this call when "mustExist" is false.
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- If the bin is not already set up, then create.
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[INFO]: <%s:%s> LSET BIN (%s) does not Exist:Creating",
         MOD, meth, tostring( ldtBinName ));

    -- We need a new LDT bin -- set it up.
    setupLdtBin( topRec, ldtBinName, newValue, createSpec );
  end

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lset control structure
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local rc = 0;

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  G_Filter = ldt_common.setReadFunctions( nil, nil );

  if (ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Insert
    subRecInsert( src, topRec, ldtCtrl, newValue );
  else
    -- Use the TopRec style Insert.
    topRecInsert( topRec, ldtCtrl, newValue );
  end

  if (commit) then
    -- Close ALL of the subrecs that might have been opened
    rc = ldt_common.closeAllSubRecs( src );
    if( rc < 0 ) then
      warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
      error( ldte.ERR_SUBREC_CLOSE );
    end

    -- No need to update the counts here, since the called functions handle
    -- that.  All we need to do is write out the record.
    rc = aerospike:update( topRec );
    if ( rc ~= 0 ) then
      warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_TOPREC_UPDATE );
    end 
  end

  GP=E and trace("[EXIT]:<%s:%s> RC(0)", MOD, meth );
  return rc;
end -- lset.add()

-- ======================================================================
-- lset.add_all() -- Add a LIST of elements to the set.
-- ======================================================================
-- Iterate thru the value list and insert each item using the regular
-- insert.  We don't expect that the list will be long enough to warrant
-- any special processing.
-- TODO: Switch to using a common "core insert" for lset.add() and 
-- lset.add_all() so that we don't perform the initial checking for EACH
-- element in this list.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) valueList: The list of Objects to be placed into the set.
-- (*) createSpec: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- ======================================================================
function lset.add_all( topRec, ldtBinName, valueList, createSpec, src )
  GP=B and info("\n\n  >>>>>>>>>>> API[ LSET ADD_ALL ] <<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.add_all()";
  local rc = 0;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      rc = lset.add( topRec, ldtBinName, valueList[i], createSpec, src, false);
      if( rc < 0 ) then
        warn("[ERROR]<%s:%s> Problem Inserting Item #(%d) [%s]", MOD, meth, i,
          tostring( valueList[i] ));
          error(ldte.ERR_INSERT);
      end
    end
    -- Close ALL of the subrecs that might have been opened
    rc = ldt_common.closeAllSubRecs( src );
    if( rc < 0 ) then
      warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
      error( ldte.ERR_SUBREC_CLOSE );
    end

    -- No need to update the counts here, since the called functions handle
    -- that.  All we need to do is write out the record.
    rc = aerospike:update( topRec );
    if ( rc ~= 0 ) then
      warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
      error( ldte.ERR_TOPREC_UPDATE );
    end
  else
    warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
      MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end
  return rc;
end -- function lset.add_all()

-- ======================================================================
-- lset.get(): Return the object matching <searchValue>
-- ======================================================================
-- Find an element (i.e. search), and optionally apply a filter.
-- Return the element if found, return an error (NOT FOUND) otherwise
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- (*) filterModule: The Lua File that contains the filter.
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Optional Arguments to feed to the filter
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Return the object, or Error (NOT FOUND)
-- ======================================================================
function
lset.get( topRec, ldtBinName, searchValue, filterModule, filter, fargs, src )
  GP=B and info("\n\n  >>>>>>>>>>>>> API[ LSET GET ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.get()";
  GP=E and trace("[ENTER]: <%s:%s> Bin(%s) Search Value(%s)",
     MOD, meth, tostring( ldtBinName), tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local resultObject = 0;
  
  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local searchKey = getKeyValue( ldtMap, searchValue );

  -- Set up our global Filter Function.  This lets us process the function 
  -- pointers once per call, and consistently for all LSET operations.
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  if(ldtMap[LS.SetTypeStore] and ldtMap[LS.SetTypeStore] == ST_SUBRECORD) then
    -- Use the SubRec style Search
    resultObject = subRecSearch( src, topRec, ldtCtrl, searchKey );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, searchKey );
  end

  -- Report an error if we did not find the object.
  if( resultObject == nil ) then
    debug("[NOT FOUND]<%s:%s> SearchKey(%s) SearchValue(%s)",
      MOD, meth, tostring(searchKey), tostring(searchValue));
    error(ldte.ERR_NOT_FOUND);
  end

  GP=E and trace("[EXIT]<%s:%s> Result(%s)",MOD,meth,tostring(resultObject));
  return resultObject;
end -- function lset.get()

-- ======================================================================
-- lset.exists()
-- ======================================================================
-- Return value 1 (ONE) if the item exists in the set, otherwise return 0.
-- We don't want to return "true" and "false" because of Lua Weirdness.
-- Note that this looks a LOT like lset.get(), except that we don't
-- return the object, nor do we apply a filter.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lset.exists( topRec, ldtBinName, searchValue, src )
  GP=B and info("\n\n  >>>>>>>>>>>>> API[ LSET EXISTS ] <<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.exists()";
  GP=E and trace("[ENTER]: <%s:%s> Search Value(%s)",
                 MOD, meth, tostring( searchValue ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local resultObject = 0;
 
  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Get the value we'll compare against (either a subset of the object,
  -- or the object "string-ified"
  local searchKey = getKeyValue( ldtMap, searchValue );

  -- Set up our global Filter Functions. This lets us
  -- process the function pointers once per call, and consistently for
  -- all LSET operations. (However, filter not used here.)
  G_Filter = ldt_common.setReadFunctions( nil, nil );

  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Search
    resultObject = subRecSearch( src, topRec, ldtCtrl, searchKey );
  else
    -- Use the TopRec style Search (this is default if the subrec style
    -- is not specifically requested).
    resultObject = topRecSearch( topRec, ldtCtrl, searchKey );
  end

  local result = 1; -- be positive.
  if( resultObject == nil ) then
    result = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s>: Exists Result(%d)",MOD, meth, result ); 
  return result;
end -- function lset.exists()

-- ======================================================================
-- lset.scan() -- Return a list containing ALL of LSET (with possible filter)
-- ======================================================================
-- Scan the entire LSET, and pass the entire set of objects thru a filter
-- (if present).  Return all objects that qualify.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) filterModule: (optional) Lua file containing user's filter function
-- (*) filter: (optional) User's Filter Function
-- (*) fargs: (optional) filter arguments
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lset.scan(topRec, ldtBinName, filterModule, filter, fargs, src)
  GP=B and info("\n\n  >>>>>>>>>>>>> API[ LSET SCAN ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.scan()";
  GP=E and trace("[ENTER]<%s:%s> BinName(%s) Module(%s) Filter(%s) Fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(filterModule), tostring(filter),
    tostring(fargs));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- Find the appropriate bin for the Search value
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local resultList = list();

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end
  
  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;
  
  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style scan
    subRecScan( src, topRec, ldtCtrl, resultList );
  else
    -- Use the TopRec style Scan (this is default Mode if the subrec style
    -- is not specifically requested).
    topRecScan( topRec, ldtCtrl, resultList );
  end

  GP=E and trace("[EXIT]: <%s:%s>: Search Returns (%s) Size : %d",
                 MOD, meth, tostring(resultList), list.size(resultList));

  return resultList; 
end -- function lset.scan()

-- ======================================================================
-- lset.remove() -- Remove an item from the LSET.
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) deleteValue:
-- (*) UserModule
-- (*) filter: the NAME of the filter function (which we'll find in FuncTable)
-- (*) fargs: Arguments to feed to the filter
-- (*) returnVal: When true, return the deleted value.
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function lset.remove( topRec, ldtBinName, deleteValue, filterModule,
                                filter, fargs, returnVal, src )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET REMOVE ] <<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.remove()";
  GP=E and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 MOD, meth, tostring( deleteValue ) );

  local rc = 0; -- Start out ok.
  -- See if we need to use a return list to get the deleted value.
  local resultList = (returnVal and list()) or nil;

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Get the value we'll compare against
  local searchKey = getKeyValue( ldtMap, deleteValue );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;
  
  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style delete
    subRecDelete( src, topRec, ldtCtrl, searchKey, resultList);
  else
    -- Use the TopRec style delete
    topRecDelete( topRec, ldtCtrl, searchKey, resultList );
  end

  -- Update the Count, then update the Record.
  local itemCount = propMap[PM.ItemCount];
  propMap[PM.ItemCount] = itemCount - 1;
  topRec[ldtBinName] = ldtCtrl;

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end
  
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  rc = aerospike:update( topRec );
  if rc  and rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  -- If the user is asking for the value back, then send it.
  if resultList then
    GP=E and trace("[EXIT]<%s:%s>: Success: DVal(%s) Key(%s) resultList(%s)",
      MOD, meth, tostring(deleteValue), tostring(searchKey),
      tostring(resultList));
    return resultList[1];
  end

  return 0;
end -- function lset.remove()

-- ========================================================================
-- lset.destroy() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- ==>  If in SubRec Mode, then Remove the ESR.  All SubRecs will be
--      cleaned up by the Namespace Supervisor (NSUP).
-- Null out the topRec bin and any other used bins.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NOTE: LSET requires some special handling for destroy that the other LDTs
-- do not require.  We have to handle BOTH the topRec bins case and the
-- regular SubRec case.
-- ========================================================================
function lset.destroy( topRec, ldtBinName, src )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET DESTROY ] <<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.destroy()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if( recPropMap == nil or recPropMap[RPM.Magic] ~= MAGIC ) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
      MOD, meth );
    error( ldte.ERR_INTERNAL );
  end
  if(ldtMap[LS.SetTypeStore] ~= nil and ldtMap[LS.SetTypeStore] == ST_SUBRECORD)
  then
    -- Use the SubRec style Destroy.  It does everything.
    subRecDestroy( src, topRec, ldtBinName, ldtCtrl );
  else
    -- Use the TopRec style Destroy (this is default if the subrec style
    -- is not specifically requested).
    topRecDestroy( topRec, ldtCtrl );
    local ldtCount = recPropMap[RPM.LdtCount];
    if( ldtCount <= 1 ) then
      -- This is the last LDT -- remove the LDT Control Property Bin
      topRec[REC_LDT_CTRL_BIN] = nil;
      -- If we are actually removing the LAST LDT, and thus making this a
      -- regular record again, then we have to UNSET the record type from
      -- LDT to regular.  We do that by passing in a NEGATIVE value.
      record.set_type( topRec, -(RT_LDT) );
    else
      recPropMap[RPM.LdtCount] = ldtCount - 1;
      topRec[REC_LDT_CTRL_BIN] = recPropMap;
      record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
    end
  end

  -- Update the Top Record.  Not sure if this returns nil or ZERO for ok,
  -- so just turn any NILs into zeros.
  rc = aerospike:update( topRec );
  if rc and  rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- function lset.destroy()

-- ========================================================================
-- lset.size() -- return the number of elements (item count) in the LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = size is greater or equal to 0.
--   res = -1: Some sort of error
-- ========================================================================
function lset.size( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET SIZE ] <<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset_size()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  local itemCount = propMap[PM.ItemCount];

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function lset.size()

-- ========================================================================
-- lset.config() -- return the config settings
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LSET Bin
-- (2) ldtBinName: The name of the LSET Bin
-- Result:
--   res = Map of config settings
--   res = -1: Some sort of error
-- ========================================================================
function lset.config( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET CONFIG ] <<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.config()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
      MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  local config = ldtSummary( ldtCtrl );

  GP=E and trace("[EXIT]:<%s:%s>:config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lset.config()

-- ========================================================================
-- lset.get_capacity() -- return the current capacity setting for this LDT
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lset.get_capacity( topRec, ldtBinName )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET GET_CAPACITY ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.get_capacity()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  local capacity = ldtMap[LC.StoreLimit];
  if( capacity == nil ) then
    capacity = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );

  return capacity;
end -- function lset.get_capacity()

-- ========================================================================
-- lset.set_capacity() -- set the current capacity setting for this LDT
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lset.set_capacity( topRec, ldtBinName, capacity )
  GP=B and info("\n\n  >>>>>>>>>> > API [ LSET SET_CAPACITY ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.lset_capacity()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- For debugging, print out our main control map.
  GP=DEBUG and ldtDebugDump( ldtCtrl );

  -- Extract the LDT map from the LDT CONTROL.
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
end -- function lset.lset_capacity()

-- ========================================================================
-- lset.ldt_exists() --
-- ========================================================================
-- return 1 if there is an lset object here, otherwise 0
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   True:  (lset exists in this bin) return 1
--   False: (lset does NOT exist in this bin) return 0
-- ========================================================================
function lset.ldt_exists( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>>>> API[ LSET EXISTS ] <<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lset.ldt_exists()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  if ldt_common.ldt_exists(topRec, ldtBinName, LDT_TYPE ) then
    GP=F and debug("[EXIT]<%s:%s> Exists", MOD, meth);
    return 1
  else
    GP=F and debug("[EXIT]<%s:%s> Does NOT Exist", MOD, meth);
    return 0
  end
end -- function lset.ldt_exists()

-- ========================================================================
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- ========================================================================
--
-- ========================================================================
-- lset.dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lset structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function lset.dump( topRec, ldtBinName, src )
  GP=B and info("\n\n  >>>>>>>> API[ DUMP ] <<<<<<<<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "dump()";
  GP=E and trace("[ENTER]<%s:%s> LDT BIN(%s)",MOD, meth, tostring(ldtBinName));

  -- set up the Sub-Rec Context, if needed.
  if( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  localDump( src, topRec, ldtBinName ); -- Dump out our entire LDT structure.

  -- Another key difference between dump and scan : 
  -- dump prints things in the logs and returns a 0
  -- scan returns the list to the client/caller 

  local ret = " \n LDT bin contents dumped to server-logs \n"; 
  return ret; 
end -- function lset.dump();

-- ======================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return lset;
-- ========================================================================
--   _      _____ _____ _____ 
--  | |    /  ___|  ___|_   _|
--  | |    \ `--.| |__   | |  
--  | |     `--. \  __|  | |  
--  | |____/\__/ / |___  | |  
--  \_____/\____/\____/  \_/  (LIB)
--                            
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
