-- Large Stack Object (LSTACK) Operations Library
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
local MOD="lib_lstack_2014_12_12.G";

-- This variable holds the version of the code. It should match the
-- stored version (the version of the code that stored the ldtCtrl object).
-- If there's a mismatch, then some sort of upgrade is needed.
-- This number is currently an integer because that is all that we can
-- store persistently.  Ideally, we would store (Major.Minor), but that
-- will have to wait until later when the ability to store real numbers
-- is eventually added.
local G_LDT_VERSION = 2;

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
local GP;      -- Global Print Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=false; -- Set B (Banners) to true to turn ON Banner Print
local D=false; -- Set D (Detail) to get more details
local DEBUG=false; -- turn on for more elaborate state dumps.
local TEST_MODE=false; -- turn on for MINIMAL sizes (better testing)

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSTACK Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSTACK module:
--
-- (*) Status = push(topRec, ldtBinName, newValue, createSpec, src)
-- (*) Status = push_all(topRec, ldtBinName, valueList, createSpec, src)
-- (*) List   = peek(topRec, ldtBinName, peekCount, src) 
-- (*) List   = pop(topRec, ldtBinName, popCount, src) 
-- (*) List   = scan(topRec, ldtBinName, src)
-- (*) List  = filter(topRec,ldtBinName,peekCount,filterModule,filter,fargs,src)
-- (*) Status = destroy(topRec, ldtBinName, src)
-- (*) Number = size(topRec, ldtBinName)
-- (*) Map    = get_config(topRec, ldtBinName)
-- (*) Status = set_capacity(topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity(topRec, ldtBinName)
-- (*) Number = exists(topRec, ldtBinName)

-- ======================================================================
-- >> Please refer to ldt/doc_lstack.md for architecture and design notes.
-- ======================================================================

-- ======================================================================
-- Aerospike Server Functions:
-- ======================================================================
-- Aerospike Record Functions:
-- status = aerospike:create( topRec )
-- status = aerospike:update( topRec )
-- status = aerospike:remove( rec ) (not currently used)
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
-- status = record.set_type( rec, recType )
-- status = record.set_flags( topRec, ldtBinName, binFlags )
-- ======================================================================
-- Notes on the SubRec functions:
-- (*) The underlying Aerospike SubRec mechanism actually manages most
--     aspects of SubRecs:  
--     + Update of dirty subrecs is automatic at Lua Context Close.
--     + Close of all subrecs is automatic at Lua Context Close.
-- (*) We cannot close a dirty subrec explicit (we can make the call, but
--     it will not take effect).  We must leave the closing of all dirty
--     SubRecs to the end -- and we'll make that IMPLICIT, because an
--     EXPLICIT call is just more work and makes no difference.
-- (*) It is an ERROR to try to open (with an open_subrec() call) a SubRec
--     that is ALREADY OPEN.  Thus, we use our "SubRecContext" functions
--     that manage a pool of open SubRecs -- which prevents us from making
--     that mistake.
-- (*) We have a LIMITED number of SubRecs that can be open at one time.
--     LDT Operations, such as Scan, that open ALL of the SubRecs are
--     REQUIRED to close the READ-ONLY SubRecs when they are done so that
--     we can open a new one.  We actually have two options here:
--     + We can make close implicit -- and just close clean SubRecs to 
--       free up slots in the SubRecContext (SRC: our pool of open SubRecs).
--       Note that this requires that we mark SubRecs dirty if we have
--       updated them (touched a bin).
--       The only downside is that this makes the SubRec library a little
--       more complicated.
--     + We can make it explicit -- but this means we must be sure to
--       actively close SubRecs, which makes coding more error-prone.
--
-- ======================================================================

-- Common LDT Errors that are used by all of the LDT files.
local ldte=require('ldt/ldt_errors');

-- We have recently moved a number of COMMON functions into the "ldt_common"
-- module, namely the subrec routines and some list management routines.
-- We will likely move some other functions in there as they become common.
local ldt_common = require('ldt/ldt_common');

-- These values should be "built-in" for our Lua, but it is either missing
-- or inconsistent, so we define it here.  We use this when we check to see
-- if a value is a LIST or a MAP.
local Map = getmetatable( map() );
local List = getmetatable( list() );

-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
local MAGIC="MAGIC";     -- the magic value for Testing LSTACK integrity

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

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE = 'T';    
local AS_FALSE = 'F';


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

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

-- LDT TYPES (only lstack is defined here)
local LDT_TYPE = "LSTACK";

-- Special Function -- if supplied by the user in the " Create Module", then
-- we call that UDF to adjust the LDT configuration settings.
local G_SETTINGS = "adjust_settings";

-- In order to tell the Server what's happening with LDT (and maybe other
-- calls), we call "set_context()" with various flags.  The server then
-- uses this to measure LDT call behavior.
local UDF_CONTEXT_LDT = 1;

-- ++====================++
-- || DEFAULT SETTINGS   ||
-- ++====================++
-- Note: These values should match those found in settings_lstack.lua.
--
-- Default storage limit for a stack -- can be overridden by setting
-- one of the packages (e.g. package.ListLargeObject) or by direct calls
-- from the  Create Module (using the "adjust_settings()" function).
local DEFAULT_CAPACITY = 0;
local TEST_MODE_DEFAULT_CAPACITY = 10000;

-- The most recently added items (i.e. the top of the stack) are held
-- directly in the Top Database Record.  Access to this list is the fastest,
-- and so we call it the "Hot List".  We want to pick a size that is
-- reasonable:  Too large and we make all access to the record sluggish, but
-- too small and we don't get much advantage.
local DEFAULT_HOTLIST_CAPACITY = 100;
local TEST_MODE_DEFAULT_HOTLIST_CAPACITY = 10;

-- When the Hot List overflows, we must move some amount of it to the Warm
-- List.  We don't want to move just one item at a time (for each stack push)
-- because then we incur an I/O for every stack write operation.  Instead
-- we want to amortize the I/O cost (of a Warm List Write) over many Hot List
-- writes.
local DEFAULT_HOTLIST_TRANSFER = 50;
local TEST_MODE_DEFAULT_HOTLIST_TRANSFER = 5;

-- Default size for an LDT Data Record (LDR).  The LDT is the Sub-Record
-- that is intially formed when data flows from the Hot List (items held
-- directly in the Top Database Record) to the Warm List (items held in a
-- Sub-Record).
local DEFAULT_LDR_CAPACITY = 200;
local TEST_MODE_DEFAULT_LDR_CAPACITY = 20;

-- Similar to the HotList Transfer, we do a similar thing when we transfer
-- data from the Warm List to the Cold List, except rather than Items (as in
-- the Hot List), for the Warm List we transfer Sub-Records (e.g. LDRs).
local DEFAULT_WARMLIST_CAPACITY = 100;
local TEST_MODE_DEFAULT_WARMLIST_CAPACITY = 10;

local DEFAULT_WARMLIST_TRANSFER = 10;
local TEST_MODE_DEFAULT_WARMLIST_TRANSFER = 2;

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

-- There are THREE different types of (Child) subrecords that are associated
-- with an LSTACK LDT:
-- (1) LDR (LSTACK Data Record) -- used in both the Warm and Cold Lists
-- (2) ColdDir Record -- used to hold lists of LDRs (the Cold List Dirs)
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT subrecords have a properties bin that holds a map that defines
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

-- The Cold Dir Records use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local COLD_DIR_LIST_BIN = "ColdDirListBin"; 
local COLD_DIR_CTRL_BIN = "ColdDirCtrlBin";

-- The Existence Sub-Records (ESRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above (and that might be all)

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are four main Record Types used in the LSTACK Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSTACK bin
-- (*) EsrRec: The Existence SubRecord (ESR) that coordinates all child
--             subrecs for a given LDT.
-- (*) LdrRec: the LSTACK Data Record (LDR) that holds user Data.
-- (*) ColdDirRec: The Record that holds a list of Sub Record Digests
--     (i.e. record pointers) to the LDR Data Records.  The Cold list is
--     a linked list of Directory pages;  each dir contains a list of
--     digests (record pointers) to the LDR data pages.
-- <+> Naming Conventions:
--   + As discussed below, all Map KeyField names are INDIRECTLY referenced
--     via descriptive variables that map to a single character (to save
--     space when the entire map is msg-packed into a record bin).
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[ldtBinName] or ldrRec[LDR_CTRL_BIN]);
--
-- <+> Recent Change in LdtMap Use: (6/21/2013 tjl)
--   + In order to maintain a common access mechanism to all LDTs, AND to
--     limit the amount of data that must be "un-msg-packed" when accessed,
--     we will use a common property map and a type-specific property map.
--     That means that the "ldtMap" that was the primary value in the LdtBin
--     is now a list, where ldtCtrl[LDT_PROP_MAP] will always be the propMap and
--     ldtCtrl[LDT_CTRL_MAP] will always be the ldtMap.  In the server code, using "C",
--     we will sometimes read the ldtCtrl[LDT_PROP_MAP] (the property map) in order to
--     perform some LDT management operations.
--   + Since Lua wraps up the LDT Control map as a self-contained object,
--     we are paying for storage in EACH LDT Bin for the map field names. 
--     Thus, even though we like long map field names for readability:
--     e.g.  ldtMap.HotEntryListItemCount, we don't want to spend the
--     space to store the large names in each and every LDT control map.
--     So -- we do another Lua Trick.  Rather than name the key of the
--     map value with a large name, we instead use a single character to
--     be the key value, but define a descriptive variable name to that
--     single character.  So, instead of using this in the code:
--     ldtMap.HotEntryListItemCount = 50;
--            123456789012345678901
--     (which would require 21 bytes of storage); We instead do this:
--     local HotEntryListItemCount='H';
--     ldtMap[HotEntryListItemCount] = 50;
--     Now, we're paying the storage cost for 'H' (1 byte) and the value.
--
--     So -- we have converted all of our LDT lua code to follow this
--     convention (fields become variables the reference a single char)
--     and the mapping of long name to single char will be done in the code.
-- ------------------------------------------------------------------------
-- ------------------------------------------------------------------------
-- Control Map Names: for Property Maps and Control Maps
-- ------------------------------------------------------------------------
-- Note:  All variables that are field names will be upper case.
-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
-- values -- within any given map.  They do NOT have to be unique across
-- the maps (and there's no need -- they serve different purposes).
-- Note that we've tried to make the mapping somewhat cannonical where
-- possible. 
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Record Level Property Map (RPM) Fields: One RPM per record
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local RPM = {
  LdtCount             = 'C',  -- Number of LDTs in this rec
  VInfo                = 'V',  -- Partition Version Info
  Magic                = 'Z',  -- Special Sauce
  SelfDigest           = 'D'   -- Digest of this record
};
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local PM = {
  ItemCount             = 'I', -- (Top): # of items in LDT
  SubRecCount           = 'S', -- (Top): # of subrecs in the LDT
  Version               = 'V', -- (Top): Code Version
  LdtType               = 'T', -- (Top): Type: stack, set, map, list
  BinName               = 'B', -- (Top): LDT Bin Name
  Magic                 = 'Z', -- (All): Special Sauce
  CreateTime            = 'C', -- (All): Creation time of this rec
  RecType               = 'R', -- (All): Type of Rec:Top,Ldr,Esr,CDir
  EsrDigest             = 'E', -- (All): Digest of ESR
  ParentDigest          = 'P', -- (Subrec): Digest of TopRec
  SelfDigest            = 'D'  -- (Subrec): Digest of THIS Record
};

-- Note: The TopRec keeps this in the single LDT Bin (RPM).
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT Data Record (LDR) Control Map Fields (Recall that each Map ALSO has
-- the PM (general property map) fields.
local LDR_ByteEntryCount       = 'C'; -- Current Count of bytes used
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Cold Directory Control Map::In addition to the General Property Map
local CDM = {
  NextDirRec           = 'N';-- Ptr to next Cold Dir Page
  PrevDirRec           = 'P';-- Ptr to Prev Cold Dir Page
  DigestCount          = 'C';-- Current Digest Count
};
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LSTACK Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- These fields are common across all LDTs:
-- Fields Common (LC) to ALL LDTs (managed by the LDT COMMON routines)
local LC = {
  UserModule             = 'P', -- Name of the User Module
  StoreLimit             = 'L', -- Max Item Count for stack
};

-- These fields are specific (LS) to LSTACK
local LS = {
  LdrEntryCountMax       = 'e', -- Max # of entries in an LDR
  LdrByteEntrySize       = 's', -- Fixed Size of a binary Object in LDR
  LdrByteCountMax        = 'b', -- Max # of bytes in an LDR
  HotEntryList           = 'H', -- The Hot Entry List
  HotEntryListItemCount  = 'i', -- The Hot List Count
  HotListMax             = 'h', -- Max Size of the Hot List
  HotListTransfer        = 'X', -- Amount to transfer from Hot List
  WarmDigestList         = 'W', -- The Warm Digest List
  WarmListDigestCount    = 'l', -- # of Digests in the Warm List
  WarmListMax            = 'w', -- Max # of Digests in the Warm List
  WarmListTransfer       = 'x', -- Amount to Transfer from the Warm List
-- Note that WarmTopXXXXCount will eventually replace the need to show if
-- the Warm Top is FULL -- because we'll always know the count (and "full"
-- will be self-evident).
  WarmTopFull            = 'g', -- AS_Boolean: Shows if Warm Top is full
  WarmTopEntryCount      = 'A', -- # of Objects in the Warm Top (LDR)
  WarmTopByteCount       = 'a', -- # Bytes in the Warm Top (LDR)

-- Note that ColdTopListCount will eventually replace the need to know if
-- the Cold Top is FULL -- because we'll always know the count of the Cold
-- Directory Top -- and so "full" will be self-evident.
  ColdTopFull            = 'f', -- AS_Boolean: Shows if Cold Top is full
  ColdTopListCount       = 'T', -- Shows List Count for Cold Top

  ColdDirListHead        = 'Z', -- Digest of the Head of the Cold List
  ColdDirListTail        = 'z', -- Digest of the Head of the Cold List
  ColdDataRecCount       = 'R',-- # of LDRs in Cold Storage
-- It's assumed that this will match the warm list size, and we'll move
-- half of the warm digest list to a cold list on each transfer.
  ColdListMax            = 'c',-- Max # of items in a cold dir list
-- This is used to LIMIT the size of an LSTACK -- we will do it efficiently
-- at the COLD DIR LEVEL.  So, for Example, if we set it to 3, then we'll
-- discard the last (full) cold Dir List when we add a new fourth Dir Head.
-- Thus, the number of FULL Cold Directory Pages "D" should be set at
-- (D + 1).
  ColdDirRecMax          = 'C',-- Max # of Cold Dir subrecs we'll have
  ColdDirRecCount        = 'r' -- # of Cold Dir sub-Records
};

-- ------------------------------------------------------------------------
-- Maintain the LSTACK letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
-- ----------------------------------------------------------------------
---- >>> Be Mindful of the LDT Common Fields that ALL LDTs must share <<<
-- ----------------------------------------------------------------------
-- A:LS.WarmTopEntryCount    a:LS.WarmTopByteCount      0:
-- B:                        b:LS.LdrByteCountMax       1:
-- C:LS.ColdDirRecMax        c:LS.ColdListMax           2:
-- D:                        d:                         3:
-- E:                        e:LS.LdrEntryCountMax      4:
-- G:                        g:LS.WarmTopFull           6:
-- H:LS.HotEntryList         h:LS.HotListMax            7:
-- I:                        i:LS.HotEntryListItemCount 8:
-- J:                        j:                         9:
-- K:                        k:LC.KeyType         
-- L:LC.StoreLimit           l:LS.WarmListDigestCount
-- N:                        n:
-- O:                        o:
-- P:                        p:
-- Q:                        q:
-- R:LS.ColdDataRecCount     r:LS.ColdDirRecCount
-- S:                        s:LS.LdrByteEntrySize
-- V:                        v:
-- W:LS.WarmDigestList       w:LS.WarmListMax
-- X:LS.HotListTransfer      x:LS.WarmListTransfer
-- Y:                        y:
-- Z:LS.ColdDirListHead      z:LS.ColdDirListTail
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- We won't bother with the sorted alphabet mapping for the rest of these
-- fields -- they are so small that we should be able to stick with visual
-- inspection to make sure that nothing overlaps.  And, note that these
-- Variable/Char mappings need to be unique ONLY per map -- not globally.
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ======================================================================

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

-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================


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

  -- General LDT Parms:
  resultMap.StoreLimit           = ldtMap[LC.StoreLimit];

  -- LDT Data Record (LDR) Settings:
  resultMap.LdrEntryCountMax     = ldtMap[LS.LdrEntryCountMax];
  resultMap.LdrByteEntrySize     = ldtMap[LS.LdrByteEntrySize];
  resultMap.LdrByteCountMax      = ldtMap[LS.LdrByteCountMax];
  --
  -- Hot Entry List Settings: List of User Entries
  resultMap.HotListMax            = ldtMap[LS.HotListMax];
  resultMap.HotListTransfer       = ldtMap[LS.HotListTransfer];
  resultMap.HotEntryListItemCount = ldtMap[LS.HotEntryListItemCount];

  -- Warm Digest List Settings: List of Digests of LDT Data Records
  resultMap.WarmListMax           = ldtMap[LS.WarmListMax];
  resultMap.WarmListTransfer      = ldtMap[LS.WarmListTransfer];
  resultMap.WarmListDigestCount   = ldtMap[LS.WarmListDigestCount];

  -- Cold Directory List Settings: List of Directory Pages
  resultMap.ColdDirListHead       = ldtMap[LS.ColdDirListHead];
  resultMap.ColdListMax           = ldtMap[LS.ColdListMax];
  resultMap.ColdDirRecMax         = ldtMap[LS.ColdDirRecMax];
  resultMap.ColdListRecCount      = ldtMap[LS.ColdDirRecCount];
  resultMap.ColdListDataRecCount  = ldtMap[LS.ColdDataRecCount];
  resultMap.ColdTopFull           = ldtMap[LS.ColdTopFull];
  resultMap.ColdTopListCount      = ldtMap[LS.ColdTopListCount];

end -- function ldtMapSummary

-- ======================================================================
-- ldtMapSummaryString()
-- ======================================================================
-- Provide a string version of the LDT Map Summary.
-- ======================================================================
local function ldtMapSummaryString( ldtMap )
  local resultMap = {};
  ldtMapSummary(resultMap, ldtMap);
  return tostring(resultMap);
end

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
  resultMap.SUMMARY              = "LSTACK Summary";

  if ( ldtCtrl == nil ) then
    warn("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    return resultMap;
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

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
-- ldtDebugDump()
-- ======================================================================
-- To aid in debugging, dump the entire contents of the ldtCtrl object
-- for LMAP.  Note that this must be done in several prints, as the
-- information is too big for a single print (it gets truncated).
-- ======================================================================
local function ldtDebugDump( ldtCtrl )
  local meth = "ldtDebugDump()";

  -- Print MOST of the "TopRecord" contents of this LMAP object.
  local resultMap                = {};
  resultMap.SUMMARY              = "LSTACK Summary";

  trace("\n\n <><><><><><><><><> [ LDT LSTACK SUMMARY ] <><><><><><><><><> \n");

  if ( ldtCtrl == nil ) then
    warn("[ERROR]<%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    trace("<<<%s>>>", tostring(resultMap));
    return 0;
  end

  if ( type(ldtCtrl) ~= "userdata" ) then
    warn("[ERROR]<%s:%s>: LDT BIN VALUE (ldtCtrl) is bad.  Type(%s)",
      MOD, meth, type(ldtCtrl));
    resultMap.ERROR =  "BAD LDT BIN VALUE";
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
  local resultMap2 = {};
  resultMap2.SUMMARY              = "LSTACK-SPECIFIC Values";

  -- Load the LMAP-specific properties
  ldtMapSummary( resultMap2, ldtMap );
  trace("\n<<<%s>>>\n", tostring(resultMap2));
  resultMap2 = nil;

  -- Print the Hash Directory
  local resultMap3 = {};
  resultMap3.SUMMARY              = "LSTACK Hot List";
  resultMap3.HotEntryList         = ldtMap[LS.HotEntryList];
  trace("\n<<<%s>>>\n", tostring(resultMap3));

end -- function ldtDebugDump()

-- ======================================================================
-- Make it easier to use ldtSummary(): Have a String version.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
  return tostring( ldtSummary( ldtCtrl ) );
end

-- ======================================================================
-- NOTE: All Sub-Record routines have been moved to ldt_common.
-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, will employ the "subrecContext" object, which
-- will hold all of the subrecords that were opened during processing. 
-- Note that some operations can potentially involve many subrec
-- operations -- and can also potentially revisit pages.
--
-- SubRecContext Design:
-- The key will be the DigestString, and the value will be the subRec
-- pointer.  At the end of an outer call, we will iterate thru the subrec
-- context and close all open subrecords.  Note that we may also need
-- to mark them dirty -- but for now we'll update them in place (as needed),
-- but we won't close them until the end.
--
-- Sub-Record functions now reside in the ldt_common.lua module.
-- ======================================================================

-- ======================================================================
-- listAppend()
-- ======================================================================
-- General tool to append one list to another.   At the point that we
-- find a better/cheaper way to do this, then we change THIS method and
-- all of the LDT calls to handle lists will get better as well.
-- ======================================================================
local function listAppend( baseList, additionalList )
  if( baseList == nil ) then
    warn("[INTERNAL ERROR] Null baselist in listAppend()" );
    error( ldte.ERR_INTERNAL );
  end
  -- local listSize = list.size( additionalList );
  local listSize = #additionalList;
  for i = 1, listSize, 1 do
    list.append( baseList, additionalList[i] );
  end -- for each element of additionalList

  return baseList;
end -- listAppend()

-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Notes on Configuration:
-- (*) In order to make the LDT code as efficient as possible, we want
--     to pick the best combination of configuration values for the Hot,
--     Warm and Cold Lists -- so that data transfers from one list to
--     the next with minimal storage upset and runtime management.
--     Similarly, we want the transfer from the LISTS to the Data pages
--     and Data Directories to be as efficient as possible.
-- (*) The HotEntryList should be the same size as the LDR Page that
--     holds the Data entries.  -- (*) The HotListTransfer should be half or one quarter the size of the
--     HotList -- so that even amounts can be transfered to the warm list.
-- (*) The WarmDigestList should be the same size as the DigestList that
--     is in the ColdDirectory Page
-- (*) The WarmListTransfer should be half or one quarter the size of the
--     list -- so that even amounts can be transfered to the cold list.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- initializeLdtCtrl:
-- ======================================================================
-- Set up the LDT Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LDT BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LDT
-- behavior.  Thus this function represents the "type" LDT MAP -- all
-- LDT control fields are defined here.
-- The LdtMap is obtained using the user's LDT Bin Name.
--
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the LDT
--
-- ======================================================================
-- Additional Notes:
-- local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
-- local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
-- local RT_SUB = 2; -- 0x2: Regular Sub Record (LDR, CDIR, etc)
-- local RT_ESR = 4; -- 0x4: Existence Sub Record
-- ======================================================================
local function initializeLdtCtrl( topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  GP=E and trace("[ENTER]: <%s:%s>:: LdtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Create the two maps and fill them in.  There's the General Property Map
  -- and the LDT specific LDT Map.
  -- Note: All Field Names start with UPPER CASE.
  local propMap = map();
  local ldtMap = map();
  local ldtCtrl = list();
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );

  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM.ItemCount] = 0; -- A count of all items in the stack
  propMap[PM.Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM.LdtType]    = LDT_TYPE; -- Validate the ldt type
  propMap[PM.Magic]      = MAGIC; -- Special Validation
  propMap[PM.BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM.RecType]    = RT_LDT; -- Record Type LDT Top Rec
  propMap[PM.EsrDigest]    = 0; -- not set yet.
  propMap[PM.CreateTime] = aerospike:get_current_time();
  propMap[PM.SelfDigest] = record.digest( topRec );

  -- Specific LDT Parms: Held in LdtMap
  ldtMap[LC.StoreLimit]  = DEFAULT_CAPACITY;  -- Store no more than this.

  -- LDT Data Record Settings: Passed into "LDR Create"
  -- Max # of Data LDR items (List Mode)
  ldtMap[LS.LdrEntryCountMax]= DEFAULT_LDR_CAPACITY;
  ldtMap[LS.LdrByteEntrySize]=  0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax] =   0; -- Max # of Data LDR Bytes (binary mode)

  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotEntryList]         = list(); -- the list of data entries
  ldtMap[LS.HotEntryListItemCount]=   0; -- Number of elements in the Top List

  -- See the definitions of these constants (above) for their explanations.
  ldtMap[LS.HotListMax]           = DEFAULT_HOTLIST_CAPACITY;
  ldtMap[LS.HotListTransfer]      = DEFAULT_HOTLIST_TRANSFER;

  -- Warm Digest List Settings: List of Digests of LDT Data Records
  ldtMap[LS.WarmTopFull] = AS_FALSE; --true when top LDR is full(for next write)
  ldtMap[LS.WarmListDigestCount]  = 0; -- Number of Warm Data Record LDRs
  ldtMap[LS.WarmListMax]          = DEFAULT_WARMLIST_CAPACITY;
  ldtMap[LS.WarmListTransfer]     = DEFAULT_WARMLIST_TRANSFER;
  ldtMap[LS.WarmDigestList]       = list.new(ldtMap[LS.WarmListMax]);
  ldtMap[LS.WarmTopEntryCount]    = 0; -- Count of entries in top warm LDR
  ldtMap[LS.WarmTopByteCount]     = 0; -- Count of bytes used in top warm LDR

  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdDirListHead]= 0; -- Head (Rec Digest) of the Cold List Dir Chain
  ldtMap[LS.ColdTopFull] = AS_FALSE; -- true when cold head is full (next write)
  ldtMap[LS.ColdDataRecCount]= 0; -- # of Cold DATA Records (data LDRs)
  ldtMap[LS.ColdDirRecCount] = 0; -- # of Cold DIRECTORY Records
  ldtMap[LS.ColdDirRecMax]   = 100; -- Max# of Cold DIRECTORY Records
  ldtMap[LS.ColdListMax]     = 100; -- # of list entries in a Cold list dir node

  -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  -- Special adjustment -- for TEST MODE.  If we're testing, then we want
  -- to use extra-small sizes to exercise the mechanism.
  -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  if TEST_MODE then
    ldtMap[LC.StoreLimit]       = TEST_MODE_DEFAULT_CAPACITY;
    ldtMap[LS.LdrEntryCountMax] = TEST_MODE_DEFAULT_LDR_CAPACITY;
    ldtMap[LS.HotListMax]       = TEST_MODE_DEFAULT_HOTLIST_CAPACITY;
    ldtMap[LS.HotListTransfer]  = TEST_MODE_DEFAULT_HOTLIST_TRANSFER;
    ldtMap[LS.WarmListMax]      = TEST_MODE_DEFAULT_WARMLIST_CAPACITY;
    ldtMap[LS.WarmListTransfer] = TEST_MODE_DEFAULT_WARMLIST_TRANSFER;
  end

    -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  GP=F and trace("[DEBUG]: <%s:%s> : LDT Summary after Init(%s)",
      MOD, meth , ldtSummaryString(ldtCtrl));

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method will also call record.set_type().
  ldt_common.setLdtRecordType( topRec );

  -- Set the BIN Flag type to show that this is an LDT Bin, with all of
  -- the special priviledges and restrictions that go with it.
  GP=F and trace("[DEBUG]:<%s:%s>About to call record.set_flags(Bin(%s)F(%s)",
    MOD, meth, ldtBinName, tostring(BF_LDT_BIN) );

  -- Put our new maps in a list, in the record, then store the record.
  topRec[ldtBinName]    = ldtCtrl;
  record.set_flags( topRec, ldtBinName, BF_LDT_BIN );

  GP=F and trace("[DEBUG]: <%s:%s> Back from calling record.set_flags()",
    MOD, meth );

  GP=E and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtCtrl;
end -- initializeLdtCtrl()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LDT Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- These are all local functions to this module and serve various
-- utility and assistance functions.
-- ======================================================================

-- ======================================================================
-- ldrSummary( ldrRec )
-- ======================================================================
-- Print out interesting stats about this LDR Record
-- ======================================================================
local function  ldrSummary( ldrRec ) 
  if( ldrRec  == nil ) then
    return "NULL Data (LDR) RECORD";
  end;
  if( ldrRec[LDR_CTRL_BIN]  == nil ) then
    return "NULL LDR CTRL BIN";
  end;
  if( ldrRec[SUBREC_PROP_BIN]  == nil ) then
    return "NULL LDR PROPERTY BIN";
  end;

  local resultMap = map();
  local ldrMap = ldrRec[LDR_CTRL_BIN];
  local ldrPropMap = ldrRec[SUBREC_PROP_BIN];

  resultMap.SelfDigest   = ldrPropMap[PM.SelfDigest];
  resultMap.ParentDigest   = ldrPropMap[PM.ParentDigest];

  resultMap.WarmList = ldrRec[LDR_LIST_BIN];
  -- resultMap.ListSize = list.size( resultMap.WarmList );
  resultMap.ListSize = #resultMap.WarmList;

  return tostring( resultMap );
end -- ldrSummary()

-- ======================================================================
-- coldDirRecSummary( coldDirRec )
-- ======================================================================
-- Print out interesting stats about this Cold Directory Rec
-- ======================================================================
local function  coldDirRecSummary( coldDirRec )
  if( coldDirRec  == nil ) then return "NULL COLD DIR RECORD"; end;
  if( coldDirRec[COLD_DIR_CTRL_BIN] == nil ) then
    return "NULL COLD DIR RECORD CONTROL MAP";
  end;

  local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

  return tostring( coldDirMap );
end -- coldDirRecSummary()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- General LIST Read/Write(entry list, digest list) and LDR FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The same mechanisms are used in different contexts.  The HotList
-- Entrylist -- is similar to the EntryList in the Warm List.  The 
-- DigestList in the WarmList is similar to the ColdDir digest list in
-- the Cold List.  LDRs pointed to in the Warmlist are the same as the
-- LDRs pointed to in the cold list.

-- ======================================================================
-- readEntryList()
-- ======================================================================
-- This method reads the entry list from Hot List.
-- It examines each entry, applies the inner UDF function (if applicable)
-- and appends viable candidates to the result list.
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) ldtCtrl:
--   (*) entryList:
--   (*) count:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readEntryList( resultList, ldtCtrl, entryList, count )

  local meth = "readEntryList()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) filter(%s) fargs(%s)",
      MOD,meth,tostring(count), tostring(G_Filter), tostring(G_FunctionArgs));
      

  -- Extract the property map and LDT map from the LDT Ctrl.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Iterate thru the entryList, gathering up items in the result list.
  -- Read <count> or <entryListSize>, whichever is smaller.
  local numRead = 0;
  local numToRead = 0;
  -- local listSize = list.size( entryList );
  local listSize = #entryList;
  if count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  GP=E and trace("\n [DEBUG]<%s> Read(%d) items from List(%s)",
      meth, numToRead, tostring(entryList) );

  GP=E and trace("\n [DEBUG]<%s> Add to ResultList(%s)",
      meth, tostring(resultList));

  -- Read back to front (LIFO order), up to "numToRead" entries
  local readValue;
  for i = listSize, 1, -1 do

    readValue = entryList[i];

    -- If the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if( G_Filter ~= nil ) then
      resultValue = G_Filter( readValue, G_FunctionArgs );
    else
      resultValue = readValue;
    end

    if( resultValue ~= nil ) then
      list.append( resultList, readValue );
      numRead = numRead + 1;
    end

    --  This is REALLY HIGH debug output.  Turn this on ONLY if there's
    --  something suspect about the building of the result list.
    --  GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
    --    MOD, meth, tostring( readValue ), tostring(resultList) );
    
    if numRead >= numToRead then
      GP=E and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s)",
        MOD, meth, numRead, ldt_common.summarizeList( resultList ));
      return numRead;
    end
  end -- for each entry in the list

  GP=E and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, ldt_common.summarizeList( resultList ));
  return numRead;
end -- readEntryList()

-- ======================================================================
-- takeEntryList()
-- ======================================================================
-- This method takes elements from the Hot List.
--
-- It examines each entry, applies the inner UDF function (if applicable)
-- and appends viable candidates to the result list.
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) ldtCtrl:
--   (*) entryList:
--   (*) count:
-- Return:
--   Implicit: entries are added to the resultList parameter
--   Explicit:
--   1:  numTaken: Number of Elements Taken.
--   2:  empty: Boolean: true if entry list was left empty (needs backfill)
-- ======================================================================
local function takeEntryList( resultList, ldtCtrl, entryList, count )

  local meth = "takeEntryList()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) filter(%s) fargs(%s)",
      MOD,meth,tostring(count), tostring(G_Filter), tostring(G_FunctionArgs));

  -- Extract the property map and LDT map from the LDT Ctrl.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Iterate thru the entryList, gathering up items in the result list.
  -- Take <count> or <entryListSize>, whichever is smaller
  local numTaken = 0;
  local numToTake = 0;
  -- local listSize = list.size( entryList );
  local listSize = #entryList;
  if count >= listSize then
    numToTake = listSize;
  else
    numToTake = count;
  end

  GP=E and trace("\n [DEBUG]<%s> Take(%d) items from List(%s)",
      meth, numToTake, tostring(entryList) );

  GP=E and trace("\n [DEBUG]<%s> Add to ResultList(%s)",
      meth, tostring(resultList));

  -- Take back to front (LIFO order), up to "numToTake" entries
  local readValue;
  for i = listSize, 1, -1 do

    readValue = entryList[i];

    -- If the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if( G_Filter ~= nil ) then
      resultValue = G_Filter( readValue, G_FunctionArgs );
    else
      resultValue = readValue;
    end

    if( resultValue ~= nil ) then
      list.append( resultList, readValue );
    end

    --  This is REALLY HIGH debug output.  Turn this on ONLY if there's
    --  something suspect about the building of the result list.
    --  GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
    --    MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numTaken = numTaken + 1;
    if numTaken >= numToTake then
      GP=E and trace("[Done]: <%s:%s> NumTaken(%d) resultListSummary(%s)",
        MOD, meth, numTaken, ldt_common.summarizeList( resultList ));
      break;
    end
  end -- for each entry in the list

  -- For however many we've taken, we need to remove that many from the
  -- Entry List.  Recall that the Entry List is in reverse order, so we will
  -- keep the unused remainder (the list front).  We use the new list.trim()
  -- operator, which takes the position then removes position to the end.
  --
  -- If we pop 6 elements from the list below, then we'll call trim 
  -- as follows:
  -- size = 16
  -- pop = 6
  -- list.trim( (size - pop) + 1 )  
  -- Release old contents starting at position 11.
  -- |<=================>|<--------->|
  --  1 2 3 4 5 6 7 8 9 0 a b c d e f
  -- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  -- |A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P]
  -- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  -- Note that we update the list IN-PLACE, so we DO NOT need to reassign
  -- the list to it's source when we're done.
  local trimPosition = (listSize - numTaken) + 1;
  local empty = (listSize == numTaken)
  GP=F and debug("[DEBUG]<%s:%s>BEFORE trim list(%s) at position(%d)", MOD,
    meth, tostring(entryList), trimPosition);

  list.trim(entryList, trimPosition);

  GP=F and debug("[DEBUG]<%s:%s>AFTER trim list(%s) at position(%d)", MOD,
    meth, tostring(entryList), trimPosition);

  GP=E and trace("[EXIT]<%s:%s> NumTaken(%d) Empty(%s) resultListSummary(%s)",
    MOD,meth,numTaken,tostring(empty),ldt_common.summarizeList(resultList));

  return numTaken, empty;
end -- takeEntryList()

-- ======================================================================
-- readByteArray()
-- ======================================================================
-- This method reads the entry list from Warm and Cold List Pages.
-- In each LDT Data Record (LDR), there are three Bins:  A Control Bin,
-- a List Bin (a List() of entries), and a Binary Bin (Compacted Bytes).
-- Similar to its sibling method (readEntryList), readByteArray() pulls a Byte
-- entry from the compact Byte array, applies the (assumed) UDF, and then
-- passes the resulting value back to the caller via the resultList.
--
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) ldtCtrl
--   (*) LDR Page:
--   (*) count:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readByteArray( resultList, ldtCtrl, ldrSubRec, count )
  local meth = "readByteArray()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) filter(%s) fargs(%s) ",
    MOD, meth, tostring(count), tostring(G_Filter), tostring(G_FunctionArgs));
            
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- Iterate thru the BYTE structure, gathering up items in the result list.
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local ldrMap = ldrSubRec[LDR_CTRL_BIN];
  local byteArray = ldrSubRec[LDR_BNRY_BIN];
  local numRead = 0;
  local numToRead = 0;
  local listSize = ldrMap[LDR_ByteEntryCount]; -- Number of Entries
  local entrySize = ldtMap[LS.LdrByteEntrySize]; -- Entry Size in Bytes
  -- When in binary mode, we rely on the LDR page control structure to track
  -- the ENTRY COUNT and the ENTRY SIZE.  Just like walking a list, we
  -- move thru the BYTE value by "EntrySize" amounts.  We will try as much
  -- as possible to treat this as a list, even though we access it directly
  -- as an array.
  --
  if count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  -- The BINARY information is held in the page's control info
  -- Current Item Count
  -- Current Size (items must be a fixed size)
  -- Max bytes allowed in the ByteBlock.
  -- Example: EntrySize = 10
  -- Address of Entry 1: 0
  -- Address of Entry 2: 10
  -- Address of Entry N: (N - 1) * EntrySize
  -- WARNING!!!  Unlike C Buffers, which start at ZERO, this byte type
  -- starts at ONE!!!!!!
  --
  -- 12345678901234567890 ...  01234567890
  -- +---------+---------+------+---------+
  -- | Entry 1 | Entry 2 | .... | Entry N | 
  -- +---------+---------+------+---------+
  --                            A
  -- To Read:  Start Here ------+  (at the beginning of the LAST entry)
  --           and move BACK towards the front.
  local readValue;
  local byteValue;
  local byteIndex = 0; -- our direct position in the byte array.
  GP=F and trace("[DEBUG]:<%s:%s>Starting loop Byte Array(%s) ListSize(%d)",
      MOD, meth, tostring(byteArray), listSize );
  for i = (listSize - 1), 0, -1 do

    byteIndex = 1 + (i * entrySize);
    byteValue = bytes.get_bytes( byteArray, byteIndex, entrySize );

--  GP=F and trace("[DEBUG]:<%s:%s>: In Loop: i(%d) BI(%d) BV(%s)",
--    MOD, meth, i, byteIndex, tostring( byteValue ));

    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    readValue = byteValue;

    -- If the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if( G_Filter ~= nil ) then
      resultValue = G_Filter( readValue, G_FunctionArgs );
    else
      resultValue = readValue;
    end

    -- If the value passes the filter (or if there is no filter), then add
    -- it to the result list.
    if( resultValue ~= nil ) then
      list.append( resultList, resultValue );
    end

    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead then
      GP=E and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultList(%s)",
        MOD, meth, numRead, tostring( resultList ));
      return numRead;
    end
  end -- for each entry in the list (packed byte array)

  GP=E and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, ldt_common.summarizeList( resultList ));
  return numRead;
end -- readByteArray()


-- ======================================================================
-- takeByteArray()
-- ======================================================================
-- This method TAKES from the BYTE ARRAY from Warm and Cold List Pages.
-- In each LDT Data Record (LDR), there are three Bins:  A Control Bin,
-- a List Bin (a List() of entries), and a Binary Bin (Compacted Bytes).
-- Similar to its sibling method (readEntryList), readByteArray() pulls a Byte
-- entry from the compact Byte array, applies the (assumed) UDF, and then
-- passes the resulting value back to the caller via the resultList.
--
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) ldtCtrl
--   (*) LDR Page:
--   (*) count:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function takeByteArray( resultList, ldtCtrl, ldrSubRec, count )
  local meth = "takeByteArray()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%s) filter(%s) fargs(%s)",
    MOD, meth, tostring(count), tostring(G_Filter), tostring(G_FunctionArgs));
            
  warn("[ERROR]<%s:%s> THIS FUNCTION UNDER CONSTRUCTION", MOD, meth);
  error( ldte.ERR_INTERNAL );

end -- takeByteArray()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LDT Data Record (LDR) FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- LDR routines act specifically on the LDR Sub-Records.

-- ======================================================================
-- ldrInsertList()
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this Sub-Rec's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrSubRec: Hotest of the Warm Sub Records
-- (*) ldtMap: the LDT control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertList(ldrSubRec, ldtMap, listIndex, insertList)
  local meth = "ldrInsertList()";
  GP=E and trace("[ENTER]<%s:%s> LDR_SR(%s) Index(%d) List(%s)",
    MOD, meth, tostring(ldrSubRec), listIndex, tostring( insertList ) );

  GP=F and trace("[DEBUG]<%s:%s> LDT MAP(%s)", MOD, meth, tostring(ldtMap));

  GP=F and trace("[DEBUG]<%s:%s> LDR Rec Summary(%s)", MOD, meth,
   ldrSummary( ldrSubRec ));

  local ldrMap = ldrSubRec[LDR_CTRL_BIN];
  local ldrValueList = ldrSubRec[LDR_LIST_BIN];
  -- local ldrIndexStart = list.size( ldrValueList ) + 1;
  local ldrIndexStart = #ldrValueList + 1;
  local ldrByteArray = ldrSubRec[LDR_BNRY_BIN]; -- might be nil

  GP=F and trace("[DEBUG]: <%s:%s> ldr: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local totalItemsToWrite = #insertList + 1 - listIndex;
  local itemSlotsAvailable = (ldtMap[LS.LdrEntryCountMax] - ldrIndexStart) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    GP=F and debug("[DEBUG]<%s:%s> ALERT!: No space available on LDR(%s)",
      MOD, meth, tostring( ldrMap ));
    debug("[INFO]<%s:%s> SlotsAvail(%d) LdrCntMax(%d) InxStart(%d)", MOD, meth,
      itemSlotsAvailable, ldtMap[LS.LdrEntryCountMax], ldrIndexStart);
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the ldr, then we flag that so the next Warm
  -- List Insert will know in advance to create a new ldr.
  if totalItemsToWrite == itemSlotsAvailable then
    ldtMap[LS.WarmTopFull] = AS_TRUE; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d)::SpaceAvail(%d):WTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and trace("[DEBUG]<%s:%s>ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, listIndex, ldrIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  GP=F and trace("[DEBUG]: <%s:%s>: Post ldr Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring(ldrValueList));

  -- Store our modifications back into the ldr Record Bins
  ldrSubRec[LDR_CTRL_BIN] = ldrMap;
  ldrSubRec[LDR_LIST_BIN] = ldrValueList;

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrInsertList()

-- ======================================================================
-- ldrInsertBytes()
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- to this LDR Byte Array.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- This method is similar to its sibling "ldrInsertList()", but rather
-- than add to the entry list in the LDR LDR_LIST_BIN, it adds to the
-- byte array in the LDR LDR_BNRY_BIN.
-- Parms:
-- (*) ldrSubRec: Hotest of the Warm LDR Records
-- (*) ldtMap: the LDT control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsertBytes( ldrSubRec, ldtMap, listIndex, insertList )
  local meth = "ldrInsertBytes()";
  GP=E and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local ldrMap = ldrSubRec[LDR_CTRL_BIN];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrMap ) );

  local entrySize = ldtMap[LS.LdrByteEntrySize];
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", MOD, meth);
    -- Let the caller handle the error.
    return -1; -- General Badness
  end

  local entryCount = 0;
  if( ldrMap[LDR_ByteEntryCount] ~= nil and
      ldrMap[LDR_ByteEntryCount] ~= 0 )
  then
    entryCount = ldrMap[LDR_ByteEntryCount];
  end
  GP=F and trace("[DEBUG]<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount);

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  -- local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local totalItemsToWrite = #insertList + 1 - listIndex;
  local maxEntries = math.floor(ldtMap[LS.LdrByteCountMax] / entrySize );
  local itemSlotsAvailable = maxEntries - entryCount;
  GP=F and
    trace("[DEBUG]: <%s:%s>:MaxEntries(%d) SlotsAvail(%d) #Total ToWrite(%d)",
    MOD, meth, maxEntries, itemSlotsAvailable, totalItemsToWrite );

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    GP=F and debug("[DEBUG]<%s:%s> ALERT!: No space available on LDR(%s)",
      MOD, meth, tostring( ldrMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the LDR, then we flag that so the next Warm
  -- List Insert will know in advance to create a new LDR.
  if totalItemsToWrite == itemSlotsAvailable then
    ldtMap[LS.WarmTopFull] = AS_TRUE; -- Remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d)::SpaceAvail(%d):WTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- Compute the new space we need in Bytes and either extend existing or
  -- allocate it fresh.
  local totalSpaceNeeded = (entryCount + newItemsStored) * entrySize;
  if ldrSubRec[LDR_BNRY_BIN] == nil then
    ldrSubRec[LDR_BNRY_BIN] = bytes( totalSpaceNeeded );
    GP=F and trace("[DEBUG]<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrSubRec[LDR_BNRY_BIN]));
  else
    GP=F and
    trace("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrSubRec[LDR_BNRY_BIN]));

    -- The API for this call changed (July 2, 2013).  Now use "ensure"
    -- bytes.set_len(ldrSubRec[LDR_BNRY_BIN], totalSpaceNeeded );
    bytes.ensure(ldrSubRec[LDR_BNRY_BIN], totalSpaceNeeded, 1);

    GP=F and
    trace("[DEBUG]:<%s:%s>AFTER: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrSubRec[LDR_BNRY_BIN]));
  end
  local ldrByteArray = ldrSubRec[LDR_BNRY_BIN];

  -- We're packing bytes into a byte array. Put each one in at a time,
  -- incrementing by "entrySize" for each insert value.
  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  -- Compute where we should start inserting in the Byte Array.
  -- WARNING!!! Unlike a C Buffer, This BYTE BUFFER starts at address 1,
  -- not zero.
  local ldrByteStart = 1 + (entryCount * entrySize);

  GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) SpaceAvail(%d) ByteStart(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable, ldrByteStart );

  local byteIndex;
  local insertItem;
  for i = 0, (newItemsStored - 1), 1 do
    byteIndex = ldrByteStart + (i * entrySize);
    insertItem = insertList[i+listIndex];

    GP=F and
    trace("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
      MOD, meth, tostring( ldrByteArray), i, tostring( insertItem ),
      byteIndex );

    bytes.put_bytes( ldrByteArray, byteIndex, insertItem );

    GP=F and trace("[DEBUG]: <%s:%s> Post Append: ByteArray(%s)",
      MOD, meth, tostring(ldrByteArray));
  end -- for each remaining entry

  -- Update the ctrl map with the new count
  ldrMap[LDR_ByteEntryCount] = entryCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post ldr Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrMap), tostring( ldrByteArray ));

  -- Store our modifications back into the ldr Record Bins
  ldrSubRec[LDR_CTRL_BIN] = ldrMap;
  ldrSubRec[LDR_BNRY_BIN] = ldrByteArray;

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrByteArray ));
  return newItemsStored;
end -- ldrInsertBytes()

-- ======================================================================
-- ldrInsert()
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
--
-- Parms:
-- (*) ldrSubRec: Hotest of the Warm LDR Sub-Records
-- (*) ldtMap: the LDT control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrInsert(ldrSubRec,ldtMap,listIndex,insertList )
  local meth = "ldrInsert()";
  return ldrInsertList(ldrSubRec,ldtMap,listIndex,insertList );
end -- ldrInsert()

-- ======================================================================
-- ldrRead()
-- ======================================================================
-- Read ALL, or up to 'count' items from this LDR, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the LDR in FIFO order.
-- Parms:
-- (*) ldrSubRec: Record object for the warm or cold LDT Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control info
-- (*) count: Return this many items
-- Return: the NUMBER of items read from this LDR.
-- ======================================================================
local function ldrRead( ldrSubRec, resultList, ldtCtrl, count )
  local meth = "ldrRead()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d)", MOD, meth, count);

  -- Extract the property map and LDT Map from the LDT Control.
  -- local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local numRead = 0;
  local ldrList = ldrSubRec[LDR_LIST_BIN];
  numRead = readEntryList(resultList, ldtCtrl, ldrList, count);
  return numRead;
end -- ldrRead()

-- ======================================================================
-- ldrTake()
-- ======================================================================
-- Read and remove up to 'count' items from this LDR, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the LDR in FIFO order.
-- Parms:
-- (*) src:
-- (*) topRec:
-- (*) ldrSubRec: Record object for the warm or cold LDT Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control info
-- (*) count: Return this many items
-- Return: the NUMBER of items read from this LDR.
-- ======================================================================
local function ldrTake( src, topRec, ldrSubRec, resultList, ldtCtrl, count )
  local meth = "ldrTake()";

  -- Extract the property map and LDT Map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local numRead = 0;
  local empty = false;

  local ldrList = ldrSubRec[LDR_LIST_BIN];

  numRead, empty = takeEntryList(resultList, ldtCtrl, ldrList, count);
  -- Update the SubRec with the updated Entry List, unless it is empty, in
  -- which case we remove it.
  if empty then
    local digestString = tostring(record.digest(ldrSubRec));
    ldt_common.removeSubRec( src, topRec, propMap, digestString );
  else
    ldrSubRec[LDR_LIST_BIN] = ldrList;
    ldt_common.updateSubRec( src, ldrSubRec );
  end
  return numRead, empty;
end -- ldrTake()

-- ======================================================================
-- digestListAccess()
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count: Return this many items from the Digest List.
-- (*) deleteFlag: When == true, remove the items that we read.
--
-- Return: Return the amount read from the Digest List.
-- =====================================================================
local function
digestListAccess(src, topRec, resultList, ldtCtrl, digestList, count, delFlag)
  local meth = "digestListAccess()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d)  DelFlag(%s)",
    MOD, meth, count, tostring(delFlag));

  GP=F and trace("[DEBUG]: <%s:%s> Count(%d) DigList(%s) ResList(%s)",
    MOD, meth, count, tostring(digestList), tostring( resultList ));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Process the DigestList bottom to top, pulling in each digest in
  -- turn, opening the ldrSubRec and reading records (as necessary), until
  -- we've read "count" items.
  -- NOTE: This method works for both the Warm and Cold lists.
  local remaining = count;
  local totalAmountRead = 0;
  local ldrItemsRead = 0;
  local empty = false;
  -- local dirCount = list.size(digestList );
  local dirCount = #digestList;
  local ldrRec;
  local digestString;

  GP=F and trace("[DEBUG]:<%s:%s>:DirCount(%d)  Reading DigestList(%s)",
    MOD, meth, dirCount, tostring(digestList) );

  -- Read each Data ldr, adding to the resultList, until we either bypass
  -- the readCount, or we hit the end (either readCount is large, or the ALL
  -- flag is set).
  local listEndMarker = #digestList; -- track the moving end of the list.
  for dirIndex = dirCount, 1, -1 do
    -- Record Digest MUST be in string form
    digestString = tostring(digestList[ dirIndex ]);
    GP=F and trace("[DEBUG]: <%s:%s>: Opening Data ldr:Index(%d)Digest(%s):",
    MOD, meth, dirIndex, digestString );
    ldrRec = ldt_common.openSubRec( src, topRec, digestString );
    
    -- resultList is passed by reference and we can just add to it.
    if delFlag then
      ldrItemsRead, empty =
        ldrTake( src, topRec, ldrRec, resultList, ldtCtrl, remaining);
    else
      ldrItemsRead = ldrRead(ldrRec, resultList, ldtCtrl, remaining);
    end
    totalAmountRead = totalAmountRead + ldrItemsRead;

    GP=F and
    trace("[DEBUG]<%s:%s>:after read/take:NumRead(%d) DirIdx(%d) ResList(%s)", 
      MOD, meth, ldrItemsRead, dirIndex, tostring( resultList ));
    
    -- If there's still more to take, AND if the delete flag is set,
    -- then ldrTake() will have removed this SubRecord.  So, we now remove
    -- the digest entry from the digestList.
    -- List.  Otherwise, just close the SubRec.
    -- if delFlag and totalAmountRead <= count then
    -- if delFlag and ldtItemsRead >= remaining then
    if delFlag then
      if empty then
        -- We've removed this Sub-Record, so remove it from the list.
        list.trim(digestList, listEndMarker);
        listEndMarker = listEndMarker - 1;
        GP=F and trace("[NOTE]<%s:%s> WDL Trim: LEM(%d) i(%d)",
          MOD, meth, listEndMarker, dirIndex);
      else
        -- We've only modified the Sub-Record.  Update it.
        ldt_common.updateSubRec( src, ldrRec );
      end

    else
      -- The Read Case.  We can close this SubRec.
      ldt_common.closeSubRecDigestString( src, digestString, false);
    end

    if ( ldrItemsRead >= remaining or totalAmountRead >= count ) then
      return totalAmountRead;
    end

    -- Get ready for the next iteration.  Adjust our numbers for the
    -- next round
    remaining = remaining - ldrItemsRead;
  end -- for each Data ldr Record

  GP=E and trace("[EXIT]: <%s:%s> totalAmountRead(%d) ResultListSummary(%s) ",
  MOD, meth, totalAmountRead, ldt_common.summarizeList(resultList));
  return totalAmountRead;
end -- digestListAccess()

-- ======================================================================
-- digestListRead()
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count: Return this many items
-- Return: Return the amount read from the Digest List.
-- ======================================================================
local function
digestListRead(src, topRec, resultList, ldtCtrl, digestList, count)

  -- Call Access with DeleteFlag = false.
  return digestListAccess(src, topRec, resultList, ldtCtrl, digestList,
                          count, false);
end -- digestListRead()

-- ======================================================================
-- digestListTake()
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count:  Return this many items
-- Return: Return the amount read from the Digest List.
-- ======================================================================
local function
digestListTake(src, topRec, resultList, ldtCtrl, digestList, count)

  -- Call Access with DeleteFlag = true
  return digestListAccess(src, topRec, resultList, ldtCtrl, digestList,
                          count, true);
end -- digestListTake()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- HOT LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The Hot List is an USER DATA ENTRY list that is managed IN THE RECORD.
-- The top N (most recent) values are held in the record, and then they
-- are aged out into the Warm List (a list of data pages) as they are
-- replaced by newer (more recent) data entries.  Hot List functions
-- directly manage the user data - and always in LIST form (not in
-- compact binary form).

-- ======================================================================
-- hotListRead()
-- ======================================================================
-- Read from the Hot List and return the contents in "resultList".
-- Parms:
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) count:  Return this many items
-- Return 'count' items from the Hot List
-- ======================================================================
local function hotListRead( resultList, ldtCtrl, count)
  local meth = "hotListRead()";
  GP=E and trace("[ENTER]:<%s:%s>Count(%d)", MOD, meth, count);

  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local hotList = ldtMap[LS.HotEntryList];

  local numRead = readEntryList(resultList, ldtCtrl, hotList, count);

  GP=E and trace("[DEBUG]<%s> HotListResult(%s)", meth, tostring(resultList));

  GP=E and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    MOD, meth, ldt_common.summarizeList(resultList) );
  return resultList;
end -- hotListRead()

-- ======================================================================
-- hotListTake()
-- ======================================================================
-- Take from the Hot List and return the contents in "resultList".
-- Parms:
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) count:  Return this many items
-- Return:
-- 1: 'count' items from the Hot List
-- 2: 'empty' -- boolean -- if the hot list was left EMPTY after the take.
-- ======================================================================
local function hotListTake( resultList, ldtCtrl, count)
  local meth = "hotListTake()";
  GP=E and trace("[ENTER]:<%s:%s>Count(%d)", MOD, meth, count);

  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local hotList = ldtMap[LS.HotEntryList];

  -- In some cases, as with large data elements, we have no HotList.
  if hotList == nil or #hotList == 0 then
    return 0, true;
  end

  local numRead, empty = takeEntryList(resultList,ldtCtrl,hotList,count);

  GP=E and trace("[DEBUG]<%s:%s> HotListResult(%s)",
    MOD, meth, tostring(resultList));
  GP=E and trace("[DEBUG]<%s:%s> HOT LIST(%s)", MOD, meth,
    tostring(ldtMap[LS.HotEntryList]));

  GP=E and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    MOD, meth, ldt_common.summarizeList(resultList) );
  return numRead, empty;
end -- hotListTake()

-- ======================================================================
-- extractHotListTransferList( ldtMap )
-- ======================================================================
-- Extract the oldest N elements (as defined in ldtMap) and create a
-- list that we return.  Also, reset the HotList to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTES:
-- (1) We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB_RECORD operations have succeeded.
-- (2) We don't need to use ldtCtrl as a parameter -- ldtMap is ok here.
-- ======================================================================
local function extractHotListTransferList( ldtMap )
  local meth = "extractHotListTransferList()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = ldtMap[LS.HotListTransfer];
  local oldHotEntryList = ldtMap[LS.HotEntryList];
  local newHotEntryList = list();
  local resultList = list.take( oldHotEntryList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  -- for i = 1, list.size(oldHotEntryList) - transAmount, 1 do 
  local oldHotListSize = #oldHotEntryList;
  for i = 1, oldHotListSize - transAmount, 1 do 
    list.append( newHotEntryList, oldHotEntryList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldHotList(%s) NewHotList(%s) ResultList(%s)",
    MOD, meth, tostring(oldHotEntryList), tostring(newHotEntryList),
    tostring(resultList));

  -- Point to the new Hot List and update the Hot Count.
  ldtMap[LS.HotEntryList] = newHotEntryList;
  oldHotEntryList = nil;
  local helic = ldtMap[LS.HotEntryListItemCount];
  ldtMap[LS.HotEntryListItemCount] = helic - transAmount;

  GP=E and trace("[EXIT]: <%s:%s> ResultList(%s)",
    MOD, meth, ldt_common.summarizeList(resultList));
  return resultList;
end -- extractHotListTransferList()

-- ======================================================================
-- hotListFull( ldtMap )
-- ======================================================================
-- Return true if the HotList is Full
-- (*) ldtMap: the map for the LDT Bin
-- NOTE: This is in its own function because it is possible that we will
-- want to add more sophistication in the future.
-- ======================================================================
local function hotListFull( ldtMap )
  -- return list.size( ldtMap[LS.HotEntryList] ) >= ldtMap[LS.HotListMax];
  return #ldtMap[LS.HotEntryList] >= ldtMap[LS.HotListMax];
end

-- ======================================================================
-- hotListHasRoom( ldtMap, insertValue )
-- ======================================================================
-- Return true if there's room, otherwise return false.
-- (*) ldtMap: the map for the LDT Bin
-- (*) insertValue: the new value to be pushed on the stack
-- NOTE: This is in its own function because it is possible that we will
-- want to add more sophistication in the future.
-- ======================================================================
local function hotListHasRoom( ldtMap, insertValue )
  local meth = "hotListHasRoom()";
  GP=E and trace("[ENTER]: <%s:%s> : ", MOD, meth );
  local result = true;  -- This is the usual case

  local hotListLimit = ldtMap[LS.HotListMax];
  local hotList = ldtMap[LS.HotEntryList];
  -- if list.size( hotList ) >= hotListLimit then
  if #hotList >= hotListLimit then
    return false;
  end

  GP=E and trace("[EXIT]: <%s:%s> Result(%s) : ", MOD, meth, tostring(result));
  return result;
end -- hotListHasRoom()

-- ======================================================================
-- hotListInsert()
-- ======================================================================
-- Insert a value at the end of the Hot Entry List.  The caller has 
-- already verified that space exists, so we can blindly do the insert.
--
-- The MODE of storage depends on what we see in the valueMap.  If the
-- valueMap holds a BINARY type, then we are going to store it in a special
-- binary bin.  Here are the cases:
-- (1) Warm List: The LDR Record employs a List Bin and Binary Bin, where
--    the individual entries are packed.  In the LDR Record, there is a
--    Map (control information) showing the status of the packed Binary bin.
-- (2) Cold List: Same LDR format as the Warm List LDR Record.
--
-- Change in plan -- All items go on the HotList, regardless of type.
-- Only when we transfer to Warm/Cold do we employ the COMPACT STORAGE
-- trick of packing bytes contiguously in the Binary Bin.
--
-- The Top LDT page (and the individual LDR pages) have the control
-- data about the byte entries (entry size, entry count).
-- Parms:
-- (*) ldtCtrl: the control structure for the LDT Bin
-- (*) newStorageValue: the new value to be pushed on the stack
-- ======================================================================
local function hotListInsert( ldtCtrl, newStorageValue  )
  local meth = "hotListInsert()";
  GP=E and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    MOD, meth, tostring(newStorageValue) );

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Update the hot list with a new element (and update the map)
  local hotList = ldtMap[LS.HotEntryList];
  -- GP=DEBUG and trace("[HEY!!]<%s:%s> Appending to Hot List(%s)", 
    -- MOD, meth,tostring(hotList));
  -- list.append( ldtMap[LS.HotEntryList], newStorageValue );
  list.append( hotList, newStorageValue );
  ldtMap[LS.HotEntryList] = hotList;
  
  -- Update the HotList Count, but the overall Count has to be
  -- updated upstream (in localPush() ).
  local hotCount = ldtMap[LS.HotEntryListItemCount];
  ldtMap[LS.HotEntryListItemCount] = (hotCount + 1);

  GP=E and trace("[EXIT]: <%s:%s> : LDT List Result(%s)",
    MOD, meth, tostring( ldtCtrl ) );

  return 0;  -- all is well
end -- hotListInsert()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||         WARM LIST FUNCTIONS         ||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- ======================================================================
-- warmListSubRecCreate()
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) ldtCtrl: The main structure of the LDT Bin.
-- ======================================================================
-- Create and initialize a new LDR, load the new digest for that
-- new LDR into the ldtMap (the warm dir list), and return it.
local function   warmListSubRecCreate( src, topRec, ldtCtrl )
  local meth = "warmListSubRecCreate()";
  GP=E and trace("[ENTER]: <%s:%s> SRC(%s) ldtCtrl(%s)", MOD, meth,
    tostring( src ), ldtSummaryString( ldtCtrl ));

  -- Set up the TOP REC prop and ctrl maps
  local propMap    = ldtCtrl[LDT_PROP_MAP];
  local ldtMap     = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- Create the Aerospike Sub-Record, initialize the bins: Ctrl, List
  -- Notes: 
  -- (1) All Field Names start with UPPER CASE.
  -- (2) Remember to add the ldrSubRec to the SRC
  local ldrSubRec = ldt_common.createSubRec(src, topRec, ldtCtrl, RT_SUB );
  local subRecPropMap = ldrSubRec[SUBREC_PROP_BIN];
  
  -- The common createSubRec() function creates the Sub-Record and sets up
  -- the property bin.  It's our job to set up the LSTACK-Specific bins
  -- for a Warm List Sub-Record.
  local subRecCtrlMap = map();
  subRecCtrlMap[LDR_ByteEntryCount] = 0; -- When Bytes are used

  ldrSubRec[LDR_CTRL_BIN] = subRecCtrlMap;
  ldrSubRec[LDR_LIST_BIN] = list();
  -- NOTE: Leave LDR_BNRY_BIN empty for now.

  -- Add our new ldrSubRec (the digest) to the WarmDigestList
  local ldrDigest = record.digest( ldrSubRec );
  -- TODO: @TOBY: Remove these trace calls when fully debugged.
  GP=DEBUG and trace("[DEBUG]<%s:%s> Appending new SubRec(%s) to WarmList(%s)",
    MOD, meth, tostring(ldrDigest), tostring(ldtMap[LS.WarmDigestList]));

  list.append( ldtMap[LS.WarmDigestList], ldrDigest );

  GP=F and trace("[DEBUG]<%s:%s>Post LDR Append:NewLDR(%s) LdtMap(%s)",
    MOD, meth, tostring(ldrDigest), tostring(ldtMap));
   
  -- Increment the Warm Count
  local warmLdrCount = ldtMap[LS.WarmListDigestCount];
  ldtMap[LS.WarmListDigestCount] = (warmLdrCount + 1);

  GP=E and trace("[EXIT]: <%s:%s> LDR Summary(%s) ",
    MOD, meth, ldrSummary(ldrSubRec));
  return ldrSubRec;
end --  warmListSubRecCreate()

-- ======================================================================
-- extractWarmListTransferList( ldtCtrl );
-- ======================================================================
-- Extract the oldest N digests from the WarmList (as defined in ldtMap)
-- and create a list that we return.  Also, reset the WarmList to exclude
-- these elements.  -- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTE: We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB-REC  operations have succeeded.
-- ======================================================================
local function extractWarmListTransferList( ldtCtrl )
  local meth = "extractWarmListTransferList()";
  GP=E and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Extract the main property map and LDT Map from the LDT Control.
  local ldtPropMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap     = ldtCtrl[LDT_CTRL_MAP];

  -- Get the first N (transfer amount) list elements
  local transAmount = ldtMap[LS.WarmListTransfer];
  local oldWarmDigestList = ldtMap[LS.WarmDigestList];
  local newWarmDigestList = list.new(ldtMap[LS.WarmListMax]);
  local resultList = list.take( oldWarmDigestList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  -- for i = 1, list.size(oldWarmDigestList) - transAmount, 1 do 
  local oldWarmListSize = #oldWarmDigestList;
  for i = 1, oldWarmListSize - transAmount, 1 do 
    newWarmDigestList[i] = oldWarmDigestList[i+transAmount];
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldWarmList(%s) NewWarmList(%s)ResList(%s) ",
    MOD, meth, tostring(oldWarmDigestList), tostring(newWarmDigestList),
    tostring(resultList));

  -- Point to the new Warm List and update the Hot Count.
  ldtMap[LS.WarmDigestList] = newWarmDigestList;
  oldWarmDigestList = nil;
  ldtMap[LS.WarmListDigestCount] = ldtMap[LS.WarmListDigestCount] - transAmount;

  GP=E and trace("[EXIT]: <%s:%s> ResultList(%s) LdtMap(%s)",
      MOD, meth, ldt_common.summarizeList(resultList), tostring(ldtMap));

  return resultList;
end -- extractWarmListTransferList()

-- ======================================================================
-- warmListHasRoom( ldtMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) ldtMap: the map for the LDT Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmListHasRoom( ldtMap )
  local meth = "warmListHasRoom()";
  local decision = 1; -- Start Optimistic (most times answer will be YES)
  GP=E and trace("[ENTER]: <%s:%s> Bin Map(%s)", 
    MOD, meth, tostring( ldtMap ));

  if ldtMap[LS.WarmListDigestCount] >= ldtMap[LS.WarmListMax] then
    decision = 0;
  end

  GP=E and trace("[EXIT]: <%s:%s> Decision(%d)", MOD, meth, decision );
  return decision;
end -- warmListHasRoom()

-- ======================================================================
-- warmListGetTop()
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtMap: the LDT control Map (ldtCtrl not needed here)
-- ======================================================================
local function warmListGetTop( src, topRec, ldtMap )
  local meth = "warmListGetTop()";
  GP=E and trace("[ENTER]: <%s:%s> ldtMap(%s)", MOD, meth, tostring( ldtMap ));

  local warmDigestList = ldtMap[LS.WarmDigestList];
-- local digestString = tostring( warmDigestList[ list.size(warmDigestList) ]);
  local digestString = tostring( warmDigestList[ #warmDigestList ]);

  GP=F and trace("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)", 
      -- MOD, meth, digestString, list.size( warmDigestList ));
      MOD, meth, digestString, #warmDigestList );

  local topWarmSubRec = ldt_common.openSubRec( src, topRec, digestString );

  GP=E and trace("[EXIT]: <%s:%s> digest(%s) result(%s) ",
    MOD, meth, digestString, ldrSummary( topWarmSubRec ) );
  return topWarmSubRec;
end -- warmListGetTop()

-- ======================================================================
-- warmListInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the warm
-- dir list -- a directory of warm lstack Data Records that will contain 
-- the data entries.
-- A New Feature to insert is the "StoreLimit" aspect.  If we are over the
-- storage limit, then before we insert into the warm list, we're going to
-- release some old storage.  This storage may be in the Warm List or in
-- the Cold List (or both), however, we only care about WarmList storage
-- BEFORE the warmlist insert. 
-- Notice that we're basically dealing in item counts, NOT in total storage
-- bytes.  That (total byte storage) is future work.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtCtrl: the control structure of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function warmListInsert( src, topRec, ldtCtrl, entryList )
  local meth = "warmListInsert()";
  local rc = 0;

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];
  local warmDigestList = ldtMap[LS.WarmDigestList];

  GP=E and trace("[ENTER]<%s:%s> WDL Size(%d) WDL(%s)",
    MOD, meth, #warmDigestList, tostring(warmDigestList));

  GP=F and trace("[DEBUG]:<%s:%s> LDT CTRL Summary(%s)", MOD, meth,
    ldtSummaryString(ldtCtrl));

  local topWarmSubRec;

  -- With regard to the Ldt Data Record (LDR) Pages, whether we create a new
  -- LDR or open an existing LDR, we save the current count and close the
  -- LDR page.
  -- Note that the last write may have filled up the warmTopSubRec, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
--if list.size( warmDigestList ) == 0 or ldtMap[LS.WarmTopFull] == AS_TRUE then
  if #warmDigestList == 0 or ldtMap[LS.WarmTopFull] == AS_TRUE then
    GP=F and trace("[DEBUG]: <%s:%s> Calling SubRec Create ", MOD, meth );
    topWarmSubRec = warmListSubRecCreate(src, topRec, ldtCtrl ); -- create new
    ldtMap[LS.WarmTopFull] = AS_FALSE; -- reset for next time.
  else
    GP=F and trace("[DEBUG]: <%s:%s> Calling Get TOP ", MOD, meth );
    topWarmSubRec = warmListGetTop( src, topRec, ldtMap ); -- open existing
  end
  GP=F and trace("[DEBUG]: <%s:%s> Post 'GetTop': LdtMap(%s) ", 
    MOD, meth, tostring( ldtMap ));

  if( topWarmSubRec == nil ) then
    warn("[ERROR] <%s:%s> Internal Error: Top Warm SubRec is NIL!!",MOD,meth);
    error( ldte.ERR_INTERNAL );
  end

  -- We have a warm SubRec -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new SubRec and write the rest.
  -- local totalEntryCount = list.size( entryList );
  local totalEntryCount = #entryList;
  GP=F and trace("[DEBUG]: <%s:%s> Calling SubRec Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  local countWritten = ldrInsert( topWarmSubRec, ldtMap, 1, entryList );
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in SubRec Insert(1)", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end
  local itemsLeft = totalEntryCount - countWritten;
  if itemsLeft > 0 then
    ldt_common.updateSubRec( src, topWarmSubRec );

    -- We're done with this Sub-Rec. Mark it closed, but it is dirty.
    ldt_common.closeSubRec( src, topWarmSubRec, true );

    GP=F and trace("[DEBUG]:<%s:%s>Calling SubRec Create: AGAIN!!", MOD, meth );
    topWarmSubRec = warmListSubRecCreate( src, topRec, ldtCtrl ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot list.
  GP=F and trace("[DEBUG]: <%s:%s> Calling SubRec Insert: List(%s) AGAIN(%d)",
    MOD, meth, tostring( entryList ), countWritten + 1);
    countWritten =
        ldrInsert( topWarmSubRec, ldtMap, countWritten+1, entryList );
    if( countWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in SubRec Insert(2)", MOD, meth);
      error( ldte.ERR_INTERNAL );
    end
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm SubRec Write: CW(%d) IL(%d) ",
        MOD, meth, countWritten, itemsLeft );
      error( ldte.ERR_INTERNAL );
    end
  end

  -- NOTE: We do NOT have to update the WarmDigest Count here; that is done
  -- in the warmListSubRecCreate() call.

  -- All done -- Save the info of how much room we have in the top Warm
  -- SubRec (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving ldtCtrl (%s) Before Update ",
    MOD, meth, tostring( ldtCtrl ));
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]: <%s:%s> SubRec Summary before storage(%s)",
    MOD, meth, ldrSummary( topWarmSubRec ));

  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC Update ", MOD, meth );
  local status = ldt_common.updateSubRec( src, topWarmSubRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ", 
    MOD, meth, tostring(status));

  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC Close ", MOD, meth );
  status = ldt_common.closeSubRec( src, topWarmSubRec, true );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));

  -- Notice that the TOTAL ITEM COUNT of the LDT doesn't change.  We've only
  -- moved entries from the hot list to the warm list.

  return rc;
end -- warmListInsert

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- COLD LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================


-- ======================================================================
-- releaseStorage()::
-- ======================================================================
-- Release the storage in this digest list.  Either iterate thru the
-- list and release it immediately (if that's the only option), or
-- deliver the digestList to a component that can schedule the digest
-- to be cleaned up later.
-- ======================================================================
-- @RAJ @TOBY TODO: Change inside to crec_release() call, after the
-- crec_release() function is (eventually) implemented.
-- ======================================================================
local function releaseStorage( src, topRec, ldtCtrl, digestList )
  local meth = "releaseStorage()";
  local rc = 0;
  GP=E and trace("[ENTER]:<%s:%s> ldtSummary(%s) digestList(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ), tostring(digestList));

  GP=F and trace("[DEBUG]<%s:%s> LSTACK SubRecord Eviction: Subrec List(%s)",
    MOD, meth, tostring(digestList));

  local subrec;
  local digestString;
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- if( digestList == nil or list.size( digestList ) == 0 ) then
  if( digestList == nil or #digestList == 0 ) then
    warn("[INTERNAL ERROR]<%s:%s> DigestList is nil or empty", MOD, meth );
  else
    -- local listSize = list.size( digestList );
    local listSize = #digestList;
    for i = 1, listSize, 1 do
      digestString = tostring( digestList[i] );
      local subrec = ldt_common.openSubRec( src, topRec, digestString );
      rc = ldt_common.removeSubRec( src, topRec, propMap, digestString );
      if( rc == nil or rc == 0 ) then
        GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
      else
        warn("[SUB DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, ldtBinName);
        error( ldte.ERR_SUBREC_DELETE );
      end
    end
  end

  GP=E and trace("[EXIT]: <%s:%s> ", MOD, meth );
  return rc;
end -- releaseStorage()

-- ======================================================================
-- coldDirHeadCreate()
-- ======================================================================
-- Set up a new Head Directory page for the cold list.  The Cold List Dir
-- pages each hold a list of digests to data pages.  Note that
-- the data pages (LDR pages) are already built from the warm list, so
-- the cold list just holds those LDR digests after the record agest out
-- of the warm list. 
--
-- New for the summer of 2013::We're going to allow data to gracefully age
-- out by limiting the number of active Cold Directory Pages that we'll have
-- in an LSTACK at one time. So, if the limit is set to "N", then we'll
-- have (N-1) FULL directory pages, and one directory page that is being
-- filled up.  We check ONLY when it's time to create a new directory head,
-- so that is on the order of once every 10,000 inserts (or so).
--
-- Parms:
-- (*) src: Subrec Context
-- (*) topRec: the top record -- needed when we create a new dir and LDR
-- (*) ldtCtrl: the control map of the top record
-- (*) Space Estimate of the number of items needed
-- Return:
-- Success: NewColdHead Sub-Record Pointer
-- Error:   Nil
-- ======================================================================
local function coldDirHeadCreate( src, topRec, ldtCtrl, spaceEstimate )
  local meth = "coldDirHeadCreate()";
  GP=E and trace("[ENTER]<%s:%s>LDT(%s)",MOD,meth,ldtSummaryString(ldtCtrl));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];
  local ldrDeleteList; -- List of LDR subrecs to be removed (eviction)
  local dirDeleteList; -- List of Cold Directory Subrecs to be removed.
  local ldrItemCount = ldtMap[LS.LdrEntryCountMax];
  local subrecDeleteCount; -- ALL subrecs (LDRs and Cold Dirs)
  local coldDirMap;
  local coldDirList;
  local coldDirRec;
  local returnColdHead; -- this is what we return
  local coldDirDigest;
  local coldDirDigestString;
  local createNewHead = true;
  local itemsDeleted = 0;
  local subrecsDeleted = 0;

  -- This is new code to deal with the expiration/eviction of old data.
  -- Usually, it will be data in the Cold List.  We will release cold list
  -- data when we create a new ColdDirectoryHead.  That's the best time
  -- to assess what's happening in the cold list. (July 2013: tjl)
  --
  -- In the unlikely event that the user has specified that they want ONE
  -- (and only one) Cold Dir Record, that means that we shouldn't actually
  -- create a NEW Cold Dir Record.  We should just free up the digest
  -- list (release the sub rec storage) and return the existing cold dir
  -- head -- just in a freshened state.
  local coldDirRecCount = ldtMap[LS.ColdDirRecCount];
  local coldDirRecMax = ldtMap[LS.ColdDirRecMax];
  GP=F and trace("[DEBUG]<%s:%s>coldDirRecCount(%s) coldDirRecMax(%s)",
    MOD, meth, tostring(coldDirRecCount), tostring(coldDirRecMax));
  if( coldDirRecMax == 1 and coldDirRecCount == 1 ) then
    GP=F and trace("[DEBUG]<%s:%s>Special Case ONE Dir", MOD, meth );
    -- We have the weird special case. We will NOT delete this Cold Dir Head
    -- and Create a new one.  Instead, we will just clean out
    -- the Digest List enough so that we have room for "newItemCount".
    -- We expect that in most configurations, the transfer list coming in
    -- will be roughly half of the array size.  We don't expect to see
    -- a "newCount" that is greater than the Cold Dir Limit.
    -- ALSO -- do NOT drop into the code below that Creates a new head.
    createNewHead = false;
    coldDirDigest = ldtMap[LS.ColdDirListHead];
    coldDirDigestString = tostring( coldDirDigest );
    coldDirRec = ldt_common.openSubRec( src, topRec, coldDirDigestString );
    if( coldDirRec == nil ) then
      warn("[INTERNAL ERROR]<%s:%s> Can't open Cold Head(%s)", MOD, meth,
        coldDirDigestString );
      error( ldte.ERR_SUBREC_OPEN );
    end
    coldDirList = coldDirRec[COLD_DIR_LIST_BIN];
    if( spaceEstimate >= ldtMap[LS.ColdListMax] ) then
      -- Just clear out the whole thing.
      ldrDeleteList = coldDirList; -- Pass this on to "release storage"
      coldDirRec[COLD_DIR_LIST_BIN] = list(); -- reset the list.
    else
      -- Take gets the [1..N] elements.
      -- Drop gets the [(N+1)..end] elements (it drops [1..N] elements)
      ldrDeleteList = list.take( coldDirList, spaceEstimate );
      local saveList = list.drop( coldDirList, spaceEstimate );
      coldDirRec[COLD_DIR_LIST_BIN] = saveList;
    end

    -- Gather up some statistics:
    -- Track the sub-record count (LDRs and Cold Dirs).  Notice that the
    -- Cold Dir here stays, so we have only LDRs.
    subrecsDeleted = list.size( ldrDeleteList );
    -- Track the items -- assume that all of the SUBRECS were full.
    itemsDeleted = subrecsDeleted * ldrItemCount;

    -- Save the changes to the Cold Head
    -- updateSubrec( src, coldDirRec, coldDirDigest );
    ldt_common.updateSubRec( src, coldDirRec );
    returnColdHead = coldDirRec;

  elseif( coldDirRecCount >= coldDirRecMax ) then
    GP=F and trace("[DEBUG]<%s:%s>Release Cold Dirs: Cnt(%d) Max(%d)",
    MOD, meth, coldDirRecCount, coldDirRecMax );
    -- Release as many cold dirs as we are OVER the max.  Release
    -- them in reverse order, starting with the tail.  We put all of the
    -- LDR subrec digests in the delete list, followed by the ColdDir 
    -- subrec.
    local coldDirCount = (coldDirRecCount + 1) - coldDirRecMax;
    local tailDigest = ldtMap[LS.ColdDirListTail];
    local tailDigestString = tostring( tailDigest );
    GP=F and trace("[DEBUG]<%s:%s>Cur Cold Tail(%s)", MOD, meth,
      tostring( tailDigestString ));
    ldrDeleteList = list();
    dirDeleteList = list();
    while( coldDirCount > 0 ) do
      if( tailDigestString == nil or tailDigestString == 0 ) then
        -- Something is wrong -- don't continue.
        warn("[INTERNAL ERROR]<%s:%s> Cold Tail is broken", MOD, meth );
        break;
      else
        -- Open the Cold Dir Record, add the digest list to the delete
        -- list and move on to the next Cold Dir Record.
        -- Note that we track the LDRs and the DIRs separately.
        -- Also note the two different types of LIST APPEND.
        coldDirRec = ldt_common.openSubRec( src, topRec, tailDigestString );
        -- Append a digest LIST to the LDR delete list
        listAppend( ldrDeleteList, coldDirRec[COLD_DIR_LIST_BIN] );
        -- Append a cold Dir Digest to the DirDelete list
        list.append( dirDeleteList, tailDigest ); 

        -- Move back one to the previous ColdDir Rec.  Make it the NEW TAIL.
        coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
        tailDigest = coldDirMap[CDM.PrevDirRec];
        GP=F and trace("[DEBUG]<%s:%s> Cur Tail(%s) Next Cold Dir Tail(%s)",
          MOD, meth, tailDigestString, tostring(tailDigest) );
        tailDigestString = tostring(tailDigest);
        
        -- It is best to adjust the new tail now, even though in some
        -- cases we might remove this cold dir rec as well.
        coldDirRec = ldt_common.openSubRec( src, topRec, tailDigestString );
        coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
        coldDirMap[CDM.NextDirRec] = 0; -- this is now the tail

        -- If we go around again -- we'll need this.
        tailDigestString = record.digest( coldDirRec );
      end -- else tail digest ok
      coldDirCount = coldDirCount - 1; -- get ready for next iteration
    end -- while; count down Cold Dir Recs

    -- Update the LAST Cold Dir that we were in.  It's the new tail
    -- updateSubrec( src, coldDirRec, coldDirDigest );
    ldt_common.updateSubRec( src, coldDirRec );

    -- Gather up some statistics:
    -- Track the sub-record counts (LDRs and Cold Dirs). 
    -- Track the items -- assume that all of the SUBRECS were full.
    itemsDeleted = list.size(ldrDeleteList) * ldrItemCount;
    subrecsDeleted = list.size(ldrDeleteList) + list.size(dirDeleteList);

  end -- cases for when we remove OLD storage

  -- If we did some deletes -- clean that all up now.
  -- Update the various statistics (item and subrec counts)
  if( itemsDeleted > 0 or subrecsDeleted > 0 ) then
    local subrecCount = propMap[PM.SubRecCount];
    propMap[PM.SubRecCount] = subrecCount - subrecsDeleted;

    local itemCount = propMap[PM.ItemCount];
    propMap[PM.ItemCount] = itemCount - itemsDeleted;

    -- Now release any freed subrecs.
    releaseStorage( src, topRec, ldtCtrl, ldrDeleteList );
    releaseStorage( src, topRec, ldtCtrl, dirDeleteList );
  end

  -- Now -- whether or not we removed some old storage above, NOW are are
  -- going to add a new Cold Directory HEAD.
  if( createNewHead == true ) then
    GP=F and trace("[DEBUG]<%s:%s>Regular Cold Head Case", MOD, meth );

    -- Create the Cold Head Record, initialize the bins: Ctrl, List
    -- Also -- now that we have a DOUBLY linked list, get the NEXT Cold Dir,
    -- if present, and have it point BACK to this new one.
    --
    -- Note: All Field Names start with UPPER CASE.
    -- Use the common createSubRec() call to create the new Cold Head
    -- subrec and set up the common properties.  All of the CH-specific
    -- stuff goes here.
    -- Remember to add the newColdHeadRec to the SRC.
    local newColdHeadRec = ldt_common.createSubRec(src,topRec,ldtCtrl,RT_CDIR);
    -- The SubRec is created and the PropMap is set up. So, now we
    -- finish the job and set up the rest.
    local newColdHeadMap     = map();
    newColdHeadMap[CDM.NextDirRec] = 0; -- no other Dir Records (yet).
    newColdHeadMap[CDM.PrevDirRec] = 0; -- no other Dir Records (yet).
    newColdHeadMap[CDM.DigestCount] = 0; -- no digests in the list -- yet.

    local newColdHeadPropMap = newColdHeadRec[SUBREC_PROP_BIN];

    -- Update our global counts ==> One more Cold Dir Record.
    ldtMap[LS.ColdDirRecCount] = coldDirRecCount + 1;

    -- Plug this directory into the (now doubly linked) chain of Cold Dir
    -- Records (starting at HEAD).
    local oldColdHeadDigest = ldtMap[LS.ColdDirListHead];
    local newColdHeadDigest = newColdHeadPropMap[PM.SelfDigest];

    newColdHeadMap[CDM.NextDirRec] = oldColdHeadDigest;
    newColdHeadMap[CDM.PrevDirRec] = 0; -- Nothing ahead of this one, yet.
    ldtMap[LS.ColdDirListHead] = newColdHeadPropMap[PM.SelfDigest];

    GP=F and trace("[DEBUG]<%s:%s> New ColdHead = (%s) Cold Next = (%s)",
      MOD, meth, tostring(newColdHeadDigest),tostring(oldColdHeadDigest));

    -- Get the NEXT Cold Dir (the OLD Head) if there is one, and set it's
    -- PREV pointer to THIS NEW HEAD.  This is the one downfall for having a
    -- double linked list, but since we now need to traverse the list in
    -- both directions, it's a necessary evil.
    if( oldColdHeadDigest == nil or oldColdHeadDigest == 0 ) then
      -- There is no Next Cold Dir, so we're done.
      GP=F and trace("[DEBUG]<%s:%s> No Next CDir (assign ZERO)",MOD, meth );
    else
      -- Regular situation:  Go open the old ColdDirRec and update it.
      local oldColdHeadDigestString = tostring(oldColdHeadDigest);
      local oldColdHeadRec =
        ldt_common.openSubRec(src,topRec,oldColdHeadDigestString);
      if( oldColdHeadRec == nil ) then
        warn("[ERROR]<%s:%s> oldColdHead NIL from openSubrec: digest(%s)",
          MOD, meth, oldColdHeadDigestString );
        error( ldte.ERR_SUBREC_OPEN );
      end
      local oldColdHeadMap = oldColdHeadRec[COLD_DIR_CTRL_BIN];
      oldColdHeadMap[CDM.PrevDirRec] = newColdHeadDigest;

      ldt_common.updateSubRec( src, oldColdHeadRec );
    end

    GP=F and trace("[REVIEW]: <%s:%s> LDTMAP = (%s) COLD DIR PROP MAP = (%s)",
      MOD, meth, tostring(ldtMap), tostring(newColdHeadPropMap));

    -- Save our updates in the records
    newColdHeadRec[COLD_DIR_LIST_BIN] = list(); -- allocate a new digest list
    newColdHeadRec[COLD_DIR_CTRL_BIN] = newColdHeadMap;
    newColdHeadRec[SUBREC_PROP_BIN] =   newColdHeadPropMap;

    ldt_common.updateSubRec( src, newColdHeadRec );

    -- NOTE: We don't want to flush the TOP RECORD until we know that the
    -- underlying children record operations are complete.  However, we can
    -- change the memory copy of the topRec here, since that won't get written
    -- back to storage until there's an explicit aerospike:update() call.
    topRec[ ldtBinName ] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
    returnColdHead = newColdHeadRec;
  end -- if we should create a new Cold HEAD

  GP=E and trace("[EXIT]: <%s:%s> New Cold Head Record(%s) ",
    MOD, meth, coldDirRecSummary( returnColdHead ));
  return returnColdHead;
end --  coldDirHeadCreate()()

-- ======================================================================
-- coldDirRecInsert(ldtCtrl, coldHeadRec,digestListIndex,digestList)
-- ======================================================================
-- Insert as much as we can of "digestList", which is a list of digests
-- to LDRs, into a -- Cold Directory Page.  Return num written.
-- It is the caller's job to allocate a NEW Dir Rec page if not all of
-- digestList( digestListIndex to end) fits.
-- Parms:
-- (*) ldtCtrl: the main control structure
-- (*) coldHeadRec: The Cold List Directory Record
-- (*) digestListIndex: The starting Read position in the list
-- (*) digestList: the list of digests to be inserted
-- Return: Number of digests written, -1 for error.
-- ======================================================================
local function coldDirRecInsert(ldtCtrl,coldHeadRec,digestListIndex,digestList)
  local meth = "coldDirRecInsert()";
  local rc = 0;
  GP=E and trace("[ENTER]:<%s:%s> ColdHead(%s) ColdDigestList(%s)",
      MOD, meth, coldDirRecSummary(coldHeadRec), tostring( digestList ));

  -- Extract the property map and LDT map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldDirList = coldHeadRec[COLD_DIR_LIST_BIN];
  local coldDirMax = ldtMap[LS.ColdListMax];

  -- Write as much as we can into this Cold Dir Page.  If this is not the
  -- first time around the startIndex (digestListIndex) may be a value
  -- other than 1 (first position).
  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( digestList ) + 1 - digestListIndex;
  local itemSlotsAvailable = (coldDirMax - digestListIndex) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    GP=F and debug("[DEBUG]<%s:%s> ALERT!!: No space available on LDR(%s)",
    MOD, meth, tostring( coldDirMap ));
    -- Deal with this at a higher level.
    return -1; -- nothing written, Error.  Bubble up to caller
  end

  -- If we EXACTLY fill up the ColdDirRec, then we flag that so the next Cold
  -- List Insert will know in advance to create a new ColdDirHEAD.
  if totalItemsToWrite == itemSlotsAvailable then
    ldtMap[LS.ColdTopFull] = AS_TRUE; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]<%s:%s>TotalItems(%d) == SpaceAvail(%d):CTop FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.  We don't expect
  -- to have a "binary mode" for just the digest list.  We could, but that
  -- would be extra complexity for very little gain.
  GP=F and trace("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, digestListIndex, list.size(digestList), newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( coldDirList, digestList[i + digestListIndex] );
  end -- for each remaining entry

  -- Update the Count of Digests on the page (should match list size).
  local digestCount = coldDirMap[CDM.DigestCount];
  coldDirMap[CDM.DigestCount] = digestCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post digest Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(coldDirMap), tostring(coldDirList));

  -- Store our modifications back into the LDR Record Bins
  coldHeadRec[COLD_DIR_CTRL_BIN] = coldDirMap;
  coldHeadRec[COLD_DIR_LIST_BIN] = coldDirList;

  GP=E and trace("[EXIT]: <%s:%s> newItemsStored(%d) Digest List(%s) map(%s)",
    MOD, meth, newItemsStored, tostring( coldDirList), tostring(coldDirMap));

  return newItemsStored;
end -- coldDirRecInsert()

-- ======================================================================
-- coldListInsert()
-- ======================================================================
-- Insert "insertList", which is a list of digest entries, into the cold
-- dir page -- a directory of cold LSTACK Data Record digests that contain 
-- the actual data entries. Note that the data pages were built when the
-- warm list was created, so all we're doing now is moving the LDR page
-- DIGESTS -- not the data itself.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) ldtCtrl: the control map of the top record
-- (*) digestList: the list of digests to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function coldListInsert( src, topRec, ldtCtrl, digestList )
  local meth = "coldListInsert()";
  local rc = 0;

  -- Extract the property map and LDT map from the LDT Control
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  GP=E and trace("[ENTER]<%s:%s>SRC(%s) LDT Summary(%s) DigestList(%s)", MOD,
    meth, tostring(src), ldtSummaryString(ldtCtrl), tostring( digestList ));

  GP=F and trace("[DEBUG 0]:Map:WDL(%s)", tostring(ldtMap[LS.WarmDigestList]));

  -- The very first thing we must check is to see if we are ALLOWED to have
  -- a cold list.  If LS.ColdDirRecMax is ZERO, then that means we are
  -- not having a cold list -- so the warmListTransfer data is effectively
  -- being deleted.  If that's the case, then we pass those digests to
  -- the "release storage" method and return.
  if( ldtMap[LS.ColdDirRecMax] == 0 ) then
    rc = releaseStorage( src, topRec, ldtCtrl, digestList );
    GP=E and trace("[Early EXIT]: <%s:%s> Release Storage RC(%d)",
      MOD,meth, rc );
    return rc;
  end

  -- Ok, we WILL do cold storage, so we have to check the status.
  -- If we don't have a cold list, then we have to build one.  Also, if
  -- the current cold Head is completely full, then we also need to add
  -- a new one.  And, if we ADD one, then we have to check to see if we
  -- need to delete the oldest one (or more than one).
  local digestString;
  local coldHeadRec;
  local transferAmount = list.size( digestList );

  local coldHeadDigest = ldtMap[LS.ColdDirListHead];
  GP=F and trace("[DEBUG]<%s:%s>Cold List Head Digest(%s), ColdFullorNew(%s)",
      MOD, meth, tostring( coldHeadDigest), tostring(ldtMap[LS.ColdTopFull]));

  if( coldHeadDigest == nil or
     coldHeadDigest == 0 or
     ldtMap[LS.ColdTopFull] == AS_TRUE )
  then
    -- Create a new Cold Directory Head and link it in the Dir Chain.
    GP=F and trace("[DEBUG]:<%s:%s>:Creating FIRST NEW COLD HEAD", MOD, meth );
    coldHeadRec = coldDirHeadCreate(src, topRec, ldtCtrl, transferAmount );
    coldHeadDigest = record.digest( coldHeadRec );
    digestString = tostring( coldHeadDigest );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Opening Existing COLD HEAD", MOD, meth );
    digestString = tostring( coldHeadDigest );
    coldHeadRec = ldt_common.openSubRec( src, topRec, digestString );
  end

  local coldDirMap = coldHeadRec[COLD_DIR_CTRL_BIN];
  local coldHeadList = coldHeadRec[COLD_DIR_LIST_BIN];

  GP=F and trace("[DEBUG]<%s:%s>Digest(%s) ColdHeadCtrl(%s) ColdHeadList(%s)",
    MOD, meth, tostring( digestString ), tostring( coldDirMap ),
    tostring( coldHeadList ));

  -- Iterate thru and transfer the "digestList" (which is a list of
  -- LDR data Sub-Record digests) into the coldDirHead.  If it doesn't all
  -- fit, then create a new coldDirHead and keep going.
  local digestsWritten = 0;
  local digestsLeft = transferAmount;
  local digestListIndex = 1; -- where in the insert list we copy from.
  while digestsLeft > 0 do
    digestsWritten =
      coldDirRecInsert(ldtCtrl, coldHeadRec, digestListIndex, digestList);
    if( digestsWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Cold Dir Insert", MOD, meth);
      error( ldte.ERR_INSERT );
    end
    digestsLeft = digestsLeft - digestsWritten;
    digestListIndex = digestListIndex + digestsWritten;
    -- If we have more to do -- then write/close the current coldHeadRec and
    -- allocate ANOTHER one (woo hoo).
    if digestsLeft > 0 then
      ldt_common.updateSubRec( src, coldHeadRec );
      -- Can't currently close a dirty SubRec.
      ldt_common.closeSubRec( src, coldHeadRec, true );
      GP=F and trace("[DEBUG]: <%s:%s> Calling Cold DirHead Create: AGAIN!!",
          MOD, meth );
      -- Note that coldDirHeadCreate() deals with the data Expiration and
      -- eviction for any data that is in cold storage.
      coldHeadRec = coldDirHeadCreate( topRec, ldtCtrl, digestsLeft );
    end
  end -- while digests left to write.
  
  -- Update the Cold List Digest Count (add to cold, subtract from warm)
  local coldDataRecCount = ldtMap[LS.ColdDataRecCount];
  ldtMap[LS.ColdDataRecCount] = coldDataRecCount + transferAmount;

  local warmListCount = ldtMap[LS.WarmListDigestCount];
  ldtMap[LS.WarmListDigestCount] = warmListCount - transferAmount;

  -- All done -- Save the info of how much room we have in the top Warm
  -- Sub-Rec (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving ldtCtrl (%s) Before Update ",
    MOD, meth, tostring( ldtCtrl ));
  topRec[ ldtBinName ] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  GP=F and trace("[DEBUG]: <%s:%s> New Cold Head Save: Summary(%s) ",
    MOD, meth, coldDirRecSummary( coldHeadRec ));
  local status = ldt_common.updateSubRec( src, coldHeadRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",
    MOD,meth, tostring(status));

  status = ldt_common.closeSubRec( src, coldHeadRec, true);
  GP=E and trace("[EXIT]: <%s:%s> SUB-REC  Close Status(%s) RC(%d)",
    MOD,meth, tostring(status), rc );

  -- Note: This is warm to cold transfer only.  So, no new data added here,
  -- and as a result, no new counts to upate (just warm/cold adjustments).

  return rc;
end -- coldListInsert

-- ======================================================================
-- coldListRead()
-- ======================================================================
-- Synopsis: March down the Cold List Directory Pages (a linked list of
-- directory pages -- that each point to lstack Data Sub-Records) and
-- read "count" data entries.  Use the same ReadDigestList method as the
-- warm list.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: The main structure of the LDT Bin.
-- (*) count:  Return this many items
-- Return: Return the amount read from the Cold Dir List.
-- ======================================================================
local function coldListRead(src, topRec, resultList, ldtCtrl, count)
  local meth = "coldListRead()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d) LdtSummary(%s)",
      MOD, meth, count, ldtSummaryString(ldtCtrl));

  -- Extract the property map and LDT map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- If there is no Cold List, then return immediately -- nothing read.
  if(ldtMap[LS.ColdDirListHead] == nil or ldtMap[LS.ColdDirListHead] == 0) then
    return 0;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items. 
  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];

  -- Outer loop -- Process each Cold Directory Page.  Each Cold Dir page
  -- holds a list of digests -- just like our WarmDigestList in the
  -- record, so the processing of that will be the same.
  -- Process the Linked List of Dir pages, head to tail
  local numRead = 0;
  local totalNumRead = 0;
  local countRemaining =  count;

  trace("[DEBUG]:<%s:%s>:Starting ColdDirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(coldDirRecDigest) );

  local coldDirMap;
  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    trace("[DEBUG]:<%s:%s>:Top of ColdDirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(coldDirRecDigest) );
    -- Open the Directory Page
    local digestString = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

    GP=F and trace("[DEBUG]<%s:%s>Cold Dir subrec digest(%s) Map(%s) List(%s)",
      MOD, meth, digestString, tostring(coldDirMap),tostring(digestList));

    numRead = digestListRead(src, topRec, resultList, ldtCtrl, digestList,
                            countRemaining);
    if numRead <= 0 then
      warn("[ERROR]:<%s:%s>:Cold List Read Error: Digest(%s)",
          MOD, meth, digestString );
      return numRead;
    end

    totalNumRead = totalNumRead + numRead;
    countRemaining = countRemaining - numRead;

    GP=F and trace("[DEBUG]:<%s:%s>:After Read: TotalRead(%d) NumRead(%d)",
          MOD, meth, totalNumRead, numRead );
    GP=F and trace("[DEBUG]:<%s:%s>:CountRemain(%d) NextDir(%s)PrevDir(%s)",
          MOD, meth, countRemaining, tostring(coldDirMap[CDM.NextDirRec]),
          tostring(coldDirMap[CDM.PrevDirRec]));

    if countRemaining <= 0 or coldDirMap[CDM.NextDirRec] == 0 then
      GP=E and trace("[EARLY EXIT]:<%s:%s>:Cold Read: (%d) Items",
          MOD, meth, totalNumRead );
        -- We no longer close a dirty SubRec, but we can mark it available.
        ldt_common.closeSubRec( src, coldDirRec, true );
        return totalNumRead;
    end

    GP=F and trace("[DEBUG]:<%s:%s>Reading NEXT DIR:", MOD, meth );
    
    -- Ok, so now we've read ALL of the contents of a Directory Record
    -- and we're still not done.  Close the old dir, open the next and
    -- keep going.
    coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

    GP=F and trace("[DEBUG]:<%s:%s>Looking at subrec digest(%s) Map(%s) L(%s)",
      MOD, meth, digestString, tostring(coldDirMap),tostring(digestList));

    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
    GP=F and trace("[DEBUG]:<%s:%s>Getting Next Digest in Dir Chain(%s)",
      MOD, meth, coldDirRecDigest );

    ldt_common.closeSubRec( src, coldDirRec, true );

  end -- while Dir Page not empty.

  GP=F and trace("[DEBUG]<%s:%s>After ColdListRead:LdtMap(%s) ColdHeadMap(%s)",
      MOD, meth, tostring( ldtMap ), tostring( coldDirMap )); 

  GP=E and trace("[EXIT]:<%s:%s>totalAmountRead(%d) ResultListSummary(%s) ",
      MOD, meth, totalNumRead, ldt_common.summarizeList(resultList));
  return totalNumRead;
end -- coldListRead()

-- ======================================================================
-- coldListTake()
-- ======================================================================
-- Synopsis: March down the Cold List Directory Pages (a linked list of
-- directory pages -- that each point to lstack Data Sub-Records) and
-- take "count" data entries.  Use the same accessDigestList method as the
-- warm list.  If we empty an LDR, we remove it.  If we empty an entire
-- ColdDir, we remove that and move in the next one.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: User-level Record holding the LDT Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) ldtCtrl: The main structure of the LDT Bin.
-- (*) count:  Return this many items
-- Return: Return the amount read from the Cold Dir List.
-- ======================================================================
local function coldListTake(src, topRec, resultList, ldtCtrl, count)
  local meth = "coldListTake()";
  GP=E and trace("[ENTER]: <%s:%s> Count(%d) LdtSummary(%s)",
      MOD, meth, count, ldtSummaryString(ldtCtrl));

  -- Extract the property map and LDT map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- If there is no Cold List, then return immediately -- nothing read.
  if(ldtMap[LS.ColdDirListHead] == nil or ldtMap[LS.ColdDirListHead] == 0) then
    return 0;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, perform take() on the LDR Records (in reverse
  -- list order), and then each page (in reverse list order), until we've
  -- taken "count" items.
  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];

  -- Outer loop -- Process each Cold Directory Page.  Each Cold Dir page
  -- holds a list of digests -- just like our WarmDigestList in the
  -- record, so the processing of that will be the same.
  -- Process the Linked List of Dir pages, head to tail.   If we empty
  -- an entire Directory, then release the current ColdDirListHead and
  -- get the next one in line, if there is one.
  local numRead = 0;
  local totalNumRead = 0;
  local countRemaining =  count;

  -- info("[DEBUG]:<%s:%s>:Starting ColdDirPage Loop: DPDigest(%s)",
  --     MOD, meth, tostring(coldDirRecDigest) );

  local coldDirMap;
  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    -- info("[DEBUG]:<%s:%s>:Top of ColdDirPage Loop: DPDigest(%s)",
    --   MOD, meth, tostring(coldDirRecDigest) );
    -- Open the Directory Page
    local digestString = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];

    GP=F and trace("[DEBUG]<%s:%s>Cold Dir subrec digest(%s) Map(%s) List(%s)",
      MOD, meth, digestString, tostring(coldDirMap),tostring(digestList));

    numRead = digestListTake(src, topRec, resultList, ldtCtrl, digestList,
                            countRemaining);
    if numRead <= 0 then
      warn("[ERROR]:<%s:%s>:Cold List Read Error: Digest(%s)",
          MOD, meth, digestString );
      return 0;
    end

    -- We are going to remove SubRecs as we go -- it's the best way to keep
    -- track of what we're doing.
    -- Look and see what happened to the digestList in the Cold Dir.
    -- If that list is now zero (i.e. it was trimmed down to nothing),
    -- then we delete this coldListDirectory SubRec (currently the dir Head)
    -- and we install the next in line (if there is one) -- as the new
    -- Cold Dir Head.
    if #digestList == 0 then
      -- We're going to remove the current ColdHead.
      GP=F and trace("[DEBUG]<%s:%s> Remove Cold Head", MOD, meth);
      local nextColdDirRecDigest = coldDirMap[CDM.NextDirRec];
      local oldColdDirDigestString = digestString;
      if nextColdDirRecDigest == nil or nextColdDirRecDigest == 0 then
        -- No ColdDir neighbors.  The cold dir is now empty.
        ldtMap[LS.ColdDirListHead] = 0;
        coldDirRecDigest = 0;
      else
        ldtMap[LS.ColdDirListHead] = nextColdDirRecDigest;
        digestString = tostring( nextColdDirRecDigest );

        coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
        coldDirRecDigest = nextColdDirRecDigest;
        coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
        coldDirMap[CDM.PrevDirRec] = 0; -- This is the new Head;
        ldtMap[LS.ColdDirListHead] = coldDirRecDigest;
      end
      ldt_common.removeSubRec(src, topRec, propMap, oldColdDirDigestString);

    else
      -- Save the updated digestList.
      coldDirRec[COLD_DIR_LIST_BIN] = digestList;
      ldt_common.updateSubRec(src, coldDirRec);
      -- jump out after this loop.
      coldDirRecDigest = 0;
    end

    totalNumRead = totalNumRead + numRead;
    countRemaining = countRemaining - numRead;

    GP=F and trace("[DEBUG]:<%s:%s>:After Take: TotalRead(%d) NumRead(%d)",
          MOD, meth, totalNumRead, numRead );
    GP=F and trace("[DEBUG]:<%s:%s>:CountRemain(%d) NextDir(%s)PrevDir(%s)",
          MOD, meth, countRemaining, tostring(coldDirMap[CDM.NextDirRec]),
          tostring(coldDirMap[CDM.PrevDirRec]));




--    if countRemaining <= 0 or coldDirMap[CDM.NextDirRec] == 0 then
--      GP=E and trace("[EARLY EXIT]:<%s:%s>:Cold Read: (%d) Items",
--          MOD, meth, totalNumRead );
--        -- We no longer close a dirty SubRec, but we can mark it available.
--        ldt_common.closeSubRec( src, coldDirRec, true );
--        return totalNumRead;
--    end
--
--    GP=F and trace("[DEBUG]:<%s:%s>Reading NEXT DIR:", MOD, meth );
    
--    -- Ok, so now we've read ALL of the contents of a Directory Record
--    -- and we're still not done.  Close the old dir, open the next and
--    -- keep going.
--    coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
--
--    GP=F and trace("[DEBUG]:<%s:%s>Looking at subrec digest(%s) Map(%s) L(%s)",
--      MOD, meth, digestString, tostring(coldDirMap),tostring(digestList));
--
--    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
--    GP=F and trace("[DEBUG]:<%s:%s>Getting Next Digest in Dir Chain(%s)",
----      MOD, meth, coldDirRecDigest );
--
--    ldt_common.closeSubRec( src, coldDirRec, true );

  end -- while Dir Page not empty.

  GP=F and trace("[DEBUG]<%s:%s>After ColdListRead:LdtMap(%s) ColdHeadMap(%s)",
      MOD, meth, tostring( ldtMap ), tostring( coldDirMap )); 

  GP=E and trace("[EXIT]:<%s:%s>totalAmountRead(%d) ResultListSummary(%s) ",
      MOD, meth, totalNumRead, ldt_common.summarizeList(resultList));
  return totalNumRead;
end -- coldListTake()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LDT General Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- General Functions that require use of many of the above functions, so
-- they cannot be shoved into any one single category.
-- ======================================================================


-- ======================================================================
-- warmListTransfer()
-- ======================================================================
-- Transfer some amount of the WarmDigestList contents (the list of LDT Data
-- Record digests) into the Cold List, which is a linked list of Cold List
-- Directory pages that each point to a list of LDRs.
--
-- There is a configuration parameter (kept in the LDT Control Bin) that 
-- tells us how much of the warm list to migrate to the cold list. That
-- value is set at LDT Create time.
--
-- There is a lot of complexity at this level, as a single Warm List
-- transfer can trigger several operations in the cold list (see the
-- function makeRoomInColdList( ldtCtrl, digestCount )
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: The top level user record (needed for create_subrec)
-- (*) ldtCtrl
-- Return: Success (0) or Failure (-1)
-- ======================================================================
local function warmListTransfer( src, topRec, ldtCtrl )
  local meth = "warmListTransfer()";
  local rc = 0;
  GP=E and trace("[ENTER]<%s:%s>\n\n <> TRANSFER TO COLD LIST <> LDT(%s)\n",
    MOD, meth, tostring(ldtCtrl) );

  -- if we haven't yet initialized the cold list, then set up the
  -- first Directory Head (a page of digests to data pages).  Note that
  -- the data pages are already built from the warm list, so all we're doing
  -- here is moving the reference (the digest value) from the warm list
  -- to the cold directory page.

  -- Build the list of items (digests) that we'll be moving from the warm
  -- list to the cold list. Use coldListInsert() to insert them.
  local transferList = extractWarmListTransferList( ldtCtrl );
  rc = coldListInsert( src, topRec, ldtCtrl, transferList );
  GP=E and trace("[EXIT]: <%s:%s> LDT(%s) ", MOD, meth, tostring(ldtCtrl) );
  return rc;
end -- warmListTransfer()

-- ======================================================================
-- hotListTransfer( ldtCtrl, insertValue )
-- ======================================================================
-- The job of hotListTransfer() is to move part of the HotList, as
-- specified by HotListTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmDigestList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec
-- (*) ldtCtrl
-- ======================================================================
local function hotListTransfer( src, topRec, ldtCtrl )
  local meth = "hotListTransfer()";
  local rc = 0;
  GP=E and trace("[ENTER]: <%s:%s> LDT Summary(%s) ",
      MOD, meth, ldtSummaryString(ldtCtrl) );
      --
  -- Extract the property map and LDT map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- First check to see if we have a special case of the Capacity Limit
  -- being within the bounds of the Warm List.  If so, AND if we need to do
  -- some fancy footwork to purge the right amount of space from the warm list
  -- to make room for the hot list transfer, THEN we'll do exactly that.
  -- Otherwise, we'll just do the 
  -- if specialWarmCapacityInsert( src, topRec, ldtCtrl ) == false then

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmListHasRoom( ldtMap ) == 0 then
    warmListTransfer( src, topRec, ldtCtrl );
  end

  -- Do this the simple (more expensive) way for now:  Build a list of the
  -- items (data entries) that we're moving from the hot list to the warm dir,
  -- then call insertWarmDir() to find a place for it.
  local transferList = extractHotListTransferList( ldtMap );
  rc = warmListInsert( src, topRec, ldtCtrl, transferList );

  GP=E and trace("[EXIT]: <%s:%s> result(%d) LdtMap(%s) ",
    MOD, meth, rc, tostring( ldtMap ));
  return rc;
end -- hotListTransfer()

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
-- buildSubRecList()
-- ========================================================================
-- Build the list of subrecs starting at location N. 
-- Position ZERO means, get them all.
-- Parms:
-- (0) src:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtCtrl: The main LDT control structure
-- (3) position: We start building the list with the first subrec that
--     holds "position" (item count, not byte count).  If position is in
--     the HotList, then all Warm and Cold recs are included.
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- ========================================================================
local function buildSubRecList( src, topRec, ldtCtrl, position )
  local meth = "buildSubRecList()";

  GP=E and trace("[ENTER]: <%s:%s> position(%s) ldtSummary(%s)",
    MOD, meth, tostring(position), ldtSummaryString( ldtCtrl ));

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local resultList;

  info("\n\n [WARNING]<%s:%s> UNDER CONSTRUCTION !!!!!!!!!\n",MOD, meth);
  info("\n\n [WARNING]<%s:%s> UNDER CONSTRUCTION !!!!!!!!!\n",MOD, meth);
--
--  -- If position puts us into or past the warmlist, make the adjustment
--  -- here.  Otherwise, drop down into the FULL MONTY
--  --
--  if( position < 0 ) then
--    warn("[ERROR]<%s:%s> BUILD SUBREC LIST ERROR: Bad Position(%d)",
--      MOD, meth, position );
--    error( ldte.ERR_INTERNAL );
--  end
--
--  -- Call buildSearchPath() to give us a searchPath object that shows us
--  -- the storage we are going to release.  It will tell us what do to in
--  -- each of the three types of storage: Hot, Warm and Cold, although, we
--  -- care only about warm and cold in this case
--  local searchPath = map();
--  buildSearchPath( topRec, ldtCtrl, searchPath, position );
--
--  -- Use the search path to show us where to start collecting digests in
--  -- the WARM List.
--  local wdList = ldtMap[LS.WarmDigestList];
--  local warmListSize = list.size(  wdList );
--
--  -- If warmListStart is outside the size of the list, then that means we
--  -- will just skip the for loop for the warm list.  Also, if the WarmPosition
--  -- is ZERO, then we treat that as the same case.
--  local resultList;
--  local warmListStart = searchPath.WarmPosition;
--  if( warmListStart == 0 or warmListStart > warmListSize ) then
--    trace("[REC LIST]<%s:%s> Skipping over warm list: Size(%d) Pos(%d)",
--      MOD, meth, warmListSize, warmListStart );
--    resultList = list(); -- add only cold list items
--  elseif( warmListStart == 1 ) then
--    -- Take it all
--    resultList = list.take( wdList, warmListSize );
--  else
--    -- Check this
--    resultList = list.drop( wdList, warmListStart - 1 );
--  end
--
--  -- Now for the harder part.  We will still have open the cold list directory
--  -- subrecords to know what is inside.  The searchPath is going to give us
--  -- a digest position in the cold list.  We will open each Cold Directory
--  -- Page until we get to the start position (which can be 1, or N)
--  local count = 0;
--
--  -- Now pull the digests from the Cold List
--  -- There are TWO types subrecords:
--  -- (*) There are the LDRs (Data Records) subrecs
--  -- (*) There are the Cold List Directory subrecs
--  -- We will read a Directory Head, and enter it's digest
--  -- Then we'll pull the digests out of it (just like a warm list)
--
--  -- If there is no Cold List, then return immediately -- nothing more read.
--  if(ldtMap[LS.ColdDirListHead] == nil or ldtMap[LS.ColdDirListHead] == 0) then
--    return resultList;
--  end
--
--  -- The challenge here is to collect the digests of all of the subrecords
--  -- that are to be released.
--
--  -- Process the coldDirList (a linked list) head to tail (that is "append"
--  -- order).  For each dir, read in the LDR Records (in reverse list order),
--  -- and then each page (in reverse list order), until we've read "count"
--  -- items.  If the 'all' flag is true, then read everything.
--  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];
--
--  -- LEFT OFF HERE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
--  -- Note that we're not using this function in production (whew)
--  info("\n\n LEFT OFF HERE<%s:%s>!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n",MOD, meth);
--  info("\n\n LEFT OFF HERE<%s:%s>!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n",MOD, meth);
--
--  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
--    -- Save the Dir Digest
--    list.append( resultList, coldDirRecDigest );
--
--    -- Open the Directory Page, read the digest list
--    local digestString = tostring( coldDirRecDigest ); -- must be a string
--    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
--    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
--    for i = 1, list.size(digestList), 1 do 
--      list.append( resultList, digestList[i] );
--    end
--
--    -- Get the next Cold Dir Node in the list
--    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
--    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
--    -- If no more, we'll drop out of the loop, and if there's more, 
--    -- we'll get it in the next round.
--    -- Close this directory subrec before we open another one.
--    ldt_common.closeSubRec( src, coldDirRec, false );
--  end -- for each coldDirRecDigest
--
--  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
--      MOD, meth, tostring( resultList ) );
--
  return resultList
end -- buildSubRecList()

-- ========================================================================
-- buildSubRecListAll()
-- ========================================================================
-- Build the list of subrecs for the entire LDT.
-- Parms:
-- (*) src: Subrec Context -- Manage the Open Subrec Pool
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtCtrl: The main LDT control structure
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- ========================================================================
local function buildSubRecListAll( src, topRec, ldtCtrl )
  local meth = "buildSubRecListAll()";

  GP=E and trace("[ENTER]: <%s:%s> LDT Summary(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ));

  -- Extract the property map and LDT Map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Copy the warm list into the result list
  local wdList = ldtMap[LS.WarmDigestList];
  local transAmount = list.size( wdList );
  local resultList = list.take( wdList, transAmount );

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(ldtMap[LS.ColdDirListHead] == nil or ldtMap[LS.ColdDirListHead] == 0) then
    return resultList;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  
  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];

  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    -- Save the Dir Digest
    list.append( resultList, coldDirRecDigest );

    -- Open the Directory Page, read the digest list
    local digestString = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    for i = 1, list.size(digestList), 1 do 
      list.append( resultList, digestList[i] );
    end

    -- Get the next Cold Dir Node in the list
    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
    -- If no more, we'll drop out of the loop, and if there's more, 
    -- we'll get it in the next round.
    -- Close this directory subrec before we open another one.
    ldt_common.closeSubRec( src, coldDirRec, false );

  end -- Loop thru each cold directory

  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList

end -- buildSubRecListAll()

-- ======================================================================
-- createSearchPath()
-- ======================================================================
-- Create a searchPath object for LSTACK that provides the details in the
-- stack for the object position.
-- Different from LLIST search position, which shows the path from the
-- Tree root all the way down to the tree leaf (and all of the inner nodes
-- from the root to the leaf), the SearchPath for a stack shows the
-- relative location in either
-- (*) The Hot List::(simple position in the directory)
-- (*) The Warm List::(Digest position in the warm list, plus the position
--     in the LDR objectList)
-- (*) The Cold List::(Cold Dir digest, plus position in the cold Dir
--     Digest List, plus position in the LDR.
-- ======================================================================


-- ======================================================================
-- locatePosition()
-- ======================================================================
-- Create a Search Path Object that shows where "position" lies in the
-- stack object.  The possible places are:
-- (*) Hot List::  Entry List Position
-- (*) Warm List:: Digest List Position, Entry List Position
-- (*) Cold List:: Directory List Pos, Digest List Pos, Entry List Pos
-- We don't open up every subrec.  We assume the following things:
-- LDRs have a FIXED number of entries, because either
-- (a) They are all the same size and the total byte capacity is set
-- (b) They have variable size entries, but we are counting only LIST items
-- Either way, every FULL Cold Dir or Warm (LDR) data page has a known
-- number of items in it.  The only items that are unknown are the
-- partially filled Warm Top and Cold Head.  Those need to be opened to
-- get an accurate reading.
-- if( position < hotListSize ) then
--   It's a hot list position
-- elseif( position < warmListSize ) then
--   Its a warm list position
-- else
--   It's a cold list position
-- end
-- TODO:
-- (*) Track Warm and Cold List Capacity
-- (*) Track WarmTop Size (how much room is left?)
-- (*) Track ColdTop Size (how much room is left?)
--
-- Parms:
-- (*) topRec: Top (LDT Holding) Record
-- (*) ldtCtrl: Main LDT Control structure
-- (*) sp: searchPath Object (we will fill this in)
-- (*) position: Find this Object Position in the LSTACK
-- Return:
-- SP: Filled in with position in Stack Object.  Location is computed in
--     terms of OBJECTS (not bytes), regardless of mode.  The mode
--     (LIST or BINARY) does determine how the position  is calculated.
--  0: Success
-- -1: ERRORS
-- ======================================================================
local function locatePosition( topRec, ldtCtrl, sp, position )
  local meth = "locatePosition()";

  -- Extract the property map and LDT Map from the LDT Control.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local directoryListPosition = 0; -- if non-zero, we're in cold list
  local digestListPosition = 0;    -- if non-zero, we're cold or warm list
  local entryListPosition = 0;     -- The place in the entry list.

  local hotListAmount = list.size( ldtMap[LS.HotEntryList] );
  local warmListMax = ldtMap[LS.WarmListMax];
  local warmFullCount = ldtMap[LS.WarmListDigestCount] - 1;
  local warmTopEntries = ldtMap[LS.WarmTopEntryCount];
  local warmListPart = (warmFullCount * warmListMax) + warmTopEntries;
  local warmListAmount = hotListAmount + warmListPart;
  if (position <= hotListAmount) then
    -- It's a hot list position:
    entryListPosition = position;
  end
end -- locatePosition

-- ======================================================================
-- localTrim( topRec, ldtCtrl, searchPath )
-- ======================================================================
-- Release the storage that is colder than the location marked in the
-- searchPath object.
--
-- It is not (yet) clear if this needs to be an EXACT operation, or just
-- an approximate one.  We would prefer that we could release the storage
-- at the LDR (page) boundary.
-- ======================================================================
local function localTrim( topRec, ldtCtrl, searchPath )
  local meth = "localTrim()";
  GP=E and trace("[ENTER]:<%s:%s> LDTSummary(%s) SearchPath(%s)",
    MOD, meth, ldtSummaryString(ldtCtrl), tostring(searchPath));
    
  -- TODO: Finish this later -- if needed at all.
  warn("[WARNING!!]<%s:%s> FUNCTION UNDER CONSTRUCTION!!! ", MOD, meth );

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- localTrim()

-- ========================================================================
-- specialHotCapacityInsert()
-- ========================================================================
-- In the odd case that the user sets the LSTACK CAPACITY at a size SMALLER
-- than the HotList Max Size, then we have to do something special with
-- inserts.  We have to redo the HotList, removing the Oldest element and
-- then insert the newest element.
-- Return:
-- TRUE if we did a special insert
-- FALSE otherwise (so the caller can proceed with a regular insert)
-- ========================================================================
local function specialHotCapacityInsert( ldtMap, newStoreValue )
  -- This is a high volume Function -- comment out all debugging when it is
  -- in a stable state.
  local meth = "specialHotCapacityInsert()";
  GP=E and trace("[ENTER]:<%s:%s> NewVal(%s) LDT Map Summary(%s)",
    MOD, meth, tostring(newStoreValue), ldtMapSummaryString(ldtMap));

  local result = false; -- This is the likely result

  -- Do we have a Capacity Setting that is LESS than or equal to the Hot
  -- List Size?
  local capacity = ldtMap[LC.StoreLimit];
  local hotListMax = ldtMap[LS.HotListMax];
  if capacity > 0 and capacity <= hotListMax then
    -- If so, check to see if we're over Capacity.   If not, we'll just
    -- fall thru and the caller will deal with the REGULAR stack push.
    local hotList = ldtMap[LS.HotEntryList];
    local hotListSize = list.size( hotList );

    GP=D and trace("[HOT LIST BEFORE]:<%s:%s> HotList(%s)",
      MOD, meth, tostring(hotList));

    if hotListSize == capacity then
      -- Trim the Hot List to size, then append the new value.
      -- The usual case (for this already unusual case) will be that we are
      -- adding ONE MORE to a FULL HotList (full in the sense that we're at
      -- the Capacity limit, even though HotList Max could be bigger).
      -- For this case, we're going to just move the list items over by one,
      -- and then put the new one at the end.
      for i = 1, (hotListSize - 1) do
        hotList[i] = hotList[i+1]; 
      end
      hotList[hotListSize] = newStoreValue;
      result = true;

    elseif hotListSize > capacity then
      -- This case is a bit more weird -- this should happen ONLY when we've
      -- just changed the capacity setting.  In this case, we have to create
      -- a new list (because we do not yet have a TRIM() operator.
      local diff = (hotListSize - capacity) + 1;
      local newHotList = list.drop(hotList, diff)
      list.append( newHotList, newStoreValue );
      result = true;
    end

    GP=D and trace("[HOT LIST AFTER]:<%s:%s> HotList(%s) Result(%s)",
      MOD, meth, tostring(hotList), tostring(result));

    -- Otherwise, the caller will just perform the regular lstack push.
  end -- end if special

  GP=E and trace("[EXIT]: <%s:%s> result(%s)", MOD, meth, tostring(result));
  return result;
end -- specialHotCapacityInsert()

-- ========================================================================
-- localPush()
-- ========================================================================
-- Do the common work of the PUSH operation -- so that multiple routines
-- here can use it.
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtCtrl: The main control structure
-- (*) newStoreValue: value to be pushed on the stack
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- ========================================================================
local function localPush( topRec, ldtCtrl, newStoreValue, src )
  local meth = "localPush()";
  GP=E and trace("[ENTER]:<%s:%s> NewStoreVal(%s) LDTSummary(%s)",
    MOD, meth, tostring(newStoreValue), ldtSummaryString(ldtCtrl));
    
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  local rc = 0;

  -- This function is pretty easy.  We always start with the HotList, and
  -- then move on from there if necessary.
  -- Considerations:
  -- (*) If the CAPACITY is actually set to LESS THAN the Hot List, then we
  --     NEVER transfer to the warm list.  Instead, if we are AT CAPACITY,
  --     we trim the Hot List and then do the simple insert.
  -- (*) If CAPACITY is set to MORE than the Hot List, then it's a
  --     Warm List Problem, and we deal with it there.
  -- (*) For Regular (non-capacity issue) inserts, If we have room in the
  --     Hot List, then we do the simple list insert.  If we don't have
  --     room, then make room -- transfer half the list out to the warm list.
  --     That may, in turn, have to make room by moving some items to the
  --     cold list. 
  -- NOTE: New for the 2014 Christmas season, our new configurator sometimes
  --       calls for a ZERO LENGTH Hot List, when objects are big.  So, we
  --       need to be ready to insert directly into the warm list.
  local hotListMax = ldtMap[LS.HotListMax];
  if hotListMax == nil or hotListMax == 0 then
    -- We don't have a hot list, so nothing to check and nothing to transfer.
    -- Just proceed directly to the warm list.  However, Warm List expects
    -- an entry LIST, not a single value.
    local entryList = list();
    list.append(entryList, newStoreValue);
    rc = warmListInsert( src, topRec, ldtCtrl, entryList );


    -- NOTE: Ok to use ldtMap and not the usual ldtCtrl here.
  elseif specialHotCapacityInsert( ldtMap, newStoreValue ) == false then
    if list.size( ldtMap[LS.HotEntryList] ) >= hotListMax then
      GP=F and trace("[DEBUG]:<%s:%s>: CALLING TRANSFER HOT LIST!!",MOD, meth );
      hotListTransfer( src, topRec, ldtCtrl );
    end
    hotListInsert( ldtCtrl, newStoreValue );
  end

  -- Update the overall Count.
  local itemCount = propMap[PM.ItemCount];
  propMap[PM.ItemCount] = (itemCount + 1);


  GP=D and trace("[DEBUG]<%s:%s> After Insert: HotList(%s)", MOD, meth,
    tostring( ldtMap[LS.HotEntryList]));
  
  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- localPush()


-- ======================================================================
-- localPeek():  Return N elements from the top of the stack.
-- ======================================================================
-- Return "count" values from the stack, in Stack (LIFO) order.
-- If "count" is zero, then return all (same as a scan).
-- Depending on "count", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotList), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Note tht the FilterModule and Filter have already been processed by
-- the outer caller, and those global values have now been set.
--
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) resultList: Fill in this structure with the result.
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtCtrl: The LDT Control Structure
-- (*) count: The number of items to read from the stack
-- Result:
--   Success: 0
--   Error: -1
-- ======================================================================
local function localPeek( src, resultList, topRec, ldtCtrl, count )
  local meth = "localPeek()";

  GP=E and trace("[ENTER]<%s:%s> PeekCount(%s)", MOD, meth, count);

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Fetch from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local resultList = hotListRead(resultList, ldtCtrl, count);
  local numRead = list.size( resultList );
  GP=F and trace("[DEBUG]: <%s:%s> HotListResult:Summary(%s)",
      MOD, meth, ldt_common.summarizeList(resultList));

  local warmCount = 0;

  -- If the list had all that we need, then done.  Return list.
  if( numRead >= count or numRead >= propMap[PM.ItemCount] ) then
    return 0;
  end

  -- We need more -- get more out of the Warm List.  Compute the correct
  -- READ count given that we've already read from the Hot List.
  local remainingCount = count - numRead;

  GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d)",
    MOD, meth, remainingCount);
  -- If no Warm List, then we're done (assume no cold list if no warm)
  local warmDigestList = ldtMap[LS.WarmDigestList];
  if #warmDigestList > 0 then
    warmCount = digestListRead(src, topRec, resultList, ldtCtrl,
                               warmDigestList, remainingCount);
  end

  -- The Warm list was not enough.  Compute the correct READ count
  -- given that we've already read from the Hot and Warm Lists.
  remainingCount = count - numRead - warmCount;
  GP=F and trace("[DEBUG]:<%s:%s>After WmRd:RC(%d)PC(%d)NR(%d)WC(%d)",
    MOD, meth, remainingCount, count, numRead, warmCount );

  GP=F and trace("[DEBUG]:<%s:%s>After WarmListRead: ldtMap(%s) ldtCtrl(%s)",
    MOD, meth, tostring(ldtMap), ldtSummaryString(ldtCtrl));

  numRead = list.size( resultList );
  -- If we've read enough, then return.
  if ( remainingCount <= 0  or numRead >= propMap[PM.ItemCount] ) then
      return 0;
  end

  -- Otherwise, go look for more in the Cold List.
  local coldCount = 
     coldListRead(src,topRec,resultList,ldtCtrl,remainingCount);

  GP=E and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    MOD, meth, count, ldt_common.summarizeList(resultList));

  return 0;
end -- function localPeek()

-- ======================================================================
-- localPop():  Pop N elements from the top of the stack.
-- ======================================================================
-- Return "count" values from the stack, in Stack (LIFO) order.
-- If "count" is zero, then return all (same as a scan).
-- Depending on "count", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotList), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Note tht the FilterModule and Filter have already been processed by
-- the outer caller, and those global values have now been set.
--
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) resultList: Fill in this structure with the result.
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtCtrl: The LDT Control Structure
-- (*) count: The number of items to read from the stack
-- Result:
--   Success: 0
--   Error: -1
-- ======================================================================
local function localPop( src, resultList, topRec, ldtCtrl, count )
  local meth = "localPop()";

  GP=E and trace("[ENTER]<%s:%s> PopCount(%d)", MOD, meth, count);

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local storeLimit = ldtMap[LC.StoreLimit];

  GP=F and trace("[DEBUG]<%s:%s> Pop with Count(%d) StoreLimit(%d)",
      MOD, meth, count, storeLimit );

  -- Pop from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  -- NOTE: For certain configurations, involving large data elements,
  -- ther may be no HotList.
  local numRead, empty = hotListTake(resultList, ldtCtrl, count);
  GP=F and trace("[DEBUG]<%s:%s> NumRead(%d) E(%s)HotListResult:Summary(%s)",
    MOD, meth, numRead, tostring(empty), ldt_common.summarizeList(resultList));

  local warmCount = 0;
  local remainingCount = 0;
  -- If the hot list take was enough, skip the warm and cold list take.
  if numRead < count then
    -- We need more -- get more out of the Warm List.  Compute the
    -- correct READ count given that we've already read from the Hot List.
    remainingCount = count - numRead;

    GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d)",
      MOD, meth, remainingCount);
    -- If no Warm List, then we're done (assume no cold list if no warm)
    local warmDigestList = ldtMap[LS.WarmDigestList];
      if #warmDigestList > 0 then
        warmCount = digestListTake(src, topRec, resultList, ldtCtrl,
                               warmDigestList, remainingCount);
    end

    -- We need more, so get more out of the COLD List.  Compute the
    -- correct READ count given that we've already read from the Hot and
    -- Warm Lists.
    remainingCount = count - numRead - warmCount;
    GP=F and trace("[DEBUG]:<%s:%s>After WmRd:RC(%d)PC(%d)NR(%d)WC(%d)",
      MOD, meth, remainingCount, count, numRead, warmCount );

    GP=F and trace("[DEBUG]:<%s:%s>After WarmListRead: ldtMap(%s) ldtCtrl(%s)",
      MOD, meth, tostring(ldtMap), ldtSummaryString(ldtCtrl));

  end -- Done with Warm List

  -- If warm List was not enough, try the cold list.
  numRead = numRead + warmCount;
  remainingCount = count - numRead;

  if numRead < count then
    local coldCount =
      coldListTake(src, topRec, resultList, ldtCtrl, remainingCount);
    GP=F and trace("[DEBUG]:<%s:%s> ColdList Take Results(%d)",
      MOD, meth, coldCount);
  end

  -- If all went well, then we have "count" fewer items.  Make the adjustment
  -- in the count.
  local itemCount = propMap[PM.ItemCount];
  propMap[PM.ItemCount] = itemCount - count;

  return 0;
end -- function localPop()


-- ========================================================================
-- This function is under construction.
-- ========================================================================
-- ========================================================================
-- lstack_delete_subrecs() -- Delete the entire lstack -- in pieces.
-- ========================================================================
-- The real delete (above) will do the correct delete, which is to remove
-- the ESR and the BIN.  THIS function is more of a test function, which
-- will remove each SUBREC individually.
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- First, fetch all of the digests of subrecords that go with this
-- LDT, then iterate thru the list and delete them.
-- Finally  -- Reset the record[ldtBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- THIS FUNCTION IS NOT CURRENTLY IN USE.
-- ========================================================================
local function lstack_delete_subrecs( src, topRec, ldtBinName )
  local meth = "lstack_delete()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Not sure if we'll ever use this function -- we took a different direction
  warn("[WARNING!!!]::LSTACK_DELETE_SUBRECS IS NOT YET IMPLEMENTED!!!");

  local rc = 0; -- start off optimistic

  -- Validate the ldtBinName before moving forward
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- TODO: Create buildSubRecList()
  local deleteList = buildSubRecList( src, topRec, ldtCtrl );
  local listSize = list.size( deleteList );
  local digestString;
  local subrec;
  for i = 1, listSize, 1 do
      -- Open the Subrecord -- and then remove it.
      digestString = tostring( deleteList[i] );
      GP=F and trace("[SUBREC DELETE]<%s:%s> About to Open and Delete(%s)",
        MOD, meth, digestString );
      subrec = ldt_common.openSubRec( src, topRec, digestString );
      if( subrec ~= nil ) then
        rc = ldt_common.removeSubRec(src, topRec, propMap, digestString );
        if( rc == nil or rc == 0 ) then
          GP=F and trace("[STATUS]<%s:%s> Successful CREC REMOVE", MOD, meth );
        else
          warn("[SUB DELETE ERROR] RC(%d) Bin(%s)", MOD, meth, rc, ldtBinName);
          error( ldte.ERR_SUBREC_DELETE );
        end
      else
        warn("[ERROR]<%s:%s> Can't open Subrec: Digest(%s)", MOD, meth,
          digestString );
      end
  end -- for each subrecord
  return rc;

end -- lstack_delete_subrecs()

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
-- WriteBlockSize:: The namespace Write Block Size (in bytes)
-- PageSize      :: Targetted Page Size (8kb to 1mb)
-- ========================================================================
local function compute_settings(ldtMap, configMap )
  local meth="compute_settings()"
  -- Perform some validation of the user's Config Parameters
  local rc = ldt_common.validateConfigParms(ldtMap, configMap);
  if rc ~= 0 then
    warn("[ERROR]<%s:%s> Unable to Set Configuration due to errors",
      MOD, meth);
    error(ldte.ERR_INPUT_PARAM);
  end

  -- Now that all of the values have been validated, we can use them
  -- safely without worry.  No more checking needed.
  local maxObjectSize   = configMap.MaxObjectSize;
  local pageSize        = configMap.TargetPageSize;
  local writeBlockSize  = configMap.WriteBlockSize;
  local recordOverHead  = configMap.RecordOverHead;
  local userPageSize    = configMap.PageSize;

  -- These are the values that we have to set.
  local hotListMax;     -- # of elements stored in the hot list
  local hotListTransfer;-- # of hot elements to move (per op) to the warm list
  local ldrListMax;     -- # of elements to store in a data sub-rec
  local warmListMax;    -- # of sub-rec digests to keep in the TopRec
  local warmListTransfer;-- # of digest entries to transfer to cold list
  local coldListMax;    -- # of digest entries to keep in a Cold Dir
  local coldDirMax;     -- # of Cold Dir sub-recs to keep.

  local ldtOverHead = 500; -- Overhead in Bytes.  Used in Hot List Calc.
  recordOverHead = recordOverHead + ldtOverHead;

  -- Set up our ceilings:
  -- First, set up our Hot List ceiling.
  --
  -- Figure out the settings for our Hot List.
  -- By default, if they can fit, we'd like to have as many as 100
  -- elements in our Hot List.  However, for Large Objects, we'll 
  -- have to settle for far fewer.
  -- In general, we would hope for at least a hot list of size 4
  -- (4 items, with a transfer size of 2) however, if we have really
  -- large objects AND/OR we have multiple LDTs in this record, then
  -- we cannot hog TopRecord space for the Hot List.
  local hotListCountCeiling = 100;
  -- We'd like the Hot List to fit in under the ceiling amount,
  -- or be 1/2 of the target page size, whichever is less.
  local halfPage = math.floor(pageSize / 2);
  local hotListByteCeiling = 50000;
  local hotListTargetBytes =
    (halfPage < hotListByteCeiling) and (halfPage) or hotListByteCeiling;

  -- If not even a pair of (Max Size) objects can fit in the Hot List,
  -- then we need to skip the Hot List altogether and have
  -- items be written directly to the Warm List (the LDR pages).
  if ((maxObjectSize * 2) > hotListTargetBytes) then
    -- Don't bother with a Hot List.  Objects are too big.
    hotListMax = 0;
    hotListTransfer = 0;
  else
    hotListMax = hotListTargetBytes / maxObjectSize;
    if hotListMax > hotListCountCeiling then
      hotListMax = hotListCountCeiling;
    end
    hotListTransfer = hotListMax / 2;
  end

  -- This is the max amount of room we expect the Hot List to use.
  local hotListFootPrint = hotListMax * maxObjectSize;

  -- In general, the WARM List is not bounded by Object Size, but it
  -- is affected by the number of OTHER LDTs and bins that might be in the
  -- record.  If LDT is not the sole LDT in the Top Record, we won't
  -- allocate the maximum amount of space.  The recordOverHead value
  -- that is passed in shows us the expected "interference" from other
  -- KVS bins or other LDTs.  
  local warmListCountCeiling = 100;
  local warmListByteCeiling = 2000;
  local warmListEntrySize = 20;

  local spaceLeft =  pageSize - (hotListFootPrint + recordOverHead);
  if spaceLeft > warmListByteCeiling then
    warmListMax = warmListCountCeiling;
    warmListTransfer = 10;
  elseif spaceLeft < (warmListEntrySize * 10) then
    warn("[WARNING]<%s:%s> Too Little Size for LSTACK WARM LIST: (%d bytes)",
      MOD, meth, spaceLeft);
    warmListMax = 8;
    warmListTransfer = 2;
  else
    warmListMax = math.floor(spaceLeft / warmListEntrySize);
    warmListTransfer = 2;
  end

  -- Similar to the Hot List calcuations, we see how much data will fit
  -- on a Ldt Data Record (LDR) sub-record. Notice that the ldt_common
  -- function has already figured out our optimal pageSize, so we'll just
  -- use that.
  if maxObjectSize > pageSize then
    local spaceLeft = (writeBlockSize - recordOverHead);
    if maxObjectSize > spaceLeft then
      warn("[WARNING]<%s:%s> Max Object Size(%d) Too Large for system(%d)",
        MOD, meth, maxObjectSize, spaceLeft);
      error(ldte.ERR_INPUT_PARM);
    end

    ldrListMax = 1;
  else
    ldrListMax = math.floor(pageSize / maxObjectSize);
  end

  if (ldrListMax < (userPageSize / maxObjectSize)) then
    ldrListMax = math.floor(userPageSize / maxObjectSize)
  end

  -- The Cold List can really stay unbounded, but if we have a conservative
  -- "Max Object Count", then we can really compute our upper bounds.
  -- However, for larger key sizes, we may have to
  -- use larger than requested PageSizes.
  coldListMax = 200;
  coldDirMax = 100;

  -- Apply our computed values.
  ldtMap[LS.LdrEntryCountMax] = ldrListMax;
  ldtMap[LS.HotListMax]       = hotListMax;
  ldtMap[LS.HotListTransfer]  = hotListTransfer;
  ldtMap[LS.WarmListMax]      = warmListMax;
  ldtMap[LS.WarmListTransfer] = warmListTransfer;
  ldtMap[LS.ColdListMax]      = coldListMax;
  ldtMap[LS.ColdDirRecMax]    = coldDirMax;
  return 0;
end 

-- ======================================================================
-- setupLdtBin()
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, firstValue, createSpec ) 
  local meth = "setupLdtBin()";
  GP=E and info("[ENTER]<%s:%s> Bin(%s)",MOD,meth,tostring(ldtBinName));

  local ldtCtrl = initializeLdtCtrl( topRec, ldtBinName );
  local propMap = ldtCtrl[LDT_PROP_MAP]; 
  local ldtMap = ldtCtrl[LDT_CTRL_MAP]; 

  -- Remember that record.set_type() for the TopRec
  -- is handled in initializeLdtCtrl()

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
    configMap.MaxObjectSize = ldt_common.getValSize(firstValue);
  end
  compute_settings(ldtMap, configMap);

  -- Sets the topRec control bin attribute to point to the 2 item list
  -- we created from initializeLdtCtrl() : 
  -- Item 1 :  the property map & Item 2 : the ldtMap
  topRec[ldtBinName] = ldtCtrl; -- store in the record
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- NOTE: The Caller will write out the LDT bin.  Also, call Create() will
  -- use the ldtCtrl return value, rather than re-access the Top-Record to
  -- get it.
  GP=E and info("[EXIT]<%s:%s> LDT(%s)",MOD,meth,tostring(ldtCtrl));
  return ldtCtrl;
end -- setupLdtBin()

-- ========================================================================
-- This function is (still) under construction
-- ========================================================================
-- lstack_trim() -- Remove all but the top N elements
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) trimCount: Leave this many elements on the stack
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NOTE: This function not currently in use.
-- NOTE: This function would also benefit from the eventual SubRec Release()
-- ========================================================================
local function lstack_trim( topRec, ldtBinName, trimCount )
  local meth = "lstack_trim()";

  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s) trimCount(%s)",
    MOD, meth, tostring(ldtBinName), tostring( trimCount ));

  warn("[NOTICE!!]<%s:%s> Under Construction", MOD, meth );
  local rc = 0;

--  -- validate the topRec, the bin and the map.  If anything is weird, then
--  -- this will kick out with a long jump error() call.
--  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
--
--  -- Move to the location (Hot, Warm or Cold) that is the trim point.
--  -- TODO: Create locatePosition()
--  local searchPath = locatePosition( topRec, ldtCtrl, trimCount );
--
--  -- From searchPath to the end, release storage.
--  -- TODO: Create localTrim()
--  localTrim( topRec, ldtCtrl, searchPath );
--

  GP=E and trace("[EXIT]: <%s:%s>", MOD, meth );

  return rc;
end -- function lstack_trim()

-- ========================================================================
-- This function is (still) under construction.
-- ========================================================================
-- lstack_subrec_list() -- Return a list of subrecs
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = (when successful) List of SUBRECs
--   res = (when error) Empty List
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- THIS FUNCTION IS NOT CURRENTLY IN USE.
-- ========================================================================
local function lstack_subrec_list( src, topRec, ldtBinName )
  local meth = "lstack_subrec_list()";

  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Extract the property map and LDT Map from the LDT Control.
  local ldtCtrl = topRec[ ldtBinName ];
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Copy the warm list into the result list
  local wdList = ldtMap[LS.WarmDigestList];
  local transAmount = list.size( wdList );
  local resultList = list.take( wdList, transAmount );

  -- Now pull the digests from the Cold List
  -- There are TWO types subrecords:
  -- (*) There are the LDRs (Data Records) subrecs
  -- (*) There are the Cold List Directory subrecs
  -- We will read a Directory Head, and enter it's digest
  -- Then we'll pull the digests out of it (just like a warm list)

  -- If there is no Cold List, then return immediately -- nothing more read.
  if(ldtMap[LS.ColdDirListHead] == nil or ldtMap[LS.ColdDirListHead] == 0) then
    return resultList;
  end

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.
  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];

  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    -- Save the Dir Digest
    list.append( resultList, coldDirRecDigest );

    -- Open the Directory Page, read the digest list
    local digestString = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
    local digestList = coldDirRec[COLD_DIR_LIST_BIN];
    for i = 1, list.size(digestList), 1 do 
      list.append( resultList, digestList[i] );
    end

    -- Get the next Cold Dir Node in the list
    local coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
    -- If no more, we'll drop out of the loop, and if there's more, 
    -- we'll get it in the next round.
    -- Close this directory subrec before we open another one.
    ldt_common.closeSubRec( src, coldDirRec, false );

  end -- Loop thru each cold directory

  GP=E and trace("[EXIT]:<%s:%s> SubRec Digest Result List(%s)",
      MOD, meth, tostring( resultList ) );

  return resultList
end -- lstack_subrec_list()

-- ======================================================================
-- localReset()
-- ======================================================================
-- Reset the LDT -- back to its pristine state.
-- ======================================================================
local function localReset( src, topRec, ldtCtrl )
  local meth = "lstackReset()";

  GP=E and trace("[ENTER]: <%s:%s> ldtCtrl(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ));
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- If there's an ESR, release it.
  -- Then reset everything and save the pristine ldtCtrl.
  ldt_common.reset( src, topRec, ldtCtrl);

  -- If we have a Hot List Size, build a new Hot List, otherwise
  -- create an empty list.
  if ldtMap[LS.HotListMax] == 0 then
    ldtMap[LS.HotList] = list();
  else
    ldtMap[LS.HotList] = list.new(ldtMap[LS.HotListMax]);
  end
  ldtMap[LS.WarmDigestList] = list.new(ldtMap[LS.WarmListMax]);
  ldtMap[LS.ColdListHead] = 0;

  GP=E and trace("[EXIT]: <%s:%s> ldtCtrl(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ));
end


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||  LSTACK External Functions                                       ||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSTACK module:
--
-- (*) Status = push(topRec, ldtBinName, newValue, createSpec, src)
-- (*) Status = push_all(topRec, ldtBinName, valueList, createSpec, src)
-- (*) List   = peek(topRec, ldtBinName, peekCount, src) 
-- (*) List   = pop(topRec, ldtBinName, popCount, src) 
-- (*) List   = scan(topRec, ldtBinName, src)
-- (*) List = filter(topRec,ldtBinName,peekCount,filterModule,filter,fargs,src)
-- (*) Status = destroy(topRec, ldtBinName, src)
-- (*) Number = size(topRec, ldtBinName)
-- (*) Map    = get_config(topRec, ldtBinName)
-- (*) Status = set_capacity(topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity(topRec, ldtBinName)

-- ======================================================================
-- The following functions are deprecated:
-- (*) Status =  create( topRec, ldtBinName, createSpec )
-- ======================================================================
-- Use this table to export the LSTACK functions to other UDF modules,
-- including the main Aerospike External LDT LSTACK module.
-- Then, each function that is exported from this module is placed in the
-- lstack table for export.  All other functions in this module are internal.
local lstack = {};

-- ======================================================================
-- lstack.create() (Deprecated)
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSTACK
-- bin, using User's name, but Aerospike TYPE (LSTACK).
--
-- The LDT starts out with the first N (default to 100) elements stored
-- directly in the record.  That list is referred to as the "Hot List. Once
-- the Hot List overflows, the entries flow into the warm list, which is a
-- list of Sub-Records.  Each Sub-Record holds N values, where N is
-- a configurable value -- but default is the same size as the Hot List.
-- Once the data overflows the warm list, it flows into the cold list,
-- which is a linked list of directory pages -- where each directory page
-- points to a list of LDT Data Record pages.  Each directory page holds
-- roughly 100 page pointers (assuming a 2k page).
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) createSpec: The UDF CreateModule Name, or config map, for
--                 setting confif values.
--
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
function lstack.create( topRec, ldtBinName, createSpec )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK CREATE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.create()";
  GP=E and trace("[ENTER]:<%s:%s>BIN(%s) createSpec(%s)",
      MOD, meth, tostring(ldtBinName), tostring( createSpec ));

  -- First, check the validity of the Bin Name.
  -- This will throw and error and jump out of Lua if ldtBinName is bad.
  ldt_common.validateBinName( ldtBinName );
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
  -- NOTE: Do NOT call validateRecBinAndMap().  Not needed here.
  
  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", MOD, meth );
  -- We need a new LDT bin -- set it up.
  local ldtCtrl = setupLdtBin( topRec, ldtBinName, nil, createSpec );

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  -- The create was done higher up because we need to do this right away
  -- in order to work with the LDT fields.  Now, after some additional
  -- changes, we call update().
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
  
  GP=E and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lstack.create()

-- =======================================================================
-- lstack.push() : Push a value onto the stack.
-- =======================================================================
-- Push a value on the stack, with the optional parm to set the LDT
-- configuration in case we have to create the LDT before calling the push.
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
--
-- Regarding push(). There are different cases, with different
-- levels of complexity:
-- (*) HotListInsert: Instant: Easy
-- (*) WarmListInsert: Result of HotList Overflow:  Medium
-- (*) ColdListInsert: Result of WarmList Overflow:  Complex
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) newValue: The value to be inserted (pushed on the stack)
-- (4) createSpec: The UDF CreateModule Name, or config map, for
--                 setting confif values on First Value Insert.
-- (5) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- NOTE: When using info/trace calls, ALL parameters must be protected
-- with "tostring()" so that we do not encounter a format error if the user
-- passes in nil or any other incorrect value/type.
-- =======================================================================
function lstack.push( topRec, ldtBinName, newValue, createSpec, src )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.PUSH ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.push()";
  GP=E and trace("[ENTER]<%s:%s> BIN(%s) NewVal(%s) createSpec(%s)", MOD, meth,
    tostring(ldtBinName), tostring( newValue ), tostring( createSpec ) );

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    trace("[Notice] <%s:%s> LSTACK CONTROL BIN does not Exist:Creating",
      MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, newValue, createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  G_Filter = ldt_common.setReadFunctions( nil, nil );

  local newStoreValue = newValue;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Call the common "localPush()" function to do the actual insert.  This
  -- is shared with the lstack.push_all() function.
  localPush( topRec, ldtCtrl, newStoreValue, src );

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  -- Must always assign the object BACK into the record bin.
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- All done, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", MOD, meth );

  -- Update the Top Record.
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- function lstack.push()

-- =======================================================================
-- lstack.push_all()
-- =======================================================================
-- Iterate thru the list and call localStackPush on each element
-- Parms:
-- (*) topRec: The AS Record
-- (*) ldtBinName: Name of the LDT record
-- (*) valueList: List of values to push onto the stack
-- (*) createSpec: Map or Name of Configure UDF
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- =======================================================================
function lstack.push_all( topRec, ldtBinName, valueList, createSpec, src )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.PUSH_ALL ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.push_all()";
  GP=E and trace("[ENTER]<%s:%s> BIN(%s) valueList(%s) createSpec(%s)", MOD,
    meth, tostring(ldtBinName), tostring(valueList), tostring(createSpec));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- Check that the Set Structure is already there, otherwise, create one. 
  if( topRec[ldtBinName] == nil ) then
    trace("[Notice] <%s:%s> LSTACK CONTROL BIN does not Exist:Creating",
      MOD, meth );

    -- set up a new LDT bin
    setupLdtBin( topRec, ldtBinName, valueList[0], createSpec );
  end

  local ldtCtrl = topRec[ldtBinName]; -- The main lmap
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  G_Filter = ldt_common.setReadFunctions( nil, nil );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Loop thru the value list.  So, for each element ...
  -- Call "localPush()", which does the following:
  -- If we have room, do the simple list insert.  If we don't have
  -- room, then make room -- transfer half the list out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.
  local rc = 0;
  local newStoreValue;
  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      local newStoreValue = valueList[i];
      localPush( topRec, ldtCtrl, newStoreValue, src );

    end -- For each item in the valueList
  else
    warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
      MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  -- All Done -- now get ready to finish up.
  -- Must always assign the object BACK into the record bin.
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- Now, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", MOD, meth );

  -- Update the Top Record.
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- end lstack.push_all()

-- ======================================================================
-- lstack.peek(): Return N elements from the top of the stack.
-- ======================================================================
-- Return "peekCount" values from the stack, in Stack (LIFO) order.
-- If "peekCount" is zero, then return all (same as a scan).
-- Depending on "peekcount", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotList), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) peekCount: The number of items to read from the stack
-- (4) filterModule: Lua file that potentially holds the filter function
-- (5) filter: The "Inner UDF" that will filter Peek output
-- (6) fargs: Arg List to the filter function (i.e. func(val, fargs)).
-- (7) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   res = (when successful) List (empty or populated) 
--   res = (when error) nil
-- Note 1: We need to switch to a two-part return, with the first value
-- being the status return code, and the second being the content (or
-- error message).
--
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- NOTE: July 2013:tjl: Now using the SubrecContext to track the open
-- subrecs.
-- ======================================================================
function
lstack.peek( topRec, ldtBinName, peekCount, filterModule, filter, fargs, src )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.PEEK ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.peek()";
  GP=E and trace("[ENTER]<%s:%s> Bin(%s) Cnt(%s) Mod((%s) filter(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(peekCount),
    tostring(filterModule), tostring(filter), tostring(fargs) );

  -- Some simple protection of faulty records or bad bin names
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=F and trace("[DEBUG]: <%s:%s> LDT List Summary(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ) );

  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  
  -- When the user passes in a "peekCount" of ZERO, then we read ALL.
  -- Actually -- we will also read ALL if count is negative.
  -- New addition -- with the STORE LIMIT addition (July 2013) we now
  -- also limit our peeks to the storage limit -- which also discards
  -- storage for LDRs holding items beyond the limit.
  -- A storeLimit of ZERO (or negative) means "no limit".
  local count = 0;
  local itemCount = propMap[PM.ItemCount];
  local storeLimit = ldtMap[LC.StoreLimit];
  -- Check for a special value.
  if( storeLimit <= 0 ) then
      storeLimit = itemCount;
  end

  -- Set our expected Count.  Note that we no longer use the "all" flag
  -- because we need to stop reading once we hit the storeLimit value.
  if( peekCount <= 0 ) then
    if( itemCount < storeLimit ) then
      count = itemCount;
    else
      count = storeLimit; -- peek NO MORE than our storage limit.
    end
  elseif( peekCount > storeLimit ) then
    count = storeLimit;
  else
    count = peekCount;
  end

  -- Set up our answer list.  Use the NEW list constructor that will
  -- allocate enough space for our full answer set.
  local resultList = list.new(count); -- everyone will fill this in

  GP=F and trace("[DEBUG]<%s:%s> Peek with Count(%d) StoreLimit(%d)",
      MOD, meth, count, storeLimit );

  -- Fetch from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local resultList = hotListRead(resultList, ldtCtrl, count);
  local numRead = list.size( resultList );
  GP=F and trace("[DEBUG]: <%s:%s> HotListResult:Summary(%s)",
      MOD, meth, ldt_common.summarizeList(resultList));


  -- If the list had all that we need, then done.  Return list.
  if( numRead >= count  or numRead >= propMap[PM.ItemCount] ) then
    return resultList;
  end

  -- We need more -- get more out of the Warm List.  We compute the correct
  -- READ count, using the count that we've already read from the Hot List.
  local remainingCount = count - numRead;

  GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d)",
    MOD, meth, remainingCount);

  -- Try reading from the Warm List.
  local warmCount = 0;
  local warmDigestList = ldtMap[LS.WarmDigestList];
  if #warmDigestList > 0 then
    warmCount = digestListRead(src, topRec, resultList, ldtCtrl,
                               warmDigestList, remainingCount);
  end

  -- The Warm list was not enough.  We need more, so get more out of the
  -- COLD List.  Compute the correct READ count given that we've already
  -- read from the Hot and Warm Lists.
  remainingCount = count - numRead - warmCount;
      GP=F and trace("[DEBUG]:<%s:%s>After WmRd:RC(%d) Cnt(%d) NR(%d) WC(%d)",
        MOD, meth, remainingCount, count, numRead, warmCount );

  GP=F and trace("[DEBUG]:<%s:%s>After WarmListRead: ldtMap(%s) ldtCtrl(%s)",
    MOD, meth, tostring(ldtMap), ldtSummaryString(ldtCtrl));

  numRead = list.size( resultList );
  -- If we've read enough, then return.
  if ( remainingCount <= 0 ) or (numRead >= propMap[PM.ItemCount] )  then
      return resultList; -- We have all we need.  Return.
  end

  -- Otherwise, go look for more in the Cold List.
  local coldCount = coldListRead(src,topRec,resultList,ldtCtrl,remainingCount);

  GP=E and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    MOD, meth, peekCount, ldt_common.summarizeList(resultList));

  return resultList;
end -- function lstack.peek() 

-- ======================================================================
-- lstack.pop(): Remove and return N elements from the top of the stack.
-- ======================================================================
-- Remove and Return "popCount" values from the stack, in Stack (LIFO) order.
-- If "popCount" is zero, then return all and EMPTY the stack.
-- Just like peek(), Depending on "Count", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
--
-- The N items are read and removed from the stack, BUT only those items
-- that match the filter (if supplied) are returned to the caller.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) count: The number of items to pop off the stack
-- (4) filterModule: The Lua file that potentially holds the filter function
-- (5) filter: The "Inner UDF" that will filter Peek output
-- (6) fargs: Arg List to the filter function (i.e. func(val, fargs)).
-- (7) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   res = (when successful) List (empty or populated) 
--   res = (when error) nil
-- Note 1: We need to switch to a two-part return, with the first value
-- being the status return code, and the second being the content (or
-- error message).
--
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- NOTE: July 2013:tjl: Now using the SubrecContext to track the open
-- subrecs.
-- ======================================================================
function
lstack.pop( topRec, ldtBinName, popCount, filterModule, filter, fargs, src )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.POP ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.pop()";

  GP=E and trace("[ENTER]<%s:%s> BIN(%s) Cnt(%s) FM(%s) Filter(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(popCount),
    tostring(filterModule), tostring(filter), tostring(fargs) );

  -- Some simple protection of faulty records or bad bin names
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=D and trace("[DEBUG]: <%s:%s> LDT List Summary(%s)",
    MOD, meth, ldtSummaryString( ldtCtrl ) );

  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  
  -- When the user passes in a "popCount" of ZERO, then we read ALL.
  -- Actually -- we will also read ALL if popCount is negative.
  -- New addition -- with the STORE LIMIT addition (July 2013) we now
  -- also limit our pops to the storage limit -- which also discards
  -- storage for LDRs holding items beyond the limit.
  -- A storeLimit of ZERO (or negative) means "no limit".
  local popAll = false;
  local count = 0;
  local itemCount = propMap[PM.ItemCount];
  local storeLimit = ldtMap[LC.StoreLimit];
  -- If StoreLimit is zero, then there's no limit.  However, for our local
  -- computation, we want to show that we're not at the limit, so we
  -- just pretend that our limit is one more than the actual count.
  if( storeLimit <= 0 ) then
      storeLimit = itemCount + 1;
  end

  -- If we are asked to remove all of the values, regardless of the way
  -- it is specified (either with a ZERO count, which means ALL, or with a
  -- count that is >= itemCount), then we actually retrieve the entire
  -- amount with a peek call, and then we do a full RESET of the lstack.
  if ( popCount <= 0 or popCount >= itemCount or popCount >= storeLimit ) then
    popAll = true
    count = itemCount;
  else
    count = popCount;
  end

  -- Set up our answer list.  Use the new List Constructor.
  local resultList = list.new(count);

  if popAll then
    -- There is currently a problem with a read followed by reset, so for
    -- now do the full pop() and reset.
    -- localPeek( src, resultList, topRec, ldtCtrl, count );
    localPop( src, resultList, topRec, ldtCtrl, count );
    -- localReset( src, topRec, ldtCtrl );
  else
    localPop( src, resultList, topRec, ldtCtrl, count );
  end

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    warn("[ERROR]<%s:%s> Problems closing subrecs in delete", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  -- Must always assign the object BACK into the record bin.
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- Update the Top Record.
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 

  GP=E and trace("[EXIT]: <%s:%s>: Count(%d) ResultListSummary(%s)",
    MOD, meth, count, ldt_common.summarizeList(resultList));

  return resultList;
end -- function lstack.pop() 

-- ========================================================================
-- lstack.size() -- return the number of elements (item count) in the stack.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the size)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack.size( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.SIZE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.size()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  local itemCount = propMap[PM.ItemCount];
  local storeLimit = ldtMap[LC.StoreLimit];
  -- Check for a special value.
  if( storeLimit <= 0 ) then
      storeLimit = itemCount;
  end

  -- Note that itemCount should never appear larger than the storeLimit,
  -- but until our internal accounting is fixed, we fudge it like this.
  if( itemCount > storeLimit ) then
      itemCount = storeLimit;
  end

  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function lstack.size()

-- ========================================================================
-- lstack.get_capacity() -- return the current capacity setting for LSTACK.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack.get_capacity( topRec, ldtBinName )
--  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.GET_CAPACITY ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end



  return ldt_common.get_capacity(topRec, ldtBinName, LDT_TYPE, G_LDT_VERSION);

--  local meth = "lstack.get_capacity()";
--
--  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s)",
--    MOD, meth, tostring(ldtBinName));
--
--  -- validate the topRec, the bin and the map.  If anything is weird, then
--  -- this will kick out with a long jump error() call.
--  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
--  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
--  local capacity = ldtMap[LC.StoreLimit];
--
--  GP=DEBUG and ldtDebugDump( ldtCtrl );
--
--  GP=E and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, capacity );
--
--  return capacity;
end -- function lstack.get_capacity()

-- ========================================================================
-- lstack.set_capacity()
-- ========================================================================
-- This is a special command to both set the new storage limit.  It does
-- NOT release storage, however.  That is done either lazily after a 
-- warm/cold insert or with an explit lstack_trim() command.
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The name of the LDT Bin
-- (*) newLimit: The new limit of the number of entries
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function lstack.set_capacity( topRec, ldtBinName, newLimit )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK SET CAPACITY ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack_set_capacity()";
  GP=E and trace("[ENTER]: <%s:%s> ldtBinName(%s) newLimit(%s)",
    MOD, meth, tostring(ldtBinName), tostring(newLimit));

  local rc = 0; -- start off optimistic

  -- Validate user parameters
  if( type( newLimit ) ~= "number" or newLimit <= 0 ) then
    warn("[PARAMETER ERROR]<%s:%s> newLimit(%s) must be a positive number",
      MOD, meth, tostring( newLimit ));
    error( ldte.ERR_INPUT_PARM );
  end

  -- Validate the ldtBinName before moving forward
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  GP=DEBUG and ldtDebugDump( ldtCtrl );

  GP=D and
  trace("[PARAMETER UPDATE]<%s:%s> StoreLimit: Old(%d) New(%d) ItemCount(%d)",
    MOD, meth, ldtMap[LC.StoreLimit], newLimit, propMap[PM.ItemCount] );

  -- Use the new "Limit" to compute how this affects the storage parameters.
  -- Basically, we want to determine how many Cold List directories this
  -- new limit equates to.  Then, if we add more than that many cold list
  -- directories, we'll release that storage.
  -- Note: We're doing this in terms of LIST mode, not yet in terms of
  -- binary mode.  We must compute this in terms of MINIMAL occupancy, so
  -- that we always have AT LEAST "Limit" items in the stack.  Therefore,
  -- we compute Hotlist as minimal size (max - transfer) as well as WarmList
  -- (max - transfer)
  -- Total Space comprises:
  -- (*) Hot List Storage
  -- ==> (HotListMax - HotListTransfer)  Items (minimum)
  -- (*) Warm List Storage
  -- ==> ((WarmListMax - WarmListTransfer) * LdrEntryCountMax)
  -- (*) ColdList (one Cold Dir) Capacity -- because there is no limit to
  --     the number of coldDir Records we can have.
  -- ==> ColdListMax * LdrEntryCountMax
  --
  -- So -- if we set the limit to 10,000 items and all of our parameters
  -- are set to 100:
  -- HotListMax = 100
  -- HotListTransfer = 50
  -- WarmListMax = 100
  -- WarmListTransfer = 50
  -- LdrEntryCountMax = 100
  -- ColdListMax = 100
  --
  -- Then, our numbers look like this:
  -- (*) Hot Storage (between 50 and 100 data elements)
  -- (*) LDR Storage (100 elements) (per Warm or Cold Digest)
  -- (*) Warm Storage (between 5,000 and 10,000 elements)
  -- (*) Cold Dir Storage ( between 5,000 and 10,000 elements for the FIRST
  --     Cold Dir (the head), and 10,000 elements for every Cold Dir after
  --     that.
  --
  -- So, a limit of 75 would keep all storage in the hot list, with a little
  -- overflow into the warm List.  An Optimal setting would set the
  -- Hot List to 100 and the transfer amount to 25, thus guaranteeing that
  -- the HotList always contained the desired top 75 elements.  However,
  -- we expect capacity numbers to be in the thousands, not tens.
  --
  -- A limit of 1,000 would limit Warm Storage to 10 (probably 10+1)
  -- warm list digest cells.
  --
  -- A limit of 10,000 would limit the Cold Storage to a single Dir list,
  -- which would release "transfer list" amount of data when that much more
  -- was coming in.
  --
  -- A limit of 20,000 would limit Cold Storage to 2:
  -- 50 Hot, 5,000 Warm, 15,000 Cold.
  --
  -- For now, we're just going to release storage at the COLD level.  So,
  -- we'll basically compute a stairstep function of how many Cold Directory
  -- records we want to use, based on the system parameters.
  -- Under 10,000:  1 Cold Dir
  -- Under 20,000:  2 Cold Dir
  -- Under 50,000:  5 Cold Dir
  --
  -- First -- if the new "capacity" is zero -- there's no work to be done,
  -- other than to save the value.  Zero capacity means "no limit".
  if( newLimit <= 0 ) then
    if( ldtMap[LC.StoreLimit] <= 0 ) then
      -- Nothing to do here.  Leave early.  Already set.
      GP=E and trace("[Early EXIT]:<%s:%s> Already Set. Return(0)", MOD, meth );
      return 0;
    end
    -- Update the LDT Control map with the new storage limit
    ldtMap[LC.StoreLimit] = newLimit;

  else
  
    -- Ok -- some real work needs to be done.  Update our CONTROL structure
    -- with the appropriate Max values.
    local hotListMin = ldtMap[LS.HotListMax] - ldtMap[LS.HotListTransfer];
    local ldrSize = ldtMap[LS.LdrEntryCountMax];
    local warmListMin =
      (ldtMap[LS.WarmListMax] - ldtMap[LS.WarmListTransfer]) * ldrSize;
    local coldListSize = ldtMap[LS.ColdListMax];
    local coldGranuleSize = ldrSize * coldListSize;
    local coldRecsNeeded = 0;
    if( newLimit < (hotListMin + warmListMin) ) then
      coldRecsNeeded = 0;
    elseif( newLimit < coldGranuleSize ) then
      coldRecsNeeded = 1;
    else
      coldRecsNeeded = math.ceil( newLimit / coldGranuleSize );
    end

    GP=F and trace("[STATUS]<%s:%s> Cold Granule(%d) HLM(%d) WLM(%d)",
      MOD, meth, coldGranuleSize, hotListMin, warmListMin );
    GP=F and trace("[UPDATE]:<%s:%s> New Cold Rec Limit(%d)", MOD, meth, 
      coldRecsNeeded );

    ldtMap[LS.ColdDirRecMax] = coldRecsNeeded;
    ldtMap[LC.StoreLimit] = newLimit;

  end -- end else update LDT CTRL

  topRec[ldtBinName] = ldtCtrl; -- ldtMap is implicitly included.
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time

  -- Update the Top Record.
  rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
  GP=E and trace("[Normal EXIT]:<%s:%s> Return(0)", MOD, meth );
  return 0;
end -- lstack.set_capacity();

-- ========================================================================
-- lstack.config() -- return the LDT config settings
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = (when successful) config Map 
--   res = (when error) nil
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
function lstack.config( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.CONFIG ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.config()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local config = ldtSummary( ldtCtrl );

  GP=E and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lstack.config()

-- ========================================================================
-- lstack.ldt_exists() --
-- ========================================================================
-- return 1 if there is an LSTACK object here, otherwise 0
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   True:  (LSTACK exists in this bin) return 1
--   False: (LSTACK does NOT exist in this bin) return 0
-- ========================================================================
function lstack.ldt_exists( topRec, ldtBinName )
  GP=B and info("\n\n >>>>>>>>>>> API[ LSTACK EXISTS ] <<<<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.ldt_exists()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  if ldt_common.ldt_exists(topRec, ldtBinName, LDT_TYPE ) then
    GP=F and trace("[EXIT]<%s:%s> Exists", MOD, meth);
    return 1
  else
    GP=F and trace("[EXIT]<%s:%s> Does NOT Exist", MOD, meth);
    return 0
  end
end -- function lstack.ldt_exists()

-- ========================================================================
-- lstack.destroy() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- NOTE: This could eventually be moved to COMMON, and be "localLdtDestroy()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function lstack.destroy( topRec, ldtBinName, src )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.DESTROY ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.destroy()";
  GP=E and trace("[ENTER]<%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));
  local rc = 0; -- start off optimistic

  -- Validate the Bin Name before moving forward
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  
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
end -- lstack.destroy()

-- ========================================================================
-- lstack.one()      -- Just return 1.  This is used for perf measurement.
-- ========================================================================
-- Do the minimal amount of work -- just return a number so that we
-- can measure the overhead of the LDT/UDF infrastructure.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) Val:  Random number val (or nothing)
-- Result:
--   res = 1 or val
-- ========================================================================
function lstack.one( topRec, ldtBinName )
  return 1;
end -- lstack.one()

-- ========================================================================
-- lstack.same()         -- Return Val parm.  Used for perf measurement.
-- ========================================================================
function lstack.same( topRec, ldtBinName, val )
  if( val == nil or type(val) ~= "number") then
    return 1;
  else
    return val;
  end
end -- lstack.same()

-- ========================================================================
-- lstack.validate()
-- ========================================================================
-- Look at the structure and SubRec pointers in this LDT.  Make sure that
-- everything is consistent.
-- When DEBUG is true, then print out header info for EACH SubRec.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The LDT bin
-- (3) src:  Optional (assuming there's no resultMap, otherwise required)
-- (4) resultMap: Optional.  If not nil, resultMap = internal information
-- Result:
--   res = 1, if all is well
--   res = 0, if there are any problems
--   else, ERROR (<0) if something blows up
-- ========================================================================
function lstack.validate( topRec, ldtBinName, src, resultMap )
  GP=B and info("\n\n >>>>>>>>> API[ LSTACK.VALIDATE ] <<<<<<<<<< \n");

  -- Tell the ASD Server that we're doing an LDT call -- for stats purposes.
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end


  local meth = "lstack.validate()";
  GP=E and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- Validate the Bin Name and ldtCtrl before moving forward
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local  ldtMap = ldtCtrl[LDT_CTRL_MAP];

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- If the user has given us a resultMap, then use it.  Otherwise, create
  -- a new one to hold all of our accounting information.
  if not resultMap then
    resultMap = map();
  end
  local result = 1; -- start off optimistic.

  if type(resultMap) ~= "userdata" then
    warn("[ERROR]<%s:%s> resultMap parameter must be a MAP", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end

  -- Assemble the information from the TopRec Property Map.  We'll use that
  -- to compare Parent and ESR values in all of the SubRecs.  Also, we're
  -- keeping this data in a map so that it's easy to move around, and to pass
  -- back to the caller if there's interest in seeing everything.
  resultMap.SubRecCount  = propMap[PM.SubRecCount];
  resultMap.ItemCount    = propMap[PM.ItemCount];
  resultMap.LdtType      = propMap[PM.LdtType];
  resultMap.BinName      = propMap[PM.BinName];
  resultMap.Magic        = propMap[PM.Magic];
  resultMap.EsrDigest    = propMap[PM.EsrDigest];
  resultMap.ParentDigest = propMap[PM.ParentDigest];

  -- Save locally for use below.
  local esrDigest        = propMap[PM.EsrDigest];
  local parentDigest     = propMap[PM.ParentDigest];

  -- ---------------------------------------------------------------------
  -- Step 1:  Assemble all of the information
  -- ---------------------------------------------------------------------
  -- Create three lists of digests:
  -- (1) Warm List Data SubRecs
  -- (2) Cold List Directory SubRecs
  -- (3) Cold List Data SubRecs
  -- ---------------------------------------------------------------------
  local subRecCount = 0;
  local warmDigestList = ldtMap[LS.WarmDigestList];
  resultMap.WarmDigestList = list.take(warmDigestList, #warmDigestList)

  subRecCount = subRecCount + #warmDigestList;

  info("[SIZE CHECK] #WarmList(%d) list.size(WarmList)(%d)", 
    #warmDigestList, list.size(warmDigestList));

  info("[VALIDATE] WarmList(%s) WarmListCopy(%s)", 
    tostring(warmDigestList), tostring(resultMap.WarmDigestList));

  -- Process the coldDirList (a linked list) head to tail (that is "append"
  -- order).  For each dir, read in the LDR Records (in reverse list order),
  -- and then each page (in reverse list order), until we've read "count"
  -- items.  If the 'all' flag is true, then read everything.
  local coldDirDigestList = list();  -- a list of cold dir maps
  local coldDataDigestList = list();
  local coldDirRecDigest = ldtMap[LS.ColdDirListHead];
  local coldDirResultMap;
  local coldDirMap; -- the map in the cold dir bin of the SubRec
  
  -- For each Cold Directory, save several things in a map.
  while coldDirRecDigest ~= nil and coldDirRecDigest ~= 0 do
    coldDirResultMap = map();
    coldDirResultMap.Digest = coldDirRecDigest;
    coldDirResultMap.DataDigestList = list();
  
    -- Open the ColdList Directory Page, read the digest list
    local digestString = tostring( coldDirRecDigest ); -- must be a string
    local coldDirRec = ldt_common.openSubRec( src, topRec, digestString );
    local coldDataDitestList = coldDirRec[COLD_DIR_LIST_BIN];
    coldDirResultMap.DataDigestList =
      list.take(coldDataDigestList, #coldDataDigestList);

    info("[VALIDATE] ColdList(%s) ColdListCopy(%s)", 
      tostring(coldDataDigestList), tostring(coldDirResultMap.DataDigestList));
  
    -- Get the next Cold Dir Node in the list
    coldDirMap = coldDirRec[COLD_DIR_CTRL_BIN];
    coldDirRecDigest = coldDirMap[CDM.NextDirRec]; -- Next in Linked List.
    -- If no more, we'll drop out of the loop, and if there's more, 
    -- we'll get it in the next round.
    -- Close this directory subrec before we open another one.
    ldt_common.closeSubRec( src, coldDirRec, false );
  
    -- Done with this entry -- remember it in the cold dir list.
    list.append( coldDirDigestList, coldDirResultMap );

  end -- Loop thru each cold directory
  
  -- ---------------------------------------------------------------------
  -- Step 2:  Process all of the information
  -- ---------------------------------------------------------------------
  -- (1) Validate each Data SubRec in the warm list
  -- (2) Validate the Cold Directory List SubRecs
  -- (3) Validate each Data SubRec in the cold list
  --
  -- ---------------------------------------------------------------------
  -- Step 2.1 :: Process the Warm List
  -- ---------------------------------------------------------------------
  local warmSubRecDigest;
  local warmSubRecDigestString;
  local warmSubRec;
  local warmEsrDigest;
  local warmSelfDigest;
  local warmParentDigest;
  local wsrPropMap;
  for i = 1, #resultMap.WarmDigestList do
    warmSubRecDigest = resultMap.WarmDigestList[i];
    warmSubRecDigestString = tostring(warmSubRecDigest);
    warmSubRec = ldt_common.openSubRec( src, topRec, warmSubRecDigestString );
    if not warmSubRec then
      warn("[ERROR]<%s:%s> Warm SubRec Data NIL for digest(%s)", MOD, meth,
        warmSubRecDigestString);
      return 0;
    end
    wsrPropMap = warmSubRec[SUBREC_PROP_BIN];
    warmEsrDigest = wsrPropMap[PM.EsrDigest];
    -- NOTE: we can't compare the byte values directly with "==" or "~=",
    -- but we CAN (apparently) compare the STRING versions.
    if tostring(warmEsrDigest) ~= tostring(esrDigest) then
      warn("[ERROR]<%s:%s> Warm Data(%s) ESR(%s) <>  TOP ESR(%s)",
        MOD, meth, warmSubRecDigestString, tostring(warmEsrDigest),
        tostring(esrDigest));
      warn("[ERROR]<%s:%s> Warm Data ESR Type(%s) Top ESR Type(%s)",
        MOD, meth, type(warmEsrDigest), type(esrDigest));
      result = 0;
    else
      info("ESRs Match(%s)", tostring(esrDigest));
    end

    if tostring(warmEsrDigest) ~= tostring(esrDigest) then
      info("ESR STRINGS << DO NOT >> Match");
    else
      info("ESR STRINGS Match(%s)", tostring(esrDigest));
    end

    warmSelfDigest = warmSubRec[PM.SelfDigest];
    if tostring(warmSubRecDigest) ~= tostring(warmSelfDigest) then
      warn("[ERROR]<%s:%s> Warm Self Digest(%s) <> Warm List Digest(%s)",
        MOD, meth, tostring(warmSelfDigest), tostring(warmSubRecDigest));
      warn("[ERROR]<%s:%s> Warm Self Digest Type(%s) Warm List Digest Type(%s)",
        MOD, meth, type(warmSubRecDigest), type(warmSelfDigest));
      result = 0;
    else
      info("SELF Digests Match(%s)", tostring(warmSelfDigest));
    end

    if tostring(warmSubRecDigest) ~= tostring(warmSelfDigest) then
      info("SELF STRINGS << DO NOT >> Match:");
    else
      info("SELF STRINGS Match(%s)", tostring(warmSelfDigest));
    end

    warmParentDigest = warmSubRec[PM.ParentDigest];
    if tostring(warmSubRecDigest) ~= tostring(parentDigest) then
      warn("[ERROR]<%s:%s> Warm Parent Digest(%s) <> Top Parent Digest(%s)",
        MOD, meth, tostring(warmParentDigest), tostring(parentDigest));
      warn("[ERROR]<%s:%s> Warm Parent Type(%s) Top Parent Type(%s)",
        MOD, meth, type(warmParentDigest), type(parentDigest));
      result = 0;
    else
      info("PARENT Digests Match(%s)", tostring(warmSelfDigest));
    end

    -- All done -- close this subRec (otherwise, we run out of space)
    ldt_common.closeSubRec( src, warmSubRec, false );

  end -- for each SubRec digest in warm list

  return result;
end -- lstack.validate()()

-- ======================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return lstack;

-- ========================================================================
--   _      _____ _____ ___  _____  _   __
--  | |    /  ___|_   _/ _ \/  __ \| | / /
--  | |    \ `--.  | |/ /_\ \ /  \/| |/ / 
--  | |     `--. \ | ||  _  | |    |    \ 
--  | |____/\__/ / | || | | | \__/\| |\  \
--  \_____/\____/  \_/\_| |_/\____/\_| \_/   (LIB)
--                                        
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
