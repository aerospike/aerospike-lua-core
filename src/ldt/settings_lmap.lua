-- Settings for Large Map (settings_lmap.lua)
-- ======================================================================
-- Copyright [2014] Aerospike, Inc.. Portions may be licensed
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

-- Track the date and iteration of the last update:
local MOD="settings_lmap_2014_12_05.A"; -- the module name used for tracing

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;     -- Use for turning on Global Print
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local D=false; -- Set D (DETAIL) for greater detailed output

-- ======================================================================
-- We now need a new ldt_common function in order to validate the
-- incoming user CONFIG values.
-- ======================================================================
local ldt_common = require('ldt/ldt_common');
local ldte       = require('ldt/ldt_errors');

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- HashType (HT) values
-- (*) Static (a fixed size Hash Table)
-- (*) Dynamic (A variable size Hash Table that uses Linear Hash Algorithm)
local HT_STATIC  ='S'; -- Use a FIXED set of bins for hash lists
local HT_DYNAMIC ='D'; -- Use a DYNAMIC set of bins for hash lists

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE='T';
local AS_FALSE='F';

-- Currently, the default for the Hash Table Management is STATIC.  When
-- it is ready (fully tested), we will enable DYNAMIC mode that uses Linear
-- Hashing and has more graceful directory growth (and shrinkage).
local DEFAULT_HASH_STATE = HT_STATIC;

-- ======================================================================
-- NOTE: It's important that these three values remain consistent with
-- the matching variable names in the ldt/lib_lmap.lua file.
-- ======================================================================
-- Switch from CompactList to Hash Table with this many objects.
-- The thresholds vary depending on expected object size.
local DEFAULT_MAX_THRESHOLD    =  0;   -- Objs over  500 kb
local DEFAULT_JUMBO_THRESHOLD  =  4;   -- Objs over  100 kb
local DEFAULT_LARGE_THRESHOLD  = 10;   -- Objs over   10 kb
local DEFAULT_MEDIUM_THRESHOLD = 20;   -- Objs around  1 kb
local DEFAULT_SMALL_THRESHOLD  = 100;  -- Objs under  20 kb

local DEFAULT_THRESHOLD        = DEFAULT_MEDIUM_THRESHOLD;

-- The BIN LIST THRESHOLD is the number of items in a single Hash Cell
-- that we can have before we convert the Hash Cell Contents into a Sub-Rec.
-- For Large Items (e.g. 100kb), we really can't tolerate even a SINGLE
-- object in the hash directory -- because that could easily blow out the
-- Top Record.  For medium size objects we can tolerate some, and for small
-- objects we can tolerate a lot.
local DEFAULT_JUMBO_BINLIST_THRESHOLD  = 0; -- can't have any.
local DEFAULT_LARGE_BINLIST_THRESHOLD  = 1;
local DEFAULT_MEDIUM_BINLIST_THRESHOLD = 4; 
local DEFAULT_SMALL_BINLIST_THRESHOLD  = 10; 
local DEFAULT_BINLIST_THRESHOLD        = DEFAULT_MEDIUM_BINLIST_THRESHOLD;

-- The default starting Hash Directory Size.  When we switch to Dynamic
-- (Linear) Hashing, we'll start Small and work our way up.
local DEFAULT_MODULO = 128; -- The default HashTable Size

local DEFAULT_THRESHOLD = DEFAULT_MEDIUM_THRESHOLD; -- a catch-all

-- ======================================================================
-- Default Capacity -- should be high, but not too high.  If the user wants
-- to go REALLY high, they should specify that.  If we don't pick a value,
-- then the user will hit a storage limit error without warning.
-- ======================================================================
local DEFAULT_JUMBO_CAPACITY   =    500;
local DEFAULT_LARGE_CAPACITY   =   5000;
local DEFAULT_MEDIUM_CAPACITY  =  50000;
local DEFAULT_SMALL_CAPACITY   = 500000;

local DEFAULT_CAPACITY         = DEFAULT_MEDIUM_CAPACITY;
local DEFAULT_TEST_CAPACITY    =  20000;


-- Initial Size of of a STATIC Hash Table.   Once we start to use Linear
-- Hashing (dynamic growth) we'll set the initial size to be small.
local DEFAULT_DISTRIB = 128;

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode
--
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main Large Set Map Field Name Mapping
-- Field definitions for those fields that we'll override
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields unique to lset & lmap 
--
-- Fields Common to ALL LDTs (LC).  Managed by the LDT COMMON routines.
local LC = {
  UserModule             = 'P'; -- User's Lua file for overrides
  StoreLimit             = 'L'; -- Used for Eviction (eventually)
};

-- Fields specific to LMAP (LS)
local LS = {
  LdrEntryCountMax       = 'e'; -- Max size of the LDR List
  LdrByteEntrySize       = 's'; -- Size of a Fixed Byte Object
  LdrByteCountMax        = 'b'; -- Max Size of the LDR in bytes
  StoreState             = 'S'; -- Store State (Compact or List)
  HashType               = 'h'; -- Hash Type (static or dynamic)
  BinaryStoreSize        = 'B'; -- Size of Object when in Binary form
  Modulo 				 = 'm'; -- Modulo used for Hash Function
  Threshold              = 'H'; -- Threshold: Compact->Regular state
  BinListThreshold       = 'l'; -- Threshold for converting from a
                                  -- cell anchor binlist to sub-record.
  OverWrite              = 'o'; -- Allow Overwrite of a Value for a given
                                  -- name.  If false (AS_FALSE), then we
                                  -- throw a UNIQUE error.
};

-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map.
  -- Note that use of these individual functions may result in odd behavior
  -- if you pick strange or incompatible values.
  --
  -- TODO: Document these functions ...
  -- ======================================================================
  --
  function exports.set_store_limit( ldtMap, value )
    ldtMap[LC.StoreLimit]       = value;
  end

  function exports.set_hash_type( ldtMap, value )
    ldtMap[LS.HashType]      = value;
  end

  -- ========================================================================
  -- exports.compute_settings()
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
  -- maxObjectSize :: the maximum object size (in bytes).
  -- maxKeySize    :: the maximum Key size (in bytes).
  -- maxObjectCount:: the maximum LDT Collection size (number of data objects).
  -- writeBlockSize:: The namespace Write Block Size (in bytes)
  -- pageSize      :: Targetted Page Size (8kb to 1mb)
  -- recordOverHead:: Amount of "other" space used in this record.
  -- ========================================================================
  -- For lstack, here are the following significant settings:
  -- (*) Hot List Size
  -- (*) Hot List Transfer amount
  -- (*) Warm List Size
  -- (*) LDT Data Record (LDR) size
  -- ========================================================================
  function exports.compute_settings(ldtMap, configMap )
 
    local meth = "compute_settings()";
    GP=E and info("[ENTER]<%s:%s> LDT Map(%s) Config Settings(%s)", MOD, meth,
      tostring(ldtMap), tostring(configMap));

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

    GP=E and info("[ENTER]<%s:%s> LDT(%s)", MOD, meth, tostring(ldtMap));
    GP=E and info("[DEBUG]<%s:%s> MaxObjSz(%s)", MOD, meth,
      tostring(maxObjectSize));
    GP=E and info("[DEBUG]<%s:%s> MaxKeySz(%s)", MOD, meth,
      tostring(maxKeySize));

    -- These are the values that we have to set.
    local storeState;          -- Start Compact or Regular
    local hashType = HT_STATIC;-- Static or Dynamic 
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

    GP=E and info("[DEBUG]<%s:%s> PageAvail(%d) CompListCeiling(%d) CompListTarget(%d)",
      MOD, meth, pageAvailableBytes, compactListCountCeiling,
      compactListTargetBytes);

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

    GP=E and trace("[FINAL OUT(1)]<%s:%s> CmptThrsh(%d) HDirSz(%d) SS(%s)",
      MOD, meth, hashDirSize, threshold, storeState);
    GP=E and trace("[FINAL OUT(2)]<%s:%s> HshType(%s) CellThr(%d) LDR(%d)",
      MOD, meth, hashType, cellListThreshold, ldrListMax);

    GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );

    return 0;

  end -- exports.compute_settings()

-- ========================================================================
-- Return the exports table to make the exported functions visible.
-- ========================================================================
return exports;


-- settings_lmap.lua
--
-- Use:  
-- local set_lmap = require('settings_lmap')
--
-- Use the functions in this module to override default ldtMap settings.

-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
