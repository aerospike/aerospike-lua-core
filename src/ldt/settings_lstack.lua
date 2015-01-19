-- Settings for Large Stack :: settings_lstack.lua
--
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
local MOD="settings_lstack_2014_12_05.A";

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;      -- Global Print Instrument.
local F=false; -- Set F (Flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local D=false; -- Set D (DETAIL) for greater detailed output

-- ======================================================================
-- We now need a new ldt_common function in order to validate the
-- incoming user CONFIG values.
-- ======================================================================
local ldt_common = require('ldt/ldt_common');
local ldte       = require('ldt/ldt_errors');

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LDT Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LC = {
  -- Fields Common to ALL LDTs (managed by the LDT COMMON routines)
  UserModule             = 'P'; -- User's Lua file for overrides
  StoreLimit             = 'L'; -- Max Items: Used for Eviction (eventually)
};

local LS = {
  -- LSTACK specific values
  LdrEntryCountMax       = 'e', -- Max # of entries in an LDR
  LdrByteEntrySize       = 's', -- Fixed Size of a binary Object in LDR
  LdrByteCountMax        = 'b', -- Max # of bytes in an LDR
  HotListMax             = 'h', -- Max Size of the Hot List
  HotListTransfer        = 'X', -- Amount to transfer from Hot List
  WarmListMax            = 'w', -- Max # of Digests in the Warm List
  WarmListTransfer       = 'x', -- Amount to Transfer from the Warm List
  ColdListMax            = 'c',-- Max # of items in a cold dir list
  ColdDirRecMax          = 'C' -- Max # of Cold Dir subrecs we'll have
};

-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

  -- ======================================================================
  -- Accessor Functions for the LSTACK LDT Control Map
  -- ======================================================================
  --
  function exports.set_store_limit( ldtMap, value )
    ldtMap[LC.StoreLimit]       = value;
  end

  function exports.set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[LS.LdrByteEntrySize] = value;
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
  -- writeBlockSize:: The namespace Write Block Size (in bytes)
  -- pageSize      :: Targetted Page Size (8kb to 1mb)
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

    GP=F and info("[INPUT]<%s:%s> MaxObjSize(%d)", MOD, meth,
      maxObjectSize);
    GP=F and info("[INPUT]<%s:%s> PageSize(%d) WriteBlkSize(%d)", MOD, meth,
      pageSize, writeBlockSize);
    GP=F and info("[INPUT]<%s:%s> RecOH(%d)", MOD, meth,
      recordOverHead);

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

    GP=E and trace("[FINAL OUT(1)]<%s:%s> LDR(%d) HLM(%d) HLT(%d)",
      MOD, meth, ldrListMax, hotListMax, hotListTransfer);
    GP=E and trace("[FINAL OUT(2)]<%s:%s> WLM(%d) WLT(%d) CLM(%d) CDM(%d)",
      MOD, meth, warmListMax, warmListTransfer, coldListMax, coldDirMax);

    GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );

    return 0;

  end -- exports.compute_settings()

  -- ========================================================================
  -- Return the exports table to make the exported functions visible.
  -- ========================================================================

return exports;


-- ========================================================================
-- This File:: settings_lstack.lua
--
-- How to Use:  
-- local set_lstack = require('settings_lstack')
--
-- Use the functions in this module to override default ldtMap settings.
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- ========================================================================
