-- Settings for Large List (settings_llist.lua)
--
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

-- Track the date and iteration of the last update:
local MOD="settings_llist_2014_12_14.A"; -- the module name used for tracing

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;     -- Global Print Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local D=false; -- Set D (DEBUG) to get more detailed debug output

-- ======================================================================
-- We now need a new ldt_common function in order to validate the
-- incoming user CONFIG values.
-- ======================================================================
local ldt_common = require('ldt/ldt_common');
local ldte       = require('ldt/ldt_errors');

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- AS_BOOLEAN TYPE 
-- NB: No easy boolean in lua use character:
local AS_TRUE='T';
local AS_FALSE='F';

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LLIST LDT Record (root) Map Fields
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- This table contains all of the fields that we use in the standard LLIST
-- control map.  Some of these are runtime values (e.g. R_TotalCount) and
-- some are configuration values (e.g. R_RootListMax).  Although only some
-- of these are needed for configuration, we leave them all in here for
-- completeness. (Last updated June 6, 2014)
--
-- Recall that the assignments here, a readable name to a single letter, is
-- done to save space in the Lua Map Object.  We don't want to store full
-- names in the actual LDT Bin value.  We instead store a single letter
-- per field.
--
-- Fields common (LC) to all LDTs (Configuration)
local LC = {
  UserModule          = 'P';-- User's Lua file for overrides
  StoreLimit          = 'L';-- Storage Capacity Limit
};

-- Fields Specific to Large List.
local LS = {
  KeyUnique           = 'U';-- Are Keys Unique? (AS_TRUE or AS_FALSE))

  -- Tree Settings (Auto Configured)
  -- DYNAMIC
  StoreState          = 'S';-- Compact or Regular Storage (dynamic)
  -- STATIC @ Create
  KeyDataType         = 'd';-- Data Type of key (Number, Integer)
  Threshold           = 'H';-- After this#:Move from compact to tree mode
  RootListMax         = 'R', -- Length of Key List (page list is KL + 1)
  NodeListMax         = 'X',-- Max # of items in a node (key+digest)
  LeafListMax         = 'x',-- Max # of items in a leaf node
};

-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
local exports = {}

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map.
  -- Note that use of these individual functions may result in odd behavior
  -- if you pick strange or incompatible values.
  function exports.set_store_limit( ldtMap, value )
    ldtMap[LC.StoreLimit]       = value;
  end

  function exports.set_key_type( ldtMap, value )
    ldtMap[LC.KeyType]      = value;
  end

  function exports.set_unique_key_false( ldtMap )
    ldtMap[LS.KeyUnique] = AS_FALSE; -- Unique values only.
  end

  function exports.set_unique_key_true( ldtMap )
    ldtMap[LS.KeyUnique] = AS_TRUE; -- Unique values only.
  end

  -- ========================================================================
  -- exports.compute_settings()
  -- ========================================================================
  -- This function takes in the user's settings and sets the appropriate
  -- values in the LDT mechanism.
  --
  -- All parameters must be numbers.  testMode is optional, but if it
  -- exists, it must be a number.
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
  function exports.compute_settings(ldtMap, configMap )
 
    local meth = "compute_settings()";

    -- Perform some validation of the user's Config Parameters.
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
    local pageSize        = configMap.TargetPageSize;
    local writeBlockSize  = configMap.WriteBlockSize;
    local recordOverHead  = configMap.RecordOverHead;

    -- These are the values we're going to set.
    local threshold; -- # of objects that we initially cache in the TopRec
    local rootListMax; -- # of Key/Digest entries we hold in the TopRec
    local nodeListMax; -- # of Key/Digest entries we hold in a Node SubRec.
    local leafListMax; -- # of Data Objects we hold in a Leaf SubRec

    local ldtOverHead = 500; -- Overhead in Bytes.  Used in Root Node calc.
    recordOverHead = recordOverHead + ldtOverHead;

    -- Set up our ceilings:
    -- First, set up our Compact List Threshold ceiling.
    -- To have a compact list for LLIST, we must have at least four
    -- elements.  And, those minimum four elements must fit in the
    -- allotted amount (no more than 100k, no more than 1/2 the page),
    -- even after allowing for record overhead.
    --
    -- The validateConfigParms() call will have pushed up our practical
    -- page size to the max limit if it was set arbitrarily small, so
    -- we start there.
    --
    -- SET UP COMPACT LIST THRESHOLD VALUE.
    local maxThresholdCeiling = 10 * 1024;
    local maxThresholdCount = 100;
    local bytesAvailable = math.floor(pageSize / 2) - recordOverHead;
    if (bytesAvailable > maxThresholdCeiling) then
      bytesAvailable = maxThresholdCeiling;
    end

    -- If we can't hold at least 4 objects, then skip the compact list.
    if ((maxObjectSize * 4) > bytesAvailable) then
      threshold = 0;
      ldtMap[LS.StoreState] = SS_REGULAR; -- start in "compact mode"
    else
      threshold = math.floor(bytesAvailable / maxObjectSize);
      if (threshold > maxThresholdCount) then
        threshold = maxThresholdCount;
      end
      ldtMap[LS.StoreState] = SS_COMPACT; -- start in "compact mode"
    end

    -- SET UP LEAF SIZE VALUES.
    -- Note that the validateConfigParms() call has already adjusted the
    -- requested pagesize to either hold at least 10 items, or to be max size.
    -- Ideally, we would like to hold at least 10 objects in the leaves,
    -- but no more than 100 objects.  Note that this max number may change
    -- as we evolve.
    local adjustedPageSize = pageSize - recordOverHead;
    local minLeafCount = 10;
    local maxLeafCount = 100;
    -- See if objects easily fit in a Leaf.
    if ((maxObjectSize * maxLeafCount) < adjustedPageSize) then
      leafListMax = maxLeafCount;
    elseif ((maxObjectSize * minLeafCount ) <= adjustedPageSize) then
      leafListMax = (adjustedPageSize / maxObjectSize) - 1;
    else
      leafListMax = adjustedPageSize / maxObjectSize;
      if leafListMax <= 2 then
        warn("[WARN]<%s:%s>MaxObjectSize(%d) too great for PageSize(%d)",
          MOD, meth, maxObjectSize, adjustedPageSize);
        error(ldte.ERR_INPUT_TOO_LARGE);
      end
    end

    -- SET UP NODE SIZE VALUES.
    -- Try for at least 100 objects in the nodes, and no more than 200.
    -- However, for larger key sizes, we may have to
    -- use larger than requested PageSizes.
    local minNodeCount = 50;
    local maxNodeCount = 200;

    -- See if the Keys easily fit in a Node.
    if ((maxKeySize * maxNodeCount ) < adjustedPageSize) then
      nodeListMax = maxNodeCount;
    elseif ((maxKeySize * minNodeCount) <= adjustedPageSize) then
      nodeListMax = (adjustedPageSize / maxKeySize) - 1;
    else
      nodeListMax = adjustedPageSize / maxKeySize;
      if nodeListMax <= 2 then
        warn("[WARN]<%s:%s>MaxKeySize(%d) too great for PageSize(%d)",
          MOD, meth, maxKeySize, adjustedPageSize);
        error(ldte.ERR_INPUT_PARM);
      end
    end

    -- SET UP ROOT SIZE VALUES.
    -- Try for at least 20 objects in the nodes, and no more than 100.
    -- However, for larger key sizes, we may have to
    -- use larger than requested PageSizes.
    local adjustedPageSize = pageSize;
    local minRootCount = 20;
    local maxRootCount = 100;
    -- See if the Keys easily fit in the Root
    if ((maxKeySize * maxRootCount) < adjustedPageSize) then
      rootListMax = maxRootCount;
    elseif ((maxKeySize * minRootCount) <= adjustedPageSize) then
      rootListMax = adjustedPageSize / maxKeySize;
      if (rootListMax <= 2) then
        warn("[WARN]<%s:%s>MaxKeySize(%d) too great for ROOT Node(%d)",
          MOD, meth, maxKeySize, adjustedPageSize);
        error(ldte.ERR_INPUT_PARM);
      end
    else
      rootListMax = adjustedPageSize / maxKeySize;
    end

    -- Apply our computed values.
    ldtMap[LS.Threshold]   = threshold;
    ldtMap[LS.RootListMax] = rootListMax;
    ldtMap[LS.NodeListMax] = nodeListMax;
    ldtMap[LS.LeafListMax] = leafListMax;
    return 0;

  end -- exports.compute_settings()

-- ======================================================================
-- Return the exports table to make the exported functions visible.
-- ======================================================================

return exports;


-- ========================================================================
-- File: settings_llist.lua
--
-- How to Use:  
-- local set_llist = require('settings_llist')
--
-- Use the functions in this module to override default ldtMap settings.
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- ========================================================================
