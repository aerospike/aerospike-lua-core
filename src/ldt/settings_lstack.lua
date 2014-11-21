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
local MOD="settings_lstack_2014_11_17.A";

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;      -- Global Print Instrument.
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print

-- ======================================================================
-- We now need a new ldt_common function in order to validate the
-- incoming user CONFIG values.
-- ======================================================================
local ldt_common = require('ldt/ldt_common');

-- ======================================================================
-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LDT Map Field Name Mapping
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LC = {
  -- Fields Common to ALL LDTs (managed by the LDT COMMON routines)
  UserModule             = 'P'; -- User's Lua file for overrides
  KeyFunction            = 'F'; -- User Supplied Key Extract Function
  KeyType                = 'k'; -- Type of Key (Always atomic for LMAP)
  StoreMode              = 'M'; -- SM_LIST or SM_BINARY
  StoreLimit             = 'L'; -- Max Items: Used for Eviction (eventually)
  Transform              = 't'; -- Transform object to storage format
  UnTransform            = 'u'; -- UnTransform from storage to Lua format
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

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- Define a Table of Packages that hold "prepackaged" settings that a user
-- can apply -- rather than having to set each setting individually.
-- ======================================================================
local package = {};
--
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
-- Since it takes a lot to configure an lstack map for a particular app,
-- we use these named packages to set a block of values in a consistent
-- way.  That way, users need to remember just a package name, rather then
-- 20 different settings -- any one of which can create strange behavior
-- if set badly.
-- For now (June 2013), we have just some generic settings for "Standard",
-- "Debug" and "Test". The "Debug" one is special, since it sets the
-- config values artificially low so that it exercises the system.
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
function package.StandardList( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.StandardList()

-- ======================================================================
-- For Jumbo Objects (around 100kb) we use a much smaller list size
-- for the Hot List and a smaller list for the SubRecords (LDR).
-- Package = "ListJumboObject"
-- ======================================================================
function package.ListJumboObject( ldtMap )
  info("[ENTER] Package.ListJumboObject: Map(%s)", tostring(ldtMap));
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 8; -- Max # of items in an LDR (List Mode)
  -- ldtMap[LS.LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 8; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 4; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 10; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 100; -- Max# of Cold DIRECTORY Records
  info("[EXIT] Package.ListJumboObject: Map(%s)", tostring(ldtMap));
end -- package.ListJumboObject()


-- ======================================================================
-- For Large Objects (around 10kb) we use a small list size
-- for the Hot List and a small list for the SubRecords (LDR), but not
-- as small as the list size for JUMBO objects (above).
-- Package = "ListLargeObject"
-- ======================================================================
function package.ListLargeObject( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 20; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0; -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 20; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 10; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 10; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 100; -- Max# of Cold DIRECTORY Records
end -- package.ListLargeObject()

-- ======================================================================
-- For Medium Objects (around 1kb), we use middle of the road numbers.
-- Package = "ListMediumObject"
-- ======================================================================
function package.ListMediumObject( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 0;  -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 200; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 20; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.ListMediumObject()

-- ======================================================================
-- For Small Objects (under 100 bytes), can use larger lists because we
-- can pack more objects in 
-- Package = "ListSmallObject"
-- ======================================================================
function package.ListSmallObject( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 200; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 200; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 400; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 20; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.ListSmallObject()

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
function package.TestModeList( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  ldtMap[LC.StoreLimit]       = 20000; -- 20k entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.TestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- Assumes that we're storing a list of four numbers which can be 
-- compressed with the "compressTest4()" function.
-- ======================================================================
function package.TestModeBinary( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_BINARY;
  ldtMap[LC.Transform]        = "compressTest4";
  ldtMap[LC.UnTransform]      = "unCompressTest4";
  ldtMap[LC.StoreLimit]       = 20000; -- 20k entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 100; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.TestModeBinary()

-- ======================================================================
-- Package = "ProdListValBinStore";
-- Specific Production Use: 
-- (*) Tuple value (5 fields of integers)
-- (*) Transforms
-- (*) Binary Storage (uses a compacted representation)
-- ======================================================================
function package.ProdListValBinStore( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_BINARY;
  ldtMap[LC.Transform]        = "listCompress_5_18";
  ldtMap[LC.UnTransform]      = "listUnCompress_5_18";
  ldtMap[LC.StoreLimit]       = 20000; -- 20k entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 200; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 18;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 2000; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 100; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 50; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 100; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 50; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 100; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.ProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
function package.DebugModeObject( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  ldtMap[LC.StoreLimit]       = 5000; -- 5000 entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeObject()


-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
function package.DebugModeList( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_LIST;
  -- ldtMap[LC.Transform]        = nil;
  -- ldtMap[LC.UnTransform]      = nil;
  ldtMap[LC.StoreLimit]       = 200; -- 200 entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 0;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 0; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 2; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use BINARY MODE.
-- ======================================================================
function package.DebugModeBinary( ldtMap )
  -- General LSTACK Parms:
  ldtMap[LC.StoreMode]        = SM_BINARY;
  ldtMap[LC.Transform]        = "compressTest4";
  ldtMap[LC.UnTransform]      = "unCompressTest4";
  ldtMap[LC.StoreLimit]       = 200; -- 200 entries
  -- LSTACK Data Record (LDR) Chunk Settings: Passed into "Chunk Create"
  ldtMap[LS.LdrEntryCountMax] = 4; -- Max # of items in an LDR (List Mode)
  ldtMap[LS.LdrByteEntrySize] = 16;  -- Byte size of a fixed size Byte Entry
  ldtMap[LS.LdrByteCountMax]  = 65; -- Max # of BYTES in an LDR (binary mode)
  -- Hot Entry List Settings: List of User Entries
  ldtMap[LS.HotListMax]       = 4; -- Max # for the List, when we transfer
  ldtMap[LS.HotListTransfer]  = 2; -- How much to Transfer at a time
  -- Warm Digest List Settings: List of Digests of LSTACK Data Records
  ldtMap[LS.WarmListMax]      = 4; -- # of Warm Data Record Chunks
  ldtMap[LS.WarmListTransfer] = 2; -- # of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  ldtMap[LS.ColdListMax]      = 4; -- # of list entries in a Cold dir node
  ldtMap[LS.ColdDirRecMax]    = 10; -- Max# of Cold DIRECTORY Records
end -- package.DebugModeBinary()

-- ======================================================================
-- applyPackage():
-- ======================================================================
-- Search our standard package names and if the user is requesting one
-- of them -- apply it on the ldtMap.
-- Parms:
-- (*) ldtCtrl: the main LDT Control structure
-- (*) packageName;
-- ======================================================================
local function applyPackage( ldtMap, packageName )
  local meth = "applyPackage()";

  GP=E and trace("[ENTER]: <%s:%s>:: ldtCtrl(%s)::\n packageName(%s)",
    MOD, meth, tostring(ldtMap), tostring( packageName ));

  local ldtPackage = package[packageName];
  if( ldtPackage ~= nil ) then
    ldtPackage( ldtMap );
  end

  GP=E and trace("[EXIT]:<%s:%s>: ldtMap after Adjust(%s)",
  MOD,meth,tostring(ldtMap));

return ldtMap;
end -- applyPackage

-- ======================================================================
-- ======================================================================
-- Define the values and operations that a user can use to modify an LDT
-- control map with certain approved settings.
-- ======================================================================

-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

  function exports.use_package( ldtMap, package_name )
    info("[MODULE] INVOKE PACKAGE(%s)", package_name );
    applyPackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LSTACK LDT Control Map
  -- ======================================================================
  --
  -- StoreMode must be SM_LIST or SM_BINARY
  function exports.set_store_mode( ldtMap, value )
    ldtMap[LC.StoreMode]        = value;
  end

  function exports.set_transform( ldtMap, value )
    ldtMap[LC.Transform]        = value;
  end

  function exports.set_untransform( ldtMap, value )
    ldtMap[LC.UnTransform]      = value;
  end

  function exports.set_store_limit( ldtMap, value )
    ldtMap[LC.StoreLimit]       = value;
  end

  function exports.set_ldr_entry_count_max( ldtMap, value )
    ldtMap[LS.LdrEntryCountMax] = value;
  end

  function exports.set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[LS.LdrByteEntrySize] = value;
  end

  function exports.set_ldr_byte_count_max( ldtMap, value )
    ldtMap[LS.LdrByteCountMax]  = value;
  end

  function exports.set_hotlist_max( ldtMap, value )
    ldtMap[LS.HotListMax]       = value;
  end

  function exports.set_hotlist_transfer( ldtMap, value )
    ldtMap[LS.HotListTransfer]  = value;
  end

  function exports.set_warmlist_max( ldtMap, value )
    ldtMap[LS.WarmListMax]      = value;
  end

  function exports.set_warmlist_transfer( ldtMap, value )
    ldtMap[LS.WarmListTransfer] = value;
  end

  function exports.set_coldlist_max( ldtMap, value )
    ldtMap[LS.ColdListMax]      = value;
  end

  function exports.set_colddir_rec_max( ldtMap, value )
    ldtMap[LS.ColdDirRecMax]    = value;
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
  -- aveObjectSize :: the average object size (in bytes).
  -- maxObjectSize :: the maximum object size (in bytes).
  -- aveKeySize    :: the average Key size (in bytes).
  -- maxKeySize    :: the maximum Key size (in bytes).
  -- aveObjectCount:: the average LDT Collection Size (number of data objects).
  -- maxObjectCount:: the maximum LDT Collection size (number of data objects).
  -- writeBlockSize:: The namespace Write Block Size (in bytes)
  -- pageSize      :: Targetted Page Size (8kb to 1mb)
  -- focus         :: the primary focus for this LDT:
  --               :: 0=no pref, 1=performance, 2=storage efficiency
  -- testMode      :: The style of testing we're doing:
  --               :: nil or zero=none,
  --               :: 1=structure test, value type NUMBER (ignore sizes)
  --               :: 2=structure test, value type OBJECT (use sizes)
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

    --[[
    -- Get our working values out of the config map, and where there are
    -- no values, use the assigned default values.
    local aveObjectSize   =
      (configMap.AveObjectSize ~= nil and configMap.AveObjectSize) or
      DEFAULT_AVE_OBJ_SIZE;

    local maxObjectSize  = 
      (configMap.MaxObjectSize ~= nil and configMap.MaxObjectSize) or
      DEFAULT_MAX_OBJ_SIZE;

    local aveKeySize     =
      (configMap.AveKeySize ~= nil and configMap.AveKeySize) or
      DEFAULT_AVE_KEY_SIZE;

    local maxKeySize     =
      (configMap.MaxKeySize ~= nil and configMap.MaxKeySize) or
      DEFAULT_MAX_KEY_SIZE;

    local aveObjectCount =
      (configMap.AveObjectCount ~= nil and configMap.AveObjectCount) or
      DEFAULT_AVE_OBJ_CNT;

    local maxObjectCount =
      (configMap.MaxObjectCount ~= nil and configMap.MaxObjectCount) or
      DEFAULT_MAX_OBJ_CNT;

    local pageSize       =
      (configMap.TargetPageSize ~= nil and configMap.TargetPageSize) or
      DEFAULT_TARGET_PAGESIZE;

    local writeBlockSize       =
      (configMap.WriteBlockSize ~= nil and configMap.WriteBlockSize) or
      DEFAULT_WRITE_BLOCK_SIZE;

    local focus          =
      (not configMap.Focus and configMap.Focus) or DEFAULT_FOCUS;

    local testMode       =
      (not configMap.TestMode and configMap.TestMode) or DEFAULT_TEST_MODE;

    GP=E and info("[ENTER]<%s:%s> LDT(%s)", MOD, meth, tostring(ldtMap));
    GP=E and info("[DEBUG]<%s:%s>AveObjSz(%s) MaxObjSz(%s)", MOD, meth,
      tostring(aveObjectSize), tostring(maxObjectSize));
    GP=E and info("[DEBUG]<%s:%s>AveKeySz(%s) MaxKeySz(%s)", MOD, meth,
      tostring(aveKeySize), tostring(maxKeySize));
    GP=E and info("[DEBUG]<%s:%s>AveObjCnt(%s) MaxObjCnt(%s)", MOD, meth,
      tostring(aveObjectCount), tostring(maxObjectCount));
    GP=E and info("[DEBUG]<%s:%s>WriteBlockSize(%s)", MOD, meth,
      tostring(writeBlockSize));
    GP=E and info("[DEBUG]<%s:%s> PS(%s) F(%s) T(%s)", MOD, meth, 
      tostring(pageSize), tostring(focus), tostring(testMode));

    if  not ldtMap or
        not aveObjectSize or
        not maxObjectSize or
        not aveObjectCount or
        not maxObjectCount or
        not aveKeySize or
        not maxKeySize or
        not writeBlockSize or
        not pageSize or
        not focus
    then
      warn("[ERROR]<%s:%s> One or more of the config setting values are NIL",
        MOD, meth);
      info("[DEBUG]<%s:%s>AveObjSz(%s) MaxObjSz(%s)", MOD, meth,
        tostring(aveObjectSize), tostring(maxObjectSize));
      info("[DEBUG]<%s:%s>AveKeySz(%s) MaxKeySz(%s)", MOD, meth,
        tostring(aveKeySize), tostring(maxKeySize));
      info("[DEBUG]<%s:%s>AveObjCnt(%s) MaxObjCnt(%s)", MOD, meth,
        tostring(aveObjectCount), tostring(maxObjectCount));
      info("[DEBUG]<%s:%s> PS(%s) F(%s) T(%s)", MOD, meth, 
        tostring(pageSize), tostring(focus), tostring(testMode));
      return -1;
    end

    if  type( aveObjectSize ) ~= "number" or
        type( maxObjectSize ) ~= "number" or
        type( aveKeySize ) ~= "number" or
        type( maxKeySize ) ~= "number" or
        type( aveObjectCount ) ~= "number" or
        type( maxObjectCount ) ~= "number" or
        type( writeBlockSize ) ~= "number" or
        type( pageSize ) ~= "number" or
        type( focus ) ~= "number" or
        type(testMode) ~= "number"
    then
      warn("[ERROR]<%s:%s> All parameters must be numbers.", MOD, meth);
      info("[DEBUG]<%s:%s>AveObjSz(%s) MaxObjSz(%s)", MOD, meth,
        type(aveObjectSize), type(maxObjectSize));
      info("[DEBUG]<%s:%s>AveKeySz(%s) MaxKeySz(%s)", MOD, meth,
        type(aveKeySize), type(maxKeySize));
      info("[DEBUG]<%s:%s>AveObjCnt(%s) MaxObjCnt(%s)", MOD, meth,
        type(aveObjectCount), type(maxObjectCount));
      info("[DEBUG]<%s:%s>WriteBlockSize(%s)", MOD, meth,
        type(writeBlockSize));
      info("[DEBUG]<%s:%s> PS(%s) F(%s) T(%s)", MOD, meth, 
        type(pageSize), type(focus), type(testMode));
      return -1;
    end

    -- Let's do some number validation before we actually apply the values
    -- given to us.  The values have to be within a reasonable range,
    -- and the average sizes cannot be larger than the max values.
    if  aveObjectSize <= 0 or maxObjectSize <= 0 or
        aveKeySize <= 0 or maxKeySize <= 0 or
        aveObjectCount <= 0 or maxObjectCount <= 0 or
        writeBlockSize <= 0 or
        pageSize <= 0 or
        focus < 0 
    then
      warn("[ERROR]<%s:%s> Settings must be greater than zero", MOD, meth);
      info("[INFO] AveObjSz(%d) MaxObjSz(%d) AveKeySz(%d) MaxKeySz(%d)",
        aveObjectSize, maxObjectSize, aveKeySize, maxKeySize);
      info("[INFO] AveObjCnt(%d) MaxObjCnt(%d) WriteBlkSz(%d)",
        aveObjectCount, maxObjectCount, writeBlockSize);
      info("[INFO] PS(%s) F(%s) T(%s)", pageSize, focus, testMode);
      return -1;
    end

    info("[DUMP] AveObjSz(%d) MaxObjSz(%d) AveKeySz(%d) MaxKeySz(%d)",
      aveObjectSize, maxObjectSize, aveKeySize, maxKeySize);
    info("[DUMP] AveObjCnt(%d) MaxObjCnt(%d) WriteBlkSz(%d)",
      aveObjectCount, maxObjectCount, writeBlockSize);
    info("[DUMP] PS(%s) F(%s) T(%s)", pageSize, focus, testMode);
    
    -- Do some specific value validation.
    -- First, Page Size must be in a valid range.
    -- We 
    local pageMinimum = 4000;
    local pageTarget = 16000;
    local pageMaximum =  writeBlockSize;  -- 128k to 1mb ceiling
    if pageSize < pageMinimum then
      warn("[ERROR]<%s:%s> PageSize(%d) is too small: %d bytes minimum",
        MOD, meth, pageSize, pageMinimum);
      pageSize = pageTarget;
      warn("[ADJUST]<%s:%s> PageSize Adjusted up to target size: %d bytes",
        MOD, meth, pageTarget);
    elseif pageSize > pageMaximum then
      warn("[ERROR]<%s:%s> PageSize (%d) Larger than Max(%d)", 
        MOD, meth, pageSize, pageMaximum);
      pageSize = pageMaximum;
      warn("[ADJUST]<%s:%s> PageSize Adjusted down to max size: %d bytes",
        MOD, meth, pageMaximum);
    else
      debug("[VALID]<%s:%s> PageSize(%d) is in range", MOD, meth, pageSize);
    end



    --]]
    -- Perform some validation of the user's Config Parameters
    local rc = ldt_common.validateConfigParms(ldtMap, configMap);
    if rc ~= 0 then
      warn("[ERROR]<%s:%s> Unable to Set Configuration due to errors",
        MOD, meth);
      return -1;
    end

    -- Now that all of the values have been validated, we can use them
    -- safely without worry.  No more checking needed.
    local aveObjectSize   = configMap.AveObjectSize;
    local maxObjectSize   = configMap.MaxObjectSize;
    local aveKeySize      = configMap.AveKeySize;
    local maxKeySize      = configMap.MaxKeySize;
    local aveObjectCount  = configMap.AveObjectCount;
    local maxObjectCount  = configMap.MaxObjectCount;
    local pageSize        = configMap.TargetPageSize;
    local writeBlockSize  = configMap.WriteBlockSize;
    local recordOverHead  = configMap.RecordOverHead;
    local focus           = configMap.Focus;
    local testMode        = configMap.TestMode;

    GP=E and info("[ENTER]<%s:%s> LDT(%s)", MOD, meth, tostring(ldtMap));
    GP=E and info("[DEBUG]<%s:%s>AveObjSz(%s) MaxObjSz(%s)", MOD, meth,
      tostring(aveObjectSize), tostring(maxObjectSize));
    GP=E and info("[DEBUG]<%s:%s>AveKeySz(%s) MaxKeySz(%s)", MOD, meth,
      tostring(aveKeySize), tostring(maxKeySize));

    -- These are the values that we have to set.
    local hotListMax;     -- # of elements stored in the hot list
    local hotListTransfer;-- # of hot elements to move (per op) to the warm list
    local ldrListMax;     -- # of elements to store in a data sub-rec
    local warmListMax;    -- # of sub-rec digests to keep in the TopRec
    local warmListTransfer;-- # of digest entries to transfer to cold list
    local coldListMax;    -- # of digest entries to keep in a Cold Dir
    local coldDirMax;     -- # of Cold Dir sub-recs to keep.


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
    local hotListByteCeiling = 20000;
    local hotListTargetSize =
      (halfPage < hotListByteCeiling) and (halfPage) or hotListByteCeiling;

    info("[DEBUG]<%s:%s> HalfPage(%d) HotListCeiling(%d) HotListTarget(%d)",
      MOD, meth, halfPage, hotListCountCeiling, hotListTargetSize);

    -- If not even a pair of (Max Size) objects can fit in the Hot List,
    -- then we need to skip the Hot List altogether and have
    -- items be written directly to the Warm List (the LDR pages).
    if ((maxObjectSize * 2) > hotListTargetSize) then
      -- Don't bother with a Hot List.  Objects are too big.
      hotListMax = 0;
      hotListTransfer = 0;
      warn("[WARNING]<%s:%s> Max ObjectSize(%d) is too large for HotList",
        MOD, meth, maxObjectSize);
      warn("[WARNING]<%s:%s> Use Smaller Object for better performance",
        MOD, meth);
    else
      hotListMax = hotListTargetSize / maxObjectSize;
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
        return -1;
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
