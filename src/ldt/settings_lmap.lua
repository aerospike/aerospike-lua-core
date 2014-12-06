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

-- ======================================================================
-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

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
  KeyFunction            = 'F'; -- User Supplied Key Extract Function
  KeyType                = 'k'; -- Type of Key (Always atomic for LMAP)
  StoreMode              = 'M'; -- SM_LIST or SM_BINARY
  StoreLimit             = 'L'; -- Used for Eviction (eventually)
  Transform              = 't'; -- Transform object to storage format
  UnTransform            = 'u'; -- UnTransform from storage to Lua format
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
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- Sub-Record Design, List Mode, Full Object Compare, limit 10,000 Objects
-- ======================================================================
function package.StandardList( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = DEFAULT_CAPACITY;
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_MEDIUM_THRESHOLD;
  ldtMap[LS.LdrEntryCountMax]      = 100; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.StandardList()

-- ======================================================================
-- This is the configuration for JUMBO Objects (around 100kb).  We can afford
-- to store only a few of these per subrecord, as well as only a few in
-- our compact list.
-- Package = "ListJumboObject"
-- Sub-Record Design, List Mode, Full Object Compare, limit 10,000 Objects
-- ======================================================================
function package.ListJumboObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = DEFAULT_JUMBO_CAPACITY;
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_JUMBO_THRESHOLD;
  ldtMap[LS.LdrEntryCountMax]      = 6; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_JUMBO_BINLIST_THRESHOLD;
end -- package.ListJumboObject()

-- ======================================================================
-- This is the configuration for Large Objects (around 10kb).  We can afford
-- to store only a few of these per subrecord, as well as only a few in
-- our compact list.
-- Package = "ListLargeObject"
-- Sub-Record Design, List Mode, Full Object Compare, limit 10,000 Objects
-- ======================================================================
function package.ListLargeObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = DEFAULT_LARGE_CAPACITY;
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_LARGE_THRESHOLD;
  ldtMap[LS.LdrEntryCountMax]      = 50; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_LARGE_BINLIST_THRESHOLD;
end -- package.ListLargeObject()

-- ======================================================================
-- This is the configuration for Medium Objects (around 1kb).  We use
-- LIST storage (as opposed to BINARY).
-- Package = "ListMediumObject"
-- Sub-Record Design, List Mode, Full Object Compare, limit 50,000 Objects
-- ======================================================================
function package.ListMediumObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = DEFAULT_MEDIUM_CAPACITY;
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_MEDIUM_THRESHOLD;
  ldtMap[LS.LdrEntryCountMax]      = 100; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_MEDIUM_BINLIST_THRESHOLD;
end -- package.ListMediumObject()

-- ======================================================================
-- This is the configuration for Small Objects (under 100bytes).  With small
-- objects we can afford larger list limits
-- Package = "ListSmallObject"
-- Sub-Record Design, List Mode, Full Object Compare, limit 10,000 Objects
-- ======================================================================
function package.ListSmallObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = DEFAULT_SMALL_CAPACITY;
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_SMALL_THRESHOLD;
  ldtMap[LS.LdrEntryCountMax]      = 200; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_SMALL_BINLIST_THRESHOLD;
end -- package.ListSmallObject()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
function package.TestModeNumber( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 10000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[LS.LdrEntryCountMax]      = 100; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeList()


-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
function package.TestModeObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 10000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[LS.LdrEntryCountMax]      = 100; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObject()

-- ======================================================================
-- Package = "TestModeObjectKey"
-- ======================================================================
function package.TestModeObjectKey( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 10000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = DEFAULT_THRESHOLD; -- Rehash after this #
  ldtMap[LS.LdrEntryCountMax]      = 100; -- Num objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObjectKey()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LMAP with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObject( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 1000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = 4; -- Rehash after this #
  ldtMap[LS.LdrEntryCountMax]      = 10; -- 10 objects per subrec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObject()

-- ======================================================================
-- Package = "DebugModeObjectTop"
-- Test the LMAP with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObjectTop( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 1000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = 4; -- Rehash after this #
  -- ldtMap[LS.LdrEntryCountMax]      = nil; -- not used in top rec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObjectTop()

-- ======================================================================
-- Package = "DebugModeNumberTop"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumberTop( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 1000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]            = 4; -- Rehash after this #
  -- ldtMap[LS.LdrEntryCountMax]      = nil; -- not used for TopRec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeNumber( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumber( ldtMap )
  ldtMap[LC.StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[LC.StoreLimit]            = 1000; -- default capacity MAX: 10,000
  -- ldtMap[LC.Transform]             = nil; -- Not used in Std List
  -- ldtMap[LC.UnTransform]           = nil; -- Not used in Std List
  ldtMap[LS.StoreState]            = SS_COMPACT; -- start in "compact mode"
  -- ldtMap[LS.BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[LS.Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[LS.Threshold]             = 4; -- Rehash after this #
  -- ldtMap[LS.LdrEntryCountMax]      = nil; -- not used for TopRec
  -- ldtMap[LS.LdrByteEntrySize]      = nil; -- not used here
  -- ldtMap[LS.LdrByteCountMax]       = nil; -- not used here
  ldtMap[LS.HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[LS.BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeNumber( ldtMap )

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
end -- applyPackage()

-- ======================================================================
-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
-- ======================================================================
local exports = {}

-- ========================================================================
-- Call one of the standard (preset) packages.  This is generally safest,
-- since we have verified that the values all fit together.
-- ========================================================================
  function exports.use_package( ldtMap, package_name )
    info("[MODULE] APPLY PACKAGE(%s)", package_name );
    applyPackage( ldtMap, package_name );
  end

  -- ======================================================================
  -- Accessor Functions for the LDT Control Map.
  -- Note that use of these individual functions may result in odd behavior
  -- if you pick strange or incompatible values.
  --
  -- TODO: Document these functions ...
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

  function exports.set_store_state( ldtMap, value )
    ldtMap[LS.StoreState]       = value;
  end

  function exports.set_hash_type( ldtMap, value )
    ldtMap[LS.HashType]      = value;
  end

  function exports.set_binary_store_size( ldtMap, value )
    ldtMap[LS.BinaryStoreSize] = value;
  end

  function exports.set_hash_dir_size( ldtMap, value )
    ldtMap[LS.Modulo]    = value;
  end

  function exports.set_compact_list_threshold( ldtMap, value )
    ldtMap[LS.Threshold]    = value;
  end

  function exports.set_hash_cell_threshold( ldtMap, value )
    ldtMap[LS.BinListThreshold]    = value;
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
  -- recordOverHead:: Amount of "other" space used in this record.
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

    info("[DEBUG]<%s:%s> PageAvail(%d) CompListCeiling(%d) CompListTarget(%d)",
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
    ldrListMax = math.floor(dataRecByteLimit / aveObjectSize);

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
