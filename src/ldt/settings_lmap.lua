-- Settings for Large Map (settings_lmap.lua)
--
-- Module Marker: Track date and iteration of last change.
local MOD="settings_lmap_2014_06_05.A"; -- the module name used for tracing

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;     -- Use for turning on Global Print
local F=true; -- Set F (flag) to true to turn ON global print
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print

-- ======================================================================
-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- HashType (HT) values
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

-- Our Hash Tables can operate in two modes:
-- (*) Static (a fixed size Hash Table)
-- (*) Dynamic (A variable size Hash Table that uses Linear Hash Algorithm)
local HS_STATIC  = 'S';
local HS_DYNAMIC = 'D';

-- Currently, the default for the Hash Table Management is STATIC.  When
-- it is ready (fully tested), we will enable DYNAMIC mode that uses Linear
-- Hashing and has more graceful directory growth (and shrinkage).
local DEFAULT_HASH_STATE = HS_STATIC;

-- Max Size of a list in a hash cell anchor before we convert to a Sub-Record
local DEFAULT_BINLIST_THRESHOLD = 4;
-- Max size of our Compact List before we convert to a full Hash Table.
local DEFAULT_THRESHOLD = 10; -- Convert to Hash Table after this many items
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
local T = {

  -- Fields Common to ALL LDTs (managed by the LDT COMMON routines)
  M_UserModule             = 'P'; -- User's Lua file for overrides
  M_KeyFunction            = 'F'; -- User Supplied Key Extract Function
  M_KeyType                = 'k'; -- Type of Key (Always atomic for LMAP)
  M_StoreMode              = 'M'; -- SM_LIST or SM_BINARY
  M_StoreLimit             = 'L'; -- Used for Eviction (eventually)
  M_Transform              = 't'; -- Transform object to storage format
  M_UnTransform            = 'u'; -- UnTransform from storage to Lua format
  
  -- Fields specific to LMAP
  M_LdrEntryCountMax       = 'e'; -- Max size of the LDR List
  M_LdrByteEntrySize       = 's'; -- Size of a Fixed Byte Object
  M_LdrByteCountMax        = 'b'; -- Max Size of the LDR in bytes
  M_StoreState             = 'S'; -- Store State (Compact or List)
  M_HashType               = 'h'; -- Hash Type (static or dynamic)
  M_BinaryStoreSize        = 'B'; -- Size of Object when in Binary form
  M_Modulo 				   = 'm'; -- Modulo used for Hash Function
  M_ThreshHold             = 'H'; -- Threshold: Compact->Regular state
  M_BinListThreshold       = 'l'; -- Threshold for converting from a
                                  -- cell anchor binlist to sub-record.
  M_OverWrite              = 'o'; -- Allow Overwrite of a Value for a given
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
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 50000; -- default capacity MAX: 50,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.StandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
function package.TestModeNumber( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeList()


-- ======================================================================
-- Package = "TestModeObject"
-- ======================================================================
function package.TestModeObject( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObject()

-- ======================================================================
-- Package = "TestModeObjectKey"
-- ======================================================================
function package.TestModeObjectKey( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 10000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = DEFAULT_THRESHHOLD; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 100; -- 100 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.TestModeObjectKey()

-- ======================================================================
-- Package = "DebugModeObject"
-- Test the LMAP with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObject( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = 10; -- 10 objects per subrec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObject()

-- ======================================================================
-- Package = "DebugModeObjectTop"
-- Test the LMAP with a small threshold and with a generic KEY extract
-- function.  Any object (i.e. a map) must have a "key" field for this to
-- work.
-- ======================================================================
function package.DebugModeObjectTop( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used in top rec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeObjectTop()

-- ======================================================================
-- Package = "DebugModeNumberTop"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumberTop( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used for TopRec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
end -- package.DebugModeNumber( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
function package.DebugModeNumber( ldtMap )
  ldtMap[T.M_StoreMode]             = SM_LIST; -- Use List Mode
  ldtMap[T.M_StoreLimit]            = 1000; -- default capacity MAX: 10,000
  ldtMap[T.M_Transform]             = nil; -- Not used in Std List
  ldtMap[T.M_UnTransform]           = nil; -- Not used in Std List
  ldtMap[T.M_StoreState]            = SS_COMPACT; -- start in "compact mode"
  ldtMap[T.M_BinaryStoreSize]       = nil; -- Not used in Std List
  ldtMap[T.M_Modulo]                = DEFAULT_DISTRIB; -- Hash Dir Size
  ldtMap[T.M_ThreshHold]            = 4; -- Rehash after this #
  ldtMap[T.M_LdrEntryCountMax]      = nil; -- not used for TopRec
  ldtMap[T.M_LdrByteEntrySize]      = nil; -- not used here
  ldtMap[T.M_LdrByteCountMax]       = nil; -- not used here
  ldtMap[T.M_HashType]              = HT_STATIC; -- Use Static Hash Dir
  ldtMap[T.M_BinListThreshold]      = DEFAULT_BINLIST_THRESHOLD;
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
    ldtMap[T.M_StoreMode]        = value;
  end

  function exports.set_transform( ldtMap, value )
    ldtMap[T.M_Transform]        = value;
  end

  function exports.set_untransform( ldtMap, value )
    ldtMap[T.M_UnTransform]      = value;
  end

  function exports.set_store_limit( ldtMap, value )
    ldtMap[T.M_StoreLimit]       = value;
  end

  function exports.set_ldr_entry_count_max( ldtMap, value )
    ldtMap[T.M_LdrEntryCountMax] = value;
  end

  function exports.set_ldr_byte_entry_size( ldtMap, value )
    ldtMap[T.M_LdrByteEntrySize] = value;
  end

  function exports.set_ldr_byte_count_max( ldtMap, value )
    ldtMap[T.M_LdrByteCountMax]  = value;
  end

  function exports.set_store_state( ldtMap, value )
    ldtMap[T.M_StoreState]       = value;
  end

  function exports.set_hash_type( ldtMap, value )
    ldtMap[T.M_HashType]      = value;
  end

  function exports.set_binary_store_size( ldtMap, value )
    ldtMap[T.M_BinaryStoreSize] = value;
  end

  function exports.set_hash_dir_size( ldtMap, value )
    ldtMap[T.M_Modulo]    = value;
  end

  function exports.set_compact_list_threshold( ldtMap, value )
    ldtMap[T.M_Threshold]    = value;
  end

  function exports.set_hash_cell_threshold( ldtMap, value )
    ldtMap[T.M_BinListThreshold]    = value;
  end

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
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
