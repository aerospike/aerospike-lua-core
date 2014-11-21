-- Settings for Large List (settings_llist.lua)
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
local MOD="settings_llist_2014_11_17.A"; -- the module name used for tracing

-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP;     -- Global Print Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print

-- ======================================================================
-- We now need a new ldt_common function in order to validate the
-- incoming user CONFIG values.
-- ======================================================================
local ldt_common = require('ldt/ldt_common');

-- ======================================================================
-- StoreMode (SM) values (which storage Mode: Binary or List?)
local SM_BINARY ='B'; -- Using a Transform function to compact values
local SM_LIST   ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
local KT_ATOMIC  ='A'; -- the set value is just atomic (number or string)
local KT_COMPLEX ='C'; -- the set value is complex. Use Function to get key.

-- SetTypeStore (ST) values
local ST_RECORD = 'R'; -- Store values (lists) directly in the Top Record
local ST_SUBRECORD = 'S'; -- Store values (lists) in Sub-Records

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE='T';
local AS_FALSE='F';

-- Switch from a single Compact list to a B+ Tree after this amount.
local DEFAULT_LARGE_THRESHOLD  =   5;  -- Objs over 100 kb
local DEFAULT_MEDIUM_THRESHOLD = 100;  -- Objs around 1 kb
local DEFAULT_SMALL_THRESHOLD  = 200;  -- Objs under  20 b

local DEFAULT_THRESHOLD        = DEFAULT_MEDIUM_THRESHOLD;

local DEFAULT_AVE_OBJ_SIZE     =     100;
local DEFAULT_MAX_OBJ_SIZE     =     200;
local DEFAULT_AVE_KEY_SIZE     =      11;
local DEFAULT_MAX_KEY_SIZE     =      21;
local DEFAULT_AVE_OBJ_CNT      =    1000;
local DEFAULT_MAX_OBJ_CNT      =   10000;
local DEFAULT_TARGET_PAGESIZE  =   16000;
local DEFAULT_WRITE_BLOCK_SIZE = 1000000;
local DEFAULT_FOCUS            =       0;
local DEFAULT_TEST_MODE        =       0;
    
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
local T = {
  -- Fields common to all LDTs (Configuration)
  M_UserModule          = 'P';-- User's Lua file for overrides
  M_KeyFunction         = 'F';-- Function to compute Key from Object
  M_KeyType             = 'k';-- Type of key (atomic, complex)
  M_StoreMode           = 'M';-- SM_LIST or SM_BINARY (applies to all nodes)
  M_StoreLimit          = 'L';-- Storage Capacity Limit
  M_Transform           = 't';-- Transform Object (from User to bin store)
  M_UnTransform         = 'u';-- Reverse transform (from storage to user)

  -- Fields Specific to Large List
  --
  -- Tree Level values (Runtime values) (Comment out)
-- R_TotalCount          = 'T';-- A count of all "slots" used in LLIST
-- R_LeafCount           = 'c';-- A count of all Leaf Nodes
-- R_NodeCount           = 'C';-- A count of all Nodes (including Leaves)
-- R_TreeLevel           = 'l';-- Tree Level (Root::Inner nodes::leaves)
  R_StoreState          = 'S';-- Compact or Regular Storage (dynamic)

  -- Tree Settings (configuration)
  R_KeyDataType         = 'd';-- Data Type of key (Number, Integer)
  R_KeyUnique           = 'U';-- Are Keys Unique? (AS_TRUE or AS_FALSE))
  R_Threshold           = 'H';-- After this#:Move from compact to tree mode

  -- Key and Object Sizes, for fixed byte arrays (Configuration)
  R_KeyByteSize         = 'B',-- Fixed Size (in bytes) of Key
  R_ObjectByteSize      = 'b',-- Fixed Size (in bytes) of Object

  -- Top Node Tree Root Directory (Runtime Values) (Comment out)
-- R_KeyByteArray        = 'J', -- Byte Array, when in compressed mode
-- R_DigestByteArray     = 'j', -- DigestArray, when in compressed mode
-- R_RootKeyList         = 'K',-- Root Key List, when in List Mode
-- R_RootDigestList      = 'D',-- Digest List, when in List Mode
-- R_CompactList         = 'Q',--Simple Compact List -- before "tree mode"

  -- Top Node Tree Root Directory (Configuration)
  R_RootListMax         = 'R', -- Length of Key List (page list is KL + 1)
  R_RootByteCountMax    = 'r',-- Max # of BYTES for keyspace in the root

  -- LLIST Inner Node Settings (Configuration)
  R_NodeListMax         = 'X',-- Max # of items in a node (key+digest)
  R_NodeByteCountMax    = 'Y',-- Max # of BYTES for keyspace in a node

  -- LLIST Tree Leaves (Data Pages) (Configuration)
  R_LeafListMax         = 'x',-- Max # of items in a leaf node
  R_LeafByteCountMax    = 'y' -- Max # of BYTES for obj space in a leaf
  
  -- LLIST Tree Leaves (Data Pages) (Runtime values) (Comment out)
-- R_LeftLeafDigest      = 'A';-- Record Ptr of Left-most leaf
-- R_RightLeafDigest     = 'Z';-- Record Ptr of Right-most leaf
  
};

-- ======================================================================
-- Define a Table of Packages
-- ======================================================================
local package = {};

  -- ======================================================================
  -- This is the standard (default) configuration
  -- Package = "StandardList"
  -- ======================================================================
    function package.StandardList( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_ATOMIC; -- Atomic Keys
    ldtMap[T.R_Threshold] = DEFAULT_THRESHOLD; -- REDO after this many inserts
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.StandardList()

  -- ======================================================================
  -- This is the configuration for Large Ordered Lists that hold JUMBO
  -- Objects, which are assumed to be about 100 kilobytes.  With JUMBO objects,
  -- we will be keeping our lists small so that we don't overflow the
  -- B+ Tree Leaf pages.
  -- However, we STILL assume that the KEY value is not much over 100 bytes.
  -- These numbers are based on a ONE MEGABYTE Sub-Rec size assumption.
  --
  -- Package = "ListJumboObject"
  -- ======================================================================
    function package.ListJumboObject( ldtMap )
    
    -- General Parameters
    -- ldtMap[T.M_Transform];   -- Set Later if/when needed
    -- ldtMap[T.M_UnTransform]; -- Set Later if/when needed
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    -- ldtMap[T.R_BinaryStoreSize]; -- Set only when in Binary Mode
    -- ldtMap[T.R_BinaryStoreSize]; -- Set only when in Binary Node
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex object ("key" field or Func)
    ldtMap[T.R_Threshold] = DEFAULT_LARGE_THRESHOLD; -- Convert Compact to tree
    -- ldtMap[T.M_KeyFunction]; -- Set Later if/when needed

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings.  Note that these are keys, so we will 
    -- actually assume relatively small keys, even though the objs themselves
    -- are large.  Keep inner nodes as if they are 20 to 50 byte keys.
    ldtMap[T.R_NodeListMax] = 200;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 5;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.ListJumboObject()


  -- ======================================================================
  -- This is the configuration for Large Ordered Lists that hold Large
  -- Objects, which are assumed to be in the range of 1 to 10 kilobytes. 
  -- With large objects, we will be keeping our lists small so that we
  -- don't overflow the B+ Tree Leaf pages.
  --
  -- Package = "ListLargeObject"
  -- ======================================================================
    function package.ListLargeObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex Keys
    ldtMap[T.R_Threshold] = DEFAULT_LARGE_THRESHOLD; -- Convert Compact to tree
    ldtMap[T.M_KeyFunction] = nil; -- Assume Key Field in Map.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings.  Note that these are keys, so we will 
    -- actually assume relatively small keys, even though the objs themselves
    -- are large.  Keep inner nodes as if they are 20 to 50 byte keys.
    ldtMap[T.R_NodeListMax] = 200;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.ListLargeObject()

  -- ======================================================================
  -- This is the configuration for Large Ordered Lists that hold Medium-size
  -- objects, assumed to be around 1 kilobytes.  With medium-size objects,
  -- we can afford to keep around 100 objects in our compact list and
  -- our B+ Leaves.  Also, we are assuming that the user will be using
  -- the "key" field, so no implicit KeyFunction() is defined.
  --
  -- Package = "ListMediumObject"
  -- ======================================================================
    function package.ListMediumObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex Keys
    ldtMap[T.R_Threshold] = DEFAULT_MEDIUM_THRESHOLD; -- Convert Compact to tree
    ldtMap[T.M_KeyFunction] = nil; -- Assume Key Field in Map.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings.  Note that these are keys, so we will 
    -- actually assume relatively small keys, even though the objs themselves
    -- are large.  Keep inner nodes as if they are 20 to 50 byte keys.
    ldtMap[T.R_NodeListMax] = 200;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 200;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.ListMediumObject()


  -- ======================================================================
  -- This is the configuration for Large Ordered Lists that hold Small
  -- objects, assumed to be between 20 to 100 bytes.  With small objects,
  -- we can afford to keep around 500 objects in our compact list and
  -- our B+ Leaves.  Also, we are assuming that the user will be using
  -- the "key" field, so no implicit KeyFunction() is defined.
  --
  -- Package = "ListSmallObject"
  -- ======================================================================
    function package.ListSmallObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys from complex objects
    ldtMap[T.R_Threshold] = DEFAULT_SMALL_THRESHOLD; -- Convert Compact to tree
    ldtMap[T.M_KeyFunction] = nil; -- Assume Key Field in Map.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings.  Note that these are keys, so we will 
    -- actually assume relatively small keys, even though the objs themselves
    -- are large.  Keep inner nodes as if they are 20 to 50 byte keys.
    ldtMap[T.R_NodeListMax] = 500;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 500;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.ListSmallObject()

  -- ======================================================================
  -- This is the standard configuration for Complex Objects.
  -- It is ASSUMED that the key is held in a map field named "Key".
  -- Otherwise, no special processing is needed.
  -- Package = "StandardMap"
  -- ======================================================================
    function package.StandardMap( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex Object, but use Key Field
    ldtMap[T.R_Threshold] = DEFAULT_THRESHOLD; -- REDO after this many inserts
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.StandardMap()

  -- ======================================================================
  -- Package = "TestModeNumber"
  -- ======================================================================
  function package.TestModeNumber( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Unique values only.
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeNumber()

  -- ======================================================================
  -- Package = "TestModeNumberDup"
  -- ======================================================================
  function package.TestModeNumberDup( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_ATOMIC; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_FALSE; -- allow Duplicates
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeNumberDup()

  -- ======================================================================
  -- Package = "TestModeObjectDup"
  -- ======================================================================
  function package.TestModeObjectDup( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 20; -- Change to TREE Ops after this many inserts
    -- Use the special function that simply returns the value held in
    -- the object's map field "key".
    ldtMap[T.M_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_FALSE; -- allow Duplicates
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 20; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 20;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 20;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeObjectDup()


  -- ======================================================================
  -- Package = "TestModeObject"
  -- ======================================================================
  function package.TestModeObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys (A Number)
    ldtMap[T.R_Threshold] = 10; -- Change to TREE Ops after this many inserts
    -- Use the special function that simply returns the value held in
    -- the object's map field "key".
    ldtMap[T.M_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Assume Unique Objects
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeObject()

  -- ======================================================================
  -- Package = "TestModeList"
  -- ======================================================================
  function package.TestModeList( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
    ldtMap[T.R_Threshold] = 2; -- Change to TREE Operations after this many inserts
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Assume Unique Objects
   
    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 100; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 100;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 100;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.TestModeList()

  -- ======================================================================
  -- Package = "TestModeBinary"
  -- ======================================================================
  function package.TestModeBinary( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = "compressTest4";
    ldtMap[T.M_UnTransform] = "unCompressTest4";
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Complex Object (need key function)
    ldtMap[T.R_Threshold] = 2; -- Change to TREE Mode after this many ops.
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    return 0;
  end -- package.TestModeBinary( ldtMap )

  -- ======================================================================
  -- Package = "DebugModeObject"
  -- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
  -- where we sort them based on a map field called "key".
  -- ======================================================================
  function package.DebugModeObject( ldtMap )
    local meth = "package.DebugModeObject()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys
    ldtMap[T.R_Threshold] = 2; -- Rehash after this many have been inserted
    ldtMap[T.M_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items

    GP=E and trace("[EXIT]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    return 0;
  end -- package.DebugModeObject()


  -- ======================================================================
  -- Package = "DebugModeObjectDup"
  -- Test the LLIST with Objects (i.e. Complex Objects in the form of MAPS)
  -- where we sort them based on a map field called "key".
  -- ASSUME that we will support DUPLICATES.
  -- ======================================================================
  function package.DebugModeObjectDup( ldtMap )
    local meth = "package.DebugModeObjectDup()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys
    ldtMap[T.R_Threshold] = 2; -- Rehash after this many have been inserted
    ldtMap[T.M_KeyFunction] = "keyExtract"; -- Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_FALSE; -- Assume there will be Duplicates

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items

    GP=E and trace("[EXIT]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    return 0;
  end -- package.DebugModeObjectDup()


  -- ======================================================================
  -- Package = "DebugModeList"
  -- Test the LLIST with very small numbers to force it to make LOTS of
  -- warm and close objects with very few inserted items.
  -- ======================================================================
  function package.DebugModeList( ldtMap )
    local meth = "package.DebugModeList()";
    
    GP=E and trace("[ENTER]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_ATOMIC; -- Atomic Keys
    ldtMap[T.R_Threshold] = 10; -- Rehash after this many have been inserted
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 10; -- Length of Key List (page list is KL + 1)
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 10;  -- Max # of items (key+digest)

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 10;  -- Max # of items

    GP=E and trace("[EXIT]<%s:%s> : ldtMap(%s)",
        MOD, meth , tostring(ldtMap));

    return 0;
  end -- package.DebugModeList()

  -- ======================================================================
  -- Package = "DebugModeBinary"
  -- Perform the Debugging style test with compression.
  -- ======================================================================
  function package.DebugModeBinary( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = "compressTest4";
    ldtMap[T.M_UnTransform] = "unCompressTest4";
    ldtMap[T.R_KeyCompare] = "debugListCompareEqual"; -- "Simple" list comp
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = 16; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- special function for list compare.
    ldtMap[T.R_Threshold] = 4; -- Rehash after this many have been inserted
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.

    return 0;
  end -- package.DebugModeBinary( ldtMap )

  -- ======================================================================
  -- Package = "DebugModeNumber"
  -- Perform the Debugging style test with a number
  -- ======================================================================
  function package.DebugModeNumber( ldtMap )
    local meth = "package.DebugModeNumber()";
    GP=E and trace("[ENTER]<%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_KeyCompare] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = 0; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_ATOMIC; -- Simple Number (atomic) compare
    ldtMap[T.R_Threshold] = 4; -- Rehash after this many have been inserted
    ldtMap[T.M_KeyFunction] = nil; -- No Special Attention Required.
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Just Unique keys for now.

    -- Top Node Tree Root Directory
    ldtMap[T.R_RootListMax] = 4; -- Length of Key List (page list is KL + 1)
    ldtMap[T.R_RootByteCountMax] = 0; -- Max bytes for key space in the root
    
    -- LLIST Inner Node Settings
    ldtMap[T.R_NodeListMax] = 4;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    ldtMap[T.R_LeafListMax] = 4;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );

    return 0;
  end -- package.DebugModeNumber( ldtMap )
-- ======================================================================


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


-- This is the table that we're exporting to the User Module.
-- Each of these functions allow the user to override the default settings.
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
  -- Set the initial Store State.  Usually this is Compact Mode (a separate
  -- list), but it can be set to "regular", which will make it start in
  -- "Tree Mode".
  -- Parm "value" must be either SS_COMPACT ('C') or SS_REGULAR ('R').
  function exports.set_store_state( ldtMap, value )
    ldtMap[T.R_StoreState]       = value;
  end

  -- StoreMode must be SM_LIST ('L') or SM_BINARY ('B').  Be sure that
  -- Binary state is working (it was under construction last I looked
  -- in June 2014).
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

  function exports.set_key_function( ldtMap, value )
    ldtMap[T.M_KeyFunction] = value;
  end

  function exports.set_binary_store_size( ldtMap, value )
    ldtMap[T.R_BinaryStoreSize] = value;
  end

  function exports.set_root_list_max( ldtMap, value )
    ldtMap[T.R_RootListMax] = value;
  end

  function exports.set_root_bytecount_max( ldtMap, value )
    ldtMap[T.R_RootByteCountMax]  = value;
  end

  function exports.set_node_list_max( ldtMap, value )
    ldtMap[T.R_NodeListMax]  = value;
  end

  function exports.set_node_bytecount_max( ldtMap, value )
    ldtMap[T.R_NodeByteCountMax]      = value;
  end

  function exports.set_leaf_list_max( ldtMap, value )
    ldtMap[T.R_LeafListMax] = value;
  end

  function exports.set_leaf_bytecount_max( ldtMap, value )
    ldtMap[T.R_LeafByteCountMax]    = value;
  end

  function exports.set_key_type( ldtMap, value )
    ldtMap[T.M_KeyType]      = value;
  end

  function exports.set_compact_list_threshold( ldtMap, value )
    ldtMap[T.R_Threshold]    = value;
  end

  function exports.set_unique_key_false( ldtMap )
    ldtMap[T.R_KeyUnique] = AS_FALSE; -- Unique values only.
  end

  function exports.set_unique_key_true( ldtMap )
    ldtMap[T.R_KeyUnique] = AS_TRUE; -- Unique values only.
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

    -- These are the values we're going to set.
    local threshold; -- # of objects that we initially cache in the TopRec
    local rootListMax; -- # of Key/Digest entries we hold in the TopRec
    local nodeListMax; -- # of Key/Digest entries we hold in a Node SubRec.
    local leafListMax; -- # of Data Objects we hold in a Leaf SubRec

    local ldtOverhead = 500; -- Overhead in Bytes.  Used in Root Node calc.

    -- Check that PageSize suits the conditions.
    -- Try for at least 10 objects in the leaves, but no more than 100.
    -- However, for larger object sizes, we may have to
    -- use larger than requested PageSizes.
    local adjustedPageSize = pageSize;
    local minLeafCount = 10;
    local maxLeafCount = 100;
    -- See if objects easily fit in a Leaf.
    if ( maxObjectSize * minLeafCount ) < adjustedPageSize then
      leafListMax = adjustedPageSize / maxObjectSize;
      if leafListMax > maxLeafCount then 
        leafListMax = maxLeafCount;
      end
      threshold = leafListMax;
    elseif maxObjectSize * minLeafCount < pageSize then
      leafListMax = minLeafCount;
      threshold = 1;
    else
      leafListMax = pageSize / maxObjectSize;
      threshold = 0;
      ldtMap[T.R_StoreState] = SS_REGULAR; -- start in "compact mode"
    end

    -- Try for at least 100 objects in the nodes, and no more than 200.
    -- However, for larger key sizes, we may have to
    -- use larger than requested PageSizes.
    local adjustedPageSize = pageSize;
    local minNodeCount = 50;
    local maxNodeCount = 200;
    -- See if the Keys easily fit in a Node.
    if ( maxKeySize * minNodeCount ) < adjustedPageSize then
      nodeListMax = adjustedPageSize / maxKeySize;
      if nodeListMax > maxNodeCount then 
        nodeListMax = maxNodeCount;
      end
    elseif maxKeySize * minNodeCount < pageSize then
      nodeListMax = minNodeCount;
    else
      nodeListMax = pageSize / maxKeySize;
    end

    -- Try for at least 20 objects in the nodes, and no more than 100.
    -- However, for larger key sizes, we may have to
    -- use larger than requested PageSizes.
    local adjustedPageSize = pageSize;
    local minRootCount = 20;
    local maxRootCount = 100;
    -- See if the Keys easily fit in the Root
    if ( maxKeySize * minRootCount ) < adjustedPageSize then
      rootListMax = adjustedPageSize / maxKeySize;
      if rootListMax > maxRootCount then 
        rootListMax = maxRootCount;
      end
    elseif maxKeySize * minRootCount < pageSize then
      rootListMax = minRootCount;
    else
      rootListMax = pageSize / maxKeySize;
    end

    -- Apply our computed values.
    ldtMap[T.R_Threshold]   = threshold;
    ldtMap[T.R_RootListMax] = rootListMax;
    ldtMap[T.R_NodeListMax] = nodeListMax;
    ldtMap[T.R_LeafListMax] = leafListMax;

    GP=E and trace("[FINAL OUT]<%s:%s> TH(%d) RL(%d) NL(%d) LL(%d)",
      MOD, meth, threshold, rootListMax, nodeListMax, leafListMax);
    GP=E and trace("[EXIT]: <%s:%s>:: LdtMap(%s)",
      MOD, meth, tostring(ldtMap) );

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
