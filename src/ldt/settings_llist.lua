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
local MOD="settings_llist_2014_06_20.A"; -- the module name used for tracing

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
local DEFAULT_LARGE_THRESHOLD  =    5; -- Objs over 100 kb
local DEFAULT_MEDIUM_THRESHOLD = 100;  -- Objs around 1 kb
local DEFAULT_SMALL_THRESHOLD  = 500;  -- Objs under  20kb
--
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
-- R_StoreState          = 'S';-- Compact or Regular Storage (dynamic)

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
  --
  -- Package = "ListJumboObject"
  -- ======================================================================
    function package.ListJumboObject( ldtMap )
    
    -- General Parameters
    ldtMap[T.M_Transform] = nil;
    ldtMap[T.M_UnTransform] = nil;
    ldtMap[T.R_StoreState] = SS_COMPACT; -- start in "compact mode"
    ldtMap[T.M_StoreMode] = SM_LIST; -- Use List Mode
    ldtMap[T.R_BinaryStoreSize] = nil; -- Don't waste room if we're not using it
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys
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
    ldtMap[T.R_LeafListMax] = 8;  -- Max # of items
    ldtMap[T.R_LeafByteCountMax] = 0; -- Max # of BYTES per data page

    return 0;
  end -- package.ListJumboObject()


  -- ======================================================================
  -- This is the configuration for Large Ordered Lists that hold Large
  -- Objects, which are assumed to be about 10 kilobytes.  With large objects,
  -- we will be keeping our lists small so that we don't overflow the
  -- B+ Tree Leaf pages.
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
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys
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
    ldtMap[T.M_KeyType] = KT_COMPLEX; -- Atomic Keys
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
    ldtMap[T.R_NodeListMax] = 200;  -- Max # of items (key+digest)
    ldtMap[T.R_NodeByteCountMax] = 0; -- Max # of BYTES

    -- LLIST Tree Leaves (Data Pages)
    -- TODO: Raise this value to 500 when we switch to Binary Search.
    ldtMap[T.R_LeafListMax] = 200;  -- Max # of items
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

return exports;


-- settings_llist.lua
--
-- Use:  
-- local set_llist = require('settings_llist')
--
-- Use the functions in this module to override default ldtMap settings.
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
