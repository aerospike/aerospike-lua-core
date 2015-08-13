-- Large Ordered List (llist.lua)
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
local MOD="lib_llist_2014_12_20.B";

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
-- (*) "D" is used for Basic DEBUG prints
-- (*) DEBUG is used for larger structure content dumps.
-- ======================================================================
local GP;      -- Global Print/debug Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set F (flag) to true to turn ON Enter/Exit print
local B=false; -- Set B (Banners) to true to turn ON Banner Print
local D=false; -- Set D (Detail) to get more Detailed Debug Output.
local S=false; -- Set S (Sanity Check) to turn on extra checks.
local DEBUG=false; -- turn on for more elaborate state dumps and checks.

local llist = {};
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large List (LLIST) Library Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Write/Update 
--
-- (*) Status = llist.add(topRec, ldtBinName, newValue, createSpec, src)
-- (*) Status = llist.add_all(topRec, ldtBinName, valueList, createSpec, src)
-- (*) Status = llist.update(topRec, ldtBinName, newValue, src)
-- (*) Status = llist.update_all(topRec, ldtBinName, valueList, src)
-- (*) List   = llist.find_first(topRec,ldtBinName, src, count)
-- (*) List   = llist.find_last(topRec,ldtBinName, src, count)
-- (*) Status = llist.remove(topRec, ldtBinName, searchValue  src) 
-- (*) Status = llist.remove_all(topRec, ldtBinName, valueList  src) 
-- (*) Status = llist.remove_range(topRec, ldtBinName, minKey, maxKey, src)
-- (*) Status = llist.destroy(topRec, ldtBinName, src)
--
-- Read
-- (*) Number = llist.size(topRec, ldtBinName )
-- (*) List   = llist.find(topRec, ldtBinName, key, filterModule, filter, fargs, src)
-- ( ) Number = llist.exists(topRec, ldtBinName, key, src)
-- (*) List   = llist.range(topRec, bin, minKey, maxKey, filterModule,filter,fargs,src)
-- (*) List   = llist.filter(topRec, ldtBinName, filterModule, filter, fargs, src)
-- (*) List   = llist.scan(topRec, ldtBinName, filterModule, filter, fargs, src)
-- (*) Map    = llist.config(topRec, ldtBinName )
-- (*) Number = llist.ldt_exists(topRec, ldtBinName)
-- ======================================================================
-- The following functions under construction:
-- (-) List   = llist.take(topRec,ldtBinName,key,filterModule,filter,fargs, src)
-- (-) Object = llist.take_min(topRec,ldtBinName, src)
-- (-) Object = llist.take_max(topRec,ldtBinName, src)
-- ======================================================================
--
-- Large List Design/Architecture
--
-- The Large Ordered List is a sorted list, organized according to a Key
-- value. Large Ordered List is implemented as B+tree with unbounded size
-- and can be used in following variants
-- Store: Key -> Value 
-- Store: Key  [This would be set implementation ]
--
-- Large Ordered List is managed continuously (i.e. it is kept sorted), so 
-- there is some additional overhead in the storage operation (to do the 
-- insertion sort), but should be fast for near sorted data like data 
-- arriving with time (Timeseries). 
-- But this prices paid at the insert comes handly for the retieval operation, 
-- since it is doing a binary search (order log(N)) rather than scan (order N).
-- ======================================================================


-- FORWARD Function DECLARATIONS
-- ======================================================================
-- We have some circular (recursive) function calls, so to make that work
-- we have to predeclare some of them here (they look like local variables)
-- and then later assign the function body to them.
-- ======================================================================
local insertParentNode;
local nodeDelete;

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte       = require('ldt/ldt_errors');
local ldt_common = require('ldt/ldt_common');

-- These values should be "built-in" for our Lua, but it is either missing
-- or inconsistent, so we define it here.  We use this when we check to see
-- if a value is a LIST or a MAP.
local Map  = getmetatable( map() );
local List = getmetatable( list() );

-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
-- ++====================++
-- || INTERNAL BIN NAMES || -- Local, but global to this module
-- ++====================++
-- The Top Rec LDT bin is named by the user -- so there's no hardcoded name
-- for each used LDT bin.
--
-- In the main record, there is one special hardcoded bin -- that holds
-- some shared information for all LDTs.
-- Note the 14 character limit on Aerospike Bin Names.
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local REC_LDT_CTRL_BIN    = "LDTCONTROLBIN"; -- Single bin for all LDT in rec

-- There are THREE different types of (Child) subrecords that are associated
-- with an LLIST LDT:
-- (1) Internal Node Subrecord:: Internal nodes of the B+ Tree
-- (2) Leaf Node Subrecords:: Leaf Nodes of the B+ Tree
-- (3) Existence Sub Record (ESR) -- Ties all children to a parent LDT
-- Each Subrecord has some specific hardcoded names that are used
--
-- All LDT subrecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN     = "SR_PROP_BIN";
--
-- The Node SubRecords (NSRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus 3 of 4 bins
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local NSR_CTRL_BIN        = "NsrControlBin";
local NSR_KEY_LIST_BIN    = "NsrKeyListBin"; -- For Var Length Keys
local NSR_DIGEST_BIN      = "NsrDigestBin"; -- Digest List

-- The Leaf SubRecords (LSRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above, plus
-- >> (14 char name limit) >>12345678901234<<<<<<<<<<<<<<<<<<<<<<<<<
local LSR_CTRL_BIN        = "LsrControlBin";
local LSR_LIST_BIN        = "LsrListBin";

-- The Existence Sub-Records (ESRs) use the following bins:
-- The SUBREC_PROP_BIN mentioned above (and that might be all)

-- ++==================++
-- || MODULE CONSTANTS ||
-- ++==================++
-- Each LDT defines its type in string form.
local LDT_TYPE = "LLIST";

-- For Map objects, we may look for a special KEY FIELD
local KEY_FIELD  = "key";

-- ======================= << DEFAULT VALUES >> ========================
-- Settings for the initial state
local DEFAULT = {
  -- Switch from a single list to B+ Tree after this amount
  THRESHOLD = 10;

  -- Switch Back to a Compact List if we drop below this amount.
  -- We want to maintain a little hysteresis so that we don't thrash
  -- back and forth between CompactMode and Regular Mode
  REV_THRESHOLD = 8;

  -- Starting value for a ROOT NODE (in terms of # of keys)
  ROOT_MAX = 100;

  -- Starting value for an INTERNAL NODE (in terms of # of keys)
  NODE_MAX = 200;

  -- Starting value for a LEAF NODE (in terms of # of objects)
  LEAF_MAX = 100;
};

-- Conventional Wisdom says that lists smaller than 20 are searched faster
-- with LINEAR SEARCH, and larger lists are searched faster with
-- BINARY SEARCH. Experiment with this value.
local LINEAR_SEARCH_CUTOFF = 0;

-- Use this to test for LdtMap Integrity.  Every map should have one.
local MAGIC="MAGIC";     -- the magic value for Testing LLIST integrity

-- The LDT Control Structure is a LIST of Objects:
-- (*) A Common Property Map
-- (*) An LDT-Specific Map
-- (*) A Storage Format Value (or Object).
local LDT_PROP_MAP  = 1;
local LDT_CTRL_MAP  = 2;

-- AS_BOOLEAN TYPE:
-- There are apparently either storage or conversion problems with booleans
-- and Lua and Aerospike, so rather than STORE a Lua Boolean value in the
-- LDT Control map, we're instead going to store an AS_BOOLEAN value, which
-- is a character (defined here).  We're using Characters rather than
-- numbers (0, 1) because a character takes ONE byte and a number takes EIGHT
local AS_TRUE = 'T';
local AS_FALSE = 'F';

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT = 'C'; -- Using "single bin" (compact) mode
local SS_REGULAR = 'R'; -- Using "Regular Storage" (regular) mode

-- << Search Constants >>
-- Use Numbers so that it translates to our C conventions
local ST = {
  FOUND     =  0,
  NOT_FOUND = -1
};

-- Values used in Compare (CR = Compare Results)
local CR = {
  LESS_THAN      = -1,
  EQUAL          =  0,
  GREATER_THAN   =  1,
  ERROR          = -2,
  INTERNAL_ERROR = -3
};

-- Errors used in Local LDT Land (different from the Global LDT errors that
-- are managed in the ldte module).
local ERR = {
  OK            =  0, -- HEY HEY!!  Success
  GENERAL       = -1, -- General Error
  NOT_FOUND     = -2  -- Search Error
};

-- Scan Status:  Do we keep scanning, or stop?
local SCAN = {
  ERROR        = -1,  -- Error during Scanning
  DONE         =  0,  -- Done scanning
  CONTINUE     =  1   -- Keep Scanning
};

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes:
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (RT.LEAF NOT used in this context)
-- (2) As a TYPE in our own propMap[PM.RecType] field: CDIR *IS* used here.
local RT = {
  REG  = 0, -- 0x0: Regular Record (Here only for completeneness)
  LDT  = 1, -- 0x1: Top Record (contains an LDT)
  NODE = 2, -- 0x2: Regular Sub Record (Node, Leaf)
  SUB  = 2, -- 0x2: Regular Sub Record (Node, Leaf)::Used for set_type
  LEAF = 3, -- xxx: Leaf Nodes:: Not used for set_type() 
  ESR  = 4  -- 0x4: Existence Sub Record
};

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF = {
  LDT_BIN     = 1, -- Main LDT Bin (Restricted)
  LDT_HIDDEN  = 2, -- LDT Bin::Set the Hidden Flag on this bin
  LDT_CONTROL = 4  -- Main LDT Control Bin (one per record)
};

-- In order to tell the Server what's happening with LDT (and maybe other
-- calls, we call "set_context()" with various flags.  The server then
-- uses this to measure LDT call behavior.
local UDF_CONTEXT_LDT = 1;

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
  ItemCount             = 'I', -- (Top): Count of all items in LDT
  Version               = 'V', -- (Top): Code Version
  SubRecCount           = 'S', -- (Top): # of subrecs in the LDT
  LdtType               = 'T', -- (Top): Type: stack, set, map, list
  BinName               = 'B', -- (Top): LDT Bin Name
  Magic                 = 'Z', -- (All): Special Sauce
  RecType               = 'R', -- (All): Type of Rec:Top,Ldr,Esr,CDir
  EsrDigest             = 'E', -- (All): Digest of ESR
  SelfDigest            = 'D', -- (All): Digest of THIS Record
  ParentDigest          = 'P'  -- (Subrec): Digest of TopRec
};

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Leaf and Node Fields (There is some overlap between nodes and leaves)
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LF_ListEntryCount       = 'L';-- # current list entries used
local LF_PrevPage             = 'P';-- Digest of Previous (left) Leaf Page
local LF_NextPage             = 'N';-- Digest of Next (right) Leaf Page

local ND_ListEntryCount       = 'L';-- # current list entries used

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Main LLIST LDT Record (root) Map Fields
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Fields Specific to LLIST LDTs (Ldt Specific --> LS)
local LS = {
  -- Stats
  LeafCount           = 'c',-- A count of all Leaf Nodes
  NodeCount           = 'C',-- A count of all Nodes (including Leaves)
  TreeLevel           = 'l',-- Tree Level (Root::Inner nodes::leaves)

  -- Config
  KeyUnique           = 'U',-- Are Keys Unique? (AS_TRUE or AS_FALSE))
  PageSize            = 'p', -- Split Page Size
  Threshold           = 'H',-- After this#:Move from compact to tree mode

  -- Property
  StoreState          = 'S',-- Compact or Regular Storage

  -- Tree Meta Data
  RootKeyList         = 'K',-- Root Key List, when in List Mode
  RootDigestList      = 'D',-- Digest List, when in List Mode
  CompactList         = 'Q',--Simple Compact List -- before "tree mode"
  LeftLeafDigest      = 'A',-- Record Ptr of Left-most leaf
  RightLeafDigest     = 'Z' -- Record Ptr of Right-most leaf
};

-- ------------------------------------------------------------------------
-- Maintain the Field letter Mapping here, so that we never have a name
-- collision: Obviously -- only one name can be associated with a character.
-- We won't need to do this for the smaller maps, as we can see by simple
-- inspection that we haven't reused a character.
-- ----------------------------------------------------------------------
-- A:LS.LeftLeafDigest         a:                         0:
-- C:LS.NodeCount              c:LS.LeafCount             2:
-- E:                          e:                         4:
-- G:                          g:                         6:
-- H:LS.Threshold              h:                         7:
-- I:                          i:                         8:
-- L:                          l:LS.TreeLevel          
-- N:SPECIAL(LBYTES)           n:SPECIAL(LBYTES)
-- O:SPECIAL(LBYTES)           o:SPECIAL(LBYTES)
-- P:                          p:
-- Q:LS.CompactList            q:
-- S:LS.StoreState             s:                        
-- V:LS.RevThreshold           v:
-- W:                          w:                        
-- Z:LS.RightLeafDigest        z:
-- -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
--
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- There are three main Record Types used in the LLIST Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LLIST bin,
--     including the Root Directory.
-- (*) InnerNodeRec: Interior B+ Tree nodes
-- (*) DataNodeRec: The Data Leaves
--
-- <+> Naming Conventions:
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[ldtBinName] or ldrRec['NodeCtrlBin']);

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
local G_PageSize = nil;
local G_WriteBlockSize = nil;
local G_Prev = nil;

-- <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> <udf> 
-- -----------------------------------------------------------------------
-- -----------------------------------------------------------------------
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

  -- General Tree Settings
  resultMap.StoreState        = ldtMap[LS.StoreState];
  resultMap.TreeLevel         = ldtMap[LS.TreeLevel];
  resultMap.LeafCount         = ldtMap[LS.LeafCount];
  resultMap.NodeCount         = ldtMap[LS.NodeCount];

  -- Top Node Tree Root Directory
  resultMap.RootKeyList        = ldtMap[LS.RootKeyList];
  resultMap.RootDigestList     = ldtMap[LS.RootDigestList];
  resultMap.CompactList        = ldtMap[LS.CompactList];
  resultMap.PageSize           = ldtMap[LS.PageSize];
  
  -- LLIST Inner Node Settings

end -- ldtMapSummary()

-- ======================================================================
-- ldtMapSummaryString( ldtMap )
-- ======================================================================
-- Return a string with the full LDT Map
-- ======================================================================
local function ldtMapSummaryString( ldtMap )
  local resultMap = {};
  ldtMapSummary(resultMap, ldtMap);
  return tostring(resultMap);
end

-- ======================================================================
-- local function Tree Summary( ldtCtrl ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the Tree Map
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtCtrl )

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  
  local resultMap             = map();
  resultMap.SUMMARY           = "LList Summary";

  -- General Properties (the Properties Bin
  propMapSummary( resultMap, propMap );

  -- General Tree Settings
  -- Top Node Tree Root Directory
  -- LLIST Inner Node Settings
  -- LLIST Tree Leaves (Data Pages)
  ldtMapSummary( resultMap, ldtMap );

  return  resultMap;
end -- ldtSummary()

-- ======================================================================
-- Do the summary of the LDT, and stringify it for internal use.
-- ======================================================================
local function ldtSummaryString( ldtCtrl )
  return tostring( ldtSummary( ldtCtrl ) );
end -- ldtSummaryString()

-- ======================================================================
-- ldtDebugDump()
-- ======================================================================
-- To aid in debugging, dump the entire contents of the ldtCtrl object
-- for LMAP.  Note that this must be done in several prints, as the
-- information is too big for a single print (it gets truncated).
-- ======================================================================
local function ldtDebugDump( ldtCtrl )
  local meth = "ldtDebugDump()";
  info("[ENTER]<%s:%s>", MOD, meth );
  info("\n\n <><><><><><><><><> [ LDT LLIST SUMMARY ] VVVV START VVVV <> \n");

  -- Print MOST of the "TopRecord" contents of this LLIST object.
  local resultMap                = {};
  resultMap.SUMMARY              = "LLIST Summary";
  local meth = "ldtDebugDump()";

  if ( ldtCtrl == nil ) then
    info("[ERROR]: <%s:%s>: EMPTY LDT BIN VALUE", MOD, meth);
    resultMap.ERROR =  "EMPTY LDT BIN VALUE";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  if( propMap[PM.Magic] ~= MAGIC ) then
    resultMap.ERROR =  "BROKEN LDT--No Magic";
    info("<<<%s>>>", tostring(resultMap));
    return 0;
  end;

  -- Load the common properties
  propMapSummary( resultMap, propMap );
  info("\nPROP MAP SUMMARY<<<%s>>>\n", tostring(resultMap));
  resultMap = nil;

  -- Reset for each section, otherwise the result would be too much for
  -- the info call to process, and the information would be truncated.
  resultMap = {};
  resultMap.SUMMARY              = "LLIST-SPECIFIC Values";

  -- Load the LLIST-specific properties
  ldtMapSummary( resultMap, ldtMap );
  info("\nLDT MAP SUMMARY<<<%s>>>\n", tostring(resultMap));
  resultMap = nil;

  info("\n\n <><><><><><><><><> [ LDT LLIST SUMMARY ] ^^^^ END ^^^^ <><> \n");

end -- function ldtDebugDump()

-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ======================================================================
-- initializeLdtCtrl:
-- ======================================================================
-- Set up the LLIST control structure with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LLIST BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LLIST
-- behavior.  Thus this function represents the "type" LLIST MAP -- all
-- LLIST control fields are defined here.
-- The LListMap is obtained using the user's LLIST Bin Name:
-- ldtCtrl = topRec[ldtBinName]
-- local propMap = ldtCtrl[LDT_PROP_MAP];
-- local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
-- ======================================================================
local function initializeLdtCtrl( topRec, ldtBinName )
  local meth = "initializeLdtCtrl()";
  local propMap = map();
  local ldtMap = map();
  local ldtCtrl = list();

  -- The LLIST control structure -- with Default Values.  Note that we use
  -- two maps -- a general propery map that is the same for all LDTS (in
  -- list position ONE), and then an LDT-specific map.  This design lets us
  -- look at the general property values more easily from the Server code.
  -- General LDT Parms(Same for all LDTs): Held in the Property Map
  propMap[PM.ItemCount] = 0; -- A count of all items in the stack
  propMap[PM.SubRecCount] = 0; -- No Subrecs yet
  propMap[PM.Version]    = G_LDT_VERSION ; -- Current version of the code
  propMap[PM.LdtType]    = LDT_TYPE; -- Validate the ldt type
  propMap[PM.Magic]      = MAGIC; -- Special Validation
  propMap[PM.BinName]    = ldtBinName; -- Defines the LDT Bin
  propMap[PM.RecType]    = RT.LDT; -- Record Type LDT Top Rec
  propMap[PM.EsrDigest]    = 0; -- not set yet.
  propMap[PM.SelfDigest]  = record.digest( topRec );

  -- NOTE: We expect that these settings should match the settings found in
  -- settings_llist.lua :: package.ListMediumObject().
  -- General Tree Settings
  -- ldtMap[LS.TotalCount] = 0;    -- A count of all "slots" used in LLIST
  ldtMap[LS.LeafCount] = 0;     -- A count of all Leaf Nodes
  ldtMap[LS.NodeCount] = 0;     -- A count of all Nodes (incl leaves, excl root)
  ldtMap[LS.TreeLevel] = 1;     -- Start off Lvl 1: Root ONLY. Leaves Come l8tr
  ldtMap[LS.KeyUnique] = AS_TRUE; -- Keys ARE unique by default.
  ldtMap[LS.StoreState] = SS_COMPACT; -- start in "compact mode"

  -- We switch from compact list to tree when we cross LS.Threshold, and we
  -- switch from tree to compact list when we drop below LS.RevThreshold.
  ldtMap[LS.Threshold] = DEFAULT.THRESHOLD;


  -- Top Node Tree Root Directory
  -- Length of Key List (page list is KL + 1)
  -- Do not set the byte counters .. UNTIL we are actually using them.
  ldtMap[LS.RootKeyList] = list();    -- Key List, when in List Mode
  ldtMap[LS.RootDigestList] = list(); -- Digest List, when in List Mode
  ldtMap[LS.CompactList] = list();-- Simple Compact List -- before "tree mode"
  ldtMap[LS.LeftLeafDigest] = nil;
  ldtMap[LS.RightLeafDigest] = nil;

  -- If the topRec already has an LDT CONTROL BIN (with a valid map in it),
  -- then we know that the main LDT record type has already been set.
  -- Otherwise, we should set it. This function will check, and if necessary,
  -- set the control bin.
  -- This method also sets this toprec as an LDT type record.
  ldt_common.setLdtRecordType( topRec );
  
  -- Set the BIN Flag type to show that this is an LDT Bin, with all of
  -- the special priviledges and restrictions that go with it.

  -- Put our new map in the record, then store the record.
  list.append( ldtCtrl, propMap );
  list.append( ldtCtrl, ldtMap );
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags( topRec, ldtBinName, BF.LDT_BIN );

  return ldtCtrl;
end -- initializeLdtCtrl()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree Leaf Nodes have four bins:
-- Each LDT Data Record (LDR) holds a small amount of control information
-- and a list.  A LDR will have four bins:
-- (1) A Property Map Bin (the same for all LDT subrecords)
-- (2) The Control Bin (a Map with the various control data)
-- (3) The Data List Bin -- where we hold Object "list entries"
-- (4) The Binary Bin -- (Optional) where we hold compacted binary entries
--    (just the as bytes values)
--
-- Records used for B+ Tree Inner Nodes have five bins:
-- (1) A Property Map Bin (the same for all LDT subrecords)
-- (2) The Control Bin (a Map with the various control data)
-- (3) The key List Bin -- where we hold Key "list entries"
-- (4) The Digest List Bin -- where we hold the digests
-- (5) The Binary Bin -- (Optional) where we hold compacted binary entries
--    (just the as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 3) or BINARY MODE (bin 5)
-- ==> 'ldtControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'ldtListBin' Contents (A List holding entries)
--  ==> 'ldtBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
  
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Initialize Interior B+ Tree Nodes  (Records) |||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree modes have three bins:
-- Chunks hold the actual entries. Each LDT Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The Data List Bin ('DataListBin') -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> 'LdtControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'LdtListBin' Contents (A List holding entries)
--  ==> 'LdtBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
-- ======================================================================
-- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> --
--           Large Ordered List (LLIST) Utility Functions
-- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> -- <><><><><> --
-- ======================================================================
-- These are all local functions to this module and serve various
-- utility and assistance functions.
-- ======================================================================

-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getLeafMap( leafSubRec )
  -- local meth = "getLeafMap()";
  return leafSubRec[LSR_CTRL_BIN]; -- this should be a map.
end -- getLeafMap

-- ======================================================================
-- Convenience function to return the Control Map given a subrec
-- ======================================================================
local function getNodeMap( nodeSubRec )
  -- local meth = "getNodeMap()";
  return nodeSubRec[NSR_CTRL_BIN]; -- this should be a map.
end -- getNodeMap

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
    if (not aerospike:exists(topRec)) then
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
    ldtCtrl, propMap = ldt_common.validateLdtBin(topRec, ldtBinName, LDT_TYPE);

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

  return ldtCtrl; -- Save the caller the effort of extracting the map.
end -- validateRecBinAndMap()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
local function summarizeList( myList )
  local resultMap = {};
  resultMap.Summary = "Summary of the List";
  local listSize  = list.size( myList );
  resultMap.ListSize = listSize;
  if resultMap.ListSize == 0 then
    resultMap.FirstElement = "List Is Empty";
    resultMap.LastElement = "List Is Empty";
  else
    resultMap.FirstElement = tostring( myList[1] );
    resultMap.LastElement =  tostring( myList[listSize] );
  end

  return tostring( resultMap );
end -- summarizeList()

-- ======================================================================
-- Produce a COMPARABLE value (our overloaded term here is "key") from
-- the user's value.
-- The value is either simple (atomic), in which case we just return the
-- value, or an object (complex), in which case we must perform some operation
-- to extract an atomic value that can be compared.  For LLIST, we do one
-- additional thing, which is to look for a field in the complex object
-- called "key" (lower case "key") if no other KeyFunction is supplied.
--
-- Parms:
-- (*) ldtMap: The basic LDT Control structure
-- (*) value: The value from which we extract a "keyValue" that can be
--            compared in an ordered compare operation.
-- Return a comparable keyValue:
-- ==> The original value, if it is an atomic type
-- ==> A Unique Identifier subset (that is atomic)
-- ==> The entire object, in string form.
-- ======================================================================
local function getKeyValue( value )
  local meth = "getKeyValue()";
  if (value == nil) then
    return nil;
  end

  local keyValue;
  if (getmetatable(value) == Map) then
    if (value[KEY_FIELD] ~= nil) then
      -- Use the default action of using the object's KEY field
      keyValue = value[KEY_FIELD];
    else
      warn("[WARNING]<%s:%s> LLIST requires a Key for Objects",
        MOD, meth );
      error( ldte.ERR_KEY_FIELD_NOT_FOUND );
    end
  else
    keyValue = value;
  end
  if (type(keyValue) ~= "number" and type(keyValue) ~= "string") then
    error(tostring(ldte.ERR_KEY_BAD) .. "(".. type(keyValue) ..")");
  end
  return keyValue;
end -- getKeyValue();



local function printKey(ldtMap, l)
    if (l == nil) then
      return nil;
    end
    local keyList = list();
    for i = 1, #l, 1 do
       -- list.append(keyList, getKeyValue( l[i]));
       local key = getKeyValue(l[i])
       list.append(keyList, key);
    end
    return keyList
end

-- ======================================================================
-- printRoot( topRec, ldtCtrl )
-- ======================================================================
-- Dump the Root contents for Debugging/Tracing purposes
-- ======================================================================
local function printRoot( topRec, ldtCtrl )
  -- Extract the property map and control map from the ldt bin list.
  local propMap    = ldtCtrl[LDT_PROP_MAP];
  local ldtMap     = ldtCtrl[LDT_CTRL_MAP];
  local keyList    = ldtMap[LS.RootKeyList];
  local digestList = ldtMap[LS.RootDigestList];
  local ldtBinName = propMap[PM.BinName];

  -- Remember that "print()" goes ONLY to the console, NOT to the log.
  info("ROOT::Bin(%s)", ldtBinName );
  info("ROOT::Keys: Size(%d) List(%s)", #keyList, tostring( printKey (ldtMap, keyList) ) );
  if (ldtMap[LS.CompactList]) then
    info("ROOT: size (%d), CompactList (%s)", #ldtMap[LS.CompactList], printKey(ldtMap, ldtMap[LS.CompactList]));
  end
  info("ROOT:: Left Leaf %s Right Leaf %s", tostring(ldtMap[LS.LeftLeafDigest]), tostring(ldtMap[LS.RightLeafDigest]));
end -- printRoot()


-- ======================================================================
-- printNode()
-- ======================================================================
-- Dump the Node contents for Debugging/Tracing purposes
-- ======================================================================
local function printNode(ldtMap, nodeSubRec, nodeLevel )
  local nodePropMap        = nodeSubRec[SUBREC_PROP_BIN];
  local nodeLdtMap        = nodeSubRec[NSR_CTRL_BIN];
  local keyList     = nodeSubRec[NSR_KEY_LIST_BIN];
  local digestList  = nodeSubRec[NSR_DIGEST_BIN];
  if keyList == nil then
    keyList = list();
  end
  if digestList == nil then
    digestList = list();
  end

  -- Remember that "print()" goes ONLY to the console, NOT to the log.
  info("NODE::Level(%s)", tostring( nodeLevel ));
  info("NODE::Digest(%s)", tostring(record.digest(nodeSubRec)));
  info("NODE::KeyList: Size(%d) List(%s)", #keyList, tostring( printKey(ldtMap, keyList) ) );
end -- printNode()

-- ======================================================================
-- printLeaf( topRec, ldtCtrl )
-- ============================================leafP=========================
-- Dump the Leaf contents for Debugging/Tracing purposes
-- ======================================================================
local function printLeaf( ldtMap, leafSubRec )
  local leafPropMap     = leafSubRec[SUBREC_PROP_BIN];
  local leafLdtMap     = leafSubRec[LSR_CTRL_BIN];
  local objList  = leafSubRec[LSR_LIST_BIN];
  if objList == nil then
    objList = list();
  end

  -- Remember that "print()" goes ONLY to the console, NOT to the log.
  info("LEAF::Objects: Count(%d) Size(%d) Key(%s)", #objList, ldt_common.getValSize(objList), tostring(printKey(ldtMap, objList))); 
  -- local leafDigest    = record.digest(leafSubRec);
  --info("LEAF (%s) ::Objects: Count(%d) Size(%d) Key(%s) left(%s) right (%s)", tostring(leafDigest), #objList, ldt_common.getValSize(objList), tostring(printKey(ldtMap, objList)), tostring(leafLdtMap[LF_PrevPage]), tostring(leafLdtMap[LF_NextPage])); 
  -- end
end -- printLeaf()

-- ======================================================================
-- rootNodeSummary( ldtCtrl )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function rootNodeSummary( ldtCtrl )
  local resultMap = ldtCtrl;

  -- Finish this -- move selected fields into resultMap and return it.

  return tostring( ldtSummary( ldtCtrl )  );
end -- rootNodeSummary()

-- ======================================================================
-- nodeSummary( nodeSubRec )
-- nodeSummaryString( nodeSubRec )
-- ======================================================================
-- Print out interesting stats about this Interior B+ Tree Node
-- ======================================================================
local function nodeSummary( nodeSubRec )
  local meth = "nodeSummary()";
  local resultMap = {};
  local nodePropMap  = nodeSubRec[SUBREC_PROP_BIN];
  local nodeCtrlMap  = nodeSubRec[NSR_CTRL_BIN];
  local keyList = nodeSubRec[NSR_KEY_LIST_BIN];
  local digestList = nodeSubRec[NSR_DIGEST_BIN];

  -- General Properties (the Properties Bin)
  resultMap.SUMMARY           = "NODE Summary";
  resultMap.PropMagic         = nodePropMap[PM.Magic];
  resultMap.PropEsrDigest     = nodePropMap[PM.EsrDigest];
  resultMap.PropRecordType    = nodePropMap[PM.RecType];
  resultMap.PropParentDigest  = nodePropMap[PM.ParentDigest];
  
  -- Node Control Map
  resultMap.ListEntryCount = nodeCtrlMap[ND_ListEntryCount];

  -- Node Contents (Object List)
  resultMap.KEY_LIST              = keyList;
  resultMap.DIGEST_LIST           = digestList;

  return resultMap;
end -- nodeSummary()

-- ======================================================================
-- ======================================================================
local function nodeSummaryString( nodeSubRec )
  return tostring( nodeSummary( nodeSubRec ) );
end -- nodeSummaryString()

-- ======================================================================
-- leafSummary( leafSubRec )
-- leafSummaryString( leafSubRec )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Leaf (Data) node
-- ======================================================================
local function leafSummary( leafSubRec )

  if( leafSubRec == nil ) then
    return "NIL Leaf Record";
  end

  local resultMap = {};
  local leafPropMap   = leafSubRec[SUBREC_PROP_BIN];
  local leafCtrlMap   = leafSubRec[LSR_CTRL_BIN];
  local leafList  = leafSubRec[LSR_LIST_BIN];
  resultMap.SUMMARY           = "LEAF Summary";

  -- General Properties (the Properties Bin)
  if leafPropMap == nil then
    resultMap.ERROR = "NIL Leaf PROP MAP";
  else
    resultMap.PropMagic         = leafPropMap[PM.Magic];
    resultMap.PropEsrDigest     = leafPropMap[PM.EsrDigest];
    resultMap.PropSelfDigest    = leafPropMap[PM.SelfDigest];
    resultMap.PropRecordType    = leafPropMap[PM.RecType];
    resultMap.PropParentDigest  = leafPropMap[PM.ParentDigest];
  end

  trace("[LEAF PROPS]: %s", tostring(resultMap));
  
  -- Leaf Control Map
  resultMap.LF_ListEntryCount = leafCtrlMap[LF_ListEntryCount];
  resultMap.LF_PrevPage       = leafCtrlMap[LF_PrevPage];
  resultMap.LF_NextPage       = leafCtrlMap[LF_NextPage];

  -- Leaf Contents (Object List)
  resultMap.LIST              = leafList;

  return resultMap;
end -- leafSummary()

-- ======================================================================
-- ======================================================================
local function leafSummaryString( leafSubRec )
  return tostring( leafSummary( leafSubRec ) );
end

-- ======================================================================
-- ======================================================================
local function showRecSummary( nodeSubRec, propMap )
  local meth = "showRecSummary()";
  -- Debug/Tracing to see what we're putting in the SubRec Context
  if( propMap == nil ) then
    info("[ERROR]<%s:%s>: propMap value is NIL", MOD, meth );
    error( ldte.ERR_SUBREC_DAMAGED );
  end
end -- showRecSummary()

-- ======================================================================
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, like insert(), search(), remove(),
-- will employ the "subrecContext" object, which will hold all of the
-- subrecords that were opened during processing.  Note that with
-- B+ Trees, operations like insert() can potentially involve many subrec
-- operations -- and can also potentially revisit pages.  In addition,
-- we employ a "compact list", which gets converted into tree inserts when
-- we cross a threshold value, so that will involve MANY subrec "re-opens"
-- that would confuse the underlying infrastructure.
--
-- SubRecContext Design:
-- The key will be the DigestString, and the value will be the subRec
-- pointer.  At the end of an outer call, we will iterate thru the subrec
-- context and close all open subrecords.  Note that we may also need
-- to mark them dirty -- but for now we'll update them in place (as needed),
-- but we won't close them until the end.
-- ======================================================================
-- NOTE: We are now using ldt_common.createSubRecContext()
-- ======================================================================

-- ======================================================================
-- keyCompare: (Compare ONLY Key values, not Object values)
-- ======================================================================
-- Compare Search Key Value with KeyList, following the protocol for data
-- compare types.  Since compare uses only atomic key types (the value
-- that would be the RESULT of the extractKey() function), 
-- CR.LESS_THAN    (-1) for searchKey <  dataKey,
-- CR.EQUAL        ( 0) for searchKey == dataKey,
-- CR.GREATER_THAN ( 1) for searchKey >  dataKey
-- Return CR.ERROR (-2) if either of the values is null (or other error)
-- Return CR.INTERNAL_ERROR(-3) if there is some (weird) internal error
-- ======================================================================
local function keyCompare(searchKey, dataKey)
  local meth = "keyCompare()";
  local result = CR.INTERNAL_ERROR; -- we should never be here.
  -- First check
  if (dataKey == nil) then
    warn("[WARNING]<%s:%s> DataKey is nil", MOD, meth );
    result = CR.ERROR;
  elseif (searchKey == nil) then
    -- a nil search key is always LESS THAN everything.
    result = CR.LESS_THAN;
  else
    if (type(searchKey) ~= type(dataKey)) then
      error(ldte.ERR_TYPE_MISMATCH);
    end
    if searchKey == dataKey then
      result = CR.EQUAL;
    elseif searchKey < dataKey then
      result = CR.LESS_THAN;
    else
      result = CR.GREATER_THAN;
    end
  end
  return result;
end -- keyCompare()

-- ======================================================================
-- objectCompare: Compare a key with a complex object
-- ======================================================================
-- Compare Search Value with data, following the protocol for data
-- compare types.
-- Parms:
-- (*) ldtMap: control map for LDT
-- (*) searchKey: Key value we're comparing (if nil, always true)
-- (*) objectValue: Atomic or Complex Object (the LIVE object)
-- Return:
-- CR.LESS_THAN    (-1) for searchKey <   objectKey
-- CR.EQUAL        ( 0) for searchKey ==  objectKey,
-- CR.GREATER_THAN ( 1) for searchKey >   objectKey
-- Return CR.ERROR (-2) if Key or Object is null (or other error)
-- Return CR.INTERNAL_ERROR(-3) if there is some (weird) internal error
-- ======================================================================
local function objectCompare( ldtMap, searchKey, objectValue )
  local meth = "objectCompare()";

  local result = CR.INTERNAL_ERROR; -- Expect result to be reassigned.

  -- First check
  if (objectValue == nil) then
    warn("[WARNING]<%s:%s> ObjectValue is nil", MOD, meth );
    result = CR.ERROR;
  elseif( searchKey == nil ) then
    result = CR.EQUAL;
  else
    -- Get the key value for the object -- this could either be the object 
    -- itself (if atomic), or the result of a function that computes the
    -- key from the object.
    local objectKey = getKeyValue(objectValue);
    if( type(objectKey) ~= type(searchKey) ) then
      warn("[INFO]<%s:%s> ObjectValue::SearchKey TYPE Mismatch", MOD, meth );
      warn("[INFO] TYPE ObjectValue(%s) TYPE SearchKey(%s)",
        type(objectKey), type(searchKey) );
      -- Generate the error here for mismatched types.
      error(ldte.ERR_TYPE_MISMATCH);
    end

    if searchKey == objectKey then
      result = CR.EQUAL;
    elseif searchKey < objectKey then
      result = CR.LESS_THAN;
    else
      result = CR.GREATER_THAN;
    end
  end -- else compare

  return result;
end -- objectCompare()

-- =======================================================================
--     Node (key) Searching:
-- =======================================================================
--        Index:   1   2   3   4 (node pointers, i.e. Sub-Rec digest values)
--     Key List: [10, 20, 30]
--     Dig List: [ A,  B,  C,  D]
--     +--+--+--+                        +--+--+--+
--     |10|20|30|                        |40|50|60| 
--     +--+--+--+                        +--+--+--+
--   1/  2|  |3  \4 (index)             /   |  |   \
--   A    B  C    D (Digest Ptr)       E    F  G    H
--
--   Child A: all values < 10
--   Child B: all values >= 10 and < 20
--   Child C: all values >= 20 and < 30
--   Child D: all values >= 30
--   (1) Looking for value 15:  (SV=15, Obj=x)
--       : 15 > 10, keep looking
--       : 15 < 20, want Child B (same index ptr as value (2)
--   (2) Looking for value 30:  (SV=30, Obj=x)
--       : 30 > 10, keep looking
--       : 30 > 20, keep looking
--       : 30 = 30, want Child D (same index ptr as value (2)
--   (3) Looking for value 31:  (SV=31, Obj=x)
--       : 31 > 10, keep looking
--       : 31 > 20, keep looking
--       : 31 > 30, At End = want child D
--   (4) Looking for value 5:  (SV=5, Obj=x)
--       : 5 < 10, Want Child A


-- ======================================================================
-- searchKeyListLinear(): Search the Key list in a Root or Inner Node
-- ======================================================================
-- Search the key list, return the index of the value that represents the
-- child pointer that we should follow.  Notice that this is DIFFERENT
-- from the Leaf Search, which treats the EQUAL case differently.
--
-- For this example:
--              +---+---+---+---+
-- KeyList      |111|222|333|444|
--              +---+---+---+---+
-- DigestList   A   B   C   D   E
--
-- Search Key 100:  Position 1 :: Follow Child Ptr A
-- Search Key 111:  Position 2 :: Follow Child Ptr B
-- Search Key 200:  Position 2 :: Follow Child Ptr B
-- Search Key 222:  Position 2 :: Follow Child Ptr C
-- Search Key 555:  Position 5 :: Follow Child Ptr E
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) keyList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then is always LESS THAN the list
-- SUCCESS: Returns a STRUCTURE (a map)
--  + Position: (where we found it if true, or where we would insert if false)
--  + Found:  (true, false)
--  + Status: Ok, or Error
-- ERRORS: Return ERR.GENERAL (bad compare)
-- ======================================================================
local function searchKeyListLinear( ldtMap, keyList, searchKey )
  local meth = "searchKeyListLinear()";

  -- Note that the parent caller has already checked for nil search key.
  
  -- Linear scan of the KeyList.  Find the appropriate entry and return
  -- the index.
  local resultIndex = 0;
  local compareResult = 0;

  -- Set up the Search Results Map.
  local resultMap = {};
  resultMap.Position = 0;
  resultMap.Found = false;
  resultMap.Status = ERR.OK;

  -- Do the List page mode search here
  local listSize = list.size( keyList );
  local entryKey;
  for i = 1, listSize, 1 do

    if (#keyList == 0) then
      resultMap.Position = i; -- Left Child Pointer
      resultMap.Found = false;
      return resultMap;
    end

    entryKey = keyList[i];
    compareResult = keyCompare( searchKey, entryKey );
    if compareResult == CR.ERROR then
      resultMap.Status = ERR.GENERAL;
      return resultMap;
    end
    if compareResult  == CR.LESS_THAN then
      -- We want the child pointer that goes with THIS index (left ptr)
      resultMap.Position = i; -- Left Child Pointer
      resultMap.Found = false;
      return resultMap;
    elseif compareResult == CR.EQUAL then
      -- Found it -- return the "right child" index (right ptr)
      resultMap.Position = i + 1;
      resultMap.Found = true;
      return resultMap;
    end
    -- otherwise, keep looking.  We haven't passed the spot yet.
  end -- for each list item

  -- return furthest right child pointer
  resultMap.Position = listSize + 1;
  resultMap.Found = false;
  return resultMap;
end -- searchKeyListLinear()

-- ======================================================================
-- searchKeyListBinary(): Search the Key list in a Root or Inner Node
-- ======================================================================
-- Search the key list, return the index of the value that represents the
-- child pointer that we should follow.  Notice that this is DIFFERENT
-- from the Leaf Search, which treats the EQUAL case differently.
--
-- For this example:
--              +---+---+---+---+
-- KeyList      |111|222|333|444|
--              +---+---+---+---+
-- DigestList   A   B   C   D   E
--
-- Search Key 100:  Digest List Position 1 :: Follow Child Ptr A
-- Search Key 111:  Digest List Position 2 :: Follow Child Ptr B
-- Search Key 200:  Digest List Position 2 :: Follow Child Ptr B
-- Search Key 222:  Digest List Position 3 :: Follow Child Ptr C
-- Search Key 555:  Digest List Position 5 :: Follow Child Ptr E
--
-- Note that in the case of Duplicate values (and some of the dups may make
-- it up into the parent node key lists), we have to ALWAYS get the LEFT-MOST
-- value (regardless of ascending/descending values) so that we get ALL of
-- them (in case we're searching for a set of values).
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) keyList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then is always LESS THAN the list
-- Return:
-- SUCCESS: Returns a STRUCTURE (a map)
--  + Position: (where we found it if true, or where we would insert if false)
--  + Found:  (true, false)
--  + Status: Ok, or Error
-- ERRORS: Return ERR.GENERAL (bad compare)
-- ======================================================================
local function searchKeyListBinary( ldtMap, keyList, searchKey )
  local meth = "searchKeyListBinary()";

  -- Note that the parent caller has already checked for nil search key.

  -- Binary Search of the KeyList.  Find the appropriate entry and return
  -- the index.  Note that we're assuming ASCENDING values first, then will
  -- generalize later for ASCENDING and DESCENDING (a dynamic compare function
  -- will help us make that pluggable).
  local resultIndex = 0;
  local compareResult = 0;
  local listSize = list.size( keyList );
  local entryKey;
  local foundStart = 0; -- shows where the value chain started, or zero

  -- Set up the Search Results Map.
  local resultMap = {};
  resultMap.Position = 0;
  resultMap.Found = false;
  resultMap.Status = ERR.OK;

  --  Initialize the Start, Middle and End numbers
  local iStart,iEnd,iMid = 1,listSize,0
  local finalState = 0; -- Shows where iMid ends up pointing.
  while iStart <= iEnd do
    -- calculate middle
    iMid = math.floor( (iStart+iEnd)/2 );
    -- get compare value from the DB List (no translate for keys)
    local entryKey = keyList[iMid];
    compareResult = keyCompare( searchKey, entryKey );

    if compareResult == CR.EQUAL then
      foundStart = iMid;
      -- If we're UNIQUE, then we're done. Otherwise, we have to look LEFT
      -- to find the first NON-matching position.
      if (ldtMap[LS.KeyUnique] ~= AS_TRUE) then
        -- There might be duplicates.  Scan left to find left-most matching
        -- key.  Note that if we fall off the front, keyList[0] should
        -- (in theory) be defined to be NIL, so the compare just fails and
        -- we stop.
        entryKey = keyList[iMid - 1];
        while searchKey == entryKey do
          iMid = iMid - 1;
          entryKey = keyList[iMid - 1];
        end
      end
      -- Right Child Pointer that goes with iMid
      resultMap.Position = iMid + 1;
      resultMap.Found = true;
      return resultMap;
    end -- if found, we've returned.

    -- Keep Searching
    if compareResult == CR.LESS_THAN then
      iEnd = iMid - 1;
      finalState = 0; -- At the end, iMid points at the Insert Point.
    else
      iStart = iMid + 1;
      finalState = 1; -- At the end, iMid points BEFORE the Insert Point.
    end
  end -- while binary search


  -- If we're here, then iStart > iEnd, so we have to return the index of
  -- the correct child pointer that matches the search.
  -- Final state shows us where we are relative to the last compare.  If our
  -- last compare:: Cmp(searchKey, entryKey) shows SK < EK, then the value
  -- of iMid 
  resultIndex = iMid + finalState;
  resultMap.Position = resultIndex;
  resultMap.Found = false;

  return resultMap;
end -- searchKeyListBinary()
  
-- ======================================================================
-- searchKeyList(): Search the Key list in a Root or Inner Node
-- ======================================================================
-- Search the key list, return the index of the value that represents the
-- child pointer that we should follow.  Notice that this is DIFFERENT
-- from the Leaf Search, which treats the EQUAL case differently.
--
-- For this example:
--              +---+---+---+---+
-- KeyList      |111|222|333|444|
--              +---+---+---+---+
-- DigestList   A   B   C   D   E
--
-- Search Key 100:  Digest List Position 1 :: Follow Child Ptr A
-- Search Key 111:  Digest List Position 2 :: Follow Child Ptr B
-- Search Key 200:  Digest List Position 2 :: Follow Child Ptr B
-- Search Key 222:  Digest List Position 3 :: Follow Child Ptr C
-- Search Key 555:  Digest List Position 5 :: Follow Child Ptr E
--
-- The Key List is ordered, so it can be searched with either linear (simple
-- but slow) or binary search (more complicated, but faster).  We have both
-- here due to the evolution of the code (simple first, complex second).
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) keyList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then is always LESS THAN the list
-- Return:
-- SUCCESS: Returns a STRUCTURE (a map)
--  + Position: (where we found it if true, or where we would insert if false)
--  + Found:  (true, false)
--  + Status: Ok, or Error
-- ERRORS: Return ERR.GENERAL (bad compare)
-- ======================================================================
local function searchKeyList( ldtMap, keyList, searchKey )
  local meth = "searchKeyList()";

  -- We can short-cut this.  If searchKey is nil, then we automatically
  -- return 1 (the first index position).
  -- Also, if the keyList is empty (which is the case when a node has been
  -- deleted down to a single child, but has not yet been merged), then we
  -- implicitly search the left child (also index 1).
  if (searchKey == nil or #keyList == 0) then
    local resultMap = {};
    resultMap.Position = 1;
    resultMap.Found = false;
    resultMap.Status = ERR.OK;
    return resultMap;
  end

  -- Depending on the state of the code, pick either the LINEAR search method
  -- or the BINARY search method.
  -- Rule of thumb is that linear is the better search for lists shorter than
  -- 20, and after that binary search is better.  However, that does ignore
  -- the size of the key and the costs of the Compare.
  if (#keyList <= LINEAR_SEARCH_CUTOFF) then
    return searchKeyListLinear( ldtMap, keyList, searchKey );
  else
    return searchKeyListBinary( ldtMap, keyList, searchKey );
  end
end --searchKeyList()

-- ======================================================================
-- searchObjectListLinear(): LINEAR Search the Object List in a Leaf Node
-- ======================================================================
-- Search the Object list, return the index of the value that is THE FIRST
-- object to match the search Key. Notice that this method is different
-- from the searchKeyList() -- since that is only looking for the right
-- leaf.  In searchObjectList() we're looking for the actual value.
-- NOTE: Later versions of this method will probably return a location
-- of where to start scanning (for value ranges and so on).  But, for now,
-- we're just looking for an exact match.
-- For this example:
--              +---+---+---+---+
-- ObjectList   |111|222|333|444|
--              +---+---+---+---+
-- Index:         1   2   3   4
--
-- Search Key 100:  Position 1 :: Insert at index location 1
-- Search Key 111:  Position 1 :: Insert at index location 1
-- Search Key 200:  Position 2 :: Insert at index location 2
-- Search Key 222:  Position 2 :: Insert at index location 2
-- Parms:
-- (*) ldtMap: Main control Map
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) objectList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then it compares LESS than everything.
-- Return: Returns a STRUCTURE (a map)
-- (*) Position: (where we found it if true, or where we would insert if false)
-- (*) Found:  (true, false)
-- (*) Status: Ok, or Error
--
-- OK: Return the Position of the first matching value.
-- ERRORS:
-- ERR.GENERAL   (-1): Trouble
-- ERR.NOT_FOUND (-2): Item not found.
-- ======================================================================
local function searchObjectListLinear( ldtMap, objectList, searchKey )
  local meth = "searchObjectListLinear()";

  local resultMap = {};
  resultMap.Position = 0;
  resultMap.Found = false;
  resultMap.Status = ERR.OK;

  -- NOTE: The caller checks for a NULL search key, so we can bypass that setep.


  -- Linear scan of the ObjectList.  Find the appropriate entry and return
  -- the index.
  local resultIndex = 0;
  local compareResult = 0;
  local objectKey;
  local storeObject; 
  local liveObject; 

  -- Do the List page mode search here
  local listSize = list.size( objectList );

  for i = 1, listSize, 1 do
    liveObject = objectList[i];

    compareResult = keyCompare(searchKey, getKeyValue(liveObject));
    if compareResult == CR.ERROR then
      resultMap.Status = ERR.GENERAL;
      return resultMap;
    end
    if compareResult  == CR.LESS_THAN then
      -- We want the child pointer that goes with THIS index (left ptr)
      resultMap.Position = i;
      return resultMap;
    elseif compareResult == CR.EQUAL then
      -- Found it -- return the index of THIS value
      resultMap.Position = i; -- Index of THIS value.
      resultMap.Found = true;
      return resultMap;
    end
    -- otherwise, keep looking.  We haven't passed the spot yet.
  end -- for each list item

  resultMap.Position = listSize + 1;
  resultMap.Found = false;

  return resultMap;
end -- searchObjectListLinear()

-- ======================================================================
-- searchObjectListBinary(): BINARY Search the Object List in a Leaf Node
-- ======================================================================
-- Search the Object list, return the index of the value that is THE FIRST
-- object to match the search Key. Notice that this method is different
-- from the searchKeyList() -- since that is only looking for the right
-- leaf.  In searchObjectList() we're looking for the actual value.
-- NOTE: Later versions of this method will probably return a location
-- of where to start scanning (for value ranges and so on).  But, for now,
-- we're just looking for an exact match.
-- For this example:
--              +---+---+---+---+
-- ObjectList   |111|222|333|444|
--              +---+---+---+---+
-- Index:         1   2   3   4
--
-- Search Key 100:  Position 1 :: Insert at index location 1
-- Search Key 111:  Position 1 :: Insert at index location 1
-- Search Key 200:  Position 2 :: Insert at index location 2
-- Search Key 222:  Position 2 :: Insert at index location 2
-- Parms:
-- (*) ldtMap: Main control Map
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) objectList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then it compares LESS than everything.
-- Return: Returns a STRUCTURE (a map)
-- (*) POSITION: (where we found it if true, or where we would insert if false)
-- (*) FOUND RESULTS (true, false)
-- (*) ERROR Status: Ok, or Error
--
-- OK: Return the Position of the first matching value.
-- ERRORS:
-- ERR.GENERAL   (-1): Trouble
-- ERR.NOT_FOUND (-2): Item not found.
-- ======================================================================
local function searchObjectListBinary( ldtMap, objectList, searchKey )
  local meth = "searchObjectListBinary()";

  local resultMap = {};
  resultMap.Status = ERR.OK;
  resultMap.Position = 0;
  resultMap.Found = false;

  -- NOTE: The caller checks for a NULL search key, so we can bypass that setep.

  -- BINARY SEARCH of the ObjectList.  Find the appropriate entry and return
  -- the index.
  local resultIndex = 0;
  local compareResult = 0;
  local objectKey;
  local storeObject; 
  local liveObject; 
  local listSize = list.size( objectList );
  local foundStart = 0; -- shows where the Dup value chain started, or zero

  --  Initialize the Start, Middle and End numbers
  local iStart,iEnd,iMid = 1,listSize,0
  local finalState = 0; -- Shows where iMid ends up pointing.
  while iStart <= iEnd do
    -- calculate middle
    iMid = math.floor( (iStart+iEnd)/2 );
    liveObject = objectList[iMid];

    compareResult = keyCompare(searchKey, getKeyValue(liveObject));

    if compareResult == CR.EQUAL then
      foundStart = iMid;
      -- If we're UNIQUE, then we're done. Otherwise, we have to look LEFT
      -- to find the first NON-matching position.
      if (ldtMap[LS.KeyUnique] ~= AS_TRUE) then
        -- There might be duplicates.  Scan left to find left-most matching
        -- key.  Note that if we fall off the front, keyList[0] should
        -- (in theory) be defined to be NIL, so the compare just fails and
        -- we stop.
        while true do
          liveObject = objectList[iMid - 1];
          compareResult = keyCompare(searchKey, getKeyValue(liveObject));
          if compareResult ~= CR.EQUAL then
            break;
          end
          iMid = iMid - 1;
        end -- done looking (left) for duplicates
      end
      -- Return the index of THIS location.
      resultMap.Position = iMid;
      resultMap.Found = true;
      return resultMap;
    end -- if found, we've returned.

    -- Keep Searching
    if compareResult == CR.LESS_THAN then
      iEnd = iMid - 1;
      finalState = 0; -- At the end, iMid points at the Insert Point.
    else
      iStart = iMid + 1;
      finalState = 1; -- At the end, iMid points BEFORE the Insert Point.
    end
  end -- while binary search

  -- If we're here, then iStart > iEnd, so we have to return the index of
  -- the correct child pointer that matches the search.
  -- Final state shows us where we are relative to the last compare.  If our
  -- last compare:: Cmp(searchKey, entryKey) shows SK < EK, then the value
  -- of iMid 
  resultMap.Position = iMid + finalState;
  resultMap.Found = false;

  return resultMap;
end -- searchObjectListBinary()

-- ======================================================================
-- searchObjectList(): Search the Object List in a Leaf Node
-- ======================================================================
-- Search the Object list, return the index of the value that is THE FIRST
-- object to match the search Key. Notice that this method is different
-- from the searchKeyList() -- since that is only looking for the right
-- leaf.  In searchObjectList() we're looking for the actual value.
-- NOTE: Later versions of this method will probably return a location
-- of where to start scanning (for value ranges and so on).  But, for now,
-- we're just looking for an exact match.
-- For this example:
--              +---+---+---+---+
-- ObjectList   |111|222|333|444|
--              +---+---+---+---+
-- Index:         1   2   3   4
--
-- Search Key 100:  Position 1 :: Insert at index location 1
-- Search Key 111:  Position 1 :: Insert at index location 1
-- Search Key 200:  Position 2 :: Insert at index location 2
-- Search Key 222:  Position 2 :: Insert at index location 2
-- Parms:
-- (*) ldtMap: Main control Map
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) objectList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then it compares LESS than everything.
-- Return: Returns a STRUCTURE (a map)
-- (*) POSITION: (where we found it if true, or where we would insert if false)
-- (*) FOUND RESULTS (true, false)
-- (*) ERROR Status: Ok, or Error
--
-- OK: Return the Position of the first matching value.
-- ERRORS:
-- ERR.GENERAL   (-1): Trouble
-- ERR.NOT_FOUND (-2): Item not found.
-- ======================================================================
local function searchObjectList( ldtMap, objectList, searchKey )
  local meth = "searchObjectList()";

  -- If we're given a nil searchKey, then we say "found" and return
  -- position 1 -- basically, to set up Scan.
  -- TODO: Must also check for EMPTY LIST -- Perhaps the caller does that?
  if( searchKey == nil ) then
    local resultMap = {};
    resultMap.Found = true;
    resultMap.Position = 1;
    resultMap.Status = ERR.OK;
    return resultMap;
  end

  -- Depending on the state of the code, pick either the LINEAR search method
  -- or the BINARY search method.
  -- Rule of thumb is that linear is the better search for lists shorter than
  -- 10, and after that binary search is better.
  local listSize = list.size(objectList);
  if (listSize <= LINEAR_SEARCH_CUTOFF) then
    return searchObjectListLinear(ldtMap, objectList, searchKey);
  else
    return searchObjectListBinary( ldtMap, objectList, searchKey );
  end

end -- searchObjectList()

-- ======================================================================
-- For debugging purposes, print the tree, starting with the root and
-- then each level down.
-- Root
-- ::Root Children
-- ::::Root Grandchildren
-- :::...::: Leaves
-- ======================================================================
local function printTree( src, topRec, ldtBinName )
  local meth = "printTree()";
  -- Start with the top level structure and descend from there.
  -- At each level, create a new child list, which will become the parent
  -- list for the next level down (unless we're at the leaves).
  -- The root is a special case of a list of parents with a single node.
  local ldtCtrl = topRec[ldtBinName];
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local nodeList = list();
  local childList = list();
  local digestString;
  local nodeSubRec;
  local treeLevel = ldtMap[LS.TreeLevel];
  local rc = 0;

  -- Remember that "print()" just goes to the console, and does NOT
  -- print out in the log.
  info("<PT> <PT> <PT> :::::   P R I N T   T R E E  ::::: <PT> <PT>");
  --info("\n======  ROOT SUMMARY ======(%s)", rootNodeSummary( ldtCtrl ));

  printRoot( topRec, ldtCtrl );

  nodeList = ldtMap[LS.RootDigestList];

  -- The Root is already printed -- now print the rest.
  for lvl = 2, treeLevel do
    local listSize = nodeList == nil and 0 or #nodeList;
    for n = 1, listSize, 1 do
      digestString = tostring( nodeList[n] );
      if digestString ~= nil and string.len(digestString) > 0 then
      nodeSubRec = ldt_common.openSubRec( src, topRec, digestString );
      if( lvl < treeLevel ) then
        -- This is an inner node -- remember all children
        local digestList  = nodeSubRec[NSR_DIGEST_BIN];
        local digestListSize = list.size( digestList );
        for d = 1, digestListSize, 1 do
          list.append( childList, digestList[d] );
        end -- end for each digest in the node
          printNode( ldtMap, nodeSubRec, lvl );
      else
        -- This is a leaf node -- just print contents of each leaf
        printLeaf( ldtMap, nodeSubRec );
      end
      end -- if digest is OK.
      -- Mark the SubRec as "done" (available).
      ldt_common.closeSubRec( src, nodeSubRec, false); -- Mark it as available
    end -- for each node in the list
    -- If we're going around again, then the old childList is the new
    -- ParentList (as in, the nodeList for the next iteration)
    nodeList = childList;
  end -- for each tree level

  info("<PT> <PT> <PT> <PT> <PT>   E N D   <PT> <PT> <PT> <PT> <PT>");
 
  -- Release ALL of the read-only subrecs that might have been opened.
  rc = ldt_common.closeAllSubRecs( src );
  if( rc < 0 ) then
    info("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

end -- printTree()

-- ======================================================================
-- Update the Leaf Page pointers for a leaf -- used on initial create
-- and leaf splits.  Each leaf has a left and right pointer (digest).
-- Parms:
-- (*) leafSubRec:
-- (*) leftDigest:  Set PrevPage ptr, if not nil
-- (*) rightDigest: Set NextPage ptr, if not nil
-- ======================================================================
local function setLeafPagePointers( src, leafSubRec, leftDigest, rightDigest )
  local meth = "setLeafPagePointers()";
  local leafCtrlMap = leafSubRec[LSR_CTRL_BIN];
  if( leftDigest ~= nil ) then
    leafCtrlMap[LF_PrevPage] = leftDigest;
  end
  if( leftDigest ~= nil ) then
    leafCtrlMap[LF_NextPage] = rightDigest;
  end
  leafSubRec[LSR_CTRL_BIN] = leafCtrlMap;
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leafSubRec );

end -- setLeafPagePointers()

-- ======================================================================
-- adjustLeafPointersAfterInsert()
-- ======================================================================
-- We've just done a Leaf split, so now we have to update the page pointers
-- so that the doubly linked leaf page chain remains intact.
-- When we create pages -- we ALWAYS create a new left page (the right one
-- is the previously existing page).  So, the Next Page ptr of the right
-- page is correct (and its right neighbors are correct).  The only thing
-- to change are the LEFT record ptrs -- the new left and the old left.
--      +---+==>+---+==>+---+==>+---+==>
--      | Xi|   |OL |   | R |   | Xj| Leaves Xi, OL, R and Xj
--   <==+---+<==+---+<==+---+<==+---+
--              +---+
--              |NL | Add in this New Left Leaf to be "R"s new left neighbor
--              +---+
--      +---+==>+---+==>+---+==>+---+==>+---+==>
--      | Xi|   |OL |   |NL |   | R |   | Xj| Leaves Xi, OL, NL, R, Xj
--   <==+---+<==+---+<==+---+<==+---+<==+---+
-- Notice that if "OL" exists, then we'll have to open it just for the
-- purpose of updating the page pointer.  This is a pain, BUT, the alternative
-- is even more annoying, which means a tree traversal for scanning.  So
-- we pay our dues here -- and suffer the extra I/O to open the left leaf,
-- so that our leaf page scanning (in both directions) is easy and sane.
-- We are guaranteed that we'll always have a left leaf and a right leaf,
-- so we don't need to check for that.  However, it is possible that if the
-- old Leaf was the left most leaf (what is "R" in this example), then there
-- would be no "OL".  The left leaf digest value for "R" would be ZERO.
--                       +---+==>+---+=+
--                       | R |   | Xj| V
--                     +=+---+<==+---+
--               +---+ V             +---+==>+---+==>+---+=+
-- Add leaf "NL" |NL |     Becomes   |NL |   | R |   | Xj| V
--               +---+             +=+---+<==+---+<==+---+
--                                 V
--
-- New for Spring 2014 are the LeftLeaf and RightLeaf pointers that we 
-- maintain from the root/control information.  That gets updated when we
-- split the left-most leaf and get a new Left-Most Leaf.  Since we never
-- get a new Right-Most Leaf (at least in regular Split operations), we
-- assign that ONLY with the initial create.
-- ======================================================================
local function adjustLeafPointersAfterInsert( src, topRec, ldtMap, leftLeaf, rightLeaf, is_right)
  local meth = "adjustLeafPointersAfterInsert()";

  local leftLeafDigest = record.digest( leftLeaf );
  local rightLeafDigest = record.digest( rightLeaf );

  local leftLeafMap = leftLeaf[LSR_CTRL_BIN];
  local rightLeafMap = rightLeaf[LSR_CTRL_BIN];

  local leftNextDigest = leftLeafMap[LF_NextPage] -- 6
  local leftPrevDigest = leftLeafMap[LF_PrevPage]  -- nil
  local rightPrevDigest = rightLeafMap[LF_PrevPage] -- nil 
  local rightNextDigest = rightLeafMap[LF_NextPage] -- nil 

  leftLeafMap[LF_NextPage] = rightLeafDigest;
  rightLeafMap[LF_PrevPage] = leftLeafDigest; 

  if (is_right) then
    -- Right side is new 
    rightLeafMap[LF_NextPage] = leftNextDigest;
    if (leftNextDigest == nil) or (leftNextDigest == 0) then
      ldtMap[LS.RightLeafDigest] = rightLeafDigest;
    else 
      local leftNextSubRec = ldt_common.openSubRec( src, topRec, tostring(leftNextDigest) );
      if (leftNextSubRec == nil) then
        warn("[ERROR]<%s:%s> oldLeftLeaf NIL from openSubrec: digest(%s)",
          MOD, meth, leftNextDigest);
        error( ldte.ERR_SUBREC_OPEN );
      end
      local leftNextSubRecMap = leftNextSubRec[LSR_CTRL_BIN];
      leftNextSubRecMap[LF_PrevPage] = rightLeafDigest; 
      leftNextSubRec[LSR_CTRL_BIN] = leftNextSubRecMap;
      ldt_common.updateSubRec( src, leftNextSubRec );
    end
  else
    -- Left side is new 
    leftLeafMap[LF_PrevPage] = rightPrevDigest;
    if (rightPrevDigest == nil) or (rightPrevDigest == 0) then
      ldtMap[LS.LeftLeafDigest] = leftLeafDigest; 
    else
      local rightPrevSubRec = ldt_common.openSubRec( src, topRec, tostring(rightPrevDigest) );
      if (rightPrevSubRec == nil) then
        warn("[ERROR]<%s:%s> oldLeftLeaf NIL from openSubrec: digest(%s)",
          MOD, meth, rightPrevDigest);
        error( ldte.ERR_SUBREC_OPEN );
      end
      local rightPrevSubRecMap = rightPrevSubRec[LSR_CTRL_BIN];
      rightPrevSubRecMap[LF_NextPage] = leftLeafDigest; 
      rightPrevSubRec[LSR_CTRL_BIN] = rightPrevSubRecMap;
      ldt_common.updateSubRec( src, rightPrevSubRec );
    end
  end

  leftLeaf[LSR_CTRL_BIN] = leftLeafMap;
  rightLeaf[LSR_CTRL_BIN] = rightLeafMap;
  ldt_common.updateSubRec(src, leftLeaf);
  ldt_common.updateSubRec(src, rightLeaf);

  return 0;
end -- adjustLeafPointersAfterInsert()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Save this code for later (for reference)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--    for i = 1, list.size( objectList ), 1 do
--      compareResult = compare( keyType, searchKey, objectList[i] );
--      if compareResult == -2 then
--        return nil -- error result.
--      end
--      if compareResult == 0 then
--        -- Start gathering up values
--        gatherLeafListData( topRec, leafSubRec, ldtMap, resultList, searchKey,
--          func, fargs, flag );
--          return resultList;
--      elseif compareResult  == 1 then
--          return resultList;
--      end
--      -- otherwise, keep looking.  We haven't passed the spot yet.
--    end -- for each list item
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- adjustLeafPointersAfterDelete()
-- ======================================================================
-- We've just done a Leaf REMOVE, so now we have to update the page
-- pointers so that the doubly linked leaf page chain remains intact.
-- Note that we never remove leaves once we've reached a MINIMAL tree,
-- which is TWO leaves (in the Unique Key case) or possibly three+ leaves
-- for the Duplicate Key case (a NULL left-most leaf and 1 or more right
-- leaves).
-- When we INSERT leaves -- we ALWAYS create a new left leaf (the right one
-- is the previously existing page)
-- ==> see the sibling function: adjustLeafPointersAfterInsert()
--
-- When we DELETE a leaf, we basically delete the leaf that became empty,
-- and then deal with its neighbors.  Eventually we may add the capability
-- for LEAF MERGE (for better balance)  but that's a more advanced issue,
-- mainly because duplicate values make that a messy problem.
--
-- So, for a given Delete Leaf "DL", we have to look at its left and right
-- neighbors to know how to adjust the Prev/Next page pointers.
--      +---+==>+---+==>+---+==>0
--      | LL|   | DL|   | RL|   Leaves LL, DL and RL
--  0<==+---+<==+---+<==+---+
--      Becomes
--      +---+==>+---+==>0
--      | LL|   |RL |   Leaves LL, RL
--  0<==+---+<==+---+
--
-- Notice that if LL does not exist, then DL was the left-most leaf, and
-- RL must become that (pointed to by the ldtMap).  Similarly, if the RL
-- does not exist, then LL becomes the new right-most leaf.
-- ======================================================================
local function adjustLeafPointersAfterDelete( src, topRec, ldtMap, leafSubRec )
  local meth = "adjustLeafPointersAfterDelete()";
  -- All of the OTHER information (e.g. parent ptr info, etc) has been taken
  -- care of.  Now we just have to adjust the Next/Prev leaf pointers before
  -- we finally release the Leaf Sub-Record.
  local leafSubRecDigest = record.digest( leafSubRec );
  local leafCtrlMap = leafSubRec[LSR_CTRL_BIN];

  -- There are potentially Left and Right Neighbors.  If they exist, then
  -- adjust their pointers appropriately. 
  local leftLeafDigest = leafCtrlMap[LF_PrevPage];
  local rightLeafDigest = leafCtrlMap[LF_NextPage];
  local leftLeafDigestString;
  local rightLeafDigestString;
  local leftLeafSubRec;
  local rightLeafSubRec;
  local leftLeafCtrlMap;
  local rightLeafCtrlMap;
  --info("Delete for %s left %s right %s", tostring(leafSubRecDigest), tostring(leftLeafDigest), tostring(rightLeafDigest))

  -- Update the right leaf first, if it is there.
  if rightLeafDigest ~= nil and rightLeafDigest ~= 0 then
    rightLeafDigestString = tostring(rightLeafDigest);
    rightLeafSubRec = ldt_common.openSubRec(src, topRec, rightLeafDigestString);

    if rightLeafSubRec == nil then
      warn("[SUBREC ERROR]<%s:%s> Could not open RL SubRec(%s)", MOD, meth,
        rightLeafDigestString );
      error(ldte.ERR_INTERNAL);
    end
    rightLeafCtrlMap = rightLeafSubRec[LSR_CTRL_BIN];

    -- This might be zero or a valid Digest.  It works either way.
    rightLeafCtrlMap[LF_PrevPage] = leftLeafDigest;
    if leftLeafDigest == 0 then
      -- This means our current leaf (leafSubRec) is already the LEFTMOST
      -- leaf, and so the right neighbor actually becomes the new LEFTMOST leaf.
      -- Open the right neighbor and set him appropriately.
      ldtMap[LS.LeftLeafDigest] = rightLeafDigest;
    end
    rightLeafSubRec[LSR_CTRL_BIN] = rightLeafCtrlMap;
    ldt_common.updateSubRec(src, rightLeafSubRec);
    leafSummary(rightLeafSubRec);
  end

  -- Now Update the LEFT leaf, if it is there.
  if leftLeafDigest ~= nil and leftLeafDigest ~= 0 then
    leftLeafDigestString = tostring(leftLeafDigest);
    leftLeafSubRec = ldt_common.openSubRec(src, topRec, leftLeafDigestString);

    if leftLeafSubRec == nil then
      warn("[SUBREC ERROR]<%s:%s> Could not open LL SubRec(%s)", MOD, meth,
        leftLeafDigestString );
      error(ldte.ERR_INTERNAL);
    end
    leftLeafCtrlMap = leftLeafSubRec[LSR_CTRL_BIN];

    -- This might be zero or a valid Digest.  It works either way.
    leftLeafCtrlMap[LF_NextPage] = rightLeafDigest;
    if rightLeafDigest == 0 then
      -- This means our current leaf (leafSubRec) is already the RIGHT-MOST
      -- leaf, and so the left neighbor actually becomes the new RIGHT-MOST
      -- leaf.  Open the left neighbor and set her appropriately.
      ldtMap[LS.RightLeafDigest] = leftLeafDigest;
    end
    leftLeafSubRec[LSR_CTRL_BIN] = leftLeafCtrlMap;
    ldt_common.updateSubRec(src, leftLeafSubRec);
    leafSummary(leftLeafSubRec);
  end


  return 0;
end -- adjustLeafPointersAfterDelete()

-- ======================================================================
-- createSearchPath: Create and initialize a search path structure so
-- that we can fill it in during our tree search.
-- Parms:
-- (*) ldtMap: topRec map that holds all of the control values
-- ======================================================================
local function createSearchPath( ldtMap )
  local sp = {};
  sp.LevelCount   = 0;       -- Number of levels in the search path.
  sp.RecList      = {};
  sp.DigestList   = {};
  sp.PositionList = {};
  sp.HasRoom      = {};
  sp.FoundList    = {}; 
  return sp;
end -- createSearchPath()

local function printSearchPath ( sp )
  local mysp = map();
  for k,v in pairs (sp) do
    if (k ~= "LevelCount") then
      local l = list();
      for i = 1, sp.LevelCount + 1 do
         list.append(l, v[i])
      end 
      mysp[k] = l;
    end
  end
  info("Sp = %s", tostring(mysp));
end

-- ======================================================================
-- updateSearchPath:
-- Add one more entry to the search path thru the B+ Tree.
-- We Rememeber the path that we took during the search
-- so that we can retrace our steps if we need to update the rest of the
-- tree after an insert or delete (although, it's unlikely that we'll do
-- any significant tree change after a delete).
-- Parms:
-- (*) SearchPath: a map that holds all of the secrets
-- (*) propMap: The Property Map (tells what TYPE this record is)
-- (*) ldtMap: Main LDT Control structure
-- (*) nodeSubRec: a subrec
-- (*) resultMap: Results from the Object or Key List Search
-- (*) keyCount: Number of keys in the list
-- ======================================================================
local function
updateSearchPath(sp, ldtMap, nodeSubRec, resultMap, keyCount, totValSize)
  local meth = "updateSearchPath()";

  local levelCount = sp.LevelCount + 1;
  local nodeRecordDigest = record.digest( nodeSubRec );
  sp.LevelCount = levelCount;

  sp.RecList[levelCount]      = nodeSubRec;
  sp.DigestList[levelCount]   = nodeRecordDigest;
  sp.PositionList[levelCount] = resultMap.Position;
  sp.FoundList[levelCount]    = resultMap.Found;
  -- Depending on the Tree Node (Root, Inner, Leaf), we might have different
  -- maximum values.  So, figure out the max, and then figure out if we've
  -- reached it for this node.
  if (totValSize > G_PageSize) then 
    sp.HasRoom[levelCount] = false; 
  else
    sp.HasRoom[levelCount] = true;
  end

  return 0;
end -- updateSearchPath()

local function addResult(ldtMap, object, resultList, keyList) 
  local filterResult = object;
  if (G_Filter ~= nil) then
    filterResult = G_Filter( object, G_FunctionArgs );
  end
  if (filterResult ~= nil) then
    if (keyList ~= nil) then
      list.append( keyList, getKeyValue(object));
    end
    list.append( resultList, filterResult);
    return 1;
  end
  return 0;
end

-- ======================================================================
-- doListScan(): Scan a List.  ALL (if count == 0) or count items.
-- ======================================================================
-- Process the contents of the list, applying any UnTransform function,
-- and appending the results to the resultList.
--
-- Parms:
-- (*) objectList
-- (*) ldtMap:
-- (*) resultList:
-- (*) count: Scan up to this amount, or ALL if zero
-- (*) leftScan: true means Scan is left->right, false is right->left
-- Return:
-- Success:  Return the number of items read.
-- Error:    Call error() function.
-- ======================================================================
local function doListScan( objectList, ldtMap, resultList, keyList, count, leftScan)
  local meth = "doListScan()";

  local storeObject; -- the transformed User Object (what's stored).
  local liveObject; -- the untransformed storeObject.

  local readAmount;
  local listSize = #objectList;
  if count ~= nil and count > 0 then
    if count > listSize then
      readAmount = listSize;
    else
      readAmount = count;
    end
  else
    readAmount = listSize;
  end

  local start;
  local finish;
  local incr;
  if leftScan then
    start = 1;
    finish = readAmount;
    incr = 1;
  else
    start = listSize;
    finish = listSize - readAmount + 1;
    incr = -1;
  end
  local foundCount = 0;

  for i = start, finish, incr do
    if (addResult(ldtMap, objectList[i], resultList, keyList) ~= 0) then
       foundCount = foundCount + 1;
    end
  end -- for each item in the list

  return readAmount;
end -- keyList, doListScan()


-- ======================================================================
-- leftListScan(): Scan a List left to right.
-- Scan ALL (if count == 0) or count items.
-- ======================================================================
-- Process the contents of the list, applying any UnTransform function,
-- and appending the results to the resultList.
--
-- Parms:
-- (*) objectList
-- (*) ldtMap:
-- (*) resultList:
-- (*) count: Scan up to this amount, or ALL if zero
-- Return:
-- Success:  Return the number of items read.
-- Error:    Call error() function.
-- ======================================================================
local function leftListScan( objectList, ldtMap, resultList, keyList, count )
  return doListScan( objectList, ldtMap, resultList, keyList, count, true);
end -- leftListScan()

-- ======================================================================
-- rightListScan(): Scan a List right to left (from the end towards the front).
-- Scan ALL (if count == 0) or count items.
-- ======================================================================
-- Process the contents of the list, applying any UnTransform function,
-- and appending the results to the resultList.
--
-- Parms:
-- (*) objectList
-- (*) ldtMap:
-- (*) resultList:
-- (*) count: Scan up to this amount, or ALL if zero
-- Return:
-- Success:  Return the number of items read.
-- Error:    Call error() function.
-- ======================================================================
local function rightListScan( objectList, ldtMap, resultList, keyList, count )
  return doListScan( objectList, ldtMap, resultList, keyList, count, false);
end -- rightListScan()

-- ======================================================================
-- fullListScan(): Scan a List.  ALL of the list.
-- ======================================================================
-- Process the FULL contents of the list, and appending the results to the resultList.
--
-- Parms:
-- (*) objectList
-- (*) ldtMap:
-- (*) resultList:
-- Return: OK if all is well, otherwise call error().
-- ======================================================================
local function fullListScan( objectList, ldtMap, resultList )
  local meth = "fullListScan()";

  local storeObject; 
  local liveObject; 

  local listSize = list.size( objectList );
  for i = 1, listSize do
    liveObject = objectList[i];
    list.append(resultList, liveObject);
  end -- for each item in the list
  return 0;
end -- fullListScan()

-- ======================================================================
-- listScan(): Scan a List
-- ======================================================================
-- Whether this list came from the Leaf or the Compact List, we'll search
-- thru it and look for matching items -- applying the FILTER on all objects
-- that match the key.
--
-- Parms:
-- (*) objectList
-- (*) startPosition:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchKey:
-- (*) flag: Termination criteria: key ~= val or key > val
-- Return: A, B, where A is the instruction and B is the return code
-- A: Instruction: 0 (SCAN.DONE==stop), 1 (SCAN.CONTINUE==continue scanning)
-- B: Error Code: B==0 ok.   B < 0 Error.
-- ======================================================================
local function
listScan(objectList, startPosition, ldtMap, resultList, keyList, searchKey, count, flag)
  local meth = "listScan()";

  -- Start at the specified location, then scan from there.  For every
  -- element that matches, add it to the resultList.
  local compareResult = flag;
  local uniqueKey = ldtMap[LS.KeyUnique]; -- AS_TRUE or AS_FALSE.
  local scanStatus = SCAN.CONTINUE;
  local storeObject; 
  local liveObject; 

  -- Later: Maybe .. Split the loop search into two -- atomic and map objects
  local listSize = list.size( objectList );
  -- We expect that the FIRST compare (at location "start") should be
  -- equal, and then potentially some number of objects after that (assuming
  -- it's NOT a unique key).  If unique, then we will just jump out on the
  -- next compare.
  for i = startPosition, listSize, 1 do
    liveObject = objectList[i];
 
    if (searchKey ~= nil) then
      compareResult = keyCompare(searchKey, getKeyValue(liveObject));
      if compareResult == CR.ERROR then
        info("[WARNING]<%s:%s> Compare Error", MOD, meth );
        return 0, CR.ERROR; -- error result.
      end
    end

    if (count ~= nil and count == 0) then
      break;
    end

    if((compareResult == CR.EQUAL)or(compareResult == flag)) then
      -- This one qualifies -- save it in result -- if it passes the filter.
      if (addResult(ldtMap, liveObject, resultList, keyList) ~= 0) then
        if (count ~= nil) then
           count = count - 1;
        end
      end

      -- If we're doing a RANGE scan, then we don't want to jump out, but
      -- if we're doing just a VALUE search (and it's unique), then we're 
      -- done and it's time to leave.
      if(uniqueKey == AS_TRUE and searchKey ~= nil and flag == CR.EQUAL) then
        scanStatus = SCAN.DONE;
        break;
      end
    else
      -- First non-equals (or non-range end) means we're done.
      scanStatus = SCAN.DONE;
      break;
    end
  end -- for each item from startPosition to end

  local resultA = scanStatus;
  local resultB = ERR.OK; -- if we got this far, we're ok.

  return resultA, resultB;
end -- listScan()

-- ======================================================================
-- fullScanLeaf():
-- ======================================================================
-- Scan ALL of a Leaf Node, and append the results in the resultList.
-- Parms:
-- (*) topRec: 
-- (*) leafSubRec:
-- (*) ldtMap:
-- (*) resultList:
-- Return:
-- 0=for OK
-- Call error() for problems
-- ======================================================================
local function fullScanLeaf(topRec, leafSubRec, ldtMap, resultList)
  local meth = "fullScanLeaf()";
  local objectList = leafSubRec[LSR_LIST_BIN];
  fullListScan( objectList, ldtMap, resultList );
  return 0;
end -- fullScanLeaf()

-- ======================================================================
-- rightScanLeaf():
-- ======================================================================
-- Scan ALL of a Leaf Node from right to left, or COUNT if it is specified,
-- then and append the results in the resultList.
-- Parms:
-- (*) topRec: 
-- (*) leafSubRec:
-- (*) ldtMap:
-- (*) resultList:
-- (*) count: Number of items (or bytes) to read
-- Return:
-- Success:  Number of Items Read
-- Error:    error() function
-- ======================================================================
local function rightScanLeaf(topRec, leafSubRec, ldtMap, resultList, keyList, count)
  local meth = "rightScanLeaf()";
  local numRead = 0;
  local objectList = leafSubRec[LSR_LIST_BIN];
  numRead = rightListScan( objectList, ldtMap, resultList, keyList, count );
  return numRead;
end -- rightScanLeaf()


-- ======================================================================
-- leftScanLeaf():
-- ======================================================================
-- Scan ALL of a Leaf Node, or COUNT if it is specified, then
-- and append the results in the resultList.
-- Parms:
-- (*) topRec: 
-- (*) leafSubRec:
-- (*) ldtMap:
-- (*) resultList:
-- (*) count: Number of items (or bytes) to read
-- Return:
-- Success:  Number of Items Read
-- Error:    error() function
-- ======================================================================
local function leftScanLeaf(topRec, leafSubRec, ldtMap, resultList, keyList, count)
  local meth = "leftScanLeaf()";
  local numRead = 0;
  local objectList = leafSubRec[LSR_LIST_BIN];
  numRead = leftListScan( objectList, ldtMap, resultList, keyList, count );
  return numRead;
end -- leftScanLeaf()

-- ======================================================================
-- scanLeaf(): Scan a Leaf Node, gathering up all of the the matching
-- value(s) in the leaf node(s).
-- ======================================================================
-- Once we've searched a B+ Tree and found "The Place", then we have the
-- option of Scanning for values, Inserting new objects or deleting existing
-- objects.  This is the function for gathering up one or more matching
-- values from the leaf node(s) and putting them in the result list.
-- Notice that if there are a LOT Of values that match the search value,
-- then we might read a lot of leaf nodes.
--
-- Leaf Node Structure:
-- (*) TopRec digest
-- (*) Parent rec digest
-- (*) This Rec digest
-- (*) NEXT Leaf
-- (*) PREV Leaf
-- (*) Min value is implicitly index 1,
-- (*) Max value is implicitly at index (size of list)
-- (*) Beginning of last value
-- Parms:
-- (*) topRec: 
-- (*) leafSubRec:
-- (*) startPosition:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchKey:
-- (*) flag:
-- Return: A, B, where A is the instruction and B is the return code
-- A: Instruction: 0 (stop), 1 (continue scanning)
-- B: Error Code: B==0 ok.   B < 0 Error.
-- ======================================================================
-- NOTE: Need to pass in leaf Rec and Start Position -- because the
-- searchPath will be WRONG if we continue the search on a second page.
local function scanLeaf(topRec, leafSubRec, startPosition, ldtMap, resultList,
                          keyList, searchKey, count, flag)
  local meth = "scanLeaf()";
  local rc = 0;
  -- Linear scan of the Leaf Node (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  -- local uniqueKey = ldtMap[LS.KeyUnique]; -- AS_TRUE or AS_FALSE;
  local scanStatus = SCAN.CONTINUE;
  local resultA = 0;
  local resultB = 0;

  -- Do the List page mode search here
  -- Later: Split the loop search into two -- atomic and map objects
  local objectList = leafSubRec[LSR_LIST_BIN];
  resultA, resultB = listScan(objectList, startPosition, ldtMap,
                resultList, keyList, searchKey, count, flag);
  return resultA, resultB;
end -- scanLeaf()

-- ======================================================================
-- treeSearch()
-- ======================================================================
-- Search the tree (start with the root and move down). 
-- Remember the search path from root to leaf (and positions in each
-- node) so that insert, Scan and Delete can use this to set their
-- starting positions.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecs
-- (*) topRec: The top level Aerospike Record
-- (*) sp: searchPath: A list of maps that describe each level searched
-- (*) ldtMap: 
-- (*) searchKey: If null, compares LESS THAN everything
-- Return: ST.FOUND(0) or ST.NOT_FOUND(-1)
-- And, implicitly, the updated searchPath Object.
-- ======================================================================
local function treeSearch( src, topRec, sp, ldtCtrl, searchKey, newVal)
  local meth = "treeSearch()";
  local rc = 0;

  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local treeLevels = ldtMap[LS.TreeLevel];

  -- Start the loop with the special Root, then drop into each successive
  -- inner node level until we get to a LEAF NODE.  We search the leaf node
  -- differently than the inner (and root) nodes, since they have OBJECTS
  -- and not keys.  To search a leaf we must compute the key (from the object)
  -- before we do the compare.
  local keyList = ldtMap[LS.RootKeyList];
  local keyCount = list.size( keyList );
  local objectList = nil;
  local objectCount = 0;
  local objectSize  = 0;
  local digestList = ldtMap[LS.RootDigestList];
  local digestListSize = 0;
  local keyListSize = 0;
  if (digestList ~= nil) then 
    digestListSize = ldt_common.getValSize(digestList);
  end
  if (keyList ~= nil) then
    keyListSize = ldt_common.getValSize(keyList);
  end
  local newValSize = ldt_common.getValSize(newVal);
  local newKeySize = ldt_common.getValSize(searchKey);
  local position = 0;
  local nodeRec = topRec;
  local nodeCtrlMap;
  local resultMap;
  local digestString;

  for i = 1, treeLevels, 1 do
    if (i < treeLevels) then
      -- It's a root or node search -- so search the keys
      resultMap = searchKeyList(ldtMap, keyList, searchKey);
      if (resultMap.Status < 0 or resultMap.Position <= 0) then
        info("[ERROR]<%s:%s> searchKeyList Problem: ResMap(%s)", MOD, meth,
          tostring(resultMap));
        error( ldte.ERR_INTERNAL );
      end
      updateSearchPath(sp, ldtMap, nodeRec, resultMap, keyCount, digestListSize + keyListSize + newKeySize);
      position = resultMap.Position;

      -- Get ready for the next iteration.  If the next level is an inner node,
      -- then populate our keyList and nodeCtrlMap.
      -- If the next level is a leaf, then populate our ObjectList and LeafMap.
      -- Remember to get the STRING version of the digest in order to
      -- call "open_subrec()" on it.
      if (#digestList == 0) then
        break; 
      end
      digestString = tostring(digestList[position]);
      -- NOTE: we're looking at the NEXT level (tl - 1) and we must be LESS
      -- than that to be an inner node.
      if( i < (treeLevels - 1) ) then
        -- Next Node is an Inner Node. 
        nodeRec = ldt_common.openSubRec( src, topRec, digestString, sp );
        keyList = nodeRec[NSR_KEY_LIST_BIN];
        keyCount = list.size( keyList );
        digestList = nodeRec[NSR_DIGEST_BIN]; 
        if (digestList ~= nil) then 
          digestListSize = ldt_common.getValSize(digestList);
        end
        if (keyList ~= nil) then
          keyListSize = ldt_common.getValSize(keyList);
        end
      else
        -- Next Node is a Leaf
        nodeRec = ldt_common.openSubRec( src, topRec, digestString, sp );
        objectList = nodeRec[LSR_LIST_BIN];
        objectCount = list.size( objectList );
        if (objectList ~= nil) then
          objectSize   = ldt_common.getValSize(objectList);
        end
      end
    else
      -- It's a leaf search -- so search the objects.  Note that objectList
      -- and objectCount were set on the previous loop iteration.
      resultMap = searchObjectList( ldtMap, objectList, searchKey );
      if (resultMap.Status == 0) then
        updateSearchPath(sp, ldtMap, nodeRec,
                          resultMap, objectCount, objectSize + newValSize);
      end
      position = resultMap.Position;
    end -- if node else leaf.
  end -- end for each tree level

  if (resultMap ~= nil and resultMap.Status == 0 and resultMap.Found) then
    position = resultMap.Position;
  else
    position = 0;
  end

  if position > 0 then
    rc = ST.FOUND;
  else
    rc = ST.NOT_FOUND;
  end

  return rc;
end -- treeSearch()

-- ======================================================================
-- Populate this leaf after a leaf split.
-- Parms:
-- (*) newLeafSubRec
-- (*) objectList
-- ======================================================================
local function populateLeaf( src, leafSubRec, objectList )
  local meth = "populateLeaf()";

  local leafCtrlMap    = leafSubRec[LSR_CTRL_BIN];
  leafSubRec[LSR_LIST_BIN] = objectList;
  local count = list.size( objectList );
  leafCtrlMap[LF_ListEntryCount] = count;

  leafSubRec[LSR_CTRL_BIN] = leafCtrlMap;
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leafSubRec );
  return 0;
end -- populateLeaf()

-- ======================================================================
-- leafUpdate()
-- Use the search position to mark the location where we will OVERWRITE
-- the value.
--
-- Parms:
-- (*) src: Sub-Rec Context
-- (*) topRec: Primary Record
-- (*) leafSubRec: the leaf subrecord
-- (*) ldtMap: LDT Control: needed for key type and storage mode
-- (*) newValue: Object to be inserted.
-- (*) position: If non-zero, then it's where we insert. Otherwise, we search
-- ======================================================================
local function
leafUpdate(src, topRec, leafSubRec, ldtMap, newValue, position)
  local meth = "leafUpdate()";
  local rc = 0;

  local objectList = leafSubRec[LSR_LIST_BIN];

  -- Unlike Insert, for Update we must point at a valid CURRENT object.
  -- So, position must be within the range of the list size.
  local listSize = list.size( objectList );
  if position >= 1 and position <= listSize then
    objectList[position] = newValue;
  else
    warn("[WARNING]<%s:%s> INVALID POSITION(%d) for List Size(%d)", MOD, meth,
    position, listSize);
    error(ldte.ERR_INTERNAL);
  end

  -- Notice that we do NOT update any counters. We just overwrote.

  leafSubRec[LSR_LIST_BIN] = objectList;
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leafSubRec );
  return rc;
end -- leafUpdate()

-- ======================================================================
-- leafInsert()
-- Use the search position to mark the location where we have to make
-- room for the new value.
-- If we're at the end, we just append to the list.
-- Parms:
-- (*) src: Sub-Rec Context
-- (*) topRec: Primary Record
-- (*) leafSubRec: the leaf subrecord
-- (*) ldtMap: LDT Control: needed for key type and storage mode
-- (*) newKey: Search Key for newValue
-- (*) newValue: Object to be inserted.
-- (*) position: If non-zero, then it's where we insert. Otherwise, we search
-- ======================================================================
local function
leafInsert(src, topRec, leafSubRec, ldtMap, newKey, newValue, position)
  local meth = "leafInsert()";
  local rc = 0;

  local objectList = leafSubRec[LSR_LIST_BIN];
  local leafCtrlMap =  leafSubRec[LSR_CTRL_BIN];

  -- If we're given an unitialized position, then do the search.
  if (position == 0) then
    local resultMap = searchObjectList( ldtMap, objectList, newKey );
    position = resultMap.Position;
  end

  -- If we've done the search and failed, report the error.
  if (position <= 0) then
    info("[ERROR]<%s:%s> Search Path Position is out of range(%d)",
      MOD, meth, position);
    error( ldte.ERR_INTERNAL );
  end

  -- Move values around, if necessary, to put newValue in a "position"
  ldt_common.listInsert( objectList, newValue, position );

  -- Update Counters
  local itemCount = leafCtrlMap[LF_ListEntryCount];
  leafCtrlMap[LF_ListEntryCount] = itemCount + 1;

  leafSubRec[LSR_LIST_BIN] = objectList;
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leafSubRec );
  return rc;
end -- leafInsert()

-- ======================================================================
-- getNodeSplitPosition()
-- Find the right place to split the B+ Tree Inner Node (or Root)
-- TODO: @TOBY: Maybe find a more optimal split position
-- Right now this is a simple arithmethic computation (split the leaf in
-- half).  This could change to split at a more convenient location in the
-- leaf, especially if duplicates are involved.  However, that presents
-- other problems, so we're doing it the easy way at the moment.
-- Parms:
-- (*) ldtMap: main control map
-- (*) keyList: the key list in the node
-- (*) nodePosition: the place in the key list for the new insert
-- (*) newKey: The new value to be inserted
-- ======================================================================
local function getNodeSplitPosition( ldtMap, keyList, nodePosition, newKey )
  local meth = "getNodeSplitPosition()";
  -- This is only an approximization
  local listSize = list.size( keyList );
  local result = (listSize * 2) / 3 ; -- beginning of 2nd half, or middle
  return result;
end -- getNodeSplitPosition

-- ======================================================================
-- getLeafSplitPosition()
-- Find the right place to split the B+ Tree Leaf
-- TODO: @TOBY: Maybe find a more optimal split position
-- Right now this is a simple arithmethic computation (split the leaf in
-- half).  This could change to split at a more convenient location in the
-- leaf, especially if duplicates are involved.  However, that presents
-- other problems, so we're doing it the easy way at the moment.
-- Parms:
-- (*) ldtMap: main control map
-- (*) objList: the object list in the leaf
-- (*) leafPosition: the place in the obj list for the new insert
-- (*) newValue: The new value to be inserted
-- ======================================================================
local function getLeafSplitPosition( ldtMap, objList, leafPosition, newValue )
  -- local listSize = list.size( objList );
  -- local result = math.floor(listSize / 2) + 1; -- beginning of 2nd half, or middle
  return leafPosition; --result;
end -- getLeafSplitPosition

-- ======================================================================
-- nodeInsert()
-- Insert a new key,digest pair into the node.  We pass in the actual
-- lists, not the nodeRec, so that we can treat Nodes and the Root in
-- the same way.  Thus, it is up to the caller to update the node (or root)
-- information, other than the list update, which is what we do here.
-- Parms:
-- (*) ldtMap:
-- (*) keyList:
-- (*) digestList:
-- (*) key:
-- (*) digest:
-- (*) position:
-- ======================================================================
local function nodeInsert( ldtMap, keyList, digestList, key, digest, position )

  if (position == 0) then
    local resultMap = searchKeyList( ldtMap, keyList, key );
    position = resultMap.Position;
  end

  ldt_common.listInsert(keyList, key, position);
  if (G_Prev) then
    ldt_common.listInsert(digestList, digest, position);
  else
    ldt_common.listInsert(digestList, digest, position + 1);
  end
end -- nodeInsert()

-- ======================================================================
-- Populate this inner node after a child split.
-- Parms:
-- (*) nodeSubRec
-- (*) keyList
-- (*) digestList
-- ======================================================================
local function populateNode( nodeSubRec, keyList, digestList)
  local nodeItemCount = list.size( keyList );
  nodeSubRec[NSR_KEY_LIST_BIN] = keyList;
  nodeSubRec[NSR_DIGEST_BIN] = digestList;

  local nodeCtrlMap = nodeSubRec[NSR_CTRL_BIN];
  nodeCtrlMap[ND_ListEntryCount] = nodeItemCount;
  nodeSubRec[NSR_CTRL_BIN] = nodeCtrlMap;
end -- populateNode()

-- ======================================================================
-- Create a new Inner Node Page and initialize it.
-- ======================================================================
-- createNodeRec( Interior Tree Nodes )
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
--
--    >>>>>>>>>>>>>12345678901234<<<<<< (14 char limit for Bin Names) 
-- (1) nodeSubRec['NsrControlBin']: The control Map (defined here)
-- (2) nodeSubRec['NsrKeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeSubRec['NsrBinaryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeSubRec['NsrDigestBin']: The Data Entry List (when in list mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,4.
--
-- NOTES:
-- (1) For the Digest Bin -- we'll be in LIST MODE for debugging, but
--     in BINARY mode for production.
-- (2) For the Digests (when we're in binary mode), we could potentially
-- save some space by NOT storing the Lock bits and the Partition Bits
-- since we force all of those to be the same,
-- we know they are all identical to the top record.  So, that would save
-- us 4 bytes PER DIGEST -- which adds up for 50 to 100 entries.
-- ======================================================================
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtCtrl: Main LDT Control Structure
-- Contents of a Node Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) NSR_CTRL_BIN:    Main Node Control structure
-- (3) NSR_KEY_LIST_BIN: Key List goes here
-- (4) NSR_DIGEST_BIN: Digest List (or packed binary) goes here
-- (5) NSR_BINARY_BIN:  Packed Binary Array (if used) goes here
-- ======================================================================
local function createNodeRec( src, topRec, ldtCtrl )
  local meth = "createNodeRec()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Create the Aerospike Sub-Record, initialize the Bins (Ctrl, List).
  -- The createSubRec() handles the record type and the SRC.
  -- It also kicks out with an error if something goes wrong.
  local nodeSubRec = ldt_common.createSubRec( src, topRec, ldtCtrl, RT.SUB );
  local nodePropMap = nodeSubRec[SUBREC_PROP_BIN];
  local nodeCtrlMap = map();

  -- Notes:
  -- (1) Item Count is implicitly the KeyList size
  -- (2) All Max Limits, Key sizes and Obj sizes are in the root map
  nodeCtrlMap[ND_ListEntryCount] = 0;  -- Current # of entries in the node list

  -- Store the new maps in the record.
  nodeSubRec[SUBREC_PROP_BIN] = nodePropMap;
  nodeSubRec[NSR_CTRL_BIN]    = nodeCtrlMap;
  nodeSubRec[NSR_KEY_LIST_BIN] = list(); -- Holds the keys
  nodeSubRec[NSR_DIGEST_BIN] = list(); -- Holds the Digests -- the Rec Ptrs


  -- NOTE: The SubRec business is Handled by subRecCreate().
  -- Also, If we had BINARY MODE working for inner nodes, we would initialize
  -- the Key BYTE ARRAY here.  However, the real savings would be in the
  -- leaves, so it may not be much of an advantage to use binary mode in nodes.

  return nodeSubRec;
end -- createNodeRec()

local function getLeftRight(splitList, splitPosition)
    if (splitPosition == 0) then
	return list(), splitList;
    end

    local leftLeafList  =  list.take( splitList, splitPosition );
    local rightLeafList =  list.drop( splitList, splitPosition );
    return leftLeafList, rightLeafList; 
end


-- ======================================================================
-- splitRootInsert()
-- Split this ROOT node, because after a leaf split and the upward key
-- propagation, there's no room in the ROOT for the additional key.
-- Root Split is different any other node split for several reasons:
-- (1) The Root Key and Digests Lists are part of the control map.
-- (2) The Root stays the root.  We create two new children (inner nodes)
--     that become a new level in the tree.
-- Parms:
-- (*) src: SubRec Context (for looking up open subrecs)
-- (*) topRec:
-- (*) sp: SearchPath (from the initial search)
-- (*) ldtCtrl:
-- (*) key:
-- (*) digest:
-- ======================================================================
local function splitRootInsert( src, topRec, sp, ldtCtrl, iKeyList, iDigestList)
  local meth = "splitRootInsert()";
  local rc = 0;
  

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  local rootLevel = 1;
  local rootPosition = sp.PositionList[rootLevel];

  local keyList = ldtMap[LS.RootKeyList];
  local digestList = ldtMap[LS.RootDigestList];

  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition =
      getNodeSplitPosition( ldtMap, keyList, rootPosition, nil) --iKeyList[1] );
  local splitKey = keyList[splitPosition];

    -- Splitting a node works as follows.  The node is split into a left
    -- piece, a right piece, and a center value that is propagated up to
    -- the parent (in this case, root) node.
    --              +---+---+---+---+---+
    -- KeyList      |111|222|333|444|555|
    --              +---+---+---+---+---+
    -- DigestList   A   B   C   D   E   F
    --
    --                      +---+
    -- New Parent Element   |333|
    --                      +---+
    --                     /X   Y\ 
    --              +---+---+   +---+---+
    -- KeyList Nd(x)|111|222|   |444|555|Nd(y)
    --              +---+---+   +---+---+
    -- DigestList   A   B   C   D   E   F
    --
  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- will let us split the current Root node list into two node lists.
  -- We propagate up the split key (the new root value) and the two
  -- new inner node digests.  Remember that the Key List is ONE CELL
  -- shorter than the DigestList.
  local leftKeyList  = list.take( keyList, splitPosition - 1 );
  local rightKeyList = list.drop( keyList, splitPosition  );
  if (leftKeyList == nil) then
    leftKeyList = list();
  end
  if (rightKeyList == nil) then
    rightKeyList = list();
  end

  local leftDigestList, rightDigestList = getLeftRight( digestList, splitPosition );

  -- Create two new Child Inner Nodes -- that will be the new Level 2 of the
  -- tree.  The root gets One Key and Two Digests.
  local leftNodeRec  = createNodeRec( src, topRec, ldtCtrl );
  local rightNodeRec = createNodeRec( src, topRec, ldtCtrl );

  local leftNodeDigest  = record.digest( leftNodeRec );
  local rightNodeDigest = record.digest( rightNodeRec );

  -- This is a different order than the splitLeafInsert, but before we
  -- populate the new child nodes with their new lists, do the insert of
  -- the new key/digest value now.
  -- Figure out WHICH of the two nodes that will get the new key and
  -- digest. Insert the new value.
  -- Compare against the SplitKey -- if less, insert into the left node,
  -- and otherwise insert into the right node.
  if (iKeyList) then
    for i = 1, #iKeyList do
      local compareResult = keyCompare( iKeyList[i], splitKey );
      if (compareResult == CR.LESS_THAN) then
        -- We choose the LEFT Node -- but we must search for the location
        nodeInsert( ldtMap, leftKeyList, leftDigestList, iKeyList[i], iDigestList[i], 0 );
      elseif (compareResult >= CR.EQUAL) then -- this works for EQ or GT
        -- We choose the RIGHT (new) Node -- but we must search for the location
        nodeInsert( ldtMap, rightKeyList, rightDigestList, iKeyList[i], iDigestList[i], 0 );
      else
        -- We got some sort of compare error.
        info("[ERROR]<%s:%s> Compare Error: CR(%d)", MOD, meth, compareResult );
        error( ldte.ERR_INTERNAL );
      end
    end
  end

  -- Populate the new nodes with their Key and Digest Lists
  populateNode( leftNodeRec, leftKeyList, leftDigestList);
  populateNode( rightNodeRec, rightKeyList, rightDigestList);
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leftNodeRec );
  ldt_common.updateSubRec( src, rightNodeRec );

  -- Replace the Root Information with just the split-key and the
  -- two new child node digests (much like first Tree Insert).
  local keyList = list();
  list.append( keyList, splitKey );
  local digestList = list();
  list.append( digestList, leftNodeDigest );
  list.append( digestList, rightNodeDigest );

  -- The new tree is now one level taller
  local treeLevel = ldtMap[LS.TreeLevel];
  ldtMap[LS.TreeLevel] = treeLevel + 1;

  -- Update the Main control map with the new root lists.
  ldtMap[LS.RootKeyList] = keyList;
  ldtMap[LS.RootDigestList] = digestList;

  return rc;
end -- splitRootInsert()

-- ======================================================================
-- splitNodeInsert()
-- Split this parent node, because after a leaf split and the upward key
-- propagation, there's no room in THIS node for the additional key.
-- Special case is "Root Split" -- and that's handled by the function above.
-- Just like the leaf split situation -- we have to be careful about 
-- duplicates.  We don't want to split in the middle of a set of duplicates,
-- if we can possibly avoid it.  If the WHOLE node is the same key value,
-- then we can't avoid it.
-- Parms:
-- (*) src: SubRec Context (for looking up open subrecs)
-- (*) topRec:
-- (*) sp: SearchPath (from the initial search)
-- (*) ldtCtrl:
-- (*) key:
-- (*) digest:
-- (*) level:
-- ======================================================================
local function splitNodeInsert( src, topRec, sp, ldtCtrl, iKeyList, iDigestList, level )
  local meth = "splitNodeInsert()";
  local rc = 0;
  
  if (level == 1) then
    -- Special Split -- Root is handled differently.
    rc = splitRootInsert( src, topRec, sp, ldtCtrl, iKeyList, iDigestList);
  else
    -- Ok -- "Regular" Inner Node Split Insert.
    -- We will split this inner node, use the existing node as the new
    -- "rightNode" and the newly created node as the new "LeftNode".
    -- We will insert the "splitKey" and the new leftNode in the parent.
    -- And, if the parent has no room, we'll recursively call this function
    -- to propagate the insert up the tree.  ((I hope recursion doesn't
    -- blow up the Lua environment!!!!  :-) ).

    -- Extract the property map and control map from the ldt bin list.
    local propMap = ldtCtrl[LDT_PROP_MAP];
    local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
    local ldtBinName = propMap[PM.BinName];

    local nodePosition = sp.PositionList[level];
    local nodeSubRecDigest = sp.DigestList[level];
    local nodeSubRec = sp.RecList[level];

    -- Open the Node get the map, Key and Digest Data
    local nodePropMap    = nodeSubRec[SUBREC_PROP_BIN];
    local nodeCtrlMap    = nodeSubRec[NSR_CTRL_BIN];
    local keyList    = nodeSubRec[NSR_KEY_LIST_BIN];
    local digestList = nodeSubRec[NSR_DIGEST_BIN];

    -- Calculate the split position and the key to propagate up to parent.
    local splitPosition =
        getNodeSplitPosition( ldtMap, keyList, nodePosition, nil); --iKeyList[1] );
    -- We already have a key list -- don't need to "extract".
    local splitKey = keyList[splitPosition];

    -- Splitting a node works as follows.  The node is split into a left
    -- piece, a right piece, and a center value that is propagated up to
    -- the parent node.
    --              +---+---+---+---+---+
    -- KeyList      |111|222|333|444|555|
    --              +---+---+---+---+---+
    -- DigestList   A   B   C   D   E   F
    --
    --                      +---+
    -- New Parent Element   |333|
    --                      +---+
    --                     /     \
    --              +---+---+   +---+---+
    -- KeyList      |111|222|   |444|555|
    --              +---+---+   +---+---+
    -- DigestList   A   B   C   D   E   F
    --
    -- Our List operators :
    -- (*) list.take (take the first N elements) 
    -- (*) list.drop (drop the first N elements, and keep the rest) 
    -- will let us split the current Node list into two Node lists.
    -- We will always propagate up the new Key and the NEW left page (digest)
    local leftKeyList  = list.take( keyList, splitPosition - 1);
    local rightKeyList = list.drop( keyList, splitPosition );
    if (leftKeyList == nil) then
      leftKeyList = list();
    end
    if (rightKeyList == nil) then
      rightKeyList = list();
    end

    local leftDigestList, rightDigestList = getLeftRight( digestList, splitPosition );

    local leftNodeRec = nodeSubRec; -- our new name for the existing node
    local rightNodeRec = createNodeRec( src, topRec, ldtCtrl );
    local rightNodeDigest = record.digest( rightNodeRec );

    -- This is a different order than the splitLeafInsert, but before we
    -- populate the new child nodes with their new lists, do the insert of
    -- the new key/digest value now.
    -- Figure out WHICH of the two nodes that will get the new key and
    -- digest. Insert the new value.
    -- Compare against the SplitKey -- if less, insert into the left node,
    -- and otherwise insert into the right node.
    if (iKeyList) then
      for i = 1, #iKeyList do
        local compareResult = keyCompare( iKeyList[i], splitKey );
        if( compareResult == CR.LESS_THAN ) then
          -- We choose the LEFT Node -- but we must search for the location
          nodeInsert( ldtMap, leftKeyList, leftDigestList, iKeyList[i], iDigestList[i], 0 );
        elseif( compareResult >= CR.EQUAL  ) then -- this works for EQ or GT
          -- We choose the RIGHT (new) Node -- but we must search for the location
          nodeInsert( ldtMap, rightKeyList, rightDigestList, iKeyList[i], iDigestList[i], 0 );
        else
          -- We got some sort of compare error.
          info("[ERROR]<%s:%s> Compare Error: CR(%d)", MOD, meth, compareResult );
          error( ldte.ERR_INTERNAL );
        end
      end
    end

    -- Populate the new nodes with their Key and Digest Lists
    populateNode( leftNodeRec, leftKeyList, leftDigestList);
    populateNode( rightNodeRec, rightKeyList, rightDigestList);
  
    -- Call update to mark the SubRec as dirty, and to force the write if we
    -- are in "early update" mode. Close will happen at the end of the Lua call.
    ldt_common.updateSubRec( src, leftNodeRec );
    ldt_common.updateSubRec( src, rightNodeRec );

    
    local iKeyList = {};
    local iDigestList = {};
    iKeyList[1] = splitKey;
    iDigestList[1] = rightNodeDigest;

    insertParentNode(src, topRec, sp, ldtCtrl, iKeyList, iDigestList,
      level - 1 );
  end -- else regular (non-root) node split

  return rc;

end -- splitNodeInsert()

-- ======================================================================
-- After a leaf split or a node split, this parent node gets a new child
-- value and digest.  This node might be the root, or it might be an
-- inner node.  If we have to split this node, then we'll perform either
-- a node split or a ROOT split (ugh) and recursively call this method
-- to insert one level up.  Of course, Root split is a special case, because
-- the root node is basically ensconced inside of the LDT control map.
-- Parms:
-- (*) src: The SubRec Context (holds open subrecords).
-- (*) topRec: The main record
-- (*) sp: the searchPath structure
-- (*) ldtCtrl: the main control structure
-- (*) key: the new key to be inserted 
-- (*) digest: The new digest to be inserted
-- (*) level: The current level in searchPath of this node
-- ======================================================================
-- NOTE: This function is FORWARD-DECLARED, so it does NOT get a "local"
-- declaration here.
-- ======================================================================
function insertParentNode(src, topRec, sp, ldtCtrl, iKeyList, iDigestList, level)
  --info("Insert Into Parent %d", #iKeyList);
  local meth = "insertParentNode()";
  local rc = 0;


  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- Check the tree level.  If it's the root, we access the node data
  -- differently from a regular inner tree node.
  local keyList;
  local digestList;
  -- Cannot trust previous search. Search again !!!
  local position = 0;
  local nodeSubRec = nil;
  if( level == 1 ) then
    -- Get the control and list data from the Root Node
    keyList    = ldtMap[LS.RootKeyList];
    digestList = ldtMap[LS.RootDigestList];
  else
    -- Get the control and list data from a regular inner Tree Node
    nodeSubRec = sp.RecList[level];
    if( nodeSubRec == nil ) then
      warn("[ERROR]<%s:%s> Nil NodeRec from SearchPath. Level(%s)",
        MOD, meth, tostring(level));
      error( ldte.ERR_INTERNAL );
    end
    keyList    = nodeSubRec[NSR_KEY_LIST_BIN];
    digestList = nodeSubRec[NSR_DIGEST_BIN];
  end

  -- If there's room in this node, then this is easy.  If not, then
  -- it's a complex split and propagate.
  if (sp.HasRoom[level]) then
    -- Regular node insert
    for i = 1, #iKeyList do
      nodeInsert(ldtMap, keyList, digestList, iKeyList[i], iDigestList[i], 0);
    end
    -- If it's a node, then we have to re-assign the list to the subrec
    -- fields -- otherwise, the change may not take effect.
    if (level > 1) then
      nodeSubRec[NSR_KEY_LIST_BIN] = keyList;
      nodeSubRec[NSR_DIGEST_BIN]   = digestList;
      ldt_common.updateSubRec( src, nodeSubRec );
    end
  else
    -- key > 1/2 * writeBlockSize is not allowed. for set that would be value size
    -- Regular node insert
    for i = 1, #iKeyList do
      nodeInsert(ldtMap, keyList, digestList, iKeyList[i], iDigestList[i], 0);
    end

    -- Complex node split and propagate up to parent.  Special case is if
    -- this is a ROOT split, which is different.
    rc = splitNodeInsert( src, topRec, sp, ldtCtrl, nil, nil, level);
  end
  return rc;
end -- insertParentNode()

-- ======================================================================
-- createLeafRec()
-- ======================================================================
-- Create a new Leaf Page and initialize it.
-- NOTE: Remember that we must create an ESR when we create the first leaf
-- but that is the caller's job
-- Contents of a Leaf Record:
-- (1) SUBREC_PROP_BIN: Main record Properties go here
-- (2) LSR_CTRL_BIN:    Main Leaf Control structure
-- (3) LSR_LIST_BIN:    Object List goes here
-- 
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec: The main AS Record holding the LDT
-- (*) ldtCtrl: Main LDT Control Structure
-- (*) firstValue: If present, store this first value in the leaf.
-- (*) valueList: If present, store this value LIST in the leaf.  Note that
--     "firstValue" and "valueList" are mutually exclusive.  If BOTH are
--     non-NIL, then the valueList wins (firstValue not inserted).
--
-- (*) pd: previous (left) Leaf Digest (or 0, if not there)
-- (*) nd: next (right) Leaf Digest (or 0, if not there)
-- ======================================================================
local function createLeafRec( src, topRec, ldtCtrl, firstValue, valueList )
  local meth = "createLeafRec()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Create the Aerospike Sub-Record, initialize the Bins (Ctrl, List).
  -- The createSubRec() handles the record type and the SRC.
  -- It also kicks out with an error if something goes wrong.
  local leafSubRec = ldt_common.createSubRec( src, topRec, ldtCtrl, RT.LEAF );
  local leafPropMap = leafSubRec[SUBREC_PROP_BIN];
  local leafCtrlMap = map();

  -- Store the new maps in the record.
  -- leafSubRec[SUBREC_PROP_BIN] = leafPropMap; (Already Set)
  leafSubRec[LSR_CTRL_BIN]    = leafCtrlMap;
  local leafItemCount = 0;

  local topDigest = record.digest( topRec );
  local leafDigest = record.digest( leafSubRec );
  
  -- If we have an initial value, then enter that in our new object list.
  -- Otherwise, create an empty list.
  local objectList;
  local leafItemCount = 0;

  -- If we have a value (or list) passed in, process it.
  if ( valueList ~= nil ) then
    objectList = valueList;
    leafItemCount = list.size(valueList);
  else
    objectList = list();
    if ( firstValue ~= nil ) then
      list.append( objectList, firstValue );
      leafItemCount = 1;
    end
  end

  -- Store stats and values in the new Sub-Record
  leafSubRec[LSR_LIST_BIN] = objectList;
  leafCtrlMap[LF_ListEntryCount] = leafItemCount;
  -- Take our new structures and put them in the leaf record.
  -- Note: leafSubRec[SUBREC_PROP_BIN]  is Already Set
  leafSubRec[LSR_CTRL_BIN] = leafCtrlMap;

  ldt_common.updateSubRec( src, leafSubRec );

  -- We now have one more Leaf.  Update the count
  local leafCount = ldtMap[LS.LeafCount];
  ldtMap[LS.LeafCount] = leafCount + 1;
  
  return leafSubRec;
end -- createLeafRec()

-- ======================================================================
-- splitLeafUpdate()
-- We already know that there isn't enough room for the item, so we'll
-- have to split the leaf in order to update it.
-- Side Effect: Could cause split upto Root
-- ======================================================================
local function
splitLeafUpdate( src, topRec, sp, ldtCtrl, key, newValue )
  local meth = "splitLeafUpdate()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  local leafLevel        = sp.LevelCount;
  local leafPosition     = sp.PositionList[leafLevel];
  local leafSubRecDigest = sp.DigestList[leafLevel];
  local leafSubRec       = sp.RecList[leafLevel];

  -- Open the Leaf and look inside.
  local objectList = leafSubRec[LSR_LIST_BIN];

  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition = leafPosition;

  local iKeyList = {};
  local iDigestList = {};
  local iCount = 0;
  --info("Split @ %s for %s in %s level=%d", tostring(splitPosition), tostring(key), tostring(printKey(ldtMap, objectList)), leafLevel);

  if (#objectList == 1) then
    leafUpdate(src, topRec, leafSubRec, ldtMap, newValue, splitPosition);
    ldt_common.updateSubRec( src, leafSubRec );
  elseif (splitPosition == 1) then

    -- PK:newVal stored in current leafSubRec 
    populateLeaf( src, leafSubRec, list());
    leafInsert(src, topRec, leafSubRec, ldtMap, key, newValue, 0);

    -- minKey:2-N
    local newLeafRec = createLeafRec( src, topRec, ldtCtrl, nil );
    objectList = ldt_common.listDelete(objectList, splitPosition);
    populateLeaf( src, newLeafRec, objectList);
    iCount = iCount + 1;
    iDigestList[iCount] = record.digest( newLeafRec );
    iKeyList[iCount] = getKeyValue(objectList[1]);

    adjustLeafPointersAfterInsert(src,topRec,ldtMap, leafSubRec, newLeafRec, true);
    
    ldt_common.updateSubRec( src, newLeafRec );
    ldt_common.updateSubRec (src, leafSubRec);

  elseif (splitPosition == #objectList) then
    -- PK:(1, N)
    objectList = ldt_common.listDelete(objectList, splitPosition);
    populateLeaf (src, leafSubRec, objectList);

    -- newKey:newVal
    local newLeafRec = createLeafRec( src, topRec, ldtCtrl, nil );
    populateLeaf(src, newLeafRec, list());
    leafInsert(src, topRec, newLeafRec, ldtMap, key, newValue, 0);
    iCount = iCount + 1;
    iKeyList[iCount]   = key
    iDigestList[iCount] = record.digest( newLeafRec);

    adjustLeafPointersAfterInsert(src, topRec, ldtMap, leafSubRec, newLeafRec, true);
    ldt_common.updateSubRec( src, newLeafRec );
    ldt_common.updateSubRec( src, leafSubRec );
  else
    local newValSize    = ldt_common.getValSize(newValue);
    local oldValSize    = ldt_common.getValSize(objectList[splitPosition]);
    local pageSize      = G_PageSize;
    local leafListSize  = ldt_common.getValSize(objectList);
    if (newValSize + leafListSize - oldValSize <= pageSize) then
      leafUpdate(src, topRec, leafSubRec, ldtMap, newValue, splitPosition);
      ldt_common.updateSubRec( src, leafSubRec );
    else
      local splitKey = objectList[splitPosition];
      local leftList, rightList = getLeftRight( objectList, splitPosition - 1);
      leafListSize  = ldt_common.getValSize(rightList);

      -- PK:leftList
      populateLeaf( src, leafSubRec, leftList);
    
      if (newValSize + leafListSize - oldValSize <= pageSize) then
        -- newKey: (newVal+rightList)
        local newLeafRec  = createLeafRec( src, topRec, ldtCtrl, nil );
        populateLeaf( src, newLeafRec, rightList );
        leafUpdate(src, topRec, newLeafRec, ldtMap, newValue, 1);
        adjustLeafPointersAfterInsert(src, topRec, ldtMap, leafSubRec, newLeafRec, true);

        iCount = iCount + 1;
        iDigestList[iCount] = record.digest( newLeafRec);
        iKeyList[iCount] = getKeyValue(rightList[1]);

        ldt_common.updateSubRec( src, newLeafRec);
      else
        -- minKey:rightList
        rightList = ldt_common.listDelete(rightList, 1);
        if (rightList ~= nil)  then
          local newLeafRec  = createLeafRec(src, topRec, ldtCtrl, nil );
          populateLeaf(src, newLeafRec, rightList);
          adjustLeafPointersAfterInsert(src ,topRec, ldtMap, leafSubRec, newLeafRec, true);
          iCount = iCount + 1;
          iDigestList[iCount] = record.digest( newLeafRec);
          iKeyList[iCount]    = getKeyValue(rightList[1]);
          ldt_common.updateSubRec( src, newLeafRec);
        end
    
        -- newKey:newVal
        local newLeafRec1 = createLeafRec(src, topRec, ldtCtrl, nil );
        populateLeaf(src, newLeafRec1, list());
        leafInsert(src, topRec, newLeafRec1, ldtMap, key, newValue, 0);
        adjustLeafPointersAfterInsert(src ,topRec, ldtMap, leafSubRec, newLeafRec1, true);

        iCount = iCount + 1;
        iDigestList[iCount] = record.digest(newLeafRec1);
        iKeyList[iCount]    = key;
 
        ldt_common.updateSubRec( src, newLeafRec1);
      end
      ldt_common.updateSubRec( src, leafSubRec);
    end
  end

  if (iCount > 0) then
    insertParentNode(src, topRec, sp, ldtCtrl, iKeyList, 
      iDigestList, leafLevel - 1 );
  end

  return 0;
end -- splitLeafUpdate()

-- ======================================================================
-- splitLeafInsert()
-- We already know that there isn't enough room for the item, so we'll
-- have to split the leaf in order to insert it.
-- The searchPath position tells us the insert location in THIS leaf,
-- but, since this leaf will have to be split, it gets more complicated.
-- We split, THEN decide which leaf to use.
-- ALSO -- since we don't want to split the page in the middle of a set of
-- duplicates, we have to find the closest "key break" to the middle of
-- the page.  More thinking needed on how to handle duplicates without
-- making the page MUCH more complicated.
-- For now, we'll make the split easier and just pick the middle item,
-- but in doing that, it will make the scanning more complicated.
-- Parms:
-- (*) src: subrecContext
-- (*) topRec
-- (*) sp: searchPath
-- (*) ldtCtrl
-- (*) newKey
-- (*) newValue
-- Return:
-- ======================================================================
local function
splitLeafInsert( src, topRec, sp, ldtCtrl, newKey, newValue )
  local meth = "splitLeafInsert()";

  -- B+tree all the values <= key is left tree. Split pick up the point 
  -- where the key would get inserted and breaks list in two. In case 
  -- the value cannot be accomodated in user defined PageSize. That value
  -- becomes a individual node in itself. 
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  local leafLevel = sp.LevelCount;
  local leafPosition = sp.PositionList[leafLevel];
  local leafSubRecDigest = sp.DigestList[leafLevel];
  local leafSubRec = sp.RecList[leafLevel];

  -- Open the Leaf and look inside.
  local objectList = leafSubRec[LSR_LIST_BIN];

  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition =
      getLeafSplitPosition( ldtMap, objectList, leafPosition, newValue );

  local iKeyList = {};
  local iDigestList = {};
  local iCount = 0;

  if (#objectList == 0) then
    -- Case 1: PK:newValue
    leafInsert(src, topRec, leafSubRec, ldtMap, newKey, newValue, 0);
    ldt_common.updateSubRec(src, leafSubRec);
  elseif (splitPosition > #objectList) then
    -- Case 2:
    -- maxKey:CurList, PK:newVal  [Move current list to left side]
    local newLeafRec = createLeafRec( src, topRec, ldtCtrl, nil );
    populateLeaf(src, newLeafRec, list());
    leafInsert(src, topRec, newLeafRec, ldtMap, newKey, newValue, 0);
    iCount = iCount + 1;
    iKeyList[iCount] = newKey
    iDigestList[iCount] = record.digest(newLeafRec);
    adjustLeafPointersAfterInsert(src, topRec, ldtMap, leafSubRec, newLeafRec, true);
    ldt_common.updateSubRec(src, newLeafRec);
    ldt_common.updateSubRec(src, leafSubRec);
  elseif (splitPosition == 1) then
    -- Case 2:
    -- newKey:newval, PK:CurList [PK-1 < newKey < PK]
    local newLeafRec     = createLeafRec(src, topRec, ldtCtrl, nil);
    populateLeaf(src, newLeafRec, list()); 
    leafInsert(src, topRec, newLeafRec, ldtMap, newKey, newValue, 0);
    adjustLeafPointersAfterInsert(src, topRec, ldtMap, newLeafRec, leafSubRec, false);
    G_Prev = true;
    ldt_common.updateSubRec(src, newLeafRec);

    iCount = iCount + 1;
    iKeyList[iCount] = getKeyValue(objectList[1]);
    iDigestList[iCount] = record.digest(newLeafRec);
  else
    local splitKey  = getKeyValue( objectList[splitPosition] );
    local leftList, rightList = getLeftRight( objectList, splitPosition - 1);
    --info("Split @ %s of %s as %s for %s in %s as L(%s) R(%s)", tostring(splitPosition), tostring(#objectList), tostring(splitKey), tostring(newKey), tostring(printKey(ldtMap, objectList)), tostring(printKey(ldtMap, leftList)), tostring(printKey(ldtMap, rightList)));

    local compareResult = keyCompare( newKey, splitKey );
    local newValSize    = ldt_common.getValSize(newValue);
    local leafListSize  = ldt_common.getValSize(rightList);
    local pageSize      = G_PageSize;

    -- PK:Y 
    populateLeaf(src, leafSubRec, leftList);

    -- Cases
    -- 1: newKey:(X+newVal), PK:Y
    -- 2: maxKey:X,  newKey:newVal, PK:Y
    if (compareResult < CR.EQUAL) then
       if (newValSize + leafListSize <= pageSize) then
          --Case 3   
          --newKey: (X+newVal)
          local newLeafRec  = createLeafRec( src, topRec, ldtCtrl, nil );
          populateLeaf( src, newLeafRec, rightList );
          leafInsert(src, topRec, newLeafRec, ldtMap, newKey, newValue, 0);
          adjustLeafPointersAfterInsert(src ,topRec, ldtMap, leafSubRec, newLeafRec, true);

          iCount = iCount + 1;
          iKeyList[iCount] = newKey;
          iDigestList[iCount] = record.digest( newLeafRec);

          ldt_common.updateSubRec(src, newLeafRec);
       else

          -- newKey:newVal
          local newLeafRec1 = createLeafRec(src, topRec, ldtCtrl, nil );
          populateLeaf(src, newLeafRec1, list());
          leafInsert(src, topRec, newLeafRec1, ldtMap, newKey, newValue, 0);
          adjustLeafPointersAfterInsert(src ,topRec, ldtMap, leafSubRec, newLeafRec1, true);

          iCount = iCount + 1;
          iKeyList[iCount] = newKey;
          iDigestList[iCount] = record.digest( newLeafRec1);
 

          -- Case 2  
          -- minKey:X
          local newLeafRec  = createLeafRec(src, topRec, ldtCtrl, nil );
          populateLeaf(src, newLeafRec, rightList );
          adjustLeafPointersAfterInsert(src ,topRec, ldtMap, newLeafRec1, newLeafRec, true);

          iCount = iCount + 1;
          iKeyList[iCount] = getKeyValue( rightList[1]);
          iDigestList[iCount] = record.digest( newLeafRec);
    
          ldt_common.updateSubRec(src, newLeafRec);
          ldt_common.updateSubRec(src, newLeafRec1);
       end
       ldt_common.updateSubRec(src, leafSubRec);
    else
      -- We got some sort of goofy error.
      warn("[ERROR]<%s:%s> Compare Error(%d) %s<=%s",MOD, meth, compareResult, tostring(newKey), tostring(splitKey) );
      error( ldte.ERR_INTERNAL );
    end
  end

  if (iCount > 0) then
    insertParentNode(src, topRec, sp, ldtCtrl, iKeyList, iDigestList,
      leafLevel - 1 );
  end

  return 0;
end -- splitLeafInsert()

-- ======================================================================
-- buildNewTree( src, topRec, ldtMap, leftLeafList, splitKey, rightLeafList );
-- ======================================================================
-- Build a brand new tree -- from the contents of the Compact List.
-- This is the efficient way to construct a new tree.
-- Note that this function is assumed to take data from the Compact List.
-- It is not meant for LARGE lists, where the supplied LEFT and RIGHT lists
-- could each overflow a single leaf.
--
-- Parms:
-- (*) src: SubRecContext
-- (*) topRec
-- (*) ldtCtrl
-- (*) leftLeafList
-- (*) splitKeyValue: The new Key for the ROOT LIST.
-- (*) rightLeafList )
-- ======================================================================
local function buildNewTree( src, topRec, ldtCtrl,
                             leftLeafList, splitKeyValue, rightLeafList )
  local meth = "buildNewTree()";
  -- info ("Split %s %s %s", tostring(splitKeyValue), tostring(leftLeafList[#leftLeafList]), tostring(rightLeafList[1]));

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  -- These are set on create -- so we can use them, even though they are
  -- (or should be) empty.
  local rootKeyList = ldtMap[LS.RootKeyList];
  local rootDigestList = ldtMap[LS.RootDigestList];

  -- Create two leaves -- Left and Right. Initialize them.  Then
  -- assign our new value lists to them.
  local leftLeafRec = createLeafRec( src, topRec, ldtCtrl, nil, leftLeafList);
  local leftLeafDigest = record.digest( leftLeafRec );
  ldtMap[LS.LeftLeafDigest] = leftLeafDigest; -- Remember Left-Most Leaf

  local rightLeafRec = createLeafRec( src, topRec, ldtCtrl, nil, rightLeafList);
  local rightLeafDigest = record.digest( rightLeafRec );
  ldtMap[LS.RightLeafDigest] = rightLeafDigest; -- Remember Right-Most Leaf

  -- Our leaf pages are doubly linked -- we use digest values as page ptrs.
  setLeafPagePointers( src, leftLeafRec, 0, rightLeafDigest );
  setLeafPagePointers( src, rightLeafRec, leftLeafDigest, 0 );

  -- Build the Root Lists (key and digests)
  list.append( rootKeyList, splitKeyValue );
  list.append( rootDigestList, leftLeafDigest );
  list.append( rootDigestList, rightLeafDigest );

  ldtMap[LS.TreeLevel] = 2; -- We can do this blind, since it's special.

  -- Note: The caller will update the top record, but we need to update the
  -- subrecs here.
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leftLeafRec );
  ldt_common.updateSubRec( src, rightLeafRec );

  return 0;
end -- buildNewTree()

-- ======================================================================
-- firstTreeInsert()
-- ======================================================================
-- For the VERY FIRST INSERT, we don't need to search.  We just put the
-- first key in the root, and we allocate a first left leaf and
-- NO SEARCH KEY.   The first search key is not put into the root until
-- the the first left leaf splits.
--
-- In general, we build our tree from the Compact List, but in those
-- cases where the Objects being stored are too large to hold in the
-- compact List (even for a little bit), we start directly with
-- a tree insert.
--
-- NOTE: There's a special condition to be aware of with regard to splitting
-- leaves.  When splitting a TIMESERIES Leaf, we should not split
-- the leaf in the middle but should instead split it at the very right,
-- because all subsequent values will flow into the next leaf.
--
-- Parms:
-- (*) src: SubRecContext
-- (*) topRec
-- (*) ldtCtrl
-- (*) newValue
-- ======================================================================
local function firstTreeInsert( src, topRec, ldtCtrl, newValue )
  local meth = "firstTreeInsert()";
  -- We know that on the VERY FIRST SubRecord create, we want to create
  -- the Existence Sub Record (ESR), which will be created by createLeafRec().

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local ldtBinName = propMap[PM.BinName];

  local rootKeyList = ldtMap[LS.RootKeyList];
  local rootDigestList = ldtMap[LS.RootDigestList];
  local keyValue = getKeyValue( newValue );

  -- Create the first Leaf, and initialize it.
  -- insert our new value into the RIGHT one.
  local leftLeafRec = createLeafRec( src, topRec, ldtCtrl, newValue, nil );
  local leftLeafDigest = record.digest( leftLeafRec );
  ldtMap[LS.LeftLeafDigest] = leftLeafDigest; -- Remember Left-Most Leaf
  ldtMap[LS.RightLeafDigest] = leftLeafDigest; -- Remember Right-Most Leaf
  -- Our leaf pages are doubly linked -- we use digest values as page ptrs.
  setLeafPagePointers( src, leftLeafRec, 0, 0);

  -- Insert our very digest into the root (and no key in root keyList yet).
  list.append( rootDigestList, leftLeafDigest );

  ldtMap[LS.TreeLevel] = 2; -- We can do this blind, since it's special.

  -- NOTE: Do NOT update the ItemCount.  The caller does that.
  -- Also, the lists are part of the ldtMap, so they DO NOT need updating.
  -- ldtMap[LS.RootKeyList] = rootKeyList;
  -- ldtMap[LS.RootDigestList] = rootDigestList;

  -- Note: The caller will update the top record, but we need to update the
  -- new left child subrec here.
  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leftLeafRec );

  return 0;
end -- firstTreeInsert()

-- ======================================================================
-- treeInsert()
-- ======================================================================
-- Search the tree (start with the root and move down).  Get the spot in
-- the leaf where the insert goes.  Insert into the leaf.  Remember the
-- path on the way down, because if a leaf splits, we have to move back
-- up and potentially split the parents bottom up.
-- Parms:
-- (*) src: subrecContext: The pool of open subrecords
-- (*) topRec
-- (*) ldtCtrl
-- (*) value
-- (*) update: when true, we overwrite unique values rather than complain.
-- Return:
-- 0: All ok, Regular insert
-- 1: Ok, but we did an UPDATE, with no count increase.
-- ======================================================================
local function treeInsert (src, topRec, ldtCtrl, value, update)
  local meth = "treeInsert()";
  local rc = 0;
  
  local insertResult = 0; -- assume regular insert

  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];


  -- For the VERY FIRST INSERT, we don't need to search.
  if (ldtMap[LS.TreeLevel] == 1) then
    firstTreeInsert( src, topRec, ldtCtrl, value );
  else
    -- It's a real insert -- so, Search first, then insert
    -- Map: Path from root to leaf, with indexes
    -- The Search path is a map of values, including lists from root to leaf
    -- showing node/list states, counts, fill factors, etc.
    local key        = getKeyValue( value );
    local sp         = createSearchPath(ldtMap);
    local status     = treeSearch( src, topRec, sp, ldtCtrl, key, value );
    local leafLevel  = sp.LevelCount;
    local leafSubRec = sp.RecList[leafLevel];
    local position   = sp.PositionList[leafLevel];

    -- If FOUND, then if UNIQUE, it's either an ERROR, or we are doing
    -- an Update (overwrite in place).
    -- Otherwise, if not UNIQUE, do the insert.
    if (status == ST.FOUND and ldtMap[LS.KeyUnique] == AS_TRUE) then
      if update then
        if (sp.HasRoom[leafLevel]) then
          -- Regular Leaf Insert
          local leafSubRec = sp.RecList[leafLevel];
          local position = sp.PositionList[leafLevel];
          rc = leafUpdate(src, topRec, leafSubRec, ldtMap, value, position);
          -- Call update_subrec() to both mark the subRec as dirty, AND to write
          -- it out if we are in "early update" mode.  In general, Dirty SubRecs
          -- are also written out and closed at the end of the Lua Context.
          ldt_common.updateSubRec( src, leafSubRec );
          insertResult = 1; -- Special UPDATE (no count stats increase).
        else
          -- Split first, then update.  This split can potentially propagate all
          -- the way up the tree to the root. This is potentially a big deal.
          splitLeafUpdate( src, topRec, sp, ldtCtrl, key, value );
          insertResult = 1; -- Special UPDATE (no count stats increase).
        end
      else
        info("[User ERROR]<%s:%s> Unique Key(%s) Violation",
          MOD, meth, tostring(getKeyValue(value) ));
        error( ldte.ERR_UNIQUE_KEY );
      end
      -- End of the UPDATE case
    else -- else, do INSERT

      if (sp.HasRoom[leafLevel]) then
        -- Regular Leaf Insert
        local leafSubRec = sp.RecList[leafLevel];
        local position = sp.PositionList[leafLevel];
        -- info("Inserting %s", tostring(key));
        rc = leafInsert(src, topRec, leafSubRec, ldtMap, key, value, position);
        -- Call update_subrec() to both mark the subRec as dirty, AND to write
        -- it out if we are in "early update" mode.  In general, Dirty SubRecs
        -- are also written out and closed at the end of the Lua Context.
        ldt_common.updateSubRec( src, leafSubRec );
      else
        -- Split first, then insert.  This split can potentially propagate all
        -- the way up the tree to the root. This is potentially a big deal.
        splitLeafInsert( src, topRec, sp, ldtCtrl, key, value );
      end
    end -- Insert
  end -- end else "real" insert

  return insertResult;
end -- treeInsert

-- ======================================================================
-- getNextLeaf( src, topRec, leafSubRec, forward  )
-- Our Tree Leaves are doubly linked -- so from any leaf we can move 
-- right or left.  Get the next leaf (right neighbor) in the chain.
-- This is called primarily by scan(), so the pages should be clean.
-- ======================================================================
local function getNextLeaf( src, topRec, leafSubRec, forward)
  local meth = "getNextLeaf()";
  local leafSubRecMap = leafSubRec[LSR_CTRL_BIN];
  local nextLeafDigest = leafSubRecMap[LF_NextPage];
  if forward == false then
    nextLeafDigest = leafSubRecMap[LF_PrevPage];
  end

  local nextLeaf = nil;
  local nextLeafDigestString;

  -- Close the current leaf before opening the next one.  It should be clean,
  -- so closing is ok.
  ldt_common.closeSubRec( src, leafSubRec, false);

  if( nextLeafDigest ~= nil and nextLeafDigest ~= 0 ) then
    nextLeafDigestString = tostring( nextLeafDigest );

    nextLeaf = ldt_common.openSubRec( src, topRec, nextLeafDigestString )
    if( nextLeaf == nil ) then
      warn("[ERROR]<%s:%s> Can't Open Leaf(%s)",MOD,meth,nextLeafDigestString);
      error( ldte.ERR_SUBREC_OPEN );
    end
  end

  return nextLeaf;

end -- getNextLeaf()

-- ======================================================================
-- computeDuplicateSplit()
-- ======================================================================
-- From a list of objects that may contain duplicates, we have to find
-- a split point that is NOT in the middle of a set of duplicates.
--              +---+---+---+---+---+---+
-- ObjectList   |222|333|333|333|333|444|
--              +---+---+---+---+---+---+
-- Offsets        1   2   3   4   5   6
-- We start at the middle point (offset 3 of 6 in this example) and look
-- increasingly on either side until we find a NON-MATCH (offset 4, then 2,
-- then 5, then 1 (success));
-- Parms:
-- (*) ldtMan
-- (*) ObjectList
-- Return: Position of the split, or ZERO if there's no split location
-- ======================================================================
-- ======================================================================
local function computeDuplicateSplit(ldtMap, objectList)
  local meth = "computeDuplicateSplit()";


  if objectList == nil then
    warn("[ERROR]<%s:%s> NIL object list", MOD, meth);
    error(ldte.ERR_INTERNAL);
  end

  local objectListSize = #objectList;
  if objectListSize <= 1 then
    return 0;
  end

  -- Start at the BEST position (the middle), and find the closest place
  -- to there to split.
  local startPosition = math.floor(list.size(objectList) / 2);
  local liveObject = objectList[startPosition];
  local startKey = getKeyValue( liveObject );
  local listLength = #objectList;
  local direction = 1; -- Start with first probe towards the end.
  local flip = -1;
  local compareKey;
  local probeIndex;

  -- Iterate, starting from the middle.  The startKey is set for the
  -- first itetation.
  for i = 1, listLength do
    probeIndex = (direction * i) + startPosition;
    if probeIndex > 0 and probeIndex <= listLength then
      liveObject = objectList[probeIndex];
      compareKey = getKeyValue( liveObject);
      if compareKey ~= startKey then
        -- We found it.  Return with THIS position.
        return probeIndex;
      end
    end
    -- Compute for next round.  Flip the direction
    direction = direction * flip;
  end -- for each Obj.. probing from the middle

  return 0;
end -- computeDuplicateSplit()

-- ======================================================================
-- convertList( src, topRec, ldtBinName, ldtCtrl )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" tree state when we get enough values.  So, at some
-- point (StoreThreshold), we take our simple list and then insert into
-- the B+ Tree.
-- Now, convertList does the SMART thing and builds the tree from the 
-- compact list without doing any tree inserts.
-- Parms:
-- (*) src: subrecContext
-- (*) topRec
-- (*) ldtBinName
-- (*) ldtCtrl
-- ======================================================================
local function convertList(src, topRec, ldtCtrl )
  local meth = "convertList()";
  
  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Get the compact List, cut it in half, build the two leaves, and
  -- copy the min value of the right leaf into the root.
  local compactList = ldtMap[LS.CompactList];

  if compactList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Empty Compact list in LDT ",
      MOD, meth);
    error( ldte.ERR_INTERNAL );
  end

  ldtMap[LS.StoreState] = SS_REGULAR; -- now in "regular" (modulo) mode

  -- There will be cases where the compact list is empty, but we're being
  -- asked to convert anyway.  No problem.  Just return.
  if #compactList == 0 then
    return 0;
  end

  -- Notice that the actual "split position" is AFTER the splitPosition
  -- value -- so if we were splitting 10, the list would split AFTER 5,
  -- and index 6 would be the first entry of the right list and thus the
  -- location of the split key.
  -- Also, we would like to be smart about our split.  If we have UNIQUE
  -- keys, we can pick any spot (e.g. the half-way point), but if we have
  -- potentially DUPLICATE keys, we need to split in a spot OTHER than
  -- in the middle of the duplicate list.
  local splitPosition;
  local splitValue;
  local leftLeafList;
  local rightLeafList;
  if (ldtMap[LS.KeyUnique] == AS_TRUE) then
    splitPosition = math.floor(list.size(compactList) * 2/3);
    splitValue = compactList[splitPosition + 1];
  -- Our List operators :
  -- (*) list.take (take the first N elements)
  -- (*) list.drop (drop the first N elements, and keep the rest)
    leftLeafList, rightLeafList = getLeftRight( compactList, splitPosition );
  else
    -- It's possible that the entire compact list is composed of a single
    -- value (e.g. "7,7,7,7 ... 7,7,7"), in which case we have to treat the
    -- "split" specially.  In fact, the entire compact list would go into
    -- the RIGHT leaf, and the left leaf would remain empty.
    splitPosition = computeDuplicateSplit(ldtMap, compactList);
    if splitPosition > 0 then
      splitValue = compactList[splitPosition + 1];
      leftLeafList  =  list.take( compactList, splitPosition );
      rightLeafList =  list.drop( compactList, splitPosition );
    else
      -- The ENTIRE LIST is the same value.  Just use the first one for
      -- the "splitValue", and the entire list goes in the right leaf.
      splitValue = compactList[1];
      leftLeafList = list();
      rightLeafList = compactList;
    end
  end
  local splitKey = getKeyValue(splitValue);

  -- Toss the old Compact List;  No longer needed.  However, we must replace
  -- it with an EMPTY list, not a NIL.
  ldtMap[LS.CompactList] = list();

  -- Now build the new tree:
  buildNewTree( src, topRec, ldtCtrl, leftLeafList, splitKey, rightLeafList);

  return 0;
end -- convertList()

-- ======================================================================
-- Starting from the right-most leaf, scan all the way to the left-most
-- leaf.  This function does not filter, but it does UnTransform.
-- Parms:
-- (*) src: subrecContext
-- (*) resultList: stash the results here
-- (*) topRec: Top DB Record
-- (*) ldtCtrl: The main LDT Control Structure
-- (*) count: Get the rightmost "COUNT" elements, or ALL if 0.
-- Return: void
-- ======================================================================
local function rightToLeftTreeScan( src, resultList, keyList, topRec, ldtCtrl, count)
  local meth = "rightToLeftTreeScan()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Scan all of the leaves.
  local leafDigest = ldtMap[LS.RightLeafDigest];
  if leafDigest == nil or leafDigest == 0 then
    debug("[DETAIL]<%s:%s> Right Leaf Digest is NIL", MOD, meth);
    error(ldte.ERR_INTERNAL);
  end

  local leafDigestString = tostring(leafDigest);
  local leafRead;
  -- We read either COUNT or ALL (if count is zero)
  local readTotal;
  if count == 0 then
    readTotal = propMap[PM.ItemCount];
  else
    readTotal = count;
  end
  local amountLeft = readTotal;

  local leafSubRec = ldt_common.openSubRec(src, topRec, leafDigestString);
  while leafSubRec ~= nil and leafSubRec ~= 0 and amountLeft > 0 do
    leafRead = rightScanLeaf(topRec, leafSubRec, ldtMap, resultList, keyList, amountLeft);
    leafSubRec = getNextLeaf( src, topRec, leafSubRec, false);
    amountLeft = amountLeft - leafRead;
  end -- loop thru each subrec

end -- rightToLeftTreeScan()

-- ======================================================================
-- Starting from the left-most leaf, scan towards the right most leaf
-- until N items (or ALL, if count == 0) have been processed.
-- This function does not filter, but it does UnTransform.
-- Parms:
-- (*) src: subrecContext
-- (*) resultList: stash the results here
-- (*) topRec: Top DB Record
-- (*) ldtCtrl: The main LDT Control Structure
-- (*) count: Get the leftmost "COUNT" elements, or ALL if 0.
-- Return: void
-- ======================================================================
local function leftToRightTreeScan( src, resultList, keyList, topRec, ldtCtrl, count)
  local meth = "leftToRightTreeScan()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Scan all of the leaves.
  local leafDigest = ldtMap[LS.LeftLeafDigest];
  if leafDigest == nil or leafDigest == 0 then
    debug("[DETAIL]<%s:%s> Left Leaf Digest is NIL", MOD, meth);
    error(ldte.ERR_INTERNAL);
  end

  local leafDigestString = tostring(leafDigest);
  local leafRead = 0;
  -- We read either COUNT or ALL (if count is zero)
  local readTotal;
  if count == 0 then
    readTotal = propMap[PM.ItemCount];
  else
    readTotal = count;
  end
  local amountLeft = readTotal;

  local leafSubRec = ldt_common.openSubRec(src, topRec, leafDigestString);
  while leafSubRec ~= nil and leafSubRec ~= 0 and amountLeft > 0 do
    leafRead = leftScanLeaf(topRec, leafSubRec, ldtMap, resultList, keyList, amountLeft);
    leafSubRec = getNextLeaf( src, topRec, leafSubRec, true);
    amountLeft = amountLeft - leafRead;
  end -- loop thru each subrec

end -- leftToRightTreeScan()

-- ======================================================================
-- Starting from the left-most leaf, scan all the way to the right-most
-- leaf.  This function does not filter.
-- Parms:
-- (*) src: subrecContext
-- (*) resultList: stash the results here
-- (*) topRec: Top DB Record
-- (*) ldtCtrl: The Truth
-- Return: void
-- ======================================================================
local function fullTreeScan( src, resultList, topRec, ldtCtrl )
  local meth = "fullTreeScan()";

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Scan all of the leaves.
  local leafDigest = ldtMap[LS.LeftLeafDigest];
  if leafDigest == nil or leafDigest == 0 then
    debug("[DEBUG]<%s:%s> Left Leaf Digest is NIL", MOD, meth);
    error(ldte.ERR_INTERNAL);
  end

  local leafDigestString = tostring(leafDigest);

  local leafSubRec = ldt_common.openSubRec(src, topRec, leafDigestString);
  while leafSubRec ~= nil and leafSubRec ~= 0 do
    fullScanLeaf(topRec, leafSubRec, ldtMap, resultList);
    leafSubRec = getNextLeaf( src, topRec, leafSubRec, true);
  end -- loop thru each subrec

end -- fullTreeScan()

-- ======================================================================
-- Given the searchPath result from treeSearch(), Scan the leaves for all
-- values that satisfy the searchPredicate and the filter.
-- Parms:
-- (*) src: subrecContext
-- (*) resultList: stash the results here
-- (*) topRec: Top DB Record
-- (*) sp: Search Path Object
-- (*) ldtCtrl: The Truth
-- (*) key: the end marker: 
-- (*) flag: Either Scan while equal to end, or Scan until val > end.
-- ======================================================================
local function treeScan( src, resultList, keyList, topRec, sp, ldtCtrl, maxKey, getCount, flag )
  local meth = "treeScan()";
  local scan_A = 0;
  local scan_B = 0;

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local leafLevel = sp.LevelCount;
  local leafSubRec = sp.RecList[leafLevel];

  local count = 0;
  local done = false;
  local startPosition = sp.PositionList[leafLevel];
  while not done do
    -- NOTE: scanLeaf() actually returns a "double" value -- the first is
    -- the scan instruction (stop=0, continue=1) and the second is the error
    -- return code.  So, if scan_B is "ok" (0), then we look to scan_A to see
    -- if we should continue the scan.
    if (getCount ~= nil) then
      scan_A, scan_B  = scanLeaf(topRec, leafSubRec, startPosition, ldtMap,
                              resultList, keyList, maxKey, getCount - #resultList, flag)
    else
      scan_A, scan_B  = scanLeaf(topRec, leafSubRec, startPosition, ldtMap,
                              resultList, keyList, maxKey, nil, flag)
    end

    -- Uncomment this next line to see the "LEAF BOUNDARIES" in the data.
    -- list.append(resultList, 999999 );

    -- Look and see if there's more scanning needed. If so, we'll read
    -- the next leaf in the tree and scan another leaf.
    if( scan_B < 0 ) then
      warn("[ERROR]<%s:%s> Problems in ScanLeaf() A(%s) B(%s)",
        MOD, meth, tostring( scan_A ), tostring( scan_B ) );
      error( ldte.ERR_INTERNAL );
    end
      
    if( scan_A == SCAN.CONTINUE ) then
      startPosition = 1; -- start of next leaf
      leafSubRec = getNextLeaf( src, topRec, leafSubRec, true );
      if( leafSubRec == nil ) then
        done = true;
      end
    else
      done = true;
    end
  end -- while not done reading the T-leaves

  return 0;
end -- treeScan()

-- ======================================================================
-- listDelete()
-- ======================================================================
-- General List Delete function that can be used to delete items or keys.
-- RETURN:
-- A NEW LIST that no longer contains the deleted item.
-- ======================================================================
local function listDelete( objectList, key, position )
  local meth = "listDelete()";
  local resultList;
  local listSize = list.size( objectList );

  if( position < 1 or position > listSize ) then
    warn("[DELETE ERR]<%s:%s> Bad pos(%d) for delete: key(%s) ListSz(%d)",
      MOD, meth, position, tostring(key), listSize);
    error( ldte.ERR_DELETE );
  end

  -- Move elements in the list to "cover" the item at Position.
  --  +---+---+---+---+
  --  |111|222|333|444|   Delete item (333) at position 3.
  --  +---+---+---+---+
  --  Moving forward, Iterate:  list[pos] = list[pos+1]
  --  This is what you would THINK would work:
  -- for i = position, (listSize - 1), 1 do
  --   objectList[i] = objectList[i+1];
  -- end -- for()
  -- objectList[i+1] = nil;  (or, call trim() )
  -- However, because we cannot assign "nil" to a list, nor can we just
  -- trim a list, we have to build a NEW list from the old list, that
  -- contains JUST the pieces we want.
  -- So, basically, we're going to build a new list out of the LEFT and
  -- RIGHT pieces of the original list.
  --
  -- Eventually we'll have OPTIMIZED list functions that will do the
  -- right thing on the ServerSide (e.g. no mallocs, no allocs, etc).
  --
  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- The special cases are:
  -- (*) A list of size 1:  Just return a new (empty) list.
  -- (*) We're deleting the FIRST element, so just use RIGHT LIST.
  -- (*) We're deleting the LAST element, so just use LEFT LIST
  if( listSize == 1 ) then
    resultList = list();
  elseif( position == 1 ) then
    resultList = list.drop( objectList, 1 );
  elseif( position == listSize ) then
    resultList = list.take( objectList, position - 1 );
  else
    resultList = list.take( objectList, position - 1);
    local addList = list.drop( objectList, position );
    local addLength = list.size( addList );
    for i = 1, addLength, 1 do
      list.append( resultList, addList[i] );
    end
  end

  -- When we do deletes with Dups -- we'll change this to have a 
  -- START position and an END position (or a length), rather than
  -- an assumed SINGLE cell.
  -- info("[NOTICE!!!]: >>>>>>>>>>>>>>>>>>>> <*>  <<<<<<<<<<<<<<<<<<<<<<");
     info("[NOTICE!!!]: Currently performing ONLY single item delete");
  -- info("[NOTICE!!!]: >>>>>>>>>>>>>>>>>>>> <*>  <<<<<<<<<<<<<<<<<<<<<<");

  return resultList;
end -- listDelete()

-- ======================================================================
-- collapseTree()
-- ======================================================================
-- Read Level TWO of the B+ Tree and collapse the contents into the root
-- node.  In some cases there might be ONE single child, in other cases there
-- might be two children.  For the two child case, we would start with a
-- tree in this shape:
--                  +=+====+=+
--  (Root Node)     |*| 30 |*|
--                  +|+====+|+
--             +-----+      +------+
-- Internal    |                   |         
-- Nodes       V                   V         
--     +=+====+=+====+=+   +=+====+=+====+=+ 
--     |*|  5 |*| 20 |*|   |*| 40 |*| 50 |*| 
--     +|+====+|+====+|+   +|+====+|+====+|+ 
--      |      |      |     |      |      |  
--    +-+   +--+   +--+     +-+    +-+    +-+
--    |     |      |          |      |      |   
--    V     V      V          V      V      V   
--  +-^-++--^--++--^--+    +--^--++--^--++--^--+
--  |1|3||6|7|8||22|26|    |30|39||40|46||51|55|
--  +---++-----++-----+    +-----++-----++-----+
--  Leaf Nodes
--
--    And we end up with a tree in this shape (one less level of inner nodes).
--
--     New (Merged) Root Node
--     +=+====+=+====+=+====+=+====+=+====+=+ 
--     |*|  5 |*| 20 |*| 30 |*| 40 |*| 50 |*| 
--     +|+====+|+====+|+====+|+====+|+====+|+ 
--      |      |      |      |      |      |  
--    +-+   +--+   +--+      ++     ++     ++
--    |     |      |          |      |      |   
--    V     V      V          V      V      V   
--  +-^-++--^--++--^--+    +--^--++--^--++--^--+
--  |1|3||6|7|8||22|26|    |30|39||40|46||51|55|
--  +---++-----++-----+    +-----++-----++-----+
--  Leaf Nodes
--
-- ======================================================================
local function collapseTree(src, topRec, ldtCtrl)
  local meth = "collapseTree()";
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local rootKeyList = ldtMap[LS.RootKeyList];
  local rootDigestList = ldtMap[LS.RootDigestList];

  -- The caller has done all of the validation -- we just need to do the
  -- actual tree collapse.  Read the contents of the root's (one or more)
  -- children and copy them into the root (see above diagram). In the case of
  -- a SINGLE CHILD, the entire contents of the child gets moved into the root.
  -- In the case of TWO children, the contents of the two children get copied
  -- into the root, but the one remaining key in the root stays (as the
  -- divider of the two chidren key lists).
  local leftSubRecDigest = ldtMap[LS.RootDigestList][1];
  local leftSubRecDigestString = tostring(leftSubRecDigest);
  local leftSubRec = ldt_common.openSubRec(src, topRec, leftSubRecDigestString);
  local leftSubRecCtrlMap = leftSubRec[NSR_CTRL_BIN];
  local leftKeyList =       leftSubRec[NSR_KEY_LIST_BIN];
  local leftDigestList =    leftSubRec[NSR_DIGEST_BIN];

  local rightSubRecDigest;
  local rightSubRecDigestString;
  local rightSubRec;
  local rightSubRecCtrlMap;
  local rightKeyList;
  local rightDigestList;

  -- We at least have a LEFT child.  Copy that info over to the root.
  local newKeyList = list.take(leftKeyList, #leftKeyList);
  local newDigestList = list.take(leftDigestList, #leftDigestList);

  -- Check for a Right Child.  If it's there, then copy over the Root Key
  -- (the separator) and then copy the 
  if #rootDigestList == 2 then
    list.append(newKeyList, rootKeyList[1]);

    rightSubRecDigest = ldtMap[LS.RootDigestList][2];
    rightSubRecDigestString = tostring(rightSubRecDigest);
    rightSubRec = ldt_common.openSubRec(src, topRec, rightSubRecDigestString);
    rightSubRecCtrlMap = rightSubRec[NSR_CTRL_BIN];
    rightKeyList =       rightSubRec[NSR_KEY_LIST_BIN];
    rightDigestList =    rightSubRec[NSR_DIGEST_BIN];

    list.concat(newKeyList, rightKeyList);
  end

  ldtMap[LS.RootKeyList] = newKeyList;
  ldtMap[LS.RootDigestList] = newDigestList;

  -- We have now merged the contents of the Root children into the root,
  -- so we can safely release the children.
  ldt_common.removeSubRec( src, topRec, propMap, leftSubRecDigestString );
  if rightSubRecDigestString ~= nil then
    ldt_common.removeSubRec( src, topRec, propMap, rightSubRecDigestString );
  end

  -- Finally, adjust the tree level to show that we now have one LESS
  -- tree level.
  local treeLevel = ldtMap[LS.TreeLevel];
  ldtMap[LS.TreeLevel] = treeLevel - 1;

  -- Sanity check
  if ldtMap[LS.TreeLevel] < 2 then
    warn("[INTERNAL ERROR]<%s:%s> Tree Level (%d) incorrect. Must be >= 2",
      MOD, meth, ldtMap[LS.TreeLevel]);
    error( ldte.ERR_SUBREC_OPEN );
  end

  return 0;
end -- collapseTree()


-- ======================================================================
-- redistributeRootChild()
-- ======================================================================
-- The SINGLE child of a root has too many children to fit in the root
-- node (roots don't always match nodes), so we can't merge the contents.
-- However, we don't want the root to point to a single child, so we're
-- going to take the SINGLE root child and split it into two root children.
-- ======================================================================
-- We start with a tree in this shape:
-- (*) Root with No Keys and a single Digest (one child)
--
--                  +=+====+=+
--  (Root Node)     |*|NIL |@|
--                  +|+====+=+
--                   +---+  
--       Internal        | 
--       Node            V
--     +=+====+=+====+=+=^==+=+====+=+====+=+ 
--     |*|  5 |*| 20 |*| 30 |*| 40 |*| 50 |*| 
--     +|+====+|+====+|+====+|+====+|+====+|+ 
--      |      |      |      |      |      |  
--    +-+    +-+     ++      |      ++     +-+
--    |      |       |       |       |       |   
--    V      V       V       V       V       V   
--  +-^-+ +--^--+ +--^--+ +--^--+ +--^--+ +--^--+
--  |1|3| |6|7|8| |22|26| |30|39| |40|46| |51|55|
--  +---+ +-----+ +-----+ +-----+ +-----+ +-----+
--  Leaf Nodes
--
--  And we end up with a tree in this shape:
--  (*) Two Root Children
--  (*) One Root Key
--
--                  +=+====+=+
--  (Root Node)     |*| 30 |*|
--                  +|+====+|+
--             +-----+      +------+
-- Internal    |                   |         
-- Nodes       V                   V         
--     +=+====+=+====+=+   +=+====+=+====+=+ 
--     |*|  5 |*| 20 |*|   |*| 40 |*| 50 |*| 
--     +|+====+|+====+|+   +|+====+|+====+|+ 
--      |      |      |     |      |      |  
--    +-+   +--+   +--+     +-+    +-+    +-+
--    |     |      |          |      |      |   
--    V     V      V          V      V      V   
--  +-^-++--^--++--^--+    +--^--++--^--++--^--+
--  |1|3||6|7|8||22|26|    |30|39||40|46||51|55|
--  +---++-----++-----+    +-----++-----++-----+
--  Leaf Nodes
--
-- Notice that this operation is very similar to "splitRoot", except that
-- some of the details are different.
-- ======================================================================
local function redistributeRootChild(src, topRec, ldtCtrl)
  local meth = "redistributeRootChild()";
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local childSubRecDigest = ldtMap[LS.RootDigestList][1];
  local childSubRecDigestString = tostring(childSubRecDigest);
  local childSubRec = ldt_common.openSubRec(src,topRec,childSubRecDigestString);
  local childKeyList = childSubRec[NSR_KEY_LIST_BIN];
  local childDigestList = childSubRec[NSR_DIGEST_BIN];
  local childCtrlMap = childSubRec[NSR_CTRL_BIN];

  -- Split this like we're doing a root split.
  -- Calculate the split position and the key to propagate up to parent.
  local splitPosition = getNodeSplitPosition( ldtMap, childKeyList, 1, nil );
  local splitKey = childKeyList[splitPosition];

  -- Splitting a node works as follows.  The node is split into a left
  -- piece, a right piece, and a center value that is propagated up to
  -- the parent (in this case, root) node.
  --              +---+---+---+---+---+
  -- KeyList      |111|222|333|444|555|
  --              +---+---+---+---+---+
  -- DigestList   A   B   C   D   E   F
  --
  --                      +---+
  -- New Parent Element   |333|
  --                      +---+
  --                     /X   Y\
  --              +---+---+   +---+---+
  -- KeyList Nd(x)|111|222|   |444|555|Nd(y)
  --              +---+---+   +---+---+
  -- DigestList   A   B   C   D   E   F
  --
  -- Our List operators :
  -- (*) list.take (take the first N elements)
  -- (*) list.drop (drop the first N elements, and keep the rest)
  -- will let us split the current Root node list into two node lists.
  -- We propagate up the split key (the new root value) and the two
  -- new inner node digests.  Remember that the Key List is ONE CELL
  -- shorter than the DigestList.
  local leftKeyList  = list.take( childKeyList, splitPosition - 1 );
  local rightKeyList = list.drop( childKeyList, splitPosition  );

  local leftDigestList, rightDigestList = getLeftRight(childDigestList, splitPosition);

  -- It is as if we now have two root children -- a Left Node(the old single
  -- child) and the new Right Node.
  local leftNodeRec  = childSubRec;
  local rightNodeRec = createNodeRec( src, topRec, ldtCtrl );

  local leftNodeDigest  = childSubRecDigest;
  local rightNodeDigest = record.digest( rightNodeRec );

  -- Populate the new nodes with their Key and Digest Lists
  populateNode( leftNodeRec, leftKeyList, leftDigestList);
  populateNode( rightNodeRec, rightKeyList, rightDigestList);

  -- Call update to mark the SubRec as dirty, and to force the write if we
  -- are in "early update" mode. Close will happen at the end of the Lua call.
  ldt_common.updateSubRec( src, leftNodeRec );
  ldt_common.updateSubRec( src, rightNodeRec );

  -- Replace the Root Information with just the split-key and the
  -- two new child node digests (much like first Tree Insert).
  local rootKeyList = list();
  list.append( rootKeyList, splitKey );
  local rootDigestList = list();
  list.append( rootDigestList, leftNodeDigest );
  list.append( rootDigestList, rightNodeDigest );

  -- Update the Main control map with the new root lists.
  ldtMap[LS.RootKeyList] = rootKeyList;
  ldtMap[LS.RootDigestList] = rootDigestList;

  return 0;
end -- redistributeRootChild()

-- ======================================================================
-- mergeRoot()
-- ======================================================================
-- After a root entry delete, we have one less entry in this root.
-- We Test to see if a MERGE of the root children is possible.
--
-- When we are down to only one or two children nodes of the root, we look
-- at the contents of those nodes to see if we can possibly merge the
-- contents of the one or two children nodes into the root node.
-- Note that since Root Nodes are often smaller than Internal Nodes, it is
-- possible that even thought we have only ONE root child, that single child
-- cannot be merged into the root.  So, in that case, we redistribute
-- the single root child into TWO children (a rebalance).
--
-- There is a special case for trees that have three levels (a root, two
-- or more child nodes, and leaves).  In this specific case, 
-- we can can use the leaf count to decide if the contents of the root's
-- children can fit in the root.  For larger trees, we look inside of the
-- two children to decide if their contents can fit.
--
-- ======================================================================
-- Note that we are called ONLY when there are no more than two root
-- children, so we don't need to consider the case where there might be
-- more than two children.
-- ======================================================================
local function mergeRoot(src, sp, topRec, ldtCtrl)
  local meth = "mergeRoot()";
  local rc = 0;
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local rootKeyList = ldtMap[LS.RootKeyList];
  local rootDigestList = ldtMap[LS.RootDigestList];

  -- --------------------------------------------------------------------
  -- The Caller has already verified that there NO MORE than two children
  -- in the root, and there may be just ONE.
  -- So we can test for several special cases:
  -- (1) There is only one child (an inner node).
  --     One of the two subcases apply.
  --     SubCase A: If the contents of that child fits in the
  --     root (it may not, because roots and nodes are not the same size),
  --     then we move the contents of the child into the root.
  --     We call collapseTree() to merge into the root.
  --     SubCase B: We redistribute the single root child into TWO children
  --     that hold equal amounts.
  --     We call redistributeRootChild() to make two children from one.
  -- (2) The tree level is 3 (one root, two nodes and N leaves).
  --     We make a special computation because we can just count the leaves
  --     and know if we can make this a two level tree.  In this case, we
  --     can do a merge and copy the contents of the two children
  --     into the Root Node.  Otherwise, do nothing.
  --     We call collapseTree() to merge into the root.
  -- (3) There are two root children that are inner nodes.  We open
  --     those two children (one will already be open) and check the contents
  --     to see they can be merged.  If they can, we copy the contents of
  --     the two nodes into the Root Node.  Otherwise, do nothing.
  --     We call collapseTree() to merge into the root.
  -- --------------------------------------------------------------------
  local leftSubRec;
  local leftSubRecDigest;
  local leftSubRecDigestString;
  local leftDigestList;

  local rightSubRec;
  local rightSubRecDigest;
  local rightSubRecDigestString;
  local rightDigestList;

  local treeLevel = ldtMap[LS.TreeLevel];
  if #rootDigestList == 1 then
    -- Case 1:  There is only one child:
    leftSubRecDigest = rootDigestList[1];
    leftSubRecDigestString = tostring(leftSubRecDigest);
    leftSubRec = ldt_common.openSubRec(src,topRec,leftSubRecDigestString);
    leftDigestList = leftSubRec[NSR_DIGEST_BIN];
    if ldt_common.getValSize(leftDigestList) < G_PageSize then
      collapseTree(src, topRec, ldtCtrl);
    else
      redistributeRootChild( src, topRec, ldtCtrl );
    end
--  elseif treeLevel == 3 then
--  Figure out way to larger collapse
    -- Case 2: Special Calculation for trees of height 3.
--    local leafCount = ldtMap[LS.LeafCount];
--    local nodeCount = ldtMap[LS.NodeCount];
--    if leafCount < rootMax then
--      collapseTree(src, topRec, ldtCtrl);
--    end -- Otherwise, DO NOTHING.
  else
    -- Case 3: "Regular" check of the children to see if they fit in the root.
    leftSubRecDigest = rootDigestList[1];
    leftSubRecDigestString = tostring(leftSubRecDigest);
    leftSubRec = ldt_common.openSubRec(src,topRec,leftSubRecDigestString);
    leftDigestList = leftSubRec[NSR_DIGEST_BIN];

    rightSubRecDigest = rootDigestList[2];
    rightSubRecDigestString = tostring(rightSubRecDigest);
    rightSubRec = ldt_common.openSubRec(src,topRec,rightSubRecDigestString);
    rightDigestList = rightSubRec[NSR_DIGEST_BIN];

    if ((ldt_common.getValSize(leftDigestList) 
           + ldt_common.getValSize(rightDigestList) + 1) 
             < G_PageSize) then
      collapseTree(src, topRec, ldtCtrl);
    end -- Otherwise, DO NOTHING.
  end

  return rc;
end -- mergeRoot()

-- ======================================================================
-- rootDelete()
-- ======================================================================
-- After a Node or Leaf delete, we're left with an empty node/leaf, which
-- means we have to delete the corresponding entry from the root.  The
-- root is special, so it gets special attention.  If we get all the way
-- down to either the last leaf or the second to last leaf, then we do
-- not want to remove the leaf.  Instead, we want to borrow some number 
-- of elements in order to balance the small tree.  Notice that Duplicate
-- keys make things a bit messy, as we may have to deal with an empty
-- left leaf, and a middle/right leaf that holds duplicate values.  It 
-- would be nice if we could always collapse into a compact list and regrow
-- from there, but there's no guarantee that the remaining list of
-- duplicate values will all fit in the compact list.
--
-- Here's the problem:  We'll highlight this by showing a related problem:
--
-- The initial split of a compact list into leaves shows the inherent
-- problem.  If we have duplicate values that will span a leaf, we cannot
-- (by definition) have a left leaf, as all values LESS than a given key
-- are down the left-most leaf path, and everything greater than or equal
-- to the node key are in the right leaves.
--
-- Consider this example, where our compact list is 10 elements, and our
-- leaves can hold up to 8 elements.  And, in this case, we have 8 duplicates
-- of a single value.  We COULD split the compact list in half, but that gives
-- us a propagated key of "444", which is not correct.
-- So, technically, we must generate THREE leaves, where the Left leaf is
-- empty, and the middle and right leaves have duplicate values in the root.
-- NOTE: we follow the rule that the compact list must fit in a single
-- leaf, so we don't have the "three leaf" problem when building a new tree.
--
--              +---+---+---+---+---+---+---+---+---+---+
-- Compact      |444|444|444|444|444|444|444|444|444|444|
-- List         +---+---+---+---+---+---+---+---+---+---+
--
--                            +~+---+~+---+~+
-- Root Key List    --------> |*|444|*|444|*|
--                            +|+---+|+---+|+
-- Root Digest List --------> A|    B|    C|
--                   +---------+     |     +-----------------+
--                   |               |                       |
--                   V               V                       V
--                 +---+   +---+---+---+---+---+   +---+---+---+---+---+
-- Leaves          |   |   |444|444|444|444|444|   |444|444|444|444|444|
--                 +---+   +---+---+---+---+---+   +---+---+---+---+---+
--                   A               B                       C
--
-- We have a similar problem when we want to collapse leaves.  We have
-- to preserve the correct state of the tree, for many different (and
-- potentially unusual) situations.
--
-- Here's the other issue with ROOT and Inner Node delete.  The Search Path
-- (SP) position must be interpreted correctly.  There are N Keys, but N+1
-- child node/leaf pointers, so we have to interpret the SP Position correctly.
-- The searchKeyList() function gives us the DIGEST index that we should
-- follow for a particular Key value.  So, the SP position values for the
-- ROOT node will be in the range: [1 .. #DigestLength].
--
--
--                      +--->    K1    K2    K3
--                      |     +~+---+~+---+~+---+~+
-- Root Key List    ----+---> |*|111|*|222|*|333|*|
--                            +|+---+|+---+|+---+|+
-- Root Digest List --------> A|    B|    C|    D|
--             +---------------+     |   +-+     +-------+
--             |           +---------+   |               |
--             V(D1)       V(D2)         V(D3)           V(D4)
--         +---+---+   +---+---+    ---+---+---+   +---+---+---+
-- Leaves  |059|099|   |111|113|   |222|225|230|   |333|444|555|
--         +---+---+   +---+---+    ---+---+---+   +---+---+---+
--             A           B             C               D
--
-- Case 1: SP Position 1 (Remove Leaf A): Remove D1(A), K1(111) (special)
-- Case 2: SP Position 2 (Remove Leaf B): Remove D2(B), K1(111)
-- Case 3: SP Position 3 (Remove Leaf C): Remove D3(C), K2(222)
-- Case 4: SP Position 4 (Remove Leaf D): Remove D4(D), K3(333)
--
-- The search path position points to the Found or Insert position, and
-- then points to ZERO when not found (or insert is at the front).  The
-- insert position also tells us how to delete.
-- In general, we delete:
-- ==> Key at position, unless position is Zero (then it is one)
-- ==> Digest at position + 1.
-- ======================================================================
local function rootDelete(src, sp, topRec, ldtCtrl)
  local meth = "rootDelete()";

  -- Our list and map has already been validated.  Just use it.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Caller has verified that there are more than two children (leaves or
  -- nodes) so we can go ahead and remove one of them.

  -- The Search Path (sp) object shows the search path from root to leaf.
  -- The Position values in SP are actually different for Leaf nodes and
  -- Inner/Root Nodes.  For Root/Inner nodes, the SP Position points to the
  -- index of the DIGEST that we follow to find the value (not the Key index).
  local rootLevel = 1;
  local rc = 0;

  local keyList     = ldtMap[LS.RootKeyList];
  local digestList  = ldtMap[LS.RootDigestList];

  local rootPosition;
  local digestPosition;
  local keyPosition;
  digestPosition = sp.PositionList[rootLevel];
  keyPosition = digestPosition - 1;
  if (keyPosition < 1) then
    keyPosition = digestPosition
  end

  if (keyPosition > #keyList) then
    keyPosition = #keyList;
  end

  local resultKeyList;
  local resultDigestList;

  -- If it's a unique Index, then the delete is simple.  Release the child
  -- sub-rec, then remove the entries from the Key and Digest Lists.
  if (ldtMap[LS.KeyUnique] == AS_TRUE) then
    -- Check for the minimal node cases:
    -- (*) ZERO Keys, and only a Left Child Pointer (one digest)
    -- (*) 1 key, two digest pointers.
    if #keyList == 0 then
      -- KeyList is already empty.  Remove the last Digest Entry.
      resultKeyList = list(); -- set this for safety down below.
      resultDigestList = list();
      ldtMap[LS.RootDigestList] = resultDigestList;
    elseif #keyList == 1 then
      -- Remove the last KeyList entry.  Remove the Right or Left child
      -- digest based on position.
      resultKeyList = list();
      ldtMap[LS.RootKeyList] = resultKeyList;
      resultDigestList = ldt_common.listDelete(digestList, digestPosition );
      ldtMap[LS.RootDigestList] = resultDigestList;
    else
      -- Remove the entry from both the Key List and the Digest List
      resultKeyList = ldt_common.listDelete(keyList, keyPosition);
      ldtMap[LS.RootKeyList] = resultKeyList;
      resultDigestList = ldt_common.listDelete(digestList,digestPosition);
      ldtMap[LS.RootDigestList] = resultDigestList;
    end
  else
    -- If we allow duplicates, then we treat deletes quite a bit differently
    -- than we treat unique value deletes.
    -- We have to address the issue that a batch of duplicate values can 
    -- result in the removal of multiple parent nodes.
    -- Treat them all individually first.
    resultKeyList    = ldt_common.listDelete(keyList, keyPosition );
    resultDigestList = ldt_common.listDelete(digestList, digestPosition );
  end

  -- Until we get our improved List Processing Function, we have to assign
  -- our newly created list back into the ldt map.
  -- The outer caller will udpate the Top Record.
  ldtMap[LS.RootKeyList]    = resultKeyList;
  ldtMap[LS.RootDigestList] = resultDigestList;

  -- Now that we've dealt with a basic delete, there is still the possibility
  -- of merging the contents of the children of the root (who have to be inner
  -- Tree nodes, not leaves) into the root itself,
  -- which means we lose one level of the tree (the opposite of root split).
  -- So, this is valid ONLY when we have trees of a level >= 3.
  local treeLevel = ldtMap[LS.TreeLevel];

  -- Todo enable root merge
  --if #resultDigestList <= 2 and treeLevel >= 3 then
    -- This function will CHECK for merge, and then merge if needed.
  --  mergeRoot(src, sp, topRec, ldtCtrl);
  --end

  return 0;
end -- rootDelete()

-- ======================================================================
-- releaseNode()
-- ======================================================================
-- Release (remove) this Node and remove the entry in the parent node.
-- ======================================================================
local function releaseNode(src, sp, nodeLevel, topRec, ldtCtrl, candidate)
  local meth = "releaseNode()";
  local nodeSubRecDigest = sp.DigestList[nodeLevel];
  local nodeSubRec       = sp.RecList[nodeLevel];

  local nodePosition;
  local digestPosition;
  local keyPosition;
  if (sp.FoundList[nodeLevel]) then
    nodePosition   = sp.PositionList[nodeLevel];
    keyPosition    = nodePosition;
    digestPosition = nodePosition + 1;
  else
    nodePosition   = sp.PositionList[nodeLevel] - 1;
    keyPosition    = (nodePosition < 1) and 1 or nodePosition;
    digestPosition = keyPosition; -- (nodePosition < 0) and 1 or nodePosition + 1;
  end

  local candidateDigest = record.digest(candidate);

  if nodeSubRec == nil then
    warn("[INTERNAL ERROR]<%s:%s> nodeSubRec is NIL: Dig(%s)",
    MOD, meth, nodeSubRecDigest);
    error(ldte.ERR_INTERNAL);
  end

  -- Remove the node.  Check for the Special root case, when this node
  -- is a child of the root (in which case we must do "rootDelete").
  if (nodeLevel == 2) then
    rootDelete( src, sp, topRec, ldtCtrl );
  else
    nodeDelete( src, sp, (nodeLevel - 1), topRec, ldtCtrl );
  end

  -- Release this node
  local digestString = tostring(nodeSubRecDigest);
  ldt_common.removeSubRec( src, topRec, ldtCtrl[LDT_PROP_MAP], digestString );

  -- We now have one LESS node.  Update the global count.
  local ldtMap         = ldtCtrl[LDT_CTRL_MAP];

end -- releasenode()

-- ======================================================================
-- nodeDelete()
-- ======================================================================
-- After a child (Node/Leaf) delete, we have to remove the entry from
-- the node (the digest list and the key list).  Notice that this can
-- in turn trigger further parent operations if this delete is the last
-- entry in THIS node.
--
-- Collapse the list to get rid of the entry in the node.  The SearchPath
-- parm shows us where the item is in the node.
-- Parms: 
-- (*) src: SubRec Context (in case we have to open more leaves)
-- (*) sp: Search Path structure
-- (*) nodeLevel: Level in the tree of this node.
-- (*) topRec:
-- (*) ldtCtrl:
--
-- Here's the other issue with ROOT and Inner Node delete.  The Search Path
-- (SP) position must be interpreted correctly.  There are N Keys, but N+1
-- child node/leaf pointers, so we have to interpret the SP Position correctly.
--
--                      +--->    K1    K2    K3
--                      |     +~+---+~+---+~+---+~+
-- Root Key List    ----+---> |*|111|*|222|*|333|*|
--                            +|+---+|+---+|+---+|+
-- Root Digest List --------> A|    B|    C|    D|
--             +---------------+     |   +-+     +-------+
--             |           +---------+   |               |
--             V(D1)       V(D2)         V(D3)           V(D4)
--         +---+---+   +---+---+    ---+---+---+   +---+---+---+
-- Leaves  |059|099|   |111|113|   |222|225|230|   |333|444|555|
--         +---+---+   +---+---+    ---+---+---+   +---+---+---+
--             A           B             C               D
--
-- Case 1: SP Position 0 (Remove Leaf A): Remove D1(A), K1(111) (special)
-- Case 2: SP Position 1 (Remove Leaf B): Remove D2(B), K1(111)
-- Case 3: SP Position 2 (Remove Leaf C): Remove D3(C), K2(222)
-- Case 4: SP Position 3 (Remove Leaf D): Remove D4(D), K3(333)
--
-- The search path position points to the Found or Insert position, and
-- then points to ZERO when not found (or insert is at the front).  The
-- insert position also tells us how to delete.
-- In general, we delete:
-- ==> Key at position, unless position is Zero (then it is one)
-- ==> Digest at position + 1.
-- ======================================================================
-- NOTE: This function is FORWARD-DECLARED, so it does NOT get a "local"
-- declaration here.
-- ======================================================================
function nodeDelete( src, sp, nodeLevel, topRec, ldtCtrl )
  local meth = "nodeDelete()";

  local rc = 0;

  -- Our list and map has already been validated.  Just use it.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- The Search Path (sp) object shows the search path from root to leaf.
  local nodeSubRec       = sp.RecList[nodeLevel];
  local nodeSubRecDigest = sp.DigestList[nodeLevel];
  local keyList          = nodeSubRec[NSR_KEY_LIST_BIN];
  local digestList       = nodeSubRec[NSR_DIGEST_BIN];

  local digestPosition;
  local keyPosition;

  digestPosition = sp.PositionList[nodeLevel];
  keyPosition = digestPosition - 1;
  if (keyPosition < 1) then
    keyPosition = digestPosition;
  end 

  if (keyPosition > #keyList) then
    keyPosition = #keyList;
  end
  local removedDigestString = tostring(digestList[digestPosition]);

  local resultDigestList;
  local resultKeyList;
  -- If we allow duplicates, then we treat deletes quite a bit differently
  -- than we treat unique value deletes. Do the unique value case first.
  if (ldtMap[LS.KeyUnique] == AS_TRUE) then
    if (#keyList == 0) then
      -- KeyList is already empty.  Remove the last Digest Entry.
      resultDigestList = list(); -- Set this for safety down below.
      resultKeyList = list();
      nodeSubRec[NSR_DIGEST_BIN] = resultDigestList;
    elseif (#keyList == 1) then
      -- Remove the last KeyList entry.  Remove the Right or Left child
      -- digest based on position.
      resultKeyList = list();
      nodeSubRec[NSR_KEY_LIST_BIN] = resultKeyList;
      resultDigestList = ldt_common.listDelete(digestList,digestPosition);
      nodeSubRec[NSR_DIGEST_BIN] = resultDigestList;
    else
      -- Remove the entry from both the Key List and the Digest List
      resultKeyList = ldt_common.listDelete(keyList, keyPosition);
      nodeSubRec[NSR_KEY_LIST_BIN] = resultKeyList;
      resultDigestList = ldt_common.listDelete(digestList,digestPosition);
      nodeSubRec[NSR_DIGEST_BIN] = resultDigestList;
    end
  else
    resultKeyList    = ldt_common.listDelete(keyList, keyPosition );
    resultDigestList = ldt_common.listDelete(digestList, digestPosition );
  end

  -- ok -- if we're left with NOTHING in the noded then collapse this node
  -- and release the entry in the parent.  If our parent is a regular node,
  -- then do the usual thing.  However, if it is the root node, then
  -- we have to do something special.  That is all handled by releaseNode();
  if #resultDigestList == 0 then
    releaseNode(src, sp, nodeLevel, topRec, ldtCtrl, nodeSubRec)
  else
    -- Mark this page as dirty and possibly write it out if needed.
    ldt_common.updateSubRec( src, nodeSubRec );
  end

  return rc;
end -- nodeDelete()

-- ======================================================================
-- releaseLeaf()
-- ======================================================================
-- Release this leaf and remove the entry in the parent node.
-- The caller has already verified that this Sub-Rec Leaf is empty.
-- Notice that we will NOT reclaim this leaf it is one of the last two
-- leaves.  We leave the last two until we can either collapse into a
-- compact list, or the tree is completely empty.
-- Parms:
-- Return: Nothing.
-- ======================================================================
local function releaseLeaf(src, sp, topRec, ldtCtrl)
  local meth = "releaseLeaf()";

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- The Search Path (sp) from root to leaf shows how we will bubble up
  -- if this leaf delete propagates up to the parent node.
  local leafLevel = sp.LevelCount;
  local leafSubRecDigest = sp.DigestList[leafLevel];
  local leafSubRec = sp.RecList[leafLevel];

  if leafSubRec == nil then
    warn("[INTERNAL ERROR]<%s:%s> LeafSubRec is NIL: Dig(%s)",
      MOD, meth, leafSubRecDigest);
    error(ldte.ERR_INTERNAL);
  end

  if (leafLevel == 2) then
    rootDelete( src, sp, topRec, ldtCtrl );
  else
    nodeDelete( src, sp, (leafLevel - 1), topRec, ldtCtrl );
  end

  adjustLeafPointersAfterDelete( src, topRec, ldtMap, leafSubRec )

  -- Release this leaf
  local digestString = tostring(leafSubRecDigest);
  ldt_common.removeSubRec( src, topRec, propMap, digestString );

  -- We now have one LESS Leaf.  Update the count.
  local leafCount = ldtMap[LS.LeafCount];
  ldtMap[LS.LeafCount] = leafCount - 1;
end -- releaseLeaf()

-- ======================================================================
-- releaseTree()
-- ======================================================================
-- Release all storage for a tree.  Reset the Root Node lists and all
-- related storage information.  It's likely that we're resetting back
-- to a compact list -- which means that ItemCount remains, but other things
-- get reset.
-- ======================================================================
local function releaseTree( src, topRec, ldtCtrl )
  local meth = "releaseTree()";

  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Remove the Root Info
  map.remove( ldtMap, LS.RootKeyList );
  map.remove( ldtMap, LS.RootDigestList );

  -- Restore the initial state in case we build up again.
  ldtMap[LS.RootKeyList] = list();
  ldtMap[LS.RootDigestList] = list();

  -- Remove the Left and Right Leaf pointers
  ldtMap[LS.LeftLeafDigest] = 0;
  ldtMap[LS.RightLeafDigest] = 0;

  -- Set the tree level back to its initial state.
  ldtMap[LS.TreeLevel] = 1;

  -- Remove the ESR, which will trigger the removal of all sub-records.
  -- This function also sets the PM.EsrDigest entry to zero.
  ldt_common.removeEsr( src, topRec, propMap);
  propMap[PM.SubRecCount] = 0; -- No More Sub-Recs

  -- Reset the Tree Node and Leaft stats.
  ldtMap[LS.LeafCount] = 0;
end -- releaseTree()

-- ======================================================================
-- collapseToCompact()
-- ======================================================================
-- We're at the point where we've removed enough items that put us UNDER
-- the compact list threshold.  So, we're going to scan the tree and place
-- the contents into the compact list.
-- RETURN:
-- true on success, false if fail. error() if problems
-- ======================================================================
local function collapseToCompact( src, topRec, ldtCtrl )
  local meth = "collapseToCompact()";

  local propMap = ldtCtrl[LDT_PROP_MAP]
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Get all of the tree contents and store it in scanList.
  local itemCount = propMap[PM.ItemCount];
  local scanList = list.new(itemCount);
  if (propMap[PM.ItemCount] > 0) then
    fullTreeScan( src, scanList, topRec, ldtCtrl );
  end

  -- scanList is the new Compact List.  Change the state back to Compact.
  if (ldt_common.getValSize(scanList) > G_PageSize) then
    return false; 
  end 
  ldtMap[LS.CompactList] = scanList;
  ldtMap[LS.StoreState] = SS_COMPACT;

  -- Erase the old tree.  Null out the Root list in the main record, and
  -- release all of the subrecs.
  releaseTree( src, topRec, ldtCtrl );
  return true;
end -- collapseToCompact()

-- ======================================================================
-- leafDelete()
-- ======================================================================
-- Collapse the list to get rid of the entry in the leaf.
-- We're not in the mode of "NULLing" out the entry, so we'll pay
-- the extra cost of collapsing the list around the item.  The SearchPath
-- parm shows us where the item is.
-- Parms: 
-- (*) src: SubRec Context (in case we have to open more leaves)
-- (*) sp: Search Path structure
-- (*) topRec:
-- (*) ldtCtrl:
-- (*) key: the key -- in case we need to look for more dups
-- ======================================================================
local function leafDelete( src, sp, topRec, ldtCtrl, key )
  local meth = "leafDelete()";

  local rc = 0;

  -- Our list and map has already been validated.  Just use it.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local leafLevel  = sp.LevelCount;
  local leafSubRec = sp.RecList[leafLevel];
  local objectList = leafSubRec[LSR_LIST_BIN];
  local position   = sp.PositionList[leafLevel];
  local endPos     = sp.LeafEndPosition;
  local resultList;
  
  -- Delete is easy if it's a single value -- more difficult if MANY items
  -- (with the same value) are deleted.
  local numRemoved;
  if (ldtMap[LS.KeyUnique] == AS_TRUE) then
    resultList = ldt_common.listDelete(objectList, position)
    leafSubRec[LSR_LIST_BIN] = resultList;
    numRemoved = 1;
  else
    -- If it's MULTI-DELETE, then we have to check the neighbors and the
    -- parent to see if we have to merge leaves after the delete.
    resultList = ldt_common.listDeleteMultiple(objectList, position, endPos);
    leafSubRec[LSR_LIST_BIN] = resultList;
    numRemoved = endPos - position + 1;
  end

  -- If this last remove has dropped us BELOW the "reverse threshold", then
  -- collapse the tree into a new compact list.  Otherwise, do the regular
  -- tree/node checking after a delete.  Notice that "collapse" will also
  -- handle the EMPTY tree case (which will probably be rare).
  local currentCount = propMap[PM.ItemCount] - numRemoved;


  -- ok -- regular delete processing.  if we're left with NOTHING in the
  -- leaf then collapse this leaf and release the entry in the parent.
  if (resultList == 0 or resultList == nil or #resultList == 0) and ldtMap[LS.LeafCount] > 2 then
    releaseLeaf(src, sp, topRec, ldtCtrl)
  else
    -- Mark this page as dirty and possibly write it out if needed.
    ldt_common.updateSubRec( src, leafSubRec );
  end

  --if count < threshold attempt a collapse
  if currentCount < ldtMap[LS.Threshold] then
    local collapsed = collapseToCompact( src, topRec, ldtCtrl );
  end

  return rc;
end -- leafDelete()


-- ======================================================================
-- treeDelete()
-- ======================================================================
-- Perform a delete:  Remove this object from the tree. 
-- Two cases:
-- (1) Unique Key
-- (2) Duplicates Allowed.
-- Case 1: Unique Key :: For this case, just collapse the object list in the
-- leaf to remove the item.  If this empties the leaf, then we remove this
-- SubRecord and remove the entry from the parent.
-- Case 2: Duplicate Keys:
-- When we do Duplicates, then we have to address the case that the leaf
-- is completely empty, which means we also need remove the subrec from
-- the leaf chain.  HOWEVER, for now, we'll just remove the items from the
-- leaf objectList, but leave the Tree Leaves in place.  And, in either
-- case, we won't update the upper nodes.
-- We will have both a COMPACT storage mode and a TREE storage mode. 
-- When in COMPACT mode, the root node holds the list directly.
-- When in Tree mode, the root node holds the top level of the tree.
-- Parms:
-- (*) src: SubRec Context
-- (*) topRec:
-- (*) ldtCtrl: The LDT Control Structure
-- (*) key:  Find and Delete the objects that match this key
-- (*) createSpec:
-- Return:
-- ERR.OK(0): if found
-- ERR.NOT_FOUND(-2): if NOT found
-- ERR.GENERAL(-1): For any other error 
-- =======================================================================
local function treeDelete( src, topRec, ldtCtrl, key )
  local meth = "treeDelete()";
  local rc = 0;

  -- Our list and map has already been validated.  Just use it.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local sp = createSearchPath(ldtMap);
  local status = treeSearch( src, topRec, sp, ldtCtrl, key, nil );

  if (status == ST.FOUND) then
    -- leafDelete() always returns zero.
    leafDelete( src, sp, topRec, ldtCtrl, key );
  else
    rc = ERR.NOT_FOUND;
  end
  return rc;
end -- treeDelete()

local function setPageSize(topRec, ldtMap, pageSize) 
  -- Upper bound of 900kb
  -- Lower bound of 1Kb
  if ((pageSize == nil) or (pageSize == 0)) then
    ldtMap[LS.PageSize] = nil;
    return
  end

  if (pageSize < 1 * 1024) then
    pageSize = 1024;
  elseif (pageSize > aerospike:get_config(topRec, "write-block-size")) then
    pageSize = aerospike:get_config(topRec, "write-block-size") - 1024;
  end
  ldtMap[LS.PageSize] = pageSize;
end

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
-- MaxObjectSize :: the maximum object size (in bytes).
-- MaxKeySize    :: the maximum Key size (in bytes).
-- WriteBlockSize:: The namespace Write Block Size (in bytes)
-- PageSize      :: Targetted Page Size (8kb to 1mb)
-- KeyUnique     :: If key is unique default: Unique
-- ========================================================================
-- ======================================================================
local function compute_settings(topRec, ldtMap, configMap )
  local meth="compute_settings()"
 

  -- Now that all of the values have been validated, we can use them
  -- safely without worry.  No more checking needed.
  local pageSize = configMap.PageSize;

  if (pageSize ~= nil and type(pageSize) ~= "number") then
    warn("[ERROR]<%s:%s> PageSize (%s) is not a number", MOD, meth, tostring(pageSize));
    error(ldte.ERR_INPUT_PARM);
  end
  local writeBlockSize = aerospike:get_config(topRec, "write-block-size");


  if (configMap.KeyUnique ~= nil) then
    if (configMap.KeyUnique == true) then
      ldtMap[LS.KeyUnique]  = AS_TRUE;
    else
      ldtMap[LS.KeyUnique]  = AS_FALSE;
    end
  else 
      ldtMap[LS.KeyUnique]  = AS_TRUE;
  end

  ldtMap[LS.StoreState] = SS_COMPACT; -- start in "compact mode"
  setPageSize(topRec, ldtMap, pageSize);

  return 0;

end 



-- ======================================================================
-- setupLdtBin()
-- Caller has already verified that there is no bin with this name,
-- so we're free to allocate and assign a newly created LDT CTRL
-- in this bin.
-- ALSO:: Caller write out the LDT bin after this function returns.
-- ======================================================================
local function setupLdtBin( topRec, ldtBinName, createSpec, firstValue) 
  local meth = "setupLdtBin()";

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
    local key  = getKeyValue( firstValue);
    configMap.MaxObjectSize = ldt_common.getValSize(firstValue);
    configMap.MaxKeySize = ldt_common.getValSize(key);
  end
  compute_settings(topRec, ldtMap, configMap);

  -- Set up our Bin according to which type of storage we're starting with.
  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    ldtMap[LS.CompactList] = list();
  end

  -- Sets the topRec control bin attribute to point to the 2 item list
  -- we created from InitializeLSetMap() : 
  -- Item 1 :  the property map & Item 2 : the ldtMap
  topRec[ldtBinName] = ldtCtrl; -- store in the record
  record.set_flags( topRec, ldtBinName, BF.LDT_BIN );

  -- NOTE: The Caller will write out the LDT bin.
  return ldtCtrl;
end -- setupLdtBin( topRec, ldtBinName ) 

-- =======================================================================
-- treeMinGet()
-- =======================================================================
-- Get or Take the object that is associated with the MINIMUM (for now, we
-- assume this means left-most) key value.  We've been passed in a search
-- path object (sp) and we use that to look at the leaf and return the
-- first value in the list.
-- =======================================================================
local function treeMinGet( sp, ldtCtrl, take )
  local meth = "treeMinGet()";
  local resultObject;

  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local leafLevel = sp.LevelCount;
  local leafSubRec = sp.RecList[leafLevel]; -- already open from the search.
  local objectList = leafSubRec[LSR_LIST_BIN];
  if( list.size(objectList) == 0 ) then
    warn("[ERROR]<%s:%s> Unexpected Empty List in Leaf", MOD, meth );
    error(ldte.ERR_INTERNAL);
  end

  -- We're here.  Get the minimum Object.  And, if "take" is true, then also
  -- remove it -- and we do that by generating a new list that excludes the
  -- first element.  We assume that the caller will update the SubRec.
  resultObject = objectList[1];
  if ( take ) then
    leafSubRec[LSR_LIST_BIN] = ldt_common.listDelete( objectList, 1 );
  end

  return resultObject;

end -- treeMinGet()

-- =======================================================================
-- treeMin()
-- =======================================================================
-- Drop down to the Left Leaf and then either TAKE or FIND the FIRST element.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) take: True if we are DELETE the MIN (first) item.
-- Result:
-- Success: Object is returned
-- Error: Error Code/String
-- =======================================================================
local function treeMin( topRec,ldtBinName, take )
  local meth = "treeMin()";

  -- Define our return value;
  local resultObject;
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- If our itemCount is ZERO, then quickly return NIL before we get into
  -- any trouble.
  if( propMap[PM.ItemCount] == 0 ) then
    debug("[ATTENTION]<%s:%s> Searching for MIN of EMPTY TREE", MOD, meth );
    return nil;
  end

  G_Filter = ldt_common.setReadFunctions( nil, nil );
  G_FunctionArgs = nil;

  -- Create our subrecContext, which tracks all open SubRecords during
  -- the call.  Then, allows us to close them all at the end.
  local src = ldt_common.createSubRecContext();

  local resultA;
  local resultB;
  local storeObject;

  -- If our state is "compact", just get the first element.
  if( ldtMap[LS.StoreState] == SS_COMPACT ) then 
    -- Do the COMPACT LIST SEARCH
    local objectList = ldtMap[LS.CompactList];
    resultObject = objectList[1];
  else
    -- It's a "regular" Tree State, so do the Tree Operation.
    -- Note that "Left-Most" is a special case, where by using a nil key
    -- we automatically go to the "minimal" position.  We can pull
    -- the value from our Search Path (sp) Object.
    local sp = createSearchPath(ldtMap);
    treeSearch( src, topRec, sp, ldtCtrl, nil, nil );
    -- We're just going to assume there's a MIN found, given that there's a
    -- non-zero tree present.  Any other error will kick out of Lua.
    resultObject = treeMinGet( sp, ldtCtrl, take );
  end -- tree extract

  -- We have either jumped out of here via error() function call, or if
  -- we got this far, then we are supposed to have a valid resultObject.
  return resultObject;
end -- treeMin();

-- =============================================================
-- localDelete()
-- =============================================================
local function localDelete(topRec, ldtCtrl, key, src)

  local rc = 0;
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- If our state is "compact", do a simple list delete, otherwise do a
  -- real tree delete.
  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList = ldtMap[LS.CompactList];
    local resultMap  = searchObjectList( ldtMap, objectList, key );
    if (resultMap.Status == ERR.OK and resultMap.Found) then
      ldtMap[LS.CompactList] =
        ldt_common.listDelete(objectList, resultMap.Position);
    end
  else
    rc = treeDelete(src, topRec, ldtCtrl, key);
  end

  -- update our count statistics if successful
  if (rc >= 0) then 
    local itemCount = propMap[PM.ItemCount];
    propMap[PM.ItemCount] = itemCount - 1; 
    rc = 0;
  end
  return rc;
end

-- ======================================================================
-- localWrite() -- Write a new value into the Ordered List.
-- ======================================================================
-- Insert a value into the list (into the B+ Tree).  We will have both a
-- COMPACT storage mode and a TREE storage mode.  When in COMPACT mode,
-- the root node holds the list directly (Ordered search and insert).
-- When in Tree mode, the root node holds the top level of the tree.
-- ======================================================================
-- ======================================================================
-- Parms:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- (*) topRec:
-- (*) ldtBinName:
-- (*) ldtCtrl: 
-- (*) newValue:
-- (*) update: When true, allow overwrite
-- =======================================================================
local function localWrite(src, topRec, ldtCtrl, newValue, update)
  local meth = "localWrite()";

  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local rc = 0;
  local objectList = ldtMap[LS.CompactList];
  local storeState = ldtMap[LS.StoreState];
  G_Prev = false;
  local valSize = ldt_common.getValSize(newValue);
  if (valSize > G_WriteBlockSize) then
     error(ldte.ERR_CAPACITY_EXCEEDED);
  end

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to turn our single list into a tree.
  if ( ( ldtMap[LS.StoreState] == SS_COMPACT ) and
         (ldt_common.getValSize(ldtMap[LS.CompactList]) 
          + valSize > G_PageSize) ) 
  then
    -- info("Size %d > %d", ldt_common.getValSize(ldtMap[LS.CompactList])
    --        + ldt_common.getValSize(newValue), G_PageSize);
    convertList(src, topRec, ldtCtrl, ldtMap);
  end
 
  local storeState = ldtMap[LS.StoreState];
  -- Do the local insert.
  local key;
  local update_done = false;
  local resultMap;
  local position = 0;

  local insertResult = 0;
  if (storeState == SS_COMPACT) then 
    -- Do the COMPACT LIST INSERT
    key = getKeyValue(newValue);
    local resultMap = searchObjectList(ldtMap, objectList, key);
    local position = resultMap.Position;
    if (resultMap.Status ~= ERR.OK) then
      warn("[Internal ERROR]<%s:%s> Key(%s), List(%s)", MOD, meth,
        tostring( key ), tostring( objectList ) );
      error( ldte.ERR_INTERNAL );
    end
    if (resultMap.Found and ldtMap[LS.KeyUnique] == AS_TRUE) then
      if update then
        ldt_common.listUpdate(objectList, newValue, position);
        update_done = true;
      else
        debug("[ERROR]<%s:%s> Unique Key Violation", MOD, meth );
        error( ldte.ERR_UNIQUE_KEY );
      end
    else
      ldt_common.listInsert(objectList, newValue, position);
      if (rc < 0) then
        warn("[ERROR]<%s:%s> Problems with Insert: RC(%d)", MOD, meth, rc );
        error( ldte.ERR_INTERNAL );
      end
    end
  else
    insertResult = treeInsert(src, topRec, ldtCtrl, newValue, update);
    update_done = insertResult == 1;
  end

  -- update our count statistics, as long as we're not in UPDATE mode.
  if (not update_done and insertResult >= 0) then -- Update Stats if success
    local propMap = ldtCtrl[LDT_PROP_MAP];
    local itemCount = propMap[PM.ItemCount];
    propMap[PM.ItemCount] = itemCount + 1; -- number of valid items goes up
  end

  return 0;
end -- function localWrite()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large List (LLIST) Library Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- See top of file for details.
-- ======================================================================
-- ======================================================================
-- We define a table of functions that are visible to both INTERNAL UDF
-- calls and to the EXTERNAL LDT functions.  We define this table, "lmap",
-- which contains the functions that will be visible to the module.

-- ======================================================================
-- llist.add() -- Insert an element into the ordered list.
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Insert a value into the list (into the B+ Tree).  We will have both a
-- COMPACT storage mode and a TREE storage mode.  When in COMPACT mode,
-- the root node holds the list directly (Ordered search and insert).
-- When in Tree mode, the root node holds the top level of the tree.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) newValue:
-- (*) createSpec:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- =======================================================================
function llist.add (topRec, ldtBinName, newValue, createSpec, src)
  local meth = "llist.add()";
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error(ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  if newValue == nil then
    info("[ERROR]<%s:%s> Input Parameter <value> is bad", MOD, meth);
    error(ldte.ERR_INPUT_PARM);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );

  -- If the record does not exist, or the BIN does not exist, then we must
  -- create it and initialize the LDT map. Otherwise, use it.
  if (ldtCtrl == nil) then
    ldtCtrl = setupLdtBin(topRec, ldtBinName, createSpec, newValue);
  end

  local ldtMap = ldtCtrl[LDT_CTRL_MAP];
  G_Filter   = ldt_common.setReadFunctions(nil, nil);
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end

  -- call localWrite() with UPDATE flag turned OFF.
  rc = localWrite(src, topRec, ldtCtrl, newValue, false);

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end

  return rc;
end -- function llist.add()

-- =======================================================================
-- llist.add_all(): Add each item in valueList to the LLIST.
-- =======================================================================
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) valueList
-- (*) createSpec:
-- Return:
-- On Success:  The number of successful inserts.
-- On Error: Error Code, Error Msg
-- =======================================================================
-- TODO: Convert this to use a COMMON local INSERT() function, not just
-- call llist.add() and do all of its validation each time.
-- =======================================================================
function llist.add_all( topRec, ldtBinName, valueList, createSpec, src )
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end
  local meth = "add_all()";

  if valueList == nil or getmetatable(valueList) ~= List or #valueList == 0 then
    info("[ERROR]<%s:%s> Input Parameter <valueList> is bad", MOD, meth);
    info("[ERROR]<%s:%s>  valueList(%s)", MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  local valListSize = #valueList;
  local firstValue = valueList[1];
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );

  if (ldtCtrl == nil) then
    ldtCtrl = setupLdtBin( topRec, ldtBinName, createSpec, firstValue );
  end

  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  G_Filter = ldt_common.setReadFunctions( nil, nil );
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  
  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end

  local rc = 0;
  local successCount = 0;
  for i = 1, valListSize, 1 do
    rc = localWrite(src, topRec, ldtCtrl, valueList[i], false);
    if (rc < 0) then
      info("[ERROR]<%s:%s> Problem Inserting Item #(%d) [%s] rc=%d", MOD, meth, i,
        tostring( valueList[i] ), rc);
      break;
    else
      successCount = successCount + 1;
    end
  end -- for each value in the list

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end

  return successCount;
end -- llist.add_all()

-- ======================================================================
-- llist.update() -- Insert an element into the ordered list if the
-- element is not already there, otherwise OVERWRITE that element
-- when UNIQUE is turned on.  If UNIQUE is turned off, then simply add
-- another duplicate value.
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Insert a value into the list (into the B+ Tree).  We will have both a
-- COMPACT storage mode and a TREE storage mode.  When in COMPACT mode,
-- the root node holds the list directly (Ordered search and insert).
-- When in Tree mode, the root node holds the top level of the tree.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) newValue:
-- (*) createSpec:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- =======================================================================
function llist.update( topRec, ldtBinName, newValue, createSpec, src )
  local meth = "llist.update()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );
  if (ldtCtrl == nil) then
    ldtCtrl = setupLdtBin( topRec, ldtBinName, createSpec, newValue );
  end

  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end


  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  G_Filter = ldt_common.setReadFunctions( nil, nil );
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  
  rc = localWrite(src, topRec, ldtCtrl, newValue, true);

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end

  return rc;
end -- function llist.update()

-- =======================================================================
-- llist.update_all(): Update each item in valueList in the LLIST.
-- =======================================================================
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) valueList
-- (*) createSpec:
-- Return:
-- Success: Count of successful operations
-- Error:  Error Code, Error Message
-- =======================================================================
function llist.update_all( topRec, ldtBinName, valueList, createSpec, src )
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local meth = "llist.update_all()";
  
  if valueList == nil or getmetatable(valueList) ~= List or #valueList == 0 then
    info("[ERROR]<%s:%s> Input Parameter <valueList> is bad", MOD, meth);
    info("[ERROR]<%s:%s>  valueList(%s)", MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  local valListSize = #valueList;
  local firstValue = valueList[1];

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, false );

  if (ldtCtrl == nil) then
    ldtCtrl = setupLdtBin( topRec, ldtBinName, createSpec, firstValue );
  end

  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end


  if( topRec[ldtBinName] == nil ) then
    setupLdtBin( topRec, ldtBinName, createSpec, firstValue );
  end

  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  G_Filter = ldt_common.setReadFunctions( nil, nil );
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  
  local rc = 0;
  local successCount = 0;
  if( valueList ~= nil and list.size(valueList) > 0 ) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      rc = localWrite(src, topRec, ldtCtrl, valueList[i], true);
      if( rc < 0 ) then
        info("[ERROR]<%s:%s> Problem Updating Item #(%d) [%s]", MOD, meth, i,
          tostring( valueList[i] ));
      else
        successCount = successCount + 1;
      end
    end -- for each value in the list
  else
    warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
      MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end
  
  return successCount;
end -- llist.update_all()

local function localFind(topRec, ldtCtrl, key, src, resultList, keyList, noerror)

  local meth = "localFind()";

  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local rc = 0;
  local resultA;
  local resultB;
  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList    = ldtMap[LS.CompactList];
    local resultMap     = searchObjectList( ldtMap, objectList, key );
    if (resultMap.Status == ERR.OK and resultMap.Found) then
      local position = resultMap.Position;
      resultA, resultB = 
          listScan(objectList, position, ldtMap, resultList, keyList, key, nil, CR.EQUAL);
      if (resultB < 0) then
        warn("[ERROR]<%s:%s> Problems with Scan: Key(%s), List(%s)", MOD, meth,
          tostring( key ), tostring( objectList ) );
        error (ldte.ERR_INTERNAL);
      end
    else
      if (noerror == true) then
        -- Send back key for the non-unique world
        list.append(resultList, nil);
      else
        error (ldte.ERR_NOT_FOUND);
      end
    end
  else
    -- Do the TREE Search
    local sp = createSearchPath(ldtMap);
    rc = treeSearch( src, topRec, sp, ldtCtrl, key, nil );
    if (rc == ST.FOUND) then
      rc = treeScan(src, resultList, keyList, topRec, sp, ldtCtrl, key, nil, CR.EQUAL);
      if (rc < 0 or list.size(resultList) == 0) then
          warn("[ERROR]<%s:%s> Tree Scan Problem: RC(%d) after a good search",
            MOD, meth, rc );
      end
    else
      if (noerror == true) then
        -- Send back key for the non-unique world
        list.append(resultList, nil);
      else
        error (ldte.ERR_NOT_FOUND);
      end
    end
  end -- tree search

end

local function doTake(topRec, ldtBinName, resultList, keyList, src, take) 
  local meth = "doTake()";
  if (take == true) then
    if (G_Filter ~= nil) and (keyList == nil) then
      warn("[ERROR]<%s:%s> Filter function defined but keyList is Not ",
          MOD, meth );
    end
	if (keyList ~= nil) then
      -- KeyList is defined use it to remove
      if (#keyList > 0) then
	    llist.remove_all(topRec, ldtBinName, keyList, src);
      end
    else
      -- no Filter use resultList to remove
      llist.remove_all(topRec, ldtBinName, resultList, src);
    end
  end
end

-- =======================================================================
-- llist.find() - Locate all items corresponding to searchKey
-- =======================================================================
-- Return all objects that correspond to this SINGLE key value.
--
-- Note that a key of "nil" will search to the leftmost part of the tree
-- and then will match ALL keys, so it is effectively a scan.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) value
-- (*) filterModule
-- (*) func:
-- (*) fargs:
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- (*) Success: resultList
-- (*) Error:   error() function is called to jump out of Lua.
-- =======================================================================
-- The find() function can do multiple things. 
-- =======================================================================
function llist.find(topRec, ldtBinName, value, filterModule, filter, fargs, src, take)
  local meth = "llist.find()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Nothing to find in an empty tree
  if (propMap[PM.ItemCount] == 0) then
    return list.new(1);
  end

  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local resultList;
  
  if value ~= nil then
    if (getmetatable(value) == List) then
      resultList = list.new(#value)
    else
      -- Todo change it when non-unique key is supported
      resultList = list.new(1);
    end
  else
    resultList = list.new(propMap[PM.ItemCount]);
  end

  local keyList = nil;
  if (take == true) and (G_Filter ~= nil) then
    keyList = list.new(#resultList);
  end

  -- set up the Read Functions (Filter)
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  G_FunctionArgs = fargs;

  if getmetatable(value) == List then
    if (list.size(value) > 0) then
      local listSize = list.size(value)
      for i = 1, listSize, 1 do
         local key = getKeyValue( value[i]);
         localFind(topRec, ldtCtrl, key, src, resultList, keyList, true)
      end -- tree search
    else
      warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
        MOD, meth, tostring(value));
      error(ldte.ERR_INPUT_PARM);
    end
  else
    local key = getKeyValue( value);
    localFind(topRec, ldtCtrl, key, src, resultList, keyList, false)
  end

  doTake(topRec, ldtBinName, resultList, keyList, src, take);

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if (rc < 0) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  return resultList;
end -- function llist.find() 

-- =======================================================================
-- llist.find_first(topRec,ldtBinName, count, src)
-- =======================================================================
-- Find the FIRST N elements of the ordered List.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) count
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- (*) Success: resultList
-- (*) Error:   error() function is called to jump out of Lua.
-- =======================================================================
function llist.find_first( topRec, ldtBinName, count, filterModule, filter, fargs, src, take )
  local meth = "llist.find_first()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  -- validate the "count" parameter, and make sure it is within range.  If it
  -- is greater than the LDT ItemCount, then adjust.
  if count == nil or type(count) ~= "number" then
    warn("[PARM ERROR]<%s:%s> invalid count parameter(%s)", MOD, meth,
      tostring(count));
    error(ldte.ERR_INPUT_PARM);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
 
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Nothing to find in an empty tree
  if (propMap[PM.ItemCount] == 0) then
    debug("[NOTICE]<%s:%s> EMPTY LLIST: Result is EMPTY LIST", MOD, meth);
    return list();
  end

  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local itemCount = propMap[PM.ItemCount];
  local readAmount;
  if count > itemCount or count <= 0 then
    readAmount = itemCount;
  else
    readAmount = count;
  end

  -- Define our return (result) list to match the expected amount.
  local resultList = list.new(readAmount);
  
  -- Set up the read functions (transform/untransform) if present.
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  G_FunctionArgs = fargs;
  
  local keyList = nil;
  if (take == true) and (G_Filter ~= nil) then
    keyList = list.new(#resultList);
  end

  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList = ldtMap[LS.CompactList];
    leftListScan( objectList, ldtMap, resultList, keyList, readAmount );
  else
    leftToRightTreeScan( src, resultList, keyList, topRec, ldtCtrl, count);
  end

  doTake(topRec, ldtBinName, resultList, keyList, src, take);

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if (rc < 0) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  return resultList;
end -- function llist.find_first() 

-- =======================================================================
-- llist.find_last(topRec,ldtBinName, count, src)
-- =======================================================================
-- Find the LAST N elements of the ordered List.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) count
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- (*) Success: resultList
-- (*) Error:   error() function is called to jump out of Lua.
-- =======================================================================
function llist.find_last( topRec, ldtBinName, count, filterModule, filter, fargs, src, take)
  local meth = "llist.find_last()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  -- validate the "count" parameter, and make sure it is within range.  If it
  -- is greater than the LDT ItemCount, then adjust.
  if count == nil or type(count) ~= "number" then
    warn("[PARM ERROR]<%s:%s> invalid count parameter(%s)", MOD, meth,
      tostring(count));
    error(ldte.ERR_INPUT_PARM);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
 
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  local itemCount = propMap[PM.ItemCount];
  -- Nothing to find in an empty tree
  if (itemCount == 0) then
    debug("[NOTICE]<%s:%s> EMPTY LLIST: Result is EMPTY LIST", MOD, meth);
    return list();
  end

  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local readAmount;
  if count > itemCount or count <= 0 then
    readAmount = itemCount;
  else
    readAmount = count;
  end

  -- Define our return (result) list to match the expected amount.
  local resultList = list.new(readAmount);
  
  -- Set up the read functions (transform/untransform) if present.
  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  local keyList = nil;
  if (take == true) and (G_Filter ~= nil) then
    keyList = list.new(#resultList);
  end

  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList = ldtMap[LS.CompactList];
    rightListScan( objectList, ldtMap, resultList, keyList, readAmount );
  else
    rightToLeftTreeScan( src, resultList, keyList, topRec, ldtCtrl, count);
  end

  doTake(topRec, ldtBinName, resultList, keyList, src, take);

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if (rc < 0) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  return resultList;
end -- function llist.find_last() 

local function localExists(topRec, ldtCtrl, key, src)
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local found = false;
  local rc = 0;
  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList    = ldtMap[LS.CompactList];
    local resultMap     = searchObjectList( ldtMap, objectList, key );
    if (resultMap.Status == ERR.OK and resultMap.Found) then
      found = true;
    else
      found = false;
    end
  else
    local sp = createSearchPath(ldtMap);
    rc = treeSearch( src, topRec, sp, ldtCtrl, key, nil );
    if (rc == ST.FOUND) then
      found = true;
    else
      found = false;
    end
  end 
  return found;
end
-- =======================================================================
-- llist.exists()
-- =======================================================================
-- Return 1 if the value (object/key) exists, otherwise return 0.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) value
-- (*) src
-- Result:
-- (*) 1 if value exists, 0 if it does not.
-- (*) Error:   error() function is called to jump out of Lua.
-- =======================================================================
function llist.exists(topRec, ldtBinName, value, src)
  local meth = "llist.exists()";
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  local propMap = ldtCtrl[LDT_PROP_MAP];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  -- Nothing to find in an empty tree
  if (propMap[PM.ItemCount] == 0) then
    return 0;
  end

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  local res;
  if (getmetatable(value) == List) then
    if (list.size(value) > 0) then
      res = list.new(#value);
      local listSize = list.size(value)
      for i = 1, listSize, 1 do
         local key = getKeyValue( value[i]);
         if (localExists(topRec, ldtCtrl, key, src)) then
           list.append(res, 1)  
         else
           list.append(res, 0)  
         end
      end -- tree search
    else
      warn("[ERROR]<%s:%s> Invalid Input Value List(%s)",
        MOD, meth, tostring(value));
      error(ldte.ERR_INPUT_PARM);
    end
  else
    res = list.new(1);
    if (localExists(topRec, ldtCtrl, value, src)) then
      list.append(res, 1);
    else
      list.append(res, 0);
    end
  end

  -- Close ALL of the subrecs that might have been opened
  rc = ldt_common.closeAllSubRecs( src );
  if (rc < 0) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  return res;
end -- llist.exists()

-- =======================================================================
-- llist.range() - Locate all items in the range of minKey to maxKey.
-- =======================================================================
-- Do the initial search to find minKey, then perform a scan until maxKey
-- is found.  Return all values that pass any supplied filters.
-- If minKey is null -- scan starts at the LEFTMOST side of the list or tree.
-- If maxKey is null -- scan will continue to the end of the list or tree.
-- Parms:
-- (*) topRec: The Aerospike Top Record
-- (*) ldtBinName: The Bin of the Top Record used for this LDT
-- (*) minKey: The starting value of the range: Nil means neg infinity
-- (*) maxKey: The end value of the range: Nil means infinity
-- (*) filterModule: The module possibly holding the user's filter
-- (*) filter: the optional predicate filter
-- (*) fargs: Arguments to the filter
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Result:
-- Success: resultList holds the result of the range query
-- Error: Error string to outside Lua caller.
-- =======================================================================
function
llist.range(topRec, ldtBinName, minKey, maxKey, count, filterModule, filter, fargs, src, take)
  local meth = "llist.range()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  -- if maxKey is null count should be a number
  if (maxKey == nil and type(count) ~= "number") then
    warn("[ERROR]<%s:%s> Range Lookup with invalid Parameter maxKey(%s), count(%s)",
        MOD, meth, tostring( maxKey ), tostring( count ));
    error(ldte.ERR_INPUT_PARM);
  end
  
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  local propMap = ldtCtrl[LDT_PROP_MAP];

  local itemCount = propMap[PM.ItemCount];

  local resultList;

  if (count ~= nil) then
    if count > itemCount or count <= 0 then
      resultList = list.new(itemCount);
    else
      resultList = list.new(count);
    end
  else
    resultList = list.new(50, 100);
  end

  G_Filter = ldt_common.setReadFunctions( filterModule, filter );
  G_FunctionArgs = fargs;
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );
  --info("pagesize %s", tostring(G_PageSize));
  
  local keyList;
  if (take == true) and (G_Filter ~= nil) then
    keyList = list.new(#resultList); 
  end

  -- Create our subrecContext, which tracks all open SubRecords during
  -- the call.  Then, allows us to close them all at the end.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local resultA; -- instruction: stop or keep scanning
  local resultB; -- Result: 0: ok,  < 0: error.
  local position;-- location where the item would be (found or not)

  if (ldtMap[LS.StoreState] == SS_COMPACT) then 
    local objectList = ldtMap[LS.CompactList];
    local resultMap  = searchObjectList( ldtMap, objectList, minKey );
    position         = resultMap.Position;

    resultA, resultB = 
        listScan(objectList, position, ldtMap, resultList, keyList, maxKey, count, CR.GREATER_THAN);
    if (resultB < 0) then
      warn("[ERROR]<%s:%s> Problems with Scan: MaxKey(%s), List(%s)", MOD,
        meth, tostring( maxKey ), tostring( objectList ) );
      error( ldte.ERR_INTERNAL );
    end

  else
    local sp = createSearchPath(ldtMap);
    rc = treeSearch(src, topRec, sp, ldtCtrl, minKey, nil);
    rc = treeScan(src, resultList, keyList, topRec, sp, ldtCtrl, maxKey, count, CR.GREATER_THAN);
    if (rc < 0 or list.size( resultList ) == 0) then
        -- it's ok to find nothing
    end
  end -- tree search
  
  doTake(topRec, ldtBinName, resultList, keyList, src, take);

  rc = ldt_common.closeAllSubRecs( src );
  if (rc < 0) then
    warn("[EARLY EXIT]<%s:%s> Problem closing subrec in search", MOD, meth );
    error( ldte.ERR_SUBREC_CLOSE );
  end

  return resultList;
end -- function llist.range() 

-- =======================================================================
-- scan(): Return all elements (no filter).
-- =======================================================================
-- Return:
-- Success: the Result List.
-- Error: Error String to outer Lua Caller (long jump)
-- =======================================================================
function llist.scan( topRec, ldtBinName, src )
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end
  return llist.find( topRec, ldtBinName, nil, nil, nil, nil, src );
end -- llist.scan()

-- =======================================================================
-- filter(): Pass all elements thru the filter and return all that qualify.
-- =======================================================================
-- Do a full scan and pass all elements thru the filter, returning all
-- elements that match.
-- Return:
-- Success: the Result List.
-- Error: error()
-- =======================================================================
function llist.filter (topRec, ldtBinName, filterModule, filter, fargs, src)
  return llist.scan(topRec, ldtBinName, filterModule, filter, fargs, src);
end -- llist.filter()


-- ======================================================================
-- llist.remove() -- remove the item(s) corresponding to key.
-- ======================================================================
-- Delete the specified item(s).
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) LdtBinName
-- (3) value: The value/key we'll search for
-- (4) src: Sub-Rec Context - Needed for repeated calls from caller
-- ======================================================================
function llist.remove( topRec, ldtBinName, value, src )
  local meth = "llist.remove()";
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end
  
  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  G_Filter = ldt_common.setReadFunctions(nil, nil);
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  local key = getKeyValue(value);
  
  rc = localDelete(topRec, ldtCtrl, key, src)

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end
  return rc;
end -- function llist.remove()

-- =======================================================================
-- llist.remove_all(): Remove each item in valueList from the LLIST.
-- =======================================================================
-- Parms:
--    topRec:     The user-level record holding the LDT Bin
--    ldtBinName: The name of the LDT Bin
--    valueList:  List of keys to be deleted
--    src:        Sub-Rec Context - Needed for repeated calls from caller
-- RETURN:
--    SUCCESS:    Number of items removed
--    Error:      Error using error ()
-- =======================================================================
function llist.remove_all( topRec, ldtBinName, valueList, src )
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local meth = "remove_all()";

  if valueList == nil or getmetatable(valueList) ~= List or #valueList == 0 then
    info("[ERROR]<%s:%s> Input Parameter <valueList> is bad", MOD, meth);
    info("[ERROR]<%s:%s>  valueList(%s)", MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end
  local valListSize = #valueList;
  
  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- Extract the property map and control map from the ldt bin list.
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  G_Filter = ldt_common.setReadFunctions(nil, nil);
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  rc = 0;
  local itemRemoved = 0;
  if (valueList ~= nil and list.size(valueList) > 0) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      local key = getKeyValue(valueList[i]);
      rc = localDelete(topRec, ldtCtrl, key, src)
      if (rc >= 0) then
        itemRemoved = itemRemoved + 1;
      end
    end -- for each value in the list
  else
    warn("[ERROR]<%s:%s> Invalid Delete Value List(%s)",
      MOD, meth, tostring(valueList));
    error(ldte.ERR_INPUT_PARM);
  end

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end
  return itemRemoved;
end -- llist.remove_all()


-- =======================================================================
-- llist.remove_range(): Remove all items in the given range
-- =======================================================================
-- Perform a range query, and if any values are returned, remove each one
-- of them individually.
-- Parms:
--    topRec:
-- (*) ldtBinName:
-- (*) valueList
-- Return:
-- Success: Count of values removed
-- Error:   Error Code and message
-- =======================================================================
function llist.remove_range (topRec, ldtBinName, minKey, maxKey, src)
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  if (minKey > maxKey) then 
    error(ldte.ERR_INPUT_PARM);
  end 

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end

  local valueList  = llist.range( topRec, ldtBinName, minKey, maxKey,
                                  nil, nil, nil, nil, src);
  local deleteCount = 0;
  
  local ldtCtrl = topRec[ ldtBinName ];
  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];
  
  G_Filter = ldt_common.setReadFunctions(nil, nil);
  G_PageSize, G_WriteBlockSize = ldt_common.getPageSize( topRec, ldtMap[LS.PageSize] );

  local rc = 0;
  if (valueList ~= nil and list.size(valueList) > 0) then
    local listSize = list.size( valueList );
    for i = 1, listSize, 1 do
      local key = getKeyValue(valueList[i]);
      rc = localDelete(topRec, ldtCtrl, key, src);
      if (rc >= 0) then
        deleteCount = deleteCount + 1;
      end
    end -- for each value in the list
  end

  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, src, rc);
  end
  
  return deleteCount;
end -- llist.remove_range()

-- ========================================================================
-- llist.destroy(): 
-- Remove the LDT entirely from the record. Release all of the storage 
-- associated with this LDT and remove the control structure of the bin.
-- If this is the LAST LDT in the record, then ALSO remove the HIDDEN LDT 
-- CONTROL BIN.
--
-- Parms:
--    topRec:     The user-level record holding the LDT Bin
--    ldtBinName: The name of the LDT Bin
--    src:        Sub-Rec Context - Needed for repeated calls from caller
-- Result:
--    0: all is well
--   -1: The Error code via error() call
-- ========================================================================
-- NOTE: This could eventually be moved to COMMON, and be "ldt_destroy()",
-- since it will work the same way for all LDTs.
-- Remove the ESR, Null out the topRec bin.
-- ========================================================================
function llist.destroy( topRec, ldtBinName, src)
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end

  ldt_common.destroy( src, topRec, ldtBinName, ldtCtrl );
  return 0;
end -- llist.destroy()

-- ========================================================================
-- llist.size() -- return the number of elements (item count) in the set.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   SUCCESS: The number of elements in the LDT
--   ERROR: The Error code via error() call
-- ========================================================================
function llist.size( topRec, ldtBinName )
  local meth = "llist.size()";

  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  local propMap = ldtCtrl[LDT_PROP_MAP];
  return propMap[PM.ItemCount];
end -- llist.size()

-- ========================================================================
-- llist.config() -- return the config settings
-- Parms:
--   topRec: the user-level record holding the LDT Bin
--   ldtBinName: The name of the LDT Bin
-- Result:
--   SUCCESS: The MAP of the config.
--   ERROR: The Error code via error() call
-- ========================================================================
function llist.config( topRec, ldtBinName )
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end
  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );
  local config = ldtSummary( ldtCtrl );

  return config;
end -- llist.config()

-- ========================================================================
-- llist.setPageSize() -- set the pageSize
-- Parms:
--   topRec: the user-level record holding the LDT Bin
--   ldtBinName: The name of the LDT Bin
--   pageSize: Page Size to be set
-- Result:
--   SUCCESS: In case the config successfully set
--   ERROR: In case the value is invalid or beyond bounds
-- ========================================================================
function llist.setPageSize( topRec, ldtBinName, pageSize )
  local meth = "llist.setPageSize()";
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  local ldtCtrl = validateRecBinAndMap( topRec, ldtBinName, true );

  -- If the record does not exist, or the BIN does not exist, then we must
  -- create it and initialize the LDT map. Otherwise, use it.
  if (topRec[ldtBinName] == nil) then
    return 0;
  end

  local ldtMap  = ldtCtrl[LDT_CTRL_MAP];

  setPageSize(topRec, ldtMap, pageSize);
  if (rc == 0) then
    topRec[ldtBinName] = ldtCtrl;
    record.set_flags(topRec, ldtBinName, BF.LDT_BIN) --Must set every time
    rc = ldt_common.commit(topRec, ldtBinName, nil, rc);
  end

  return rc;
end -- function llist.setPageSize()

-- ========================================================================
-- llist.ldt_exists() --
-- Parms:
--   topRec: the user-level record holding the LDT Bin
--   ldtBinName: The name of the LDT Bin
-- Result:
--   1: LLIST exists in this bin
--   0: LLIST does NOT exist in this bin
-- ========================================================================
function llist.ldt_exists (topRec, ldtBinName)
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  if ldt_common.ldt_exists(topRec, ldtBinName, LDT_TYPE ) then
    return 1
  else
    return 0
  end
end -- llist.ldt_exists

-- ========================================================================
-- llist.dump(): Debugging/Tracing mechanism -- show the WHOLE tree.
-- ========================================================================
function llist.dump (topRec, ldtBinName, src)
  local rc = aerospike:set_context( topRec, UDF_CONTEXT_LDT );
  if (rc ~= 0) then
    error( ldte.ERR_NS_LDT_NOT_ENABLED);
  end

  if (src == nil) then
    src = ldt_common.createSubRecContext();
  end
  printTree( src, topRec, ldtBinName );
  return 0;
end -- llist.dump()


-- ==> Define all functions before this end section.
return llist;

