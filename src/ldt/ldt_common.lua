-- Large Data Type (LDT) Common Functions
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
local MOD="ldt_common_2014_12_20.A";

local GP;      -- Global Print/Debug Instrument
local F=false; -- Set F (flag) to true to turn ON global print

-- We need this for being able to figure out the exact type of USERDATA
-- objects.
local Map = getmetatable( map() );
local List = getmetatable( list() );
local Bytes = getmetatable( bytes() );

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LDT COMMON Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following functions were moved into common for two main reasons:
-- (1) Mandatory: All of the LDT routines must use the following
--     functions in the same way with the same function:
--     + Sub-Rec Context Functions
--     + Key, Read, Write Functions (those using inner UDFs)
--     + Propery Map Management
-- (2) Convenience: We know that many of the LDT routines need some common
--     support for Lists (search, insert, delete, summarize) and Common 
--     Object summary.  This Convenience list may grow over time.
-- ======================================================================
local ldt_common = {};
-- ======================================================================

-- ++==================++
-- || External Modules ||
-- ++==================++
-- Common LDT functions that are used by ALL of the LDTs.
-- local LDTC = require('ldt/ldt_common');
local ldte=require('ldt/ldt_errors');

-- ++==================++
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ++==================++
local MAGIC="MAGIC";     -- the magic value for Testing LSTACK integrity

-- Aerospike has a legacy issue -- a very long time ago it picked 16 bytes
-- as the buffer size limit for a bin name.  So, the name, plus a type
-- byte and a null terminator must fit in 16 bytes, which means that the
-- bin cant' be longer than 14 characters.
local AS_BIN_NAME_LIMIT = 14;

-- AS_BOOLEAN TYPE:
-- NB: No easy boolean in lua pick chars
local AS_TRUE='T';    
local AS_FALSE='F';

-- Record Types -- Must be numbers, even though we are eventually passing
-- in just a "char" (and int8_t).
-- NOTE: We are using these vars for TWO purposes -- and I hope that doesn't
-- come back to bite me.
-- (1) As a flag in record.set_type() -- where the index bits need to show
--     the TYPE of record (CDIR NOT used in this context)
-- (2) As a TYPE in our own propMap[PM.RecType] field: CDIR *IS* used here.
local RT_REG = 0; -- 0x0: Regular Record (Here only for completeneness)
local RT_LDT = 1; -- 0x1: Top Record (contains an LDT)
local RT_SUB = 2; -- 0x2: Regular Sub Record (Anything other than ESR)
local RT_ESR = 4; -- 0x4: Existence Sub Record

-- Bin Flag Types -- to show the various types of bins.
-- NOTE: All bins will be labelled as either (1:RESTRICTED OR 2:HIDDEN)
-- We will not currently be using "Control" -- that is effectively HIDDEN
local BF_LDT_BIN     = 1; -- Main LDT Bin (Restricted)
local BF_LDT_HIDDEN  = 2; -- LDT Bin::Set the Hidden Flag on this bin
local BF_LDT_CONTROL = 4; -- Main LDT Control Bin (one per record)

-- Our Dirty Map has two settings:  Dirty and Busy.
-- Dirty means that it has been written, and thus cannot be closed.
-- Busy means that it is read-only, but currently in use and cannot be closed.
local DM_DIRTY = 'D';
local DM_BUSY  = 'B';

-- We maintain a pool, or "context", of sub-records that are open.  That allows
-- us to look up subRecs and get the open reference, rather than bothering
-- the lower level infrastructure.  There's also a limit to the number
-- of open subRecs.
local G_OPEN_SR_LIMIT = 4000;

-- In order to keep the Sub-Rec Pool somewhat well-behaved, we will clean
-- (remove the clean, non-busy, read-only sub-recs) the pool periodically.
-- We'll start at 100, but if we clean and do NOT release any
-- sub-recs, then we must adjust our threshold higher so that we don't
-- thrash.  We'll keep bumping up the clean threshold until we hit the
-- real SR Limit.
local G_OPEN_SR_CLEAN_THRESHOLD = 100;

-- When the user wants to override the default settings, or register some
-- functions, the user module with the "adjust_settings" function will be
-- used.
local G_SETTINGS = "adjust_settings";

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

-- All LDT subRecords have a properties bin that holds a map that defines
-- the specifics of the record and the LDT.
-- NOTE: Even the TopRec has a property map -- but it's stashed in the
-- user-named LDT Bin
-- >> (14 char name limit) 12345678901234 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
local SUBREC_PROP_BIN   = "SR_PROP_BIN";

-- Each LDT Flavor (stack, list, map, set) has its own SubRec bins
-- that are specific to the needs of the type.  They are not common.
--
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- There are four main Record Types used in the LSTACK Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSTACK bin
-- (*) EsrRec: The Existence SubRecord (ESR) that coordinates all child
--             subRecs for a given LDT.
-- (*) LdrRec: the LDT Data Record (LDR) that holds user Data.
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
--     is now a list, where ldtCtrl[1] will always be the propMap and
--     ldtCtrl[2] will always be the ldtMap.  In the server code, using "C",
--     we will sometimes read the ldtCtrl[1] (the property map) in order to
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
  LdtCount             = 'C';  -- Number of LDTs in this rec
  VInfo                = 'V';  -- Partition Version Info
  Magic                = 'Z';  -- Special Sauce
  SelfDigest           = 'D';  -- Digest of this record
};
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT specific Property Map (PM) Fields: One PM per LDT bin:
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local PM = {
  ItemCount             = 'I'; -- (Top): # of items in LDT
  SubRecCount           = 'S'; -- (Top): # of subRecs in the LDT
  Version               = 'V'; -- (Top): Code Version
  LdtType               = 'T'; -- (Top): Type: stack, set, map, list
  BinName               = 'B'; -- (Top): LDT Bin Name
  Magic                 = 'Z'; -- (All): Special Sauce
  CreateTime            = 'C'; -- (All): Creation time of this rec
  RecType               = 'R'; -- (All): Type of Rec:Top,Ldr,Esr,CDir
  EsrDigest             = 'E'; -- (All): Digest of ESR
  ParentDigest          = 'P'; -- (SubRec): Digest of TopRec
  SelfDigest            = 'D'; -- (SubRec): Digest of THIS Record
}

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- LDT Map Fields Common to ALL LDTs (managed by the LDT COMMON routines)
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
local LC = {
  -- Fields Common to ALL LDTs (managed by the LDT COMMON routines)
  UserModule             = 'P'; -- User's Lua file for overrides
  StoreLimit             = 'L'; -- Max Items: Used for Eviction (eventually)
};

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

-- -----------------------------------------------------------------------
-- setReadFunctions()()
-- -----------------------------------------------------------------------
-- We follow this hierarchical lookup pattern for the read filter function:
-- (*) User Supplied Module (might be different from create module)
-- (*) Create Module
--
-- (*) Create Module
-- Parms:
-- (*) ldtMap:
-- (*) userModule:
-- (*) filter:
-- RETURN: L_Filter, is assigned to G_Filter
-- -----------------------------------------------------------------------
function ldt_common.setReadFunctions(userModule, filter )
  local meth = "setReadFunctions()";

  local L_Filter = nil;
  local userModuleRef;   -- Hold the imported user module table
  local createModuleRef; -- Hold the imported create module table
  
  if( filter ~= nil ) then
    if( type(filter) ~= "string" or filter == "" ) then
      info("[WARNING]<%s:%s> Bad filter Name: type(%s) filter(%s)",
        MOD, meth, type(filter), tostring(filter) );
      error( ldte.ERR_FILTER_BAD );
    else
      if (userModule ~= nil and type(userModule) == "string" ) then
        userModuleRef = require(userModule);
        if (userModuleRef ~= nil and userModuleRef[filter] ~= nil) then
          L_Filter = userModuleRef[filter];
        end
      end

      -- If we didn't find anything, BUT the user supplied a function name,
      -- then we have a problem.  We have to complain.
      if (L_Filter == nil) then
        info("[WARNING]<%s:%s> filter not found: type(%s) filter(%s)",
          MOD, meth, type(filter), tostring(filter) );
        error( ldte.ERR_FILTER_NOT_FOUND );
      end
    end
  end -- if filter not nil
  return L_Filter;
end -- setReadFunctions()


-- ======================================================================
-- <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS> - <USER FUNCTIONS>
-- ======================================================================


-- ======================================================================
-- propMapSummary( resultMap, propMap )
-- ======================================================================
-- Add the propMap properties to the supplied resultMap.
-- ======================================================================
function ldt_common.propMapSummary( resultMap, propMap )

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


-- =============================
-- Begin SubRecord Function Area
-- =============================
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
-- status = record.set_ttl( topRec, ttl )
-- ======================================================================
-- Notes on the SubRec functions:
-- (*) The underlying Aerospike SubRec mechanism actually manages most
--     aspects of SubRecs:  
--     + Update of dirty subRecs is automatic at Lua Context Close.
--     + Close of all subRecs is automatic at Lua Context Close.
-- (*) We cannot close a dirty subRec explicit (we can make the call, but
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
-- SUB RECORD CONTEXT DESIGN NOTE:
-- All "outer" functions, will employ the "subrecContext" object, which
-- will hold all of the subRecords that were opened during processing. 
-- Note that some operations can potentially involve many subRec
-- operations -- and can also potentially revisit pages.
--
-- SubRecContext Design:
-- The key will be the DigestString, and the value will be the subRec
-- pointer.  At the end of an outer call, we will iterate thru the subRec
-- context and close all open subRecords.  Note that we may also need
-- to mark them dirty -- but for now we'll update them in place (as needed),
-- but we won't close them until the end.
-- ======================================================================
function ldt_common.createSubRecContext()
  local meth = "createSubRecContext()";

  local srcCtrl  = {};
  local recMap   = {};
  local dirtyMap = {};
  recMap.ItemCount = 0;
  recMap.CleanThreshold = G_OPEN_SR_CLEAN_THRESHOLD;
  srcCtrl[1] = recMap; -- recMap
  srcCtrl[2] = dirtyMap; -- dirtyMap
  return srcCtrl;
end -- createSubRecContext()

-- ======================================================================
-- cleanSRC(): Clean the Sub-Rec Context, flushing any clean Sub-Recs
-- so that we can have more slots to read new Sub-Recs.
-- We expect that we may have MANY open clean pages (from things like
-- an LDT Scan), but we should NOT have over 20 dirty Sub-Recs.
-- ======================================================================
local function cleanSRC( srcCtrl )
  local meth = "cleanSRC()";

  local recMap   = srcCtrl[1];
  local dirtyMap = srcCtrl[2];

  -- Iterate thru the SubRecContext and close all CLEAN Sub-Records.
  -- Keep track of the count as we close them.
  local digestString;
  local subRec;
  local rc = 0;
  local closeCount = 0;
  for name, value in pairs( recMap ) do
    if name and value and (name ~= "ItemCount") and (name ~= "CleanThreshold") then
      if (type(name) == "string") then
        digestString = name;
        subRec = value;

        -- We'll assume it's a digest, since it shouldn't be anything else.

        -- If this guy is CLEAN (not dirty) then we can close it and free up
        -- a slot for a NEW Sub-Rec to come in.
        local state = dirtyMap[digestString];
        if (state ~= DM_DIRTY and state ~= DM_BUSY ) then
          rc = aerospike:close_subrec( subRec );
          if( rc ~= nil and rc < 0 ) then
            warn("[ERROR]<%s:%s> Error closing SubRec: rc(%s) Digest(%s)",
              MOD, meth, tostring(rc), tostring( digestString ));
          else
            -- We're ok.  It's closed.  Decrement the count and free up the
            -- slot.

            -- Try out our NEW map Remove Function. Note that we no longer
            -- remove map entrys by assigning "nil".
            dirtyMap[digestString] = nil;
            recMap[digestString] = nil;

            local itemCount = recMap.ItemCount;
            recMap.ItemCount = itemCount - 1;
            closeCount = closeCount + 1;
          end
        end -- else we have something to close
      end -- it's the right type for a Digest Field.
    elseif (not name or not value) then
      warn("[INTERNAL ERROR]<%s:%s> Name(%s) Val(%s) NIL Problem", MOD, meth,
        tostring(name), tostring(value));
    end -- if both name and value NOT NIL
  end -- for all fields in SRC
end -- cleanSRC()

-- ======================================================================
-- Given an already opened subRec (probably one that was recently created),
-- add it to the subRec context.  As assume, for now, that the SubRec
-- is already Dirty (most will be).
-- Parms:
-- (*) srcCtrl: The
-- (*) digest: The actual digest of the record.  We will "stringify" it.
-- (*) dirty: Boolean that says if this SubRec is already Dirty.
-- ======================================================================
function ldt_common.addSubRecToContext( srcCtrl, subRec, dirty )
  local meth = "addSubRecContext()";

  if (srcCtrl == nil) then
    warn("[ERROR]<%s:%s> Bad SubRec Context: SRC is NIL", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];

  local digest = record.digest( subRec );
  local digestString = tostring( digest );
  recMap[digestString] = subRec;
  if( dirty ~= nil and dirty == true ) then
    dirtyMap[digestString] = DM_DIRTY;
  else
    dirtyMap[digestString] = DM_BUSY;
  end

  local itemCount = recMap.ItemCount;
  recMap.ItemCount = itemCount + 1;
  return 0;
end -- addSubRecToContext()

-- ======================================================================
-- Create and Init ESR
-- ======================================================================
-- The Existence SubRecord is the synchronization point for the lDTs that
-- have multiple records (one top rec and many children).  It's a little
-- like the baby sitter for the children -- it helps keeps track of them.
-- And, when the ESR is gone, we kill the children. (BRUA-HAHAHAH!!!)
--
-- All LDT Sub-Recs have a properties bin that describes the Sub-Rec.  This
-- bin contains a map that is "un-msg-packed" by the C code on the server
-- and read.  It must be the same for all LDT recs.
--
-- ======================================================================
function ldt_common.createAndInitESR(srcCtrl, topRec, ldtCtrl )
  local meth = "createAndInitESR()";

  local rc = 0;

  local esrRec    = aerospike:create_subrec( topRec );
  if (esrRec == nil) then
    warn("[ERROR]<%s:%s> Problems Creating ESR", MOD, meth );
    error( ldte.ERR_SUBREC_CREATE );
  end

  -- Set the record type as "ESR"
  record.set_type( esrRec, RT_ESR );

  local esrDigest = record.digest( esrRec);
  local topDigest = record.digest( topRec );
  local topPropMap = ldtCtrl[1];

  -- Initialize the PropertyMap in the new ESR
  local esrPropMap = map();
  topPropMap[PM.EsrDigest]    = esrDigest;
  esrPropMap[PM.EsrDigest]    = esrDigest;
  esrPropMap[PM.RecType]      = RT_ESR;
  esrPropMap[PM.Magic]        = MAGIC;
  esrPropMap[PM.ParentDigest] = topDigest;
  esrPropMap[PM.SelfDigest]   = esrDigest;

  -- Even though the ESR is really IMPLICIT, we'll add it to the total
  -- SubRec Count.  Also, this SHOULD be the FIRST SubRec, and as a result,
  -- the SubRec count might be nil.  Double check that.
  local subRecCount = topPropMap[PM.SubRecCount];
  if (subRecCount == nil) then
    subRecCount = 0;
  end
  topPropMap[PM.SubRecCount] = subRecCount + 1;

  -- NOTE: We have to make sure that the TopRec propMap also gets saved.
  esrRec[SUBREC_PROP_BIN] = esrPropMap;
  
  rc = aerospike:update_subrec( esrRec );
  --  Note that update_subrec() returns nil on success
  if rc then
    warn("[ERROR]<%s:%s> SubRec(%s) rc(%s)", MOD, meth,
      tostring(esrDigest), tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  -- Add this open ESR SubRec to our SubRec Context, which implicitly 
  -- marks it as dirty.
  ldt_common.addSubRecToContext( srcCtrl, esrRec, true );

  return esrDigest;

end -- createAndInitESR()

-- ======================================================================
-- createSubRec(): 
-- ======================================================================
-- Create and initialize a new SubRec. This function covers the general
-- function needed for ALL subRecs.  Any specific data management, such
-- as use-specific items (e.g. LStack Cold Dir, LList Root Node) are 
-- handled by the caller.  We just do the common stuff here, and we also
-- plug the new SubRec into the SRC.
-- ======================================================================
function ldt_common.createSubRec( srcCtrl, topRec, ldtCtrl, recType )
  local meth = "createSubRec()";

  -- We have a global limit on the number of subRecs that we can have
  -- open at a time.  If we're at (or above) the limit, then we must
  -- exit with an error (better here than in the subRec code).
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  local itemCount = recMap.ItemCount;
  local cleanThreshold = recMap.CleanThreshold;
  local rc = 0;

  -- Access the TopRec Control Maps
  local propMap = ldtCtrl[1];
  local ldtMap = ldtCtrl[2];

  -- Set up the SubRec Control Maps
  local subRecPropMap = map();

  local esrDigest;

  -- If this is the FIRST SubRec (i.e. There is NOT already an ESR), then
  -- we will create the ESR first, then create this first SubRec.
  -- There is one ESR created per LMAP bin, not per Sub-Rec.
  if( propMap[PM.EsrDigest] == nil or propMap[PM.EsrDigest] == 0 ) then
    esrDigest = ldt_common.createAndInitESR( srcCtrl, topRec,ldtCtrl);
    propMap[PM.EsrDigest] = esrDigest;
  else
    esrDigest = propMap[PM.EsrDigest];
  end
  subRecPropMap[PM.EsrDigest] = esrDigest;

  -- Check our counts -- if we don't have room for another open rec,
  -- we should try a "CLEAN" to free up some slots.  If that fails, then
  -- we're screwed.  Notice that "createESR" doesn't need to check the
  -- counts because by definition it's the FIRST SUB-REC.
  if (itemCount >= cleanThreshold) then
    cleanSRC( srcCtrl ); -- Flush the clean pages.  Ignore errors.

    -- Reset the local var after clean
    itemCount = recMap.ItemCount;

    -- If we were unable to release any pages with clean, then UP our
    -- clean threshold until we hit the real limit.
    if (itemCount >= cleanThreshold and cleanThreshold < G_OPEN_SR_LIMIT) then
      cleanThreshold = cleanThreshold + G_OPEN_SR_CLEAN_THRESHOLD;
      -- A quick check and adjustment, just in case our math was wrong.
      if ( cleanThreshold > G_OPEN_SR_LIMIT ) then
        cleanThreshold = G_OPEN_SR_LIMIT;
      end
      recMap.CleanThreshold = cleanThreshold;
    else
      warn("[ERROR]<%s:%s> SRC Count(%d) Exceeded Limit(%d)", MOD, meth,
        recMap.ItemCount, G_OPEN_SR_LIMIT );
      error( ldte.ERR_TOO_MANY_OPEN_SUBRECS );
    end
  end

  local newSubRec = aerospike:create_subrec( topRec );
  if( newSubRec == nil ) then
    warn("[ERROR]<%s:%s>Problems Creating New Subrec", MOD,meth );
    error( ldte.ERR_SUBREC_CREATE );
  end
  record.set_type( newSubRec, RT_SUB ); -- Always RT_SUB for this call.

  local subRecDigest = record.digest( newSubRec );
  local topRecDigest = record.digest( topRec );
  local subRecDigestString = tostring( subRecDigest );
  
  -- Recalc ItemCount after the clean.
  itemCount = recMap.ItemCount;

  -- topRec's digest is the parent digest for this new Sub-Rec 
  subRecPropMap[PM.ParentDigest] = topRecDigest;

  -- Subrec's (its own) digest is the selfDigest :)
  -- I think we need to STOP saving SELF DIGEST (Verify Raj doesn't need it)
  subRecPropMap[PM.SelfDigest]   = subRecDigest;
  subRecPropMap[PM.Magic]        = MAGIC;
  subRecPropMap[PM.RecType]      = recType; -- Specific to LDT's Needs
  subRecPropMap[PM.CreateTime]   = aerospike:get_current_time();
  subRecPropMap[PM.EsrDigest]    = esrDigest;

  newSubRec[SUBREC_PROP_BIN] = subRecPropMap;

  -- Create a immediate commit. This won't be rolled back in case of 
  -- failure 
  rc = aerospike:update_subrec( newSubRec );
  --  Note that update_subrec() returns nil on success
  if rc then
    warn("[ERROR]<%s:%s> SubRec(%s) rc(%s)", MOD, meth,
      subRecDigestString, tostring(rc));
    error( ldte.ERR_SUBREC_UPDATE );
  end

  -- Update the LDT sub-rec count.  Remember that any changes to a record
  -- are remembered until the final Lua Close, then the record(s) will be
  -- flushed to storage.
  local subRecCount = propMap[PM.SubRecCount];
  propMap[PM.SubRecCount] = subRecCount + 1;
  -- This will mark the SubRec as dirty.
  local rc = ldt_common.addSubRecToContext( srcCtrl, newSubRec, true);

  return newSubRec;
end --  createSubRec()

-- ======================================================================
-- openSubRec()
-- ======================================================================
-- Return a ptr to the Open Sub-Rec.  We either find the Sub-Rec in our
-- table of open Sub-Recs, or we Open a new one.  If we reach the limit
-- of open Sub-Recs (last known size of the limit was 4000), then we try
-- to find a CLEAN Sub-Rec and close it.  Note that we cannot close
-- a dirty Sub-Rec -- that is an error.  Also, it is an error to try
-- to open an existing open Sub-Rec, so that's why we have this pool
-- in the first place.
-- Parms:
-- srcCtrl: the Sub-Record Control Structure
-- topRec: The Aerospike Record holding the LDT that uses Sub-Recs.
-- digestString: The identifier of the Sub-Rec.
--
-- ======================================================================
function ldt_common.openSubRec( srcCtrl, topRec, digestString, sp)
  local meth = "openSubRec()";

  -- Do some checks while we're in DEBUG mode.
  if  (digestString == nil or type(digestString) ~= "string") then
    warn("[ERROR]<%s:%s> Parm DigestString is NIL or NOT a string. It is(%s)",
        MOD, meth, type(digestString));
    error( ldte.ERR_INTERNAL );
  end

  -- We have a global limit on the number of subRecs that we can have
  -- open at a time.  If we're at (or above) the limit, then we must
  -- exit with an error (better here than in the subRec code).
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  local itemCount = recMap.ItemCount;
  local cleanThreshold = recMap.CleanThreshold;

  local rc = 0;

  -- First, look to see if the Sub-Rec is already open.  If so, then
  -- return the Sub-Rec ptr.
  -- If not, see if we can open a new Sub-Rec easily.
  -- If we are at the limit, then do a clean before we open.
  local subRec = recMap[digestString];
  if (subRec == nil) then
    -- Check our counts -- if we have a lot of open sub-recs, then try to
    -- clean out the "non-dirty" ones before we open more. If that fails, then
    -- we'll bump up our "clean threshold" until we hit the max.
    -- If we hit the max and we STILL need more, then return with ERROR.
    -- Notice that "createESR" doesn't need to check the
    -- counts because by definition it's the FIRST SUB-REC.
    if (itemCount >= cleanThreshold) then
      cleanSRC( srcCtrl ); -- Flush the clean pages.  Ignore errors.

      -- Reset the local var after clean (clean() changed it).
      itemCount = recMap.ItemCount;

      -- If we were unable to release any pages with clean, then UP our
      -- clean threshold until we hit the real limit.
      if (itemCount >= cleanThreshold) then
        if (cleanThreshold < G_OPEN_SR_LIMIT) then
          cleanThreshold = cleanThreshold + G_OPEN_SR_CLEAN_THRESHOLD;
          -- A quick check and adjustment, just in case our math was wrong.
          if ( cleanThreshold > G_OPEN_SR_LIMIT ) then
            cleanThreshold = G_OPEN_SR_LIMIT;
          end
          recMap.CleanThreshold = cleanThreshold;
        else
          warn("[ERROR]<%s:%s> SRC Count(%d) CT(%d) Exceeded Limit(%d)",
            MOD, meth, itemCount, cleanThreshold, G_OPEN_SR_LIMIT );
          error( ldte.ERR_TOO_MANY_OPEN_SUBRECS );
        end
     end
    end

    -- Recalc ItemCount after the (possible) clean.
    itemCount = recMap.ItemCount;
    -- NOTE: WE DO NOT NEED TO  BUMP ITEM COUNT HERE.  That's done later.
    --     recMap.ItemCount = itemCount + 1;
    subRec = aerospike:open_subrec( topRec, digestString );
    if (subRec == nil) then
      warn("[ERROR]<%s:%s> SubRec Open Failure: Digest(%s) Parent Digest(%s)", MOD, meth,
        digestString, sp and tostring(sp.DigestList[1]) or "NULL" );
      error( ldte.ERR_SUBREC_OPEN );
    end
    -- Add this open SubRec to our SubRec Context.
    local rc = ldt_common.addSubRecToContext( srcCtrl, subRec, false);
  end
  return subRec;
end -- openSubRec()

-- ======================================================================
-- closeSubRecDigestString()
-- ======================================================================
-- Close the sub-Record -- providing it is NOT dirty.  For all dirty
-- sub-Records, we have to wait until the end of the UDF call, as THAT is
-- when all dirty sub-Records get written out and closed.
--
-- ALSO, for PRODUCTION USE, we do not actually close the records here,
-- but instead we mark them as no longer busy -- which will make it possible
-- to close it when we issue a CLEAN on the Sub-Rec Pool.
-- Parms:
-- (*) srcCtrl:
-- (*) digestString:
-- (*) dirty: Optional Parm: True or other (false or nil)
-- ======================================================================
function ldt_common.closeSubRecDigestString( srcCtrl, digestString, dirty)
  local meth = "closeSubRecDigestString()";

  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  local itemCount = recMap.ItemCount;
  local rc = 0;

  local subRec = recMap[digestString];
  if (subRec == nil) then
    warn("[INTERNAL ERROR]<%s:%s> Rec not found for Digest(%s) in map(%s)",
      MOD, meth, tostring(digestString), tostring(recMap));
    error( ldte.ERR_INTERNAL );
  end

  if dirty == nil then
    dirty = false;
  end

  local dirtyStatus = (dirtyMap[digestString] == DM_DIRTY) or dirty;

  if (not dirtyStatus) then
    rc = aerospike:close_subrec( subRec );
    if( rc ~= nil and rc < 0 ) then
      warn("[ERROR]<%s:%s> Error closing SubRec: rc(%s) Digest(%s)",
          MOD, meth, tostring(rc), tostring( digestString ));
    else 
      -- Now erase this subrec from the SRC maps.
      recMap[digestString] = nil;
      dirtyMap[digestString] = nil;
      recMap.ItemCount = itemCount - 1;
    end
  end
  return 0;
end -- closeSubRecDigestString()

-- ======================================================================
-- closeSubRec(): Given a Sub-Rec ptr, close the subRec.
-- ======================================================================
function ldt_common.closeSubRec( srcCtrl, subRec, dirty )
  local meth = "closeSubRec";
  if (subRec == nil) then
    warn("[ERROR]<%s:%s> NULL subRec", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end

  local digest = record.digest( subRec );
  local digestString = tostring( digest );
  if (digestString == nil) then
    warn("[ERROR]<%s:%s> INVALID subRec", MOD, meth );
    error( ldte.ERR_INTERNAL );
  end
  return ldt_common.closeSubRecDigestString( srcCtrl, digestString, dirty );
end -- closeSubRec()

-- ======================================================================
-- markUnBusy()
-- ======================================================================
-- Do not close the sub-rec, but instead mark the sub-rec as NOT BUSY if
-- it is currently "DM_BUSY" (as in, "in use").  When it comes time to clean,
-- we can safely close any non-busy sub-recs, but we cannot close dirty ones.
-- Parms:
-- (*) srcCtrl:
-- (*) digestString:
-- ======================================================================
function ldt_common.markUnBusy( srcCtrl, digestString )
  local meth = "markUnBusy()";
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  local rc = 0;

  local subRec = recMap[digestString];
  if (subRec == nil) then
    warn("[INTERNAL ERROR]<%s:%s> Rec not found for Digest(%s) in map(%s)",
      MOD, meth, tostring(digestString), tostring(recMap));
    error( ldte.ERR_INTERNAL );
  end

  local cleaned = 0;
  local subRecStatus = dirtyMap[digestString];
  if (subRecStatus ~= nil and subRecStatus == DM_BUSY) then
    dirtyMap[digestString] = nil;
    cleaned = 1;
  elseif (subRecStatus ~= nil and subRecStatus == DM_DIRTY) then
    rc = aerospike:update_subrec( subRec );
    --  Note that update_subrec() returns nil on success
    if rc then
      warn("[ERROR]<%s:%s> SubRec(%s) rc(%s)", MOD, meth,
        digestString, tostring(rc));
      error( ldte.ERR_SUBREC_UPDATE );
    end
  end
  return 0;
end -- markUnBusy()

-- ======================================================================
-- updateSubRec()
-- ======================================================================
-- Update the subRecord -- and then mark it dirty.
-- ======================================================================
function ldt_common.updateSubRec( srcCtrl, subRec )
  local meth = "updateSubRec()";
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  local rc = 0;

  if not subRec then
    warn("[ERROR]<%s:%s> Unexpected nil value for subRec", MOD, meth);
    error( ldte.ERR_INTERNAL );
  end

  local digest = record.digest( subRec );
  local digestString = tostring( digest );

  -- Note that we DO NOT want to update the SubRec before the END of the
  -- Lua Call Context.  However, we DO have to mark the record as DIRTY
  -- so that we don't try to close it when we're looking for available
  -- slots when trying to close a clean Sub-Rec.
  dirtyMap[digestString] = DM_DIRTY;
  return rc;
end -- updateSubRec()

-- ======================================================================
-- markSubRecDirty()
-- ======================================================================
function ldt_common.markSubRecDirty( srcCtrl, digestString )
  local meth = "markSubRecDirty()";

  -- Pull up the dirtyMap, find the entry for this digestString and
  -- mark it dirty.  We don't even care what the existing value used to be.
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];
  dirtyMap[digestString] = DM_DIRTY;
  return 0;
end -- markSubRecDirty()

-- ======================================================================
-- closeAllSubRecs()
-- ======================================================================
-- Close all Read-only Sub-Recs, because that's how we free up Sub-Recs
-- that we know are no longer busy.
-- ======================================================================
function ldt_common.closeAllSubRecs( srcCtrl )
  local meth = "closeAllSubRecs()";

  if (srcCtrl == nil) then
    return 0;
  end
  
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];

  if (not recMap) or (not recMap.ItemCount) or recMap.ItemCount == 0 then
    return 0;
  end

  -- Iterate thru the SubRecContext and close all Sub-Records.
  local digestString;
  local rec;
  for name, value in pairs( recMap ) do
    if name and value then
      if name and value and (name ~= "ItemCount") and (name ~= "CleanThreshold") then
        -- Now we have to check for NON-NIL map values.
        if value then
          ldt_common.markUnBusy( srcCtrl, name );
        end
      end
    else
      warn("[INTERNAL ERROR]<%s:%s> Name(%s) Val(%s) NIL Problem", MOD, meth,
        tostring(name), tostring(value));
    end -- for name/value NOT NIL
  end -- for all fields in SRC
  return 0; -- Any errors would have jumped out with error().
end -- closeAllSubRecs()

-- ======================================================================
-- removeSubRec()
-- ======================================================================
-- Remove this SubRec from the pool and also close system
-- ======================================================================
function ldt_common.removeSubRec( srcCtrl, topRec, propMap, digestString )
  local meth = "removeSubRec()";
  local recMap = srcCtrl[1];
  local dirtyMap = srcCtrl[2];

  -- If the subRec digestString is valid, then remove it from the SRC and
  -- make the call to actually remove the SubRec.
  if (digestString == nil or type(digestString) ~= "string") then
    info("[WARNING]<%s:%s> Attempt to remove invalid SubRec(%s)", MOD, meth,
      tostring(digestString));
    return -1;
  end

  -- If it's not already open, which is possible, then try to open it,
  -- because apparently we can remove only open sub-recs.
  local subRec = recMap[digestString];
  if (subRec ~= nil) then
    if ( recMap[digestString] ~= nil ) then
      -- We can do this blind -- since whether or not it's there, we're
      -- removing it from both maps.
      dirtyMap[digestString] = nil;
      recMap[digestString] = nil;

      local itemCount = recMap.ItemCount;
      recMap.ItemCount = itemCount - 1;
    end
  else
    subRec = aerospike:open_subrec( topRec, digestString );
  end

  local rc = aerospike:remove_subrec( subRec );
  rc = rc or 0; -- set to ZERO if null.

  -- We now have one less sub-rec.  Decrement the global count for
  -- this LDT.
  local subRecCount = propMap[PM.SubRecCount];
  propMap[PM.SubRecCount] = subRecCount - 1;
  return rc; -- Mask the error for now:: TODO::@TOBY::Figure this out.
end -- removeSubRec()

-- ===========================
-- End SubRecord Function Area
-- ===========================

-- ======================================================================
-- listAppendList()
-- ======================================================================
-- General tool to append one list to another.   At the point that we
-- find a better/cheaper way to do this, then we change THIS method and
-- all of the LDT calls to handle lists will get better as well.
-- ======================================================================
function ldt_common.listAppendList( baseList, additionalList )
  local returnList;
  if( baseList == nil ) then
    warn("[INTERNAL ERROR] Null baselist in listAppend()" );
    -- error( ldte.ERR_INTERNAL );
    returnList = additionalList;
  end

  local listSize = list.size( additionalList );
  for i = 1, listSize, 1 do
    list.append( baseList, additionalList[i] );
  end -- for each element of additionalList
  returnList = baseList;

  return returnList;
end -- listAppendList()

-- =========================================================================
-- ldt_common.validateCodeAndData()
-- =========================================================================
-- validate the Code and Data.
-- =========================================================================
function ldt_common.validateCodeAndData( codeVersion, dataVersion )

  -- Code versions must be valid
  if not ( codeVersion and dataVersion ) then
    warn("[INTERNAL ERROR: Version Data corrupted");
    error( ldte.ERR_INTERNAL );
  end

  if not ( codeVersion > dataVersion ) then
    warn("[INTERNAL ERROR: Code and Data Mismatch. Please reload data.");
    info("Automatic Data Upgrade not yet enabled");
    error( ldte.ERR_INTERNAL );
  end
end -- ldt_common.validateCodeAndData()

-- ======================================================================
-- validateBinName()
-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- Parms:
-- (*) ldtBinName: user's bin name for this LDT
-- Return: either nothing (if ok) or jump out with error.
-- ======================================================================
function ldt_common.validateBinName( ldtBinName )
  local meth = "ldt_common.validateBinName()";
  if ldtBinName == nil  then
    warn("[ERROR EXIT]:<%s:%s> Null Bin Name", MOD, meth );
    error( ldte.ERR_NULL_BIN_NAME );
  elseif type( ldtBinName ) ~= "string"  then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Not a String", MOD, meth );
    error( ldte.ERR_BIN_NAME_NOT_STRING );
  elseif ldtBinName == "" then
    warn("[ERROR EXIT]:<%s:%s> Empty Bin Name", MOD, meth );
    error( ldte.ERR_NULL_BIN_NAME );
  elseif string.len( ldtBinName ) > AS_BIN_NAME_LIMIT then
    warn("[ERROR EXIT]:<%s:%s> Bin Name Too Long", MOD, meth );
    error( ldte.ERR_BIN_NAME_TOO_LONG );
  end
end -- ldt_common.validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the ldtBinName and ldtMap are valid, otherwise
-- jump out with an error() call.
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName: User's Name for the LDT Bin
-- (*) mustExist: When true, ldtCtrl must exist, otherwise error
-- (*) ldtType: Caller must tell us the type of LDT
-- (*) codeVersion: Caller must tell us the Version of the LDT Code
-- Return:
--   ldtCtrl -- if "mustExist" is true, otherwise unknown.
-- ======================================================================
function
ldt_common.validateRecBinAndMap(topRec,ldtBinName,mustExist,ldtType,codeVersion)
  local meth = "ldt_common.validateRecBinAndMap()";

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
    -- Check Top Record Existence.  Note that this is not a serious error.
    if (not aerospike:exists(topRec)) then
      info("[ERROR]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
     
    -- Control Bin Must Exist, in this case, ldtCtrl is what we check.
    -- This is not a serious error.
    if (not topRec[ldtBinName]) then
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    ldtCtrl = topRec[ldtBinName] ; -- The main LDT Control structure
    propMap = ldtCtrl[1];

    -- Extract the property map and Ldt control map from the Ldt bin list.
    if propMap[PM.Magic] ~= MAGIC then
      warn("[ERROR]:<%s:%s>LDT BIN(%s) Corrupted (no magic):PropMap(%s)",
            MOD, meth, tostring( ldtBinName ), tostring(propMap));
      error( ldte.ERR_BIN_DAMAGED );
    end

    -- We now know that we must validate the LDT TYPE early on, because it
    -- could be a common mistake to access an LDT bin with the wrong type.
    if propMap[PM.LdtType] ~= ldtType then
      warn("[ERROR]:<%s:%s>LDT Type Mismatch: BIN(%s) Data(%s)  Op(%s)",
          MOD, meth, tostring(ldtBinName), propMap[PM.LdtType], ldtType);
      error( ldte.ERR_TYPE_MISMATCH );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic. ...  and be the right LDT Type.
    if (topRec and topRec[ldtBinName]) then
      ldtCtrl = topRec[ldtBinName]; -- The main LdtMap structure
      propMap = ldtCtrl[1];
      if propMap and propMap[PM.Magic] ~= MAGIC then
          warn("[ERROR]<%s:%s>LDTBIN(%s) Corrupted (no magic):PropMap(%s)",
            MOD, meth, tostring( ldtBinName ), tostring(propMap));
        error( ldte.ERR_BIN_DAMAGED );
      end
      -- If the Bin is there, and it is an LDT bin, then it must be the
      -- correct type.  It is a common mistake to access an LDT bin with
      -- the wrong type.
      if propMap[PM.LdtType] ~= ldtType then
        warn("[ERROR]:<%s:%s>LDT Type Mismatch: BIN(%s) data(%s) op(%s)",
          MOD, meth, tostring(ldtBinName), propMap[PM.LdtType], ldtType);
        error( ldte.ERR_TYPE_MISMATCH );
      end
      
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

    if( codeVersion > dataVersion ) then
      warn("[EXIT]<%s:%s> Code Version (%d) <> Data Version(%d)",
        MOD, meth, codeVersion, dataVersion );
      warn("[Please reload data:: Automatic Data Upgrade not yet available");
      error( ldte.ERR_VERSION_MISMATCH );
    end
  end -- final version check
  return ldtCtrl; -- Save the caller the effort of extracting the map.
end -- ldt_common.validateRecBinAndMap()

-- ======================================================================
-- checkBin():
-- ======================================================================
-- Look into the soul of the bin and see if there is a valid instance of
-- the specified LDT in there.  If so, return true, otherwise return false;
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName: User's Name for the LDT Bin
-- (*) ldtType: Caller must tell us the type of LDT
-- Return:
--   TRUE if the bin holds a valid LDT of the specified type
--   FALSE if anything is wrong.
-- ======================================================================
function ldt_common.checkBin( topRec, ldtBinName, ldtType )
  local meth = "ldt_common.checkBin()";

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  -- Notice that we can't use our local method for this check, because we
  -- don't want to jump out in the error case.
  local result = false; -- Default is "NO", we have to prove it to get "YES".
  if ldtBinName ~= nil and
    type(ldtBinName) == "string" and
    string.len( ldtBinName ) <= AS_BIN_NAME_LIMIT
  then
    -- So we must have the following to get a "YES" (true)
    -- (*) Must have a record.
    -- (*) Must have a valid Bin
    -- (*) Must have a valid Map in the bin, with MAGIC and the right type
    if aerospike:exists(topRec) and topRec[ldtBinName] ~= nil then
      local ldtCtrl = topRec[ldtBinName] ; -- The main LDT Control structure
      local propMap = ldtCtrl[1];
      if propMap[PM.Magic] == MAGIC and propMap[PM.LdtType] == ldtType then
        result = true;
      end
    end
  end 
  return result;
end -- ldt_common.checkBin()

-- ======================================================================
-- validateLdtBin():
-- ======================================================================
-- Look into the soul of the bin and see if there is a valid instance of
-- the specified LDT in there.  If anything is wrong, we will "error out".
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName: User's Name for the LDT Bin
-- (*) ldtType: Caller must tell us the type of LDT
-- ======================================================================
function ldt_common.validateLdtBin( topRec, ldtBinName, ldtType)
  local meth = "ldt_common.validateLdtBin()";

  -- We assume that the caller has already checked the validity of the
  -- LDT Bin Name, and also figured out if the Top Record needs to be
  -- there or not.  Once that is determined, then we do the checks for
  -- the LDT Control Structure -- to see if everything is the right type.
  -- Check that ldtCtrl is a LIST, and the first element is a MAP, where
  -- the Map (the Prop Map) has MAGIC.
  local ldtCtrl = topRec[ldtBinName] ; -- The main LDT Control structure

  -- NOTE: Caller has already validated that the ldtCtrl is not null.
  local propMap;
  if getmetatable(ldtCtrl) == List then
    propMap = ldtCtrl[1];
    if getmetatable(propMap) == Map then
      -- Extract the property map and Ldt control map from the Ldt bin list.
      -- Notice that a corrupted bin is serious error, and hence deserves
      -- a warning.
      if propMap[PM.Magic] ~= MAGIC then
        warn("[ERROR]:<%s:%s>LDT BIN(%s) Corrupted (no magic)",
          MOD, meth, tostring( ldtBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
      -- We now know that we must validate the LDT TYPE early on, because it
      -- could be a common mistake to access an LDT bin with the wrong type.
      if propMap[PM.LdtType] ~= ldtType then
        warn("[ERROR]:<%s:%s>LDT Type Mismatch:  BIN(%s) Data(%s) Op(%s)",
          MOD, meth, tostring(ldtBinName), tostring(propMap[PM.LdtType]),
          tostring(ldtType));
        error( ldte.ERR_TYPE_MISMATCH );
      end
      
    else
      warn("[ERROR]:<%s:%s>LDT BIN(%s) is lacking a Property Map",
        MOD, meth, tostring( ldtBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
  else
    warn("[ERROR]:<%s:%s>LDT BIN(%s) does not have proper LDT Ctrl Structure",
      MOD, meth, tostring( ldtBinName ) );
    error( ldte.ERR_BIN_DAMAGED );
  end
  return ldtCtrl, propMap;
end -- ldt_common.validateLdtBin()

-- Default Values that we use if the user does not supply them.
local DEFAULT_MAX_OBJ_SIZE     =     200;
local DEFAULT_MAX_KEY_SIZE     =      22;
local DEFAULT_MINIMUM_PAGESIZE =    4000;
local DEFAULT_TARGET_PAGESIZE  =    8192;
local DEFAULT_WRITE_BLOCK_SIZE = 1000000;
local DEFAULT_RECORD_OVERHEAD  =     500;

-- -----------------------------------------------------------------------
-- getPageSize()
-- -----------------------------------------------------------------------
-- Find the page size as in ldtMap or pick the system configuration value.
-- Do not let the pageSize be bigger than writeBlockSize
-- Parms:
-- (*) ldtMap:
-- RETURN: PageSize
-- -----------------------------------------------------------------------
function ldt_common.getPageSize( topRec, pageSize )
  local meth = "getPageSize()";
  if (pageSize == nil) then
    pageSize = aerospike:get_config(topRec, "ldt-page-size");
  end
  return pageSize, aerospike:get_config(topRec, "write-block-size")
end -- getPageSize()

-- ========================================================================
-- ldt_common.validateConfigParms()
-- ========================================================================
-- The user had supplied us with a set of configuration parameters, but
-- we have to validate the values -- both the TYPE and the acceptable
-- ranges.  This function will be called by all of the "apply_setting()"
-- functions (which invoke the compute_configuration() functions).
--
-- Here are the rules:
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
-- If anything is wrong, we generate a warning and leave the ldtMap
-- unchanged.  We don't error out, but instead return a -1 error code.
-- ========================================================================
function ldt_common.validateConfigParms( ldtMap, configMap )
  local meth = "ldt_common.validateConfigParms()";

  local maxObjectSize  = 
    (configMap.MaxObjectSize ~= nil and configMap.MaxObjectSize) or
    DEFAULT_MAX_OBJ_SIZE;

  local maxKeySize     =
    (configMap.MaxKeySize ~= nil and configMap.MaxKeySize) or
    DEFAULT_MAX_KEY_SIZE;

  local pageSize       =
    (configMap.TargetPageSize ~= nil and configMap.TargetPageSize) or
    DEFAULT_TARGET_PAGESIZE;

  local writeBlockSize       =
    (configMap.WriteBlockSize ~= nil and configMap.WriteBlockSize) or
    DEFAULT_WRITE_BLOCK_SIZE;

  local recordOverHead       =
    (configMap.RecordOverHead ~= nil and configMap.RecordOverHead) or
    DEFAULT_RECORD_OVERHEAD;

  local userPageSize         =
    (configMap.PageSize ~= nil and configMap.PageSize) or
    DEFAULT_TARGET_PAGESIZE;

  if  ldtMap          == nil or
      maxObjectSize   == nil or type(maxObjectSize)  ~= "number" or
      maxKeySize      == nil or type(maxKeySize)     ~= "number" or
      writeBlockSize  == nil or type(writeBlockSize) ~= "number" or
      recordOverHead  == nil or type(recordOverHead) ~= "number" or
      pageSize        == nil or type(pageSize)       ~= "number"
  then
    warn("[ERROR]<%s:%s> One or more of the config setting values are NIL or NAN",
      MOD, meth);
    info("[DEBUG]<%s:%s>MaxKeySz(%s) MaxObjSz(%s)", MOD, meth,
      tostring(maxKeySize), tostring(maxObjectSize));
    info("[DEBUG]<%s:%s>WriteBlockSize(%s) RecordOH(%s)", MOD, meth,
      tostring(writeBlockSize), tostring(recordOverHead));
    info("[DEBUG]<%s:%s> PS(%s)", MOD, meth, 
      tostring(pageSize));
    return -1;
  end

  -- Let's do some number validation before we actually apply the values
  -- given to us.  The values have to be within a reasonable range
  if  maxObjectSize <= 0 or maxKeySize <= 0 or
      writeBlockSize <= 0 or recordOverHead < 0 or
      pageSize <= 0
  then
    warn("[ERROR]<%s:%s> Config Settings must be greater than zero", MOD, meth);
    info("[INFO] MaxObjSz(%d) MaxKeySz(%d)",
      maxObjectSize, maxKeySize);
    info("[INFO] WriteBlkSz(%d) RecOH(%d)",
      writeBlockSize, recordOverHead);
    info("[INFO] PS(%s) F(%s) T(%s)", pageSize);
    return -1;
  end

  -- Set the page size to the correct value based on the MaxObjectSize
  local pageMinimum   = DEFAULT_MINIMUM_PAGESIZE;
  local pageTarget    = DEFAULT_TARGET_PAGESIZE;
  local pageMaximum   = writeBlockSize;  -- 128k to 1mb ceiling

  if maxObjectSize > writeBlockSize then
    error(ldte.ERR_INPUT_TOO_LARGE .. ":Max Object Size (" .. maxObjectSize .. ") Exceeds WriteBlockSize(" .. writeBlockSize .. ")");
  elseif (maxObjectSize * 4) > pageSize then
    if (maxObjectSize * 4) < pageTarget then
      pageSize = pageTarget;
    elseif ( maxObjectSize * 4 )  < writeBlockSize then
      pageSize = maxObjectSize * 4;
    else
      pageSize = writeBlockSize;
    end
  else
    debug("[VALID]<%s:%s> PageSize(%d) Approved", MOD, meth, pageSize);
  end

  if pageSize < pageMinimum then
    pageSize = pageTarget;
  elseif pageSize > pageMaximum then
    pageSize = pageMaximum;
  end

  -- At the end, write all of the values back.  Some things we may have
  -- recomputed, others we may have just set default values.
  configMap.MaxObjectSize   = maxObjectSize;
  configMap.MaxKeySize      = maxKeySize;
  configMap.TargetPageSize  = pageSize;
  configMap.PageSize        = userPageSize;
  configMap.WriteBlockSize  = writeBlockSize;
  configMap.RecordOverHead  = recordOverHead;

  return 0;

end -- ldt_common.validateConfigParms()

-- ======================================================================
-- processModule()
-- ======================================================================
-- We expect to see several things from a user module.
-- (*) An adjust_settings() function: where a user overrides default settings
-- (*) Various filter functions (callable later during search)
-- adjust_settings() function, which puts these values in the control map.
-- ======================================================================
function ldt_common.processModule(ldtMap, configMap, moduleName)
  local meth = "processModule()";

  if (moduleName ~= nil) then
    local userModule = require(moduleName);
    if (userModule == nil) then
      error( ldte.ERR_USER_MODULE_NOT_FOUND );
    else
      local userSettings = userModule[G_SETTINGS];
      if (userSettings ~= nil) then
        userSettings(configMap); -- Update based on user need
      end
    end
  else
    warn("[ERROR]<%s:%s>User Module is NIL", MOD, meth );
  end
end -- processModule()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of excess information in the console or log.
-- Show Size, First Element, Last Element
-- ======================================================================
function ldt_common.summarizeList( myList )
  if( myList == nil ) then return "NULL LIST"; end;

  local resultMap = {};
  resultMap.Summary = "Summary of the List";
  local listSize  = list.size( myList );
  resultMap.ListSize = listSize;
  if resultMap.ListSize == 0 then
    resultMap.ListStatus = "List Is Empty";
  else
    resultMap.FirstElement = tostring( myList[1] );
    resultMap.LastElement =  tostring( myList[ listSize ] );
  end

  return tostring( resultMap );
end -- summarizeList()


-- ======================================================================
-- Dump the List (usually ResultList) with multiple prints so that we
-- can see the whole thing (regular Logging limits each print to something
-- like 1k or 2k per info/trace line).
-- ======================================================================
function ldt_common.dumpList( myList )
  if( myList == nil ) then
     info("NULL LIST");
     return;
  end

  -- Iterate thru the list (myList) and print items out 10 (or so) at a time.
  local subSize = 10;
  local count = 0;
  local remainderList = myList;
  local takeSize;
  local frontList;
  while ( list.size( remainderList ) > 0 ) do
    if( list.size( remainderList ) > subSize ) then
      takeSize = subSize;
    else
      takeSize = list.size( remainderList );
    end
    frontList = list.take( remainderList, takeSize );
    info("\n<LIST:[%d : %d] %s", count, count + takeSize, tostring(frontList));
    remainderList = list.drop( remainderList, takeSize );
  end

end -- ldt_common.dumpList()


-- ======================================================================
-- Summarize the MAP (usually ResultMap) so that we don't create
-- huge amounts of excess information in the console or the log.
-- Show Size and two Name/Values.  Unlike Summarize List, we really can't
-- make much sense of "first and last" items.
-- ======================================================================
function ldt_common.summarizeMap( myMap )
  local meth = "ldt_common.summarizeMap()"

  if( myMap == nil ) then return "NULL MAP"; end;

  local resultMap = {};
  local summaryMap = {};
  resultMap.Summary = "Summary of the MAP";
  local mapSize  = map.size( myMap );
  resultMap.MapSize = mapSize;
  if resultMap.MapSize == 0 then
    resultMap.MapStatus = "Map Is Empty";
  else
    local limit = 5;
    for name, value in map.pairs( myMap ) do
      if name and value then
        summaryMap[name] = value;
        limit = limit - 1;
        if( limit < 1 ) then
          resultMap.Summary = summaryMap;
        end
      else
        warn("[INTERNAL ERROR]<%s:%s> Name(%s) Val(%s) NIL Problem", MOD, meth,
          tostring(name), tostring(value));
      end
    end -- for each pair
  end -- if non zero

  return tostring( resultMap );
end -- summarizeMap()


-- ======================================================================
-- Dump the MAP (usually ResultMAP) with multiple prints so that we
-- can see the whole thing (regular Logging limits each print to something
-- like 1k or 2k per info/trace line).
-- ======================================================================
function ldt_common.dumpMap( myMap, msg )
  if( myMap == nil ) then
     info("NULL MAP");
     return;
  end

  info("\n <<<<> DUMP MAP [%s]<>>>>", tostring(msg));

  -- Iterate thru the map (myMap) and print items out 10 (or so) at a time.
  local subSize = 10;
  local count = 0;
  local subCount = 0;
  local subMap = {};
  for name, value in map.pairs( myMap ) do
    if name and value then
      subMap[name] = value;
      subMap["type::"..tostring(name)] = type(name);
      subCount = subCount + 1;
      count = count + 1;
      if( subCount > subSize ) then
        info("\nSubMap[%d:%d] Map(%s)",count-subCount,count,tostring(subMap));
        subMap = {}; -- start a new map for the next round.
        subCount = 0;
      end
    end
  end -- for each pair
  -- Print anything remaining -- after we fall out of the for loop.
  if( map.size( subMap ) ) then
      info("\nSubMap[%d:%d] Map(%s)", count-subCount, count, tostring(subMap));
  end
  info("\n<>>>>> END OF MAP <<<<>");

end -- ldt_common.dumpMap()

-- ======================================================================
-- ldt_common.dumpValue( myValue )
-- ======================================================================
-- If the value is either a LIST or a MAP, iterate thru the pieces and
-- print out each part, otherwise dump the whole thing.
-- ======================================================================
function ldt_common.dumpValue( myValue )
  info("ENTER DUMP VALUE");

  if( myValue == nil ) then
     info("NULL VALUE");
     return;
  end

  info("\n <<<<> DUMP VALUE : Type(%s)<>>>>", type(myValue));

  -- Iterate thru the map (myMap) and print items, one per line
  if ( getmetatable(myValue) == Map ) then
    info("Value is a MAP:");
    for name, value in map.pairs( myValue ) do
      if name and value then
        info("Name(%s) Value(%s)", tostring(name), tostring(value));
      end
    end -- for each pair

  elseif ( getmetatable(myValue) == List ) then
    info("Value is a LIST:");
    for i = 1, #myValue do
      info("I[%d]= (%s)", i, tostring(myValue[i]));
    end
  else
    info("Value is OTHER:");
    info("Value(%s)", tostring(myValue));
  end
  info("\n<>>>>> END OF Value DUMP <<<<>");

end -- ldt_common.dumpValue()

-- ======================================================================
-- ldt_common.listUpdate()
-- ======================================================================
-- General List Insert function that can be used to UPDATE (overwrite
-- in place) keys, digests or objects.  
-- Return:
-- Success: 0
-- Error: Error String
-- ======================================================================
function ldt_common.listUpdate( valList, newValue, position )
  local meth = "ldt_common.listUpdate()";
  -- Unlike Insert, for Update we must point at a valid CURRENT object.
  -- So, position must be within the range of the list size.
  local listSize = list.size( valList );
  if position >= 1 and position <= listSize then
    valList[position] = newValue;
  else
    warn("[WARNING]<%s:%s> INVALID POSITION(%d) for List Size(%d)", MOD, meth,
      position, listSize);
    error(ldte.ERR_INTERNAL);
  end
  return 0; -- Always OK
end -- ldt_common.listUpdate()


-- ======================================================================
-- ldt_common.listInsert()
-- ======================================================================
-- General List Insert function that can be used to insert
-- keys, digests or objects.  
-- Return:
-- Success: 0
-- Error: Error String
-- ======================================================================
function ldt_common.listInsert( valList, newValue, position )
  local meth = "ldt_common.listInsert()";

  local listSize = list.size( valList );
  if (listSize == 0 or position > listSize or position == 0) then
    -- Just append to the list
    list.append( valList, newValue );
  else
    list.insert( valList, position, newValue);
  end
  return 0; -- Always OK
end -- ldt_common.listInsert()

-- ======================================================================
-- ldt_common.listDelete()
-- ======================================================================
-- General List Delete function for removing a SINGLE ITEM from a list.
-- RETURN:
-- A NEW LIST that no longer includes the deleted item.
-- ======================================================================
function ldt_common.listDelete( objectList, position )
  local meth = "listDelete()";
  local resultList;
  local listSize = list.size( objectList );

  if (listSize == 0) then
    return objectList;
  end

  if (position < 1 or position > listSize) then
    warn("[DELETE ERROR]<%s:%s> Bad position(%d) for delete. ListSize(%d)",
      MOD, meth, position, #objectList);
    error( ldte.ERR_DELETE );
  end
  list.remove(objectList, position);
  return objectList; --resultList;
end -- ldt_common.listDelete()

-- ======================================================================
-- ldt_common.listDeleteMultiple()
-- ======================================================================
-- General List Delete function for removing MULTIPLE items from a list.
-- Parms:
-- (*) objectList: the original list of items
-- (*) startPos: The Starting index of the item(s) we're deleting
-- (*) endPos: The ending index of the item(s) we're deleting.
-- RETURN:
-- A NEW LIST that no longer includes the deleted items.
--
-- ======================================================================
function ldt_common.listDeleteMultiple( objectList, startPos, endPos )
  local meth = "listDeleteMultiple()";
  local resultList;
  local listSize = list.size( objectList );

  if( startPos < 1 or endPos > listSize or startPos > endPos) then
    warn("[DELETE ERROR]<%s:%s> Bad positions: Start(%d) End(%d)for delete.",
      MOD, meth, startPos, endPos );
    error( ldte.ERR_DELETE );
  end

  -- Move elements in the list to "cover" the items that are at the range
  -- of "startPos to endPos.  This has to work for all cases:
  -- (*) The entire list
  -- (*) Only One item (start == end)  (including start or end)
  -- (*) Any Range in the middle
  --  +---+---+---+---+---+---+---+---+
  --  |111|222|333|444|555|666|777|888|  Delete items from Pos 3 to 5
  --  +---+---+---+---+---+---+---+---+
  --     1   2   3   4   5   6   7   8 (indexes -- Lua starts at 1, not 0)
  
  -- We're going to build a new list out of the LEFT and
  -- RIGHT pieces of the original list.
  --
  -- Our List operators :
  -- (*) list.take (take the first N elements) 
  -- (*) list.drop (drop the first N elements, and keep the rest) 
  -- The special cases are:
  -- (*) A list of size 1:  Just return a new (empty) list.
  -- (*) We're deleting the FIRST element, so just use RIGHT LIST.
  -- (*) We're deleting the LAST element, so just use LEFT LIST
  if( listSize == 1 or (startPos == 1 and endPos == listSize)) then
    -- We are deleting everything -- just get a new empty list.
    resultList = list();
  elseif( startPos == 1 ) then
    -- There is no front section -- just create a list of the elements tht
    -- are beyong the end of endPos.
    resultList = list.drop( objectList, endPos );
  elseif( endPos == listSize ) then
    -- There is no back section -- just take the front part.
    resultList = list.take( objectList, startPos - 1 );
  else
    -- Remove the middle section -- take the front part and then
    -- append the back part.
    resultList = list.take( objectList, startPos - 1);
    local addList = list.drop( objectList, endPos );
    local addLength = list.size( addList );
    for i = 1, addLength, 1 do
      list.append( resultList, addList[i] );
    end
  end
  return resultList;
end -- ldt_common.listDeleteMultiple()

-- =======================================================================
-- searchOrderedList()
-- =======================================================================
-- Search an Ordered list for an item.  This is the simple Linear Search method.
--
-- (*) valList: the list of Values from the record bin
-- (*) searchKey: the "value"  we're searching for
-- Return A,B:
-- A: Return the position if found, else return ZERO.
-- B: The Correct position to insert, if not found (the index of where
--    this value will go, and all current values will shift to the right.
-- Recall the Lua Arrays start with index ONE (not zero)
-- =======================================================================
local function searchOrderedList( valList, searchKey )
    local meth = "searchOrderedList()";
    local foundPos = 0;
    local insertPos = 0;

    -- Nothing to search if the list is null or empty
    if (valList == nil or list.size( valList ) == 0) then
        return 0,0;
    end

    -- Search the list for the item (searchKey) return the position if found.
    -- Note that searchKey may be the entire object, or it may be a subset.
    local listSize = list.size(valList);
    local item;
    local dbKey;
    for i = 1, listSize, 1 do
        item = valList[i];
        -- a value that does not exist, will have a nil valList item
        -- so we'll skip this if-loop for it completely                  
        if item ~= nil and item == searchKey then
            foundPos = i;
            break;
        elseif searchKey < item and insertPos == 0 then
            insertPos = i;
            break;
        end -- end if not null and equals
    end -- end for each item in the list
    return foundPos, insertPos;
end -- searchOrderedList()

-- =========================================================================
-- ldt_common.validateList()
-- =========================================================================
-- validate that the list passed in is in sorted order, with no duplicates
-- =========================================================================
function ldt_common.validateList( valList )
    local result = true;

    if( valList == nil ) then
        return false;
    end

    local listSize = list.size(valList);
    for i = 1, ( listSize - 1), 1 do
        if( valList[i] == nil or valList[i+1] == nil ) then
            return false;
        end
        if( valList[i] >= valList[i+1] == nil ) then
            return false;
        end
    end
    return true;
end -- ldt_common.validateList()

local function updateTop(topRec)
  local meth = "updateTop()";
  -- Now that we've changed the top rec, do the update to make sure the
  -- changes are saved.
  local rc = 0;
  local is_create = false;

  if (not aerospike:exists(topRec)) then
    is_create = true;
    rc = aerospike:create(topRec);
  else 
    rc = aerospike:update(topRec);
  end
 
  if (rc ~= 0) then
    if (is_create) then
      warn("[ERROR]<%s:%s>Problems Creating TopRec rc(%d)", MOD, meth, rc );
      error(ldte.ERR_TOPREC_CREATE);
    else
      warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%d)", MOD, meth, rc );
      error(ldte.ERR_TOPREC_UPDATE);
    end
  end
  return rc;
end


-- ======================================================================
-- When we create the initial LDT Control Bin for the entire record (the
-- first time ANY LDT is initialized in a record), we create a property
-- map in it with various values.
-- ======================================================================
function ldt_common.setLdtRecordType( topRec )
  local meth = "setLdtRecordType()";

  local rc = 0;
  local recPropMap;

  -- Check for existence of the main record control bin.  If that exists,
  -- then we're already done.  Otherwise, we create the control bin, we
  -- set the topRec record type (to LDT) and we praise the lord for yet
  -- another miracle LDT birth.
  if (topRec[REC_LDT_CTRL_BIN] == nil) then
    record.set_type( topRec, RT_LDT );
    recPropMap = map();
    -- vinfo will be a 5 byte value, but it will be easier for us to store
    -- a FULL NUMBER (8 bytes) of value ZERO.
    local vinfo = 0;
    recPropMap[RPM.VInfo] = vinfo; 
    recPropMap[RPM.LdtCount] = 1; -- this is the first one.
    recPropMap[RPM.Magic] = MAGIC;
    recPropMap[RPM.SelfDigest] = record.digest(topRec);
  else
    -- Not much to do -- increment the LDT count for this record.
    recPropMap = topRec[REC_LDT_CTRL_BIN];
    local ldtCount = recPropMap[RPM.LdtCount];
    recPropMap[RPM.LdtCount] = ldtCount + 1;
  end

  topRec[REC_LDT_CTRL_BIN] = recPropMap;
  -- Set this control bin as HIDDEN
  record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );

  rc = updateTop(topRec)

  return rc;
end -- setLdtRecordType()

-- ========================================================================
-- ldt_common.removeEsr(): Remove the ESR.
-- ========================================================================
-- Remove the Existence Sub-Record (ESR).  This is done as part of 
-- LDT Remove, or just resetting back to a Compact List where we are
-- giving up all Sub-Records.
--
-- Parms:
-- (1) src: Sub-Rec Context - Needed for repeated calls from caller
-- (2) topRec: the user-level record holding the LDT Bin
-- (3) propMap: Location of the ESR
-- (4) ldtBinName: LDT Bin (mostly for tracing/debugging)
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function ldt_common.removeEsr( src, topRec, propMap)
  local meth = "removeEsr()";
  local rc = 0; -- start off optimistic

  -- Get the ESR and delete it -- if it exists.  If we have ONLY an initial
  -- compact list, then the ESR will be ZERO.
  local esrDigest = propMap[PM.EsrDigest];
  if (esrDigest ~= nil and esrDigest ~= 0) then
    local esrDigestString = tostring(esrDigest);
    rc = ldt_common.removeSubRec( src, topRec, propMap, esrDigestString );
    if (rc == nil or rc == 0) then
      propMap[PM.EsrDigest] = 0;
    else
      warn("[ESR DELETE ERROR]<%s:%s>RC(%d) ESR(%s)",
        MOD, meth, rc, esrDigestString);
      error( ldte.ERR_SUBREC_DELETE );
    end
  end
end -- removeEsr()

-- ========================================================================
-- ldt_common.destroy(): Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- The caller (the parent) has already validated the bin, type and codeVer,
-- so that is not checked here.
--
-- Parms:
-- (1) src: Sub-Rec Context - Needed for repeated calls from caller
-- (2) topRec: the user-level record holding the LDT Bin
-- (3) ldtBinName: The name of the LDT Bin
-- (4) ldtType: 
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function ldt_common.destroy( src, topRec, ldtBinName, ldtCtrl)
  local meth = "localLdtDestroy()";
  local rc = 0; -- start off optimistic

  -- Extract the property map and LDT control map from the LDT bin list.
  -- local ldtCtrl = topRec[ ldtBinName ];
  local propMap = ldtCtrl[1];

  -- Get the ESR and delete it -- if it exists.  If we have not yet created
  -- any sub-records, then the ESR will be ZERO.
  local esrDigest = propMap[PM.EsrDigest];
  if (esrDigest ~= nil and esrDigest ~= 0) then
    local esrDigestString = tostring(esrDigest);
    local esrRec = ldt_common.openSubRec(src, topRec, esrDigestString );
    if (esrRec ~= nil) then
      rc = ldt_common.removeSubRec( src, topRec, propMap, esrDigestString );
      if (rc ~= nil and rc ~= 0) then
        warn("[ESR DELETE ERROR]<%s:%s>RC(%d) Bin(%s) ESR Digest(%s)",
          MOD, meth, rc, ldtBinName, esrDigestString);
        error( ldte.ERR_SUBREC_DELETE );
      end
    else
      warn("[ESR DELETE ERROR]<%s:%s> ERROR on ESR(%s) Open", MOD, meth,
        esrDigestString);
    end
  end

  -- Remove the bin entry from the record
  topRec[ldtBinName] = nil;

  -- Get the Common LDT (Hidden) bin, and update the LDT count.  If this
  -- is the LAST LDT in the record, then remove the Hidden Bin entirely.
  local recPropMap = topRec[REC_LDT_CTRL_BIN];
  if (recPropMap == nil or recPropMap[RPM.Magic] ~= MAGIC) then
    warn("[INTERNAL ERROR]<%s:%s> Prop Map for LDT Hidden Bin invalid",
      MOD, meth );
    error( ldte.ERR_BIN_DAMAGED );
  end
  local ldtCount = recPropMap[RPM.LdtCount];
  if (ldtCount <= 1) then
    topRec[REC_LDT_CTRL_BIN] = nil;
    -- If we are actually removing the LAST LDT, and thus making this a
    -- regular record again, then we have to UNSET the record type from
    -- LDT to regular.  We do that by passing in a NEGATIVE value.
    record.set_type( topRec, -(RT_LDT) );
  else
    recPropMap[RPM.LdtCount] = ldtCount - 1;
    topRec[REC_LDT_CTRL_BIN] = recPropMap;
    record.set_flags(topRec, REC_LDT_CTRL_BIN, BF_LDT_HIDDEN );
  end
  
  rc = aerospike:update(topRec);
  if rc ~= nil and rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
  return 0;
end -- ldt_common.destroy()


-- ========================================================================
-- ldt_common.reset(): Restore this LDT to its pristine state.
-- ========================================================================
-- Release all of the Sub-Rec storage associated with this LDT and reset
-- the count to zero.  The caller will have to take care of the LDT specific
-- structures (e.g. compact list, hot list).
--
-- The caller (the parent) has already validated the bin, type and codeVer,
-- so that is not checked here.
--
-- Parms:
-- (1) src: Sub-Rec Context - Needed for repeated calls from caller
-- (2) topRec: the user-level record holding the LDT Bin
-- (3) ldtCtrl: The LDT Control structure.
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function ldt_common.reset( src, topRec, ldtCtrl)
  local meth = "ldt_common.reset()";

  -- Extract the property map and LDT control map from the LDT bin list.
  -- local ldtCtrl = topRec[ ldtBinName ];
  local propMap = ldtCtrl[1];
  local ldtBinName = propMap[PM.BinName];

  -- Get the ESR and delete it -- if it exists.  If we have not yet created
  -- any sub-records, then the ESR will be ZERO.
  local esrDigest = propMap[PM.EsrDigest];
  if (esrDigest ~= nil and esrDigest ~= 0) then
    local esrDigestString = tostring(esrDigest);
    local esrRec = ldt_common.openSubRec(src, topRec, esrDigestString );
    if (esrRec ~= nil) then
      local rc = ldt_common.removeSubRec( src, topRec, propMap, esrDigestString );
      if (rc ~= nil and rc ~= 0) then
        warn("[ESR DELETE ERROR]<%s:%s>RC(%d) Bin(%s) ESR Digest(%s)",
          MOD, meth, rc, ldtBinName, esrDigestString);
        error( ldte.ERR_SUBREC_DELETE );
      end
    else
      warn("[ESR DELETE ERROR]<%s:%s> ERROR on ESR(%s) Open", MOD, meth,
        esrDigestString);
    end
  end

  -- Reset out counts.
  propMap[PM.ItemCount] = 0;
  propMap[PM.SubRecCount] = 0;

  -- The Caller has more changes to make, so the caller will update the
  -- Top Record.
  -- Update the Top Record.
  local rc = aerospike:update( topRec );
  if rc ~= nil and rc ~= 0 then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
  return 0;
end -- ldt_common.reset()

-- ========================================================================
-- ldt_common.size() -- return the number of elements (item count) in the set.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   SUCCESS: The number of elements in the LDT
--   ERROR: The Error code via error() call
-- ========================================================================
function ldt_common.size( topRec, ldtBinName, ldtType, codeVer )
  local meth = "ldt_common.size()";

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl =
    ldt_common.validateRecBinAndMap(topRec,ldtBinName,true,ldtType,codeVer);

  -- Extract the property map and control map from the ldt bin list.
  -- local ldtCtrl = topRec[ ldtBinName ];
  local propMap = ldtCtrl[1];
  local itemCount = propMap[PM.ItemCount];
  return itemCount;
end -- ldt_common.size()

-- ========================================================================
-- ldt_common.config() -- return the config settings
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) ldtType: The LDT Type (needed for type check)
-- (4) codeVer: Check the Data version against the code version.
--
-- NOTE: Since we are relying on LDT-SPECIFIG information being passed
-- back -- perhaps we need to have two calls -- FULL CONFIG and then just
-- "Propery Map" (COMMON CONFIG) calls.  This common routine can do the
-- common config information, but not the LDT-Specific stuff.
--
-- Result:
--   SUCCESS: The MAP of the config.
--   ERROR: The Error code via error() call
-- ========================================================================
function ldt_common.config( topRec, ldtBinName, ldtType, codeVer)
  local meth = "ldt_common.config()";
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl =
    ldt_common.validateRecBinAndMap(topRec,ldtBinName,true,ldtType,codeVer);
  local resultMap = map();
  ldt_common.propMapSummary( resultMap, ldtCtrl );
  return resultMap;
end -- function ldt_common.config()

-- ========================================================================
-- ldt_common.get_capacity() -- return the current capacity setting for this LDT
-- Capacity is in terms of Number of Elements.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function ldt_common.get_capacity( topRec, ldtBinName, ldtType, codeVer )
  local meth = "ldt_common.get_capacity()";
  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl =
    ldt_common.validateRecBinAndMap(topRec, ldtBinName, true, ldtType,codeVer);
  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and LDT control map from the LDT bin list.
  local ldtMap = ldtCtrl[2];
  local capacity = ldtMap[LC.StoreLimit];
  if (capacity == nil) then
    capacity = 0;
  end
  return capacity;
end -- function ldt_common.get_capacity()

-- ========================================================================
-- ldt_common.set_capacity() -- set the current capacity setting for this LDT
-- ========================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The name of the LDT Bin
-- (*) capacity: the new capacity (in terms of # of elements)
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function ldt_common.set_capacity(topRec,ldtBinName,capacity,ldtType,codeVer)
  local meth = "ldt_common.set_capacity()";
  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local ldtCtrl =
    ldt_common.validateRecBinAndMap(topRec,ldtBinName,true,ldtType,codeVer);

  local ldtCtrl = topRec[ ldtBinName ];
  -- Extract the property map and LDT control map from the LDT bin list.
  local ldtMap = ldtCtrl[2];
  if (capacity ~= nil and type(capacity) == "number" and capacity >= 0) then
    ldtMap[LC.StoreLimit] = capacity;
  else
    warn("[ERROR]<%s:%s> Bad Capacity Value(%s)",MOD,meth,tostring(capacity));
    error( ldte.ERR_INTERNAL );
  end

  -- All done, store the record
  -- Update the Top Record with the new control info
  topRec[ldtBinName] = ldtCtrl;
  record.set_flags(topRec, ldtBinName, BF_LDT_BIN );--Must set every time
  local rc = aerospike:update( topRec );
  if ( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>TopRec Update Error rc(%s)",MOD,meth,tostring(rc));
    error( ldte.ERR_TOPREC_UPDATE );
  end 
  return 0;
end -- function ldt_common.set_capacity()

-- ldt_common.exists( topRec, ldtBinName, ldtType )

-- ========================================================================
-- ldt_common.ldt_exists()
-- ========================================================================
-- Return TRUE if this LDT exists in the bin, FALSE otherwise.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- (3) ldtType: The LDT Type (needed for type check)
--
-- Result:
--   If bin exists, return TRUE
--   Otherwise, return FALSE
-- ========================================================================
function ldt_common.ldt_exists( topRec, ldtBinName, ldtType )
  local meth = "ldt_common.ldt_exists()";
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  local result = ldt_common.checkBin(topRec, ldtBinName, ldtType);
  return result;
end -- function ldt_common.ldt_exists()


function ldt_common.commit(topRec, ldtBinName, src, rc)
  if (rc == 0) then
    rc = ldt_common.closeAllSubRecs(src);
    if (rc < 0) then
      warn("[ERROR] Problems closing subrecs in delete");
      error( ldte.ERR_SUBREC_CLOSE );
    end

    rc = updateTop(topRec);
 
    if rc and rc ~= 0 then
      warn("[ERROR] TopRec Update Error rc(%s)", tostring(rc));
      error(ldte.ERR_TOPREC_UPDATE);
    end 
    return 0;
  elseif (rc == -2) then
    ldt_common.closeAllSubRecs(src);
    return -2;
  else
    warn("[ERROR EXIT]: Return(%s)", tostring(rc));
    error(ldte.ERR_DELETE);
  end
end


-- ======================================================================
-- << END >>  GENERAL COMMON FUNCTIONS 
-- ======================================================================

function ldt_common.getValSize(value)
  if value ~= nil then
    local valType = type(value);
    if (valType == "number") then
      return 8;
    elseif (valType == "string") then
      return string.len(value);
    elseif (getmetatable(value) == Map) then
      return map.nbytes(value);
    elseif (getmetatable(value) == List) then
      return list.nbytes(value);
    elseif (getmetatable(value) == Bytes) then
      return bytes.size(value);
    else
      return 0;
    end
  end
  return 0;
end

-- ========================================================================
-- Return the ldt_commonm MAP (or table) that contains all of the functions
-- that we're exporting from this module.
-- ========================================================================
return ldt_common;

-- ========================================================================
--  _     ____ _____    ____                                      
-- | |   |  _ |_   _|  / ___|___  _ __ ___  _ __ ___   ___  _ __  
-- | |   | | | || |   | |   / _ \| '_ ` _ \| '_ ` _ \ / _ \| '_ \ 
-- | |___| |_| || |   | |__| (_) | | | | | | | | | | | (_) | | | |
-- |_____|____/ |_|    \____\___/|_| |_| |_|_| |_| |_|\___/|_| |_| (LIB)
--
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
