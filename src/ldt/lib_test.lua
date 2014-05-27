-- Test Module for trying things out.
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
local MOD="lib_test_2014_05_27.A";

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
-- (*) DEBUG is used for larger structure content dumps.
-- ======================================================================
local GP;     -- Global Print Instrument
local F=false; -- Set F (flag) to true to turn ON global print
local E=false; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=false; -- Set B (Banners) to true to turn ON Banner Print
local GD;     -- Global Debug instrument.
local DEBUG=false; -- turn on for more elaborate state dumps.

-- ++==================++
-- || External Modules ||
-- ++==================++
-- Set up our "outside" links.
-- Get addressability to the Function Table: Used for compress/transform,
-- keyExtract, Filters, etc. 
local functionTable = require('ldt/UdfFunctionTable');

-- We import all of our error codes from "ldt_errors.lua" and we access
-- them by prefixing them with "ldte.XXXX", so for example, an internal error
-- return looks like this:
-- error( ldte.ERR_INTERNAL );
local ldte = require('ldt/ldt_errors');

-- We have recently moved a number of COMMON functions into the "ldt_common"
-- module, namely the subrec routines and some list management routines.
-- We will likely move some other functions in there as they become common.
local ldt_common = require('ldt/ldt_common');
--
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  TEST Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LSTACK module:
--
-- (*) List   = test.list_test( topRec, ldtBinName, size )
-- (*) Map    = test.map_test( topRec, ldtBinName, size )
-- (*) Number = test.one_test( topRec, ldtBinName )
-- (*) Object = test.same_test( topRec, ldtBinName, value )

-- Use this table to export the TEST functions to other UDF modules,
-- including the main Aerospike External UDF TEST module.
-- Then, each function that is exported from this module is placed in the
-- test table for export.  All other functions in this module are internal.
local test = {};

-- ========================================================================
-- test.list_test()         -- Create a LIST of N elements and return it.
-- ========================================================================
-- Create and return a list of "size" elements.
-- ========================================================================
function test.list_test( topRec, ldtBinName, size )
  if( size == nil or type(size) ~= "number") then
    error("BAD SIZE PARAMETER");
  end

  local resultList = list();

  for i = 1, size, 1 do
    list.append(resultList, i);
  end
  return resultList;

end -- test.list_test()

-- ========================================================================
-- test.map_test()         -- Create a MAP of N elements and return it.
-- ========================================================================
-- Create and return a map of "size" pairs, where each pair is a sequential
-- set of numbers:
-- name ranges from 1 to "size"
-- value ranges from 10,000 +1 to 10,000 + "size"
-- ========================================================================
function test.map_test( topRec, ldtBinName, size )
  if( size == nil or type(size) ~= "number") then
    error("BAD SIZE PARAMETER");
  end
  local resultMap = map();

  for i = 1, size, 1 do
    resultMap[i] = i + 10000;
  end
  return resultMap;

end -- test.map_test()

-- ======================================================================
-- test.one()      -- Just return 1.  This is used for perf measurement.
-- ========================================================================
-- Do the minimal amount of work -- just return a number so that we
-- can measure the overhead of the LDT/UDF infrastructure.
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) Val:  Random number val (or nothing)
-- Result:
--   res = 1 or val
-- ========================================================================
function test.one( topRec, ldtBinName )
  return 1;
end -- test.one()

-- ========================================================================
-- test.same()         -- Return Val parm.  Used for perf measurement.
-- ========================================================================
function test.same( topRec, ldtBinName, val )
  if( val == nil or type(val) ~= "number") then
    return 1;
  else
    return val;
  end
end -- test.same()


-- =========================================================================
-- The Annotated Stack
-- =========================================================================
-- Take a list of items to be pushed onto the stack. Put each one into
-- a map, along with its (server-size) insert time, which is a system
-- call from Lua.
-- Parms:
-- (*) topRec: The main Aerospike Record
-- (*) binName: The name of the LDT Stack Bin in the AS Record.
-- (*) valueList: the list of values to be inserted.
-- (*) createSpec: the Creation Specification for the LSTACK push() call()
-- =========================================================================
function annotated_stack_write( topRec, binName, valueList, createSpec)
  local meth = "annotated_stack_write()";
  GP=E and trace("[ENTER]:<%s:%s>:Bin(%s) List(%s) C Spec(%s)",
    MOD, meth, tostring(binName), tostring(valueList), tostring(createSpec));

  -- For each item in "valueList", create a map, append the current system
  -- time and push that map onto the stack.
  -- It would be faster to call the system time ONCE and just reuse the
  -- value, but it might be more accurate it call it for each value.
  -- Recall that arrays in Lua begin with index=1.
  local listSize = list.size(valueList);
  local newMap;
  for i = 1 , listSize, 1 do
    newMap = map();
    newMap.value = valueList[i];
    newMap.time = aerospike:get_current_time();
    -- No real need to check error code, since any real error will
    -- jump out of the Lua Context entirely.
    lstack.push( topRec, binName, newMap, createSpec );
  end -- for()

  GP=E and trace("[EXIT]:<%s:%s> ", MOD, meth);
end

-- =========================================================================
-- |||||||||||||||||||| The CAPPED COLLECTION PACKAGE ||||||||||||||||||||||
-- =========================================================================
-- The CC package implements a Capped Collection that uses
-- Large Data Types (LDT) as the data containers.  The CC Package accepts
-- inserts into a Large List up to a certain size.  When the list reaches
-- a specified size, then the oldest element is removed as the new element
-- is inserted.
--
-- When a new item is added to the Value List, it is also added to the 
-- Time Index that associates its insert time with the value.  When it is 
-- necessary to remove an entry, the Time Index is searched for the oldest
-- value, and that value is them removed from the Value List.
--
-- The CC package exercises Two LISTS and One Map.
-- The Function of this package is as follows:
-- (*) There are three bins used in the main record:
--     1. A Control Bin that holds the control map (User Named bin)
--     2. A Value Bin that holds the value list  (Fixed Name)
--     3. A Time Bin that holds the time index   (Fixed Name)
-- (*) The Value List holds the items that are inserted.  The items can be
--     simple (e.g. a Number or String) or they can be complex (i.e. a map),
--     where the "key()" function extracts a simple value from the object.
-- (*) The Control Map maintains statistics about the capped collection
-- (*) The Time Index registers a time for each value as it is inserted.
--
-- =========================================================================

-- ++===========++
-- || CONSTANTS ||
-- ++===========++
local CC_MAGIC = "MAGIC";

-- Record Bins for the Capped Collection
-- (*) Recall that the user names the main CC Bin.

-- (*) The VALUE bin : Keeps the sorted list of elements
local VAL_LIST_BIN = "VAL_LIST_BIN";

-- (*) The Time Map Bin  : For keeping the TIME info of the list elements
local TIME_LIST_BIN = "TIME_LIST_BIN";

-- Keep a reasonable list size.  Make this a parameter later.
local G_MAX_LIST = 1000;

-- =======================================================================
-- searchOrderedList()
-- =======================================================================
-- Search an Ordered list for an item.  
-- This is the simple Linear Search method.  There is also a Binary Search
-- function.
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
    GP=F and trace("[ENTER]: <%s:%s> Looking for searchKey(%s) in List(%s)",
        MOD, meth, tostring(searchKey), tostring(valList));

    local foundPos = 0;
    local insertPos = 0;

    -- Nothing to search if the list is null or empty
    if( valList == nil or list.size( valList ) == 0 ) then
        GP=F and trace("[DEBUG]<%s:%s> EmptyList", MOD, meth );
        return 0,0;
    end

    -- Search the list for the item (searchKey) return the position if found.
    -- Note that searchKey may be the entire object, or it may be a subset.
    local listSize = list.size(valList);
    local item;
    local dbKey;
    for i = 1, listSize, 1 do
        item = valList[i];
        GP=F and trace("[COMPARE]<%s:%s> index(%d) SV(%s) and ListVal(%s)",
            MOD, meth, i, tostring(searchKey), tostring(item));
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

    GP=F and trace("[EXIT]<%s:%s> Result: FindPos(%d) InsertPos(%d)",
        MOD, meth, foundPos, insertPos );
    return foundPos, insertPos;
end -- searchOrderedList()

-- ======================================================================
-- General List Delete function for removing items from a list.
-- RETURN:
-- A NEW LIST that no longer includes the deleted item.
-- ======================================================================
local function listDelete( objectList, position )
  local meth = "listDelete()";
  local resultList;
  local listSize = list.size( objectList );

  GP=F and trace("[ENTER]<%s:%s>List(%s) size(%d) Position(%s)", MOD,
  meth, tostring(objectList), listSize, tostring(position) );

  if( position < 1 or position > listSize ) then
    warn("[DELETE ERROR]<%s:%s> Bad position(%d) for delete.",
      MOD, meth, position );
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
  --
  -- So, basically, we're going to build a new list out of the LEFT and
  -- RIGHT pieces of the original list.
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

  GP=F and trace("[EXIT]<%s:%s>List(%s)", MOD, meth, tostring(resultList));
  return resultList;
end -- listDelete()

-- =========================================================================
-- validateList()
-- validate that the list passed in is in sorted order, with no duplicates
-- =========================================================================
local function validateList( valList )
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
end

-- =========================================================================
-- removeOldest()
-- =========================================================================
-- Remove the oldest value from the Capped Collection.  So, we find the
-- value with the SMALLEST TIME value from the TIME List -- and use that time
-- object to locate the corresponding key for the value in the Value List.
-- =========================================================================
local function removeOldest( topRec, ccCtrl, src)
  local meth = "removeOldest()";
  GP=F and trace("[ENTER]:<%s:%s> CC Bin(%s)()", MOD, meth, tostring(ccCtrl));
  local rc = 0;

  -- Get the smallest Time value
  local object = llist.take_first( topRec, VAL_LIST_BIN, src);
  if( object == nil ) then
    warn("[NOT FOUND]<%s:%s> Couldn't find Oldest Element", MOD, meth );
  end
  local key = object.PayLoad;

  -- Remove the Value that was associated with the smallest time value.
  if( key ~=nil ) then
    rc = llist.remove( topRec, TIME_LIST_BIN, key, src);
  end
  GP=F and trace("[exit]<%s:%s> RC(%d)()", MOD, meth, rc );

end -- removeOldest()

-- =========================================================================
-- updateObject()
-- =========================================================================
-- This object "userObject" has been found in the CC.  So, that object
-- must be updated in the Time List with a new time.  The object stays
-- in the Value List.
-- =========================================================================
local function updateObject( topRec, ccCtrl, userObject )
  local meth = "updateObject()";
    GP=F and trace("[ENTER]:<%s:%s>:Setup Record()", MOD, meth );

  -- Locate the userObject in the TIME LIST,(extracting the key)
  -- and update it in place.
  rc = llist.update( topRec, TIME_LIST_BIN, userObject, src);

  GP=F and trace("[exit]<%s:%s> RC(%d)()", MOD, meth, rc );

end -- updateObject()

-- =========================================================================
-- setupCCBin()
-- =========================================================================
-- Create all of the components of the Capped Collection.
-- =========================================================================
local function setupCCBin( topRec, ccBinName )
    local meth = "setupCCBin()";
    GP=F and trace("[ENTER]:<%s:%s>:Setup Record()", MOD, meth );

    local rc = 0;

    -- First -- create the record if it does not exist.
    if( not aerospike:exists( topRec ) ) then
        GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
        rc = aerospike:create( topRec );
        if( rc ~= 0 ) then
            warn("[ERROR]<%s:%s>Problems Creating TopRec rc(%d)",MOD,meth,rc);
            error( ldte.ERR_TOPREC_CREATE );
        end
    end

    -- three new structures: One control Map and two Large LIST LDTs.
    local ccCtrl  = map();

    -- Control information kept for this Collection
    ccCtrl[CC_MinValue]     = 0;
    ccCtrl[CC_MaxValue]     = 0;
    ccCtrl[CC_TotalOps]     = 0;
    ccCtrl[CC_TotalCount]   = 0;
    ccCtrl[CC_CurrentCount] = 0;
    ccCtrl[CC_Duplicates]   = 0;
    ccCtrl[CC_Magic]        = CC_MAGIC;
    ccCtrl[CC_CcBin]  = ccBinName;
    ccCtrl[CC_ValBin] = VAL_LIST_BIN;
    ccCtrl[CC_TimeBin] = TIME_LIST_BIN;

    topRec[ccBinName] = ccCtrl;

    -- Create our special "CreateSpec" map that tells the LLIST LDT
    -- how to configure the structure for storing complex Map Objects.
    -- Notice that there are several ways of specifying the LDT configuration,
    -- and the "Map Method" is the simplest and probably easiest.
    local createSpec = map();
    createSpec.Package = "StandardMap";

    -- Create the two LLIST LDTs:
    llist.create( topRec, VAL_LIST_BIN, createSpec );
    llist.create( topRec, TIME_LIST_BIN, createSpec );

    GP=F and trace("[EXIT]:<%s:%s>:Setup Record()", MOD, meth );
end -- setupCCBin()

-- ======================================================================
-- validateCC()
-- ======================================================================
-- Check that the topRec, the main BinName and the two LLIST bins
-- are all valid.  If they are NOT, then jump out with an error() call.
-- Notice that we look at different things depending on whether or not
-- "mustExist" is true (it is true for READ, false for WRITE).
-- Parms:
-- (*) topRec: The main Aerospike Record
-- (*) ccBinName: The user-defined Bin Name for the CC Object.
-- (*) mustExist: When "true" all must exist and be valid. 
--                Otherwise, only some things must be consistent.
-- ======================================================================
local function validateCC( topRec, ccBinName, mustExist )
  local meth = "validateCC()";
  GP=E and trace("[ENTER]:<%s:%s> BinName(%s) MustExist(%s)",
    MOD, meth, tostring( ccBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  ldt_common.validateBinName( ccBinName );

  local ccCtrl;

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid user CC Bin
  -- (*) Must have a valid Map in the user CC Bin.
  -- (*) Must have a valid value CC Bin
  -- (*) Must have a valid Time CC Bin
  --
  -- Otherwise, If "mustExist" is false, then basically we're just going
  -- to check that our bin includes MAGIC, if it is non-nil.
  -- For now, we're going to return LDT errors, just because they have
  -- already been defined for most of the problems.
  if mustExist == true then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end

    -- Control Bin Must Exist
    if( topRec[ccBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> CC BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(ccBinName) );
      error( ldte.ERR_BIN_DOES_NOT_EXIST );
    end

    -- check that our bin is (mostly) there
    ccCtrl = topRec[ccBinName]; -- The main  CC Map structure

    if ccCTRL[CC_Magic] ~= MAGIC then
      GP=E and warn("[ERROR EXIT]:<%s:%s>CC BIN(%s) Corrupted (no magic:1)",
            MOD, meth, tostring( ccBinName ) );
      error( ldte.ERR_BIN_DAMAGED );
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if( topRec == nil ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error( ldte.ERR_TOP_REC_NOT_FOUND );
    end
    -- If the record does not yet exist in storage, then create it here.
    -- What got passed in to us might just be a memory-only copy.
    if( not aerospike:exists( topRec ) ) then
      GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
      rc = aerospike:create( topRec );
      if( rc ~= 0 ) then
        warn("[ERROR]<%s:%s>Problems Creating TopRec rc(%d)", MOD, meth, rc );
        error( ldte.ERR_TOPREC_CREATE );
      end
    end

    if ( topRec[ccBinName] ~= nil ) then
      ccCtrl = topRec[ccBinName]; -- The main CC Map structure
      if ccCtrl[CC_Magic] ~= CC_MAGIC then
        GP=E and warn("[ERROR EXIT]:<%s:%s> CC BIN(%s) Corrupted (no magic:2)",
              MOD, meth, tostring( ccBinName ) );
        error( ldte.ERR_BIN_DAMAGED );
      end
    end -- if worth checking
  end -- else for must exist
  GP=E and trace("[EXIT]:<%s:%s> Ok", MOD, meth );

  return ccCtrl; -- to be trusted ONLY in the mustExist == true case;
end -- validateCCBin()

-- =========================================================================
-- cc_read()
-- =========================================================================
-- Capped Collection Write takes the incoming key value and returns the
-- "payload" that is associated with it.
-- Parms:
-- (*) topRec: The main Aerospike Record
-- (*) binName: The name of the CC Bin
-- (*) key: The atomic search value for the user's object
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- Return:
-- Success: The Payload Object
-- Error: Error Code
-- =========================================================================
function test.cc_read( topRec, ccBinName, key, src )
  local meth = "test.cc_read()";
  GP=F and trace("[ENTER]:<%s:%s>:key(%s)", MOD, meth, tostring(key));

  -- Validate that everything is there and in the right state.
  local ccCtrl = validateCC( topRec, ccBinName, true );

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  -- Search the Value List for the object.  If found, then return the
  -- "payLoad", which will be a field inside the "foundObject" map.
  local result = "NOT FOUND";
  local foundObject = llist.find(topRec, ccValBin, key, nil, nil, nil, src);
  if ( foundObject ~= nil and foundObject.PayLoad ~= nil ) then
    result = foundObject.PayLoad;
  end

  GP=E and trace("[EXIT]:<%s:%s> result(%s)", MOD, meth, tostring(result));
  return result;
end -- test.cc_read()

-- =========================================================================
-- cc_write()
-- =========================================================================
-- Capped Collection Write takes the incoming key/value and creates two
-- different Map Objects:
-- (1) It creates a Value Map with the input value as "key" and the user value
--     as the "payload":
--     valueMap.Key = key
--     valueMap.Payload = value;
-- (2) it creates a Time Map with the insert time value as the ordered
--     search value and the key as the payload:
--     timeMap.Time = insertTime
--     timeMap.Key = key
-- Parms:
-- (*) topRec: The main Aerospike Record
-- (*) ccBinName: The name of the CC Bin in the AS Record.
-- (*) key: The atomic search value for the user's object
-- (*) value: The payload that the user wants to store with the key
-- (*) createSpec: the Creation Specification for the CC collection.
-- (*) src: Sub-Rec Context - Needed for repeated calls from caller
-- =========================================================================
function test.cc_write( topRec, ccBinName, key, value, createSpec, src)
  local meth = "write_type_test()";
  GP=F and trace("[ENTER]:<%s:%s>:key(%s) value(%s) C Spec(%s)",
      MOD, meth, tostring(key), tostring(value), tostring(createSpec));

  -- Validate that everything is there and in the right state.
  -- If the record doesn't exist, then set it up.
  validateCC( topRec, ccBinName, false );

  -- If the record does not exist, or the BIN does not exist, then we must
  -- create it and initialize the LDT map. Otherwise, use it.
  if( topRec[ccBinName] == nil ) then
    GP=F and trace("[INFO]<%s:%s>LLIST CONTROL BIN does not Exist:Creating",
      MOD, meth );
    -- set up our new CC Bin
    setupCCBin( topRec, ccBinName, createSpec );
  end

  local ccCtrl = topRec[ccBinName];

  -- Gather up our statistics
  local minValue   = ccCtrl[CC_MinValue];
  local maxValue   = ccCtrl[CC_MaxValue];
  local totalOps   = ccCtrl[CC_TotalOps];  -- total ops (Read and Write)
  local totalCnt   = ccCtrl[CC_TotalCount]; -- total inserted
  local currentCnt = ccCtrl[CC_CurrentCount]; -- current size
  local duplicates = ccCtrl[CC_Duplicates];

  -- Get the bin names for the two LLIST LDTs comprising the CC.
  local ccValBin   = ccCtrl[CC_ValBin];
  local ccTimeBin  = ccCtrl[CC_TimeBin];

  -- Search for the value.  If not found, insert it.  If found,
  -- then udpate the TIME associated with the value.
  local addedCount = 1;
  local key = getKeyValue( ccCtrl, value );
  local insertTime = aerospike:get_current_time();

  -- Create the objects that we're going to store in the lists.
  -- userObject goes in the "Value List"
  -- timeObject goes in the "Time List"
  -- Recall that by default, a map field called "Key" will be used as
  -- the search value if no other key specifier is given.
  local userObject = map();
  userObject.Key = key;
  userObject.PayLoad = value;

  local timeObject = map();
  timeObject.Key = insertTime;
  timeObject.PayLoad  = key;

  -- Init our subrecContext, if necessary.  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  if ( src == nil ) then
    src = ldt_common.createSubRecContext();
  end

  local foundObject = llist.find(topRec,ccValBin,newObject,nil,nil,nil,src);
  if( foundObject == nil ) then
    -- Didn't find it -- so insert it into the val LLIST.
    -- If we're already at capacity, then remove the oldest.
    if( ccCtrl[CC_ItemCount] >= ccCtrl[CC_MaxCount] ) then
        removeOldest( topRec, ccCtrl );
        addedCount = 0;
    end
    llist.add( topRec, ccValBin, userObject, nil, src);
    llist.add( topRec, ccTimeBin, timeObject, nil, src);
  else
    -- We DID find it.  We need to remove the old object and insert the
    -- new one in the Time List.
    updateObject( topRec, ccCtrl, userObject );
    addedCount = 0;
    duplicates = duplicates + 1;
  end

  -- Update the total count, if we added anything.
  totalCnt = totalCnt + addedCount;
  currentCnt = currentCnt + addedCount;

  -- One more operation (an insert or update)
  totalOps = totalOps + 1; 

  -- Update our stats (based on key, not time)
  if ( key < minValue ) then
    minValue = key;
  end
  if ( key > maxValue ) then
    maxValue = key;
  end

  -- Update our official stats in the map
  ctrlMap[CC_MinValue]     = minValue;
  ctrlMap[CC_MaxValue]     = maxValue;
  ctrlMap[CC_TotalOps]     = totalOps;
  ctrlMap[CC_TotalCount]   = totalCnt;
  ctrlMap[CC_CurrentCount] = currentCnt;
  ctrlMap[CC_Duplicates]   = duplicates;

  -- Update any bins that might have changed.
  topRec[ccBinName]     = ccCtrl;
  topRec[ccValBin]      = valList; 
  topRec[ccTimeBin]     = timeMap;
  
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );
  if( rc ~= 0 ) then
    warn("[ERROR]<%s:%s>Problems Updating TopRec rc(%d)",MOD,meth,rc);
    error( ldte.ERR_TOPREC_UPDATE );
  end

  GP=F and trace("[EXIT]:<%s:%s> key(%s) Val(%s) Time(%s)", MOD, meth,
    tostring( key ), tostring( value ), tostring( insertTime ));

  return 0;
end -- test.cc_write()

-- =========================================================================
-- =========================================================================
-- ========================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return test;
-- ========================================================================
--
-- /$$$$$$$$ /$$$$$$$$  /$$$$$$  /$$$$$$$$
-- |__  $$__/| $$_____/ /$$__  $$|__  $$__/
--    | $$   | $$      | $$  \__/   | $$   
--    | $$   | $$$$$   |  $$$$$$    | $$   
--    | $$   | $$__/    \____  $$   | $$   
--    | $$   | $$       /$$  \ $$   | $$   
--    | $$   | $$$$$$$$|  $$$$$$/   | $$   
--    |__/   |________/ \______/    |__/   (LIB)
--                                                           
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
