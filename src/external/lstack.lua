-- Large Stack Object (LSTACK) Operations.

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

-- Track the updates to this module
local MOD="ext_lstack_2014_08_06.C";

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSTACK Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The following external functions are defined in the LSTACK module:
--
-- (*) Status = push( topRec, ldtBinName, newValue, createSpec )
-- (*) Status = push_all( topRec, ldtBinName, valueList, createSpec )
-- (*) List   = peek( topRec, ldtBinName, peekCount ) 
-- (*) List   = pop( topRec, ldtBinName, popCount ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, peekCount,filterModule,filter,fargs)
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- (*) Status = ldt_exists( topRec, ldtBinName )
-- (*) Status = ldt_validate( topRec, ldtBinName )
-- ======================================================================
-- Reference the LSTACK LDT Library Module
local lstack = require('ldt/lib_lstack');

-- Reference the LDT COMMON Library Module
local ldt_common = require('ldt/ldt_common');

-- ======================================================================
-- || create        || (deprecated)
-- || lstack_create || (deprecated)
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
--
-- Parms
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- (3) createSpec: The map (not list) of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function create( topRec, ldtBinName, createSpec )
  return lstack.create( topRec, ldtBinName, createSpec );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_create( topRec, ldtBinName, createSpec )
  return lstack.create( topRec, ldtBinName, createSpec );
end

-- =======================================================================
-- push()
-- lstack_push()
-- =======================================================================
-- Push a value on the stack, with the optional parm to set the LDT
-- configuration in case we have to create the LDT before calling the push.
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec, nil );
end -- push()

function create_and_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec, nil );
end -- create_and_push()

-- OLD EXTERNAL FUNCTIONS
function lstack_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec, nil );
end -- end lstack_push()

function lstack_create_and_push( topRec, ldtBinName, newValue, createSpec )
  return lstack.push( topRec, ldtBinName, newValue, createSpec, nil );
end -- lstack_create_and_push()

-- =======================================================================
-- Stack Push ALL
-- =======================================================================
-- Iterate thru the list and call localStackPush on each element
-- Notice that the "createSpec" can be either the old style map or the
-- new style user modulename.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function push_all( topRec, ldtBinName, valueList, createSpec )
  return lstack.push_all( topRec, ldtBinName, valueList, createSpec, nil );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_push_all( topRec, ldtBinName, valueList, createSpec )
  return lstack.push_all( topRec, ldtBinName, valueList, createSpec, nil );
end

-- =======================================================================
-- peek() -- with and without filters
-- lstack_peek() -- with and without filters
--
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- =======================================================================
-- NEW EXTERNAL FUNCTIONS
function peek( topRec, ldtBinName, peekCount )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, nil, nil, nil );
end -- peek()

function filter( topRec, ldtBinName, peekCount, filterModule, filter, fargs )
  return lstack.peek(topRec,ldtBinName,peekCount,filterModule,filter,fargs, nil );
end -- peek_then_filter()

-- OLD EXTERNAL FUNCTIONS (didn't have filterModule in the first version)
function lstack_peek( topRec, ldtBinName, peekCount )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, nil, nil, nil );
end -- lstack_peek()

-- OLD EXTERNAL FUNCTIONS (didn't have filterModule in the first version)
function lstack_peek_then_filter( topRec, ldtBinName, peekCount, filter, fargs )
  return lstack.peek( topRec, ldtBinName, peekCount, nil, filter, fargs, nil );
end -- lstack_peek_then_filter()


-- =======================================================================
-- scan() -- without filters (just get everything)
--
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function scan( topRec, ldtBinName )
  return lstack.peek( topRec, ldtBinName, 0, nil, nil, nil, nil );
end -- scan()

-- =======================================================================
-- pop() -- Return and remove values from the top of stack
-- =======================================================================
function pop( topRec, ldtBinName, peekCount, filterModule, filter, fargs )
  return lstack.pop(topRec,ldtBinName,peekCount,filterModule,filter,fargs, nil );
end -- peek_then_filter()

-- ========================================================================
-- size() -- return the number of elements (item count) in the stack.
-- get_size() -- return the number of elements (item count) in the stack.
-- lstack_size() -- return the number of elements (item count) in the stack.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the size)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function size()

function get_size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function get_size()

-- OLD EXTERNAL FUNCTIONS
function lstack_size( topRec, ldtBinName )
  return lstack.size( topRec, ldtBinName );
end -- function get_size()

-- ========================================================================
-- get_capacity() -- return the current capacity setting for LSTACK.
-- lstack_get_capacity() -- return the current capacity setting for LSTACK.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function get_capacity( topRec, ldtBinName )
  return lstack.get_capacity( topRec, ldtBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_get_capacity( topRec, ldtBinName )
  return lstack.get_capacity( topRec, ldtBinName );
end

-- ========================================================================
-- config() -- return the lstack config settings.
-- get_config() -- return the lstack config settings.
-- lstack_get_config() -- return the lstack config settings.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) ldtBinName: The name of the LSO Bin
-- Result:
--   res = (when successful) config Map 
--   res = (when error) nil
-- NOTE: Any parameter that might be printed (for trace/debug purposes)
-- must be protected with "tostring()" so that we do not encounter a format
-- error if the user passes in nil or any other incorrect value/type.
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

function get_config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_config( topRec, ldtBinName )
  return lstack.config( topRec, ldtBinName );
end

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lstack_remove() -- Remove the LDT entirely from the record.
-- ========================================================================
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Question  -- Reset the record[ldtBinName] to NIL (does that work??)
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) binName: The name of the LSO Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function destroy( topRec, ldtBinName )
  return lstack.destroy( topRec, ldtBinName, nil );
end -- destroy()

-- OLD EXTERNAL FUNCTIONS
function lstack_remove( topRec, ldtBinName )
  return lstack.destroy( topRec, ldtBinName, nil );
end -- lstack_remove()
-- ========================================================================
-- lstack_set_storage_limit()
-- lstack_set_capacity()
-- set_storage_limit()
-- set_capacity()
-- ========================================================================
-- This is a special command to both set the new storage limit.  It does
-- NOT release storage, however.  That is done either lazily after a 
-- warm/cold insert or with an explit lstack_trim() command.
-- Parms:
-- (*) topRec: the user-level record holding the LSO Bin
-- (*) ldtBinName: The name of the LSO Bin
-- (*) newLimit: The new limit of the number of entries
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
-- NEW EXTERNAL FUNCTIONS
function lstack_set_capacity( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

function set_capacity( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

-- OLD EXTERNAL FUNCTIONS
function lstack_set_storage_limit( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

function set_storage_limit( topRec, ldtBinName, newLimit )
  return lstack.set_capacity( topRec, ldtBinName, newLimit );
end

-- ========================================================================
-- ldt_exists() -- return 1 if LDT (with the right shape and size) exists
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_exists( topRec, ldtBinName )
  return lstack.ldt_exists( topRec, ldtBinName );
end

-- ========================================================================
-- ldt_validate() -- return 1 if LDT is in good shape.
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_validate( topRec, ldtBinName )
  return lstack.validate( topRec, ldtBinName, nil, nil );
end

-- ========================================================================
-- MEASUREMENT FUNCTIONS:
-- See how fast we can call a MINIMAL function.
-- (*) one(): just call and return:
-- (*) same(): Return what's passed in to verify the call.
-- ========================================================================
-- one()          -- Just return 1.  This is used for perf measurement.
-- same()         -- Return Val parm.  Used for perf measurement.
-- ========================================================================
-- Do the minimal amount of work -- just return a number so that we
-- can measure the overhead of the LDT/UDF infrastructure.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) Val:  Random number val (or nothing)
-- Result:
--   res = 1 or val
-- ========================================================================
function one( topRec, ldtBinName )
  return 1;
end

function same( topRec, ldtBinName, val )
  if( val == nil or type(val) ~= "number") then
    return 1;
  else
    return val;
  end
end


-- =======================================================================
-- Bulk Number Load Operations
-- =======================================================================
-- Add significant amounts to an LDT -- to aid in testing LSTACK.
-- From "startValue", add "count" many more items, incrementing by 1 each time.
-- If the caller wants a pseudo-random pattern, she has some options:
-- (1) Call this function with random intervals -- like this:
--    (2..299, 1..99, 5..599, 3..399)
-- (2) Call this function with interleaved ranges (increment by, say, 3)
--    (0..299<incr 3>, 1..299<incr 3>, 2..299<incr 3>
--    First Range:  0, 3, 6, 9 ...
--    Second Range: 1, 4, 7, 10 ...
--    Third Range:  2, 5, 8, 11 ...
-- (3) Build a similar function that doesn't increment, but instead uses
--     math.random.  Notice, however, that if we use random, then we have to
--     configure it correctly so that it doesn't complain about duplicates.
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) startValue: The starting value to be inserted
-- (*) count:   The Number of values to insert
-- (*) incr:  The amount to increment each time to get the next value.
--            if (-1), then use the RANDOM function
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function
bulk_number_load(topRec, ldtBinName, startValue, count, incr, createSpec)
  local meth = "bulk_number_load()";
  info("[ENTER]<%s:%s> Bin(%s) SV(%s) C(%s) Incr(%s) CS(%s)", MOD, meth,
    tostring(ldtBinName), tostring(startValue), tostring(count), 
    tostring(incr), tostring(createSpec));

  -- Check the input values for non-nil
  if startValue == nil or count == nil or incr == nil then
    warn("Input Error: nil Parameters: startValue(%s) Count(%s) Incr(%s)",
      tostring(startValue), tostring(count), tostring(incr));
    error("Nil Input Parameters");
  end

  -- Check the input values for valid types (numbers only)
  if type(startValue) ~= "number" or type(count) ~= "number" or
     type(incr) ~= "number"
  then
    warn("Input Error: Bad Param types: startValue(%s) Count(%s) Incr(%s)",
      type(startValue), type(count), type(incr));
    error("Bad Input Parameter Types");
  end

  -- Init our subrecContext. .  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  local src = ldt_common.createSubRecContext();

  local rc = 0;
  local value;
  local rand = false;
  if( incr == -1 ) then
    -- set up for RANDOM values, not incremented values
    rand = true;
    incr = 1;
  end
  local endValue = count * incr;

  for i = 1, endValue, incr do
    if rand then
      value = math.random(1, 10000);
    else
      value = startValue + i;
    end
      rc = lstack.push( topRec, ldtBinName, value, createSpec, src );
      if ( rc < 0 ) then
          warn("<%s:%s>Error Return from Add Value(%d)",MOD, meth, rc );
          error("INTERNAL ERROR");
      end
  end

  info("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- bulk_number_load()


-- =======================================================================
-- Bulk Object Load Operations
-- =======================================================================
-- Add Objects to Large Stack -- to aid in testing the auxilliary features
-- such as filters, compression, etc.
-- This will be based on the "PERSON OBJECT", which has the following fields:
-- Note that only FirstName, and LastName are required.
--
-- "FirstName": User First Name (String)
-- "LastName": User Last Name (String)
-- "DOB": User data of birth (String)
-- "SSNum": User social security number (Number)
-- "HomeAddr": User Home Address (String)
-- "HomePhone": User Home Phone Number (Number)
-- "CellPhone": User Cell Phone Number (Number)
-- "DL": User Driver's License number (String)
-- "UserPref": User Preferences (Map)
-- "UserCom": User Comments (List)
-- "Hobbies": User Hobbies (list)
--
-- Although this function takes the same parameters as bulk_number_load(), the
-- values are used differently.
--
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) startValue: The starting value to be inserted
-- (*) count:   The Number of values to insert
-- (*) incr:  The amount to increment each time to get the next value.
--            if (-1), then use the RANDOM function
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function
bulk_object_load(topRec, ldtBinName, startValue, count, incr, createSpec)
  local meth = "bulk_object_load()";
  info("[ENTER]<%s:%s> Bin(%s) SV(%s) C(%s) Incr(%s) CS(%s)", MOD, meth,
    tostring(ldtBinName), tostring(startValue), tostring(count), 
    tostring(incr), tostring(createSpec));

  -- Check the input values for non-nil
  if startValue == nil or count == nil or incr == nil then
    warn("Input Error: nil Parameters: startValue(%s) Count(%s) Incr(%s)",
      tostring(startValue), tostring(count), tostring(incr));
    error("Nil Input Parameters");
  end

  -- Check the input values for valid types (numbers only)
  if type(startValue) ~= "number" or type(count) ~= "number" or
     type(incr) ~= "number"
  then
    warn("Input Error: Bad Param types: startValue(%s) Count(%s) Incr(%s)",
      type(startValue), type(count), type(incr));
    error("Bad Input Parameter Types");
  end

  -- Init our subrecContext. .  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  local src = ldt_common.createSubRecContext();

  local rc = 0;
  local value;
  if( incr == -1 ) then
    -- set up for RANDOM values, not incremented values
    rand = true;
    incr = 1;
  end
  local personObject;
  for i = 1, count*incr, incr do
    if rand then
      value = math.random(1, 10000);
    else
      value = startValue + i;
    end
    personObject = ldt_common.createPersonObject( value, incr );

  rc = lstack.push( topRec, ldtBinName, personObject, createSpec, src );
      if ( rc < 0 ) then
          warn("<%s:%s>Error Return from Add Value(%d)",MOD, meth, rc );
          error("INTERNAL ERROR");
      end
  end

  info("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- bulk_object_load()

-- ========================================================================
--   _      _____ _____ ___  _____  _   __
--  | |    /  ___|_   _/ _ \/  __ \| | / /
--  | |    \ `--.  | |/ /_\ \ /  \/| |/ / 
--  | |     `--. \ | ||  _  | |    |    \ 
--  | |____/\__/ / | || | | | \__/\| |\  \
--  \_____/\____/  \_/\_| |_/\____/\_| \_/   (EXTERNAL)
--                                        
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
