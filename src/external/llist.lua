-- Large Ordered List (LLIST) Operations
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

-- Track the updates to this module
local MOD="ext_llist_2014_08_06.A";

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LLIST Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LLIST module:
--
-- (*) Status = add( topRec, ldtBinName, newValue, userModule )
-- (*) Status = add_all( topRec, ldtBinName, valueList, userModule )
-- (*) List   = find( topRec, bin, value, module, filter, fargs )
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, userModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = set_capacity( topRec, ldtBinName, new_capacity)
-- (*) Status = get_capacity( topRec, ldtBinName )
-- (*) Number = ldt_exists( topRec, ldtBinName )
-- (*) Number = ldt_validate( topRec, ldtBinName )
-- ======================================================================
-- Deprecated Functions
-- (*) function create( topRec, ldtBinName, createSpec )
-- ======================================================================
-- Reference the LLIST LDT Library Module:
local llist = require('ldt/lib_llist');

-- Reference the common routines, which include the Sub-Rec Context
-- functions for managing the open sub-recs.
local ldt_common = require('ldt/ldt_common');
-- ======================================================================

-- ======================================================================
-- create() :: Deprecated
-- ======================================================================
-- Create/Initialize a Large Ordered List  structure in a bin, using a
-- single LLIST -- bin, using User's name, but Aerospike TYPE (AS_LLIST).
--
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) createSpec: The map or module that contains Create Settings
-- ======================================================================
function create( topRec, ldtBinName, createSpec )
  return llist.create( topRec, ldtBinName, createSpec );
end

-- =======================================================================
-- add() -- insert a value into the ordered list.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) newValue: The value to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function add( topRec, ldtBinName, newValue, createSpec )
  return llist.add( topRec, ldtBinName, newValue, createSpec, nil)
end

-- =======================================================================
-- add_all() -- Iterate thru the list and insert each element
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) valueList: The value to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function add_all( topRec, ldtBinName, valueList, createSpec )
  return llist.add_all( topRec, ldtBinName, valueList, createSpec, nil);
end

-- =======================================================================
-- find() -- Locate the object(s) associated with searchKey.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) searchKey: The key value for the objects to be found
-- =======================================================================
function find( topRec, ldtBinName, searchKey )
  return llist.find( topRec, ldtBinName, searchKey, nil);
end

-- =======================================================================
-- range() -- Locate the object(s) between minKey and maxKey
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) minKey: The key value for the beginning of the range search
-- (*) maxKey: The key value for the END       of the range search
-- (*) userModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function range( topRec, ldtBinName, minKey, maxKey, userModule, filter, fargs )
  return llist.range( topRec, ldtBinName, minKey, maxKey, userModule,
                      filter, fargs, nil);
end

-- =======================================================================
-- scan(): Return all elements
-- =======================================================================
-- Use the library find() call with no key (match all) and no filters.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- =======================================================================
function scan( topRec, ldtBinName )
  return llist.find( topRec, ldtBinName, nil, nil, nil, nil, nil);
end

-- =======================================================================
-- filter(): Pass all elements thru the filter and return all that qualify.
-- =======================================================================
-- Use the library find() call with no key, but WITH filters.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) key: The key of the objects to be searched (key==nil means ALL)
-- (*) userModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function filter( topRec, ldtBinName, key, userModule, filter, fargs )
  return llist.find( topRec, ldtBinName, key, userModule, filter, fargs, nil);
end

-- ======================================================================
-- remove(): Remove all items corresponding to the specified key.
-- ======================================================================
-- Remove (Delete) the item(s) that correspond to "key".
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName
-- (3) key: The key we'll search for
-- ======================================================================
function remove( topRec, ldtBinName, key )
  return llist.remove( topRec, ldtBinName, key, nil);
end

-- ========================================================================
-- destroy(): Remove the LDT entirely from the record.
-- ========================================================================
-- Destroy works essentially the same way for all LDTs. 
-- Release all of the storage associated with this LDT and remove the
-- control structure of the bin.  If this is the LAST LDT in the record,
-- then ALSO remove the HIDDEN LDT CONTROL BIN.
--
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  return llist.destroy( topRec, ldtBinName, nil);
end -- destroy()

-- ========================================================================
-- size() -- return the number of elements (item count) in the LDT.
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function size( topRec, ldtBinName )
  return llist.size( topRec, ldtBinName );
end

-- ========================================================================
-- config() -- return the config settings
-- get_config() -- return the config settings
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
end

function get_config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
end

-- ========================================================================
-- get_capacity() -- return the current capacity setting for this LDT.
-- set_capacity() -- set the current capacity setting for this LDT.
-- ========================================================================
-- Parms:
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- Result:
--   rc >= 0  (the current capacity)
--   rc < 0: Aerospike Errors
-- ========================================================================
function get_capacity( topRec, ldtBinName )
  return llist.get_capacity( topRec, ldtBinName );
end

function set_capacity( topRec, ldtBinName, capacity )
  return llist.set_capacity( topRec, ldtBinName, capacity );
end

-- ========================================================================
-- ldt_exists() -- return 1 if LDT (with the right shape and size) exists
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_exists( topRec, ldtBinName )
  return llist.ldt_exists( topRec, ldtBinName );
end


-- ========================================================================
-- ldt_validate() -- return 1 if LDT is in good shape.
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_validate( topRec, ldtBinName )
  return llist.validate( topRec, ldtBinName );
end

-- ========================================================================
-- Dump: Debugging/Tracing mechanism -- show the WHOLE tree.
-- ========================================================================
function dump( topRec, ldtBinName )
  return llist.dump( nil, topRec, ldtBinName );
end

-- =======================================================================
-- Bulk Number Load Operations
-- =======================================================================
-- Add significant amounts to a list -- to aid in testing LLIST.
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
  for i = 1, count*incr, incr do
    if rand then
      value = math.random(1, 10000);
    else
      value = startValue + i;
    end
      rc = llist.add( topRec, ldtBinName, value, createSpec, src );
      if ( rc < 0 ) then
          warn("<%s:%s>Error Return from Add Value(%d)",MOD, meth, rc );
          error("INTERNAL ERROR");
      end
  end

  info("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- bulk_number_load()

-- ========================================================================
--   _      _     _____ _____ _____ 
--  | |    | |   |_   _/  ___|_   _|
--  | |    | |     | | \ `--.  | |  
--  | |    | |     | |  `--. \ | |  
--  | |____| |_____| |_/\__/ / | |  
--  \_____/\_____/\___/\____/  \_/   (EXTERNAL)
--                                  
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --

