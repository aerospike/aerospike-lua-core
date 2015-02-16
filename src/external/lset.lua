-- AS Large Set (LSET) Operations

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
local MOD="ext_lset_2014_12_03.A";

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LSET Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The following external functions are defined in the LSET module:
--
-- (*) Status = add( topRec, ldtBinName, newValue, createSpec )
-- (*) Status = add_all( topRec, ldtBinName, valueList, createSpec )
-- (*) Object = get( topRec, ldtBinName, searchValue ) 
-- (*) Number = exists( topRec, ldtBinName, searchValue ) 
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, filterModule, filter, fargs )
-- (*) Status = remove( topRec, ldtBinName, searchValue ) 
-- (*) Object = take( topRec, ldtBinName, searchValue ) 
-- (*) Status = destroy( topRec, ldtBinName )
-- (*) Number = size( topRec, ldtBinName )
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Number = ldt_exists( topRec, ldtBinName )
-- (*) Number = ldt_validate( topRec, ldtBinName )
-- ======================================================================
-- Reference the LMAP LDT Library Module:
local lset = require('ldt/lib_lset');

-- Reference the common routines, which include the Sub-Rec Context
-- functions for managing the open sub-recs.
local ldt_common = require('ldt/ldt_common');

-- ======================================================================
-- || create      || (deprecated)
-- || lset_create || (deprecated)
-- ======================================================================
-- Create/Initialize a AS LSet structure in a record, using multiple bins
--
-- We will use predetermined BIN names for this initial prototype:
-- 'LSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- There can be ONLY ONE set in a record, as we are using preset fixed names
-- for the bin.
-- +========================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... |
-- +========================================================================+
-- Set Ctrl Bin is a Map -- containing control info and the list of
-- bins (each of which has a list) that we're using.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) ldtBinName: The name of the bin for the AS Large Set
-- (*) createSpec: A map of create specifications:  Most likely including
--               :: a package name with a set of config parameters.
-- ======================================================================
function create( topRec, ldtBinName, createSpec )
  return lset.create( topRec, ldtBinName, createSpec, nil);
end

function lset_create( topRec, ldtBinName, createSpec )
  return lset.create( topRec, ldtBinName, createSpec, nil);
end

-- ======================================================================
-- add() -- Add an object to the LSET
-- lset_insert()  :: Deprecated
-- lset_create_and_insert()  :: Deprecated
-- ======================================================================
function add( topRec, ldtBinName, newValue, createSpec )
  return lset.add( topRec, ldtBinName, newValue, createSpec, nil );
end -- add()

function lset_insert( topRec, ldtBinName, newValue )
  return lset.add( topRec, ldtBinName, newValue, nil, nil );
end -- lset_insert()

function lset_create_and_insert( topRec, ldtBinName, newValue, createSpec )
  return lset.add( topRec, ldtBinName, newValue, createSpec, nil );
end -- lset_create_and_insert()

-- ======================================================================
-- add_all() -- Add a LIST of objects to the LSET.
-- lset_insert_all() :: Deprecated
-- ======================================================================
function add_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, nil, nil );
end -- add_all()

function lset_insert_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, nil, nil );
end

function lset_create_and_insert_all( topRec, ldtBinName, valueList )
  return lset.add_all( topRec, ldtBinName, valueList, createSpec, nil );
end

-- ======================================================================
-- get(): Return the object matching <searchValue>
-- get_with_filter() :: not currently exposed in the API
-- lset_search()
-- lset_search_then_filter()
-- ======================================================================
function get( topRec, ldtBinName, searchValue )
  return lset.get( topRec, ldtBinName, searchValue, nil, nil, nil, nil);
end -- get()

function get_then_filter(topRec,ldtBinName,searchValue,filterModule,filter,fargs)
  return lset.get(topRec,ldtBinName,searchValue,filterModule,filter,fargs, nil);
end -- get_with_filter()

function lset_search( topRec, ldtBinName, searchValue )
  return lset.get( topRec, ldtBinName, searchValue, nil, nil, nil,nil);
end -- lset_search()

function
lset_search_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.get(topRec, ldtBinName, searchValue, nil, filter, fargs,nil);
end -- lset_search_then_filter()

-- ======================================================================
-- exists() -- return 1 if item exists, otherwise return 0.
-- exists_with_filter() :: Not currently exposed in the API
-- lset_exists() -- with and without filter
-- ======================================================================
function exists( topRec, ldtBinName, searchValue )
  return lset.exists( topRec, ldtBinName, searchValue, nil,nil, nil,nil );
end -- lset_exists()

function exists_with_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.exists( topRec, ldtBinName, searchValue, nil,filter, fargs,nil );
end -- lset_exists_with_filter()

function lset_exists( topRec, ldtBinName, searchValue )
  return lset.exists( topRec, ldtBinName, searchValue, nil,nil, nil,nil );
end -- lset_exists()

function
lset_exists_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.exists( topRec, ldtBinName, searchValue, nil,filter, fargs,nil );
end -- lset_exists_then_filter()

-- ======================================================================
-- scan() -- Return a list containing ALL of LSET
-- lset_scan() :: Deprecated
-- ======================================================================
function scan( topRec, ldtBinName )
  return lset.scan(topRec,ldtBinName,nil, nil, nil,nil);
end -- scan()

function lset_scan( topRec, ldtBinName )
  return lset.scan(topRec,ldtBinName,nil, nil, nil,nil);
end -- lset_search()

-- ======================================================================
-- filter() -- Return a list containing all of LSET that passed <filter>
-- lset_scan_then_filter() :: Deprecated
-- ======================================================================
function filter(topRec, ldtBinName, filterModule, filter, fargs)
  return lset.scan(topRec, ldtBinName, filterModule, filter, fargs,nil);
end -- filter()

-- This was defined to use only predefined filter UDFs. Now Deprecated.
function lset_scan_then_filter(topRec, ldtBinName, filter, fargs)
  return lset.scan(topRec, ldtBinName, nil, filter,fargs,nil);
end -- lset_search_then_filter()

-- ======================================================================
-- remove() -- remove <searchValue> from the LSET
-- take() -- remove and RETURN <searchValue> from the LSET
-- lset_delete() :: Deprecated
-- Return Status (OK or error)
-- ======================================================================
function remove( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,false,nil);
end -- remove()

function take( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,true,nil );
end -- remove()

function lset_delete( topRec, ldtBinName, searchValue )
  return lset.remove(topRec,ldtBinName,searchValue,nil,nil,nil,false,nil);
end -- lset_delete()

-- ======================================================================
-- remove_with_filter()
-- lset_delete_then_filter()
-- ======================================================================
function remove_with_filter( topRec, ldtBinName, searchValue, filterModule,
  filter, fargs )
  return lset.remove(topRec,ldtBinName,searchValue,filterModule,
    filter,fargs,false,nil);
end -- delete_then_filter()

function
lset_delete_then_filter( topRec, ldtBinName, searchValue, filter, fargs )
  return lset.remove(topRec,ldtBinName,searchValue,nil,filter,fargs,false,nil);
end -- lset_delete_then_filter()

-- ========================================================================
-- destroy() -- Remove the LDT entirely from the record.
-- lset_remove() :: Deprecated
-- ========================================================================
-- Completely remove this LDT: all data and the bin content.
-- If this is the LAST LDT in the record, then ALSO remove the
-- HIDDEN LDT CONTROL BIN.
-- ==>  Remove the ESR, Null out the topRec bin.  The rest will happen
-- during NSUP cleanup.
-- Parms:
-- (1) topRec: the user-level record holding the LSET Bin
-- (2) ldtBinName: The name of the LSET Bin
-- Result:
--   res = 0: all is well
--   res = -1: Some sort of error
-- ========================================================================
function destroy( topRec, ldtBinName )
  return lset.destroy( topRec, ldtBinName, nil );
end

function lset_remove( topRec, ldtBinName )
  return lset.destroy( topRec, ldtBinName, nil );
end

-- ========================================================================
-- size() -- Return the number of objects in the LSET.
-- lset_size() :: Deprecated
-- ========================================================================
function size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

function get_size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

function lset_size( topRec, ldtBinName )
  return lset.size( topRec, ldtBinName );
end

-- ========================================================================
-- config()      -- return the config settings in the form of a map
-- get_config()  -- return the config settings in the form of a map
-- lset_config() -- return the config settings in the form of a map
-- ========================================================================
function config( topRec, ldtBinName )
  return lset.config( topRec, ldtBinName );
end

function get_config( topRec, ldtBinName )
  return lset.config( topRec, ldtBinName );
end

function lset_config( topRec, ldtBinName )
  return lset.config( topRec, ldtBinName );
end

-- ========================================================================
-- ========================================================================
--
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function dump( topRec, ldtBinName )

  -- Init our subrecContext. .  The SRC tracks all open
  -- SubRecords during the call. Then, allows us to close them all at the end.
  -- For the case of repeated calls from Lua, the caller must pass in
  -- an existing SRC that lives across LDT calls.
  local src = ldt_common.createSubRecContext();
  return lset.dump( topRec, ldtBinName, src )
end

-- ========================================================================
-- ldt_exists() -- return 1 if LDT (with the right shape and size) exists
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_exists( topRec, ldtBinName )
  return lset.ldt_exists( topRec, ldtBinName );
end

-- ========================================================================
-- ldt_validate() -- return 1 if LDT is in good shape.
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_validate( topRec, ldtBinName )
  return lset.validate( topRec, ldtBinName );
end

-- =======================================================================
-- Bulk Number Load Operations
-- =======================================================================
-- Add significant amounts to a set -- to aid in testing LSET.
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
      rc = lset.add( topRec, ldtBinName, value, createSpec, src );
      if ( rc < 0 ) then
          warn("<%s:%s>Error Return from Add Value(%d)",MOD, meth, rc );
          error("INTERNAL ERROR");
      end
  end

  info("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- bulk_number_load()

-- ========================================================================
--   _      _____ _____ _____ 
--  | |    /  ___|  ___|_   _|
--  | |    \ `--.| |__   | |  
--  | |     `--. \  __|  | |  
--  | |____/\__/ / |___  | |  
--  \_____/\____/\____/  \_/  (EXTERNAL)
--                            
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
