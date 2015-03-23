-- Large Map (LMAP) Operations (Last Update 2014.03.10)

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
local MOD="ext_lmap_2014_12_03.A";

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<   LMAP Main Functions   >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The following external functions are defined in the LMAP module:
--
-- Status = put( topRec, ldtBinName, newName, newValue, createSpec) 
-- Status = put_all( topRec, ldtBinName, nameValueMap, createSpec)
-- List   = get( topRec, ldtBinName, searchName )
-- List   = exists( topRec, ldtBinName, searchName )
-- Map    = scan( topRec, ldtBinName )
-- List   = name_list( topRec, ldtBinName )
-- List   = filter( topRec, ldtBinName, filterModule, filter, fargs )
-- Object = remove( topRec, ldtBinName, searchName )
-- Status = destroy( topRec, ldtBinName )
-- Number = size( topRec, ldtBinName )
-- Map    = get_config( topRec, ldtBinName )
-- Status = ldt_exists( topRec, ldtBinName )
-- Status = ldt_validate( topRec, ldtBinName )
-- ======================================================================
-- Reference the LMAP LDT Library Module:
local lmap = require('ldt/lib_lmap');
local llist = require('ldt/lib_llist');
local ldt_common = require('ldt/ldt_common');
local IS_LIST = false

-- ======================================================================
-- create() ::  (deprecated)
-- ======================================================================
-- Create/Initialize a Map structure in a bin, using a single LMAP
-- bin, using User's name, but Aerospike TYPE (AS_LMAP)
--
-- The LMAP starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record -- in the first lmap bin. 
-- Once the first lmap list goes over its item-count limit, we switch to 
-- standard mode and the entries get collated into a single LDR. We then
-- generate a digest for this LDR, hash this digest over N bins of a digest
-- list. 
-- Please refer to lmap_design.lua for details. 
-- 
-- Parameters: 
-- (1) topRec: the user-level record holding the LMAP Bin
-- (2) ldtBinName: The name of the LMAP Bin
-- (3) createSpec: The createSpec containing the "adjust_settings()" function
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
function create( topRec, ldtBinName, createSpec )
  if (IS_LIST == false) then
    return lmap.create( topRec, ldtBinName, createSpec );
  else
    return llist.create( topRec, ldtBinName, createSpec );
  end
end -- create()

-- ======================================================================
-- put() -- Insert a Name/Value pair into the LMAP
-- put_all() -- Insert multiple name/value pairs into the LMAP
-- ======================================================================
function put( topRec, ldtBinName, newName, newValue, createSpec )
  if (IS_LIST == false) then
    return lmap.put( topRec, ldtBinName, newName, newValue, createSpec, nil);
  else 
    local newValMap = map.new(1);
    newValMap['key'] = newName
    newValMap['value'] = newValue
    return llist.add( topRec, ldtBinName, newValMap, createSpec, nil);
  end
end -- put()

function put_all( topRec, ldtBinName, nameValMap, createSpec )
  if (IS_LIST == false) then
    return lmap.put_all( topRec, ldtBinName, nameValMap, createSpec, nil);
  else
    local valList = list();
    local newValMap = map.new(#nameValMap);
    for name, value in map.pairs( nameValMap ) do
      newValMap['key'] = name;
      newValMap['value'] = value;
      list.append(valList, newValMap);
    end
    return llist.add_all( topRec, ldtBinName, valList, createSpec, nil);
  end
end -- put_all()

-- ========================================================================
-- get() -- Return a map containing the requested name/value pair.
-- ========================================================================
function get( topRec, ldtBinName, searchName )
  if (IS_LIST == false) then
    return lmap.get(topRec, ldtBinName, searchName, nil, nil, nil, nil);
  else
    local newLookupMap = map.new(1);
    newLookupMap['key'] = searchName;
    local resultList = llist.find(topRec, ldtBinName, newLookupMap, nil)
    local resultMap = map.new(#resultList);
    for i = 1, #resultList, 1 do
      local l = resultList[i];
      resultMap[l['key']] = l['value'];
    end
    return resultMap;
  end
end -- get()

-- ========================================================================
-- exists() -- Return 1 if the item exists, else return 0.
-- ========================================================================
function exists( topRec, ldtBinName, searchName )
  if (IS_LIST == false) then
    return lmap.exists(topRec, ldtBinName, searchName);
  else 
    local newLookupMap = map.new(1);
    newLookupMap['key'] = searchName;
    local rc = llist.find(topRec, ldtBinName, newLookupMap);
    if (rc ~= nil) then
      return 1;
    else 
      return 0;
    end
  end
end -- exists()

-- ========================================================================
-- scan() -- Return a map containing ALL name/value pairs.
-- ========================================================================
function scan( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.scan(topRec, ldtBinName, nil, nil, nil, nil);
  else
    local resultList = llist.scan(topRec, ldtBinName, nil, nil, nil, nil, nil);
    local resultMap = map.new(#resultList);
    for i = 1, #resultList, 1 do
      local l = resultList[i];
      resultMap[l['key']] = l['value'];
    end
    return resultMap;
  end
end -- scan()

-- ========================================================================
-- filter() -- Return a map containing all Name/Value pairs that passed
--             thru the supplied filter( fargs ).
-- ========================================================================
function filter( topRec, ldtBinName, filterModule, filter, fargs )
  if (IS_LIST == false) then
    return lmap.scan(topRec, ldtBinName, filterModule, filter, fargs, nil);
  else 
    local resultList = llist.scan(topRec, ldtBinName, nil, filterModule, filter, fargs, nil);
    local resultMap = map.new(#resultList);
    for i = 1, #resultList, 1 do
      local l = resultList[i];
      resultMap[l['key']] = l['value'];
    end
    return resultMap;
  end
end -- filter()

-- ========================================================================
-- remove() -- Remove the name/value pair matching <searchName>
-- ========================================================================
function remove( topRec, ldtBinName, searchName )
  if (IS_LIST == false) then
    return lmap.remove(topRec, ldtBinName, searchName, nil, nil, nil, nil );
  else
    local newLookupMap = map.new(1);
    newLookupMap['key'] = searchName;
    return llist.remove(topRec, ldtBinName, newLookupMap, nil);
  end
end -- remove()

-- ========================================================================
-- destroy() - Entirely obliterate the LDT (record bin value and all)
-- ========================================================================
function destroy( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.destroy( topRec, ldtBinName, nil );
  else
    return llist.destroy( topRec, ldtBinName, nil );
  end
end -- destroy()

-- ========================================================================
-- size() -- return the number of elements (item count) in the set.
-- ========================================================================
function size( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.size( topRec, ldtBinName );
  else
    return llist.size( topRec, ldtBinName );
  end
end -- size()

-- ========================================================================
-- config()     -- return the config settings
-- get_config() -- return the config settings
-- ========================================================================
function config( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.config( topRec, ldtBinName );
  else
    return llist.config( topRec, ldtBinName );
  end
end -- config()

function get_config( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.config( topRec, ldtBinName );
  else
    return llist.config( topRec, ldtBinName );
  end
end -- get_config()

-- ========================================================================
-- ldt_exists() -- return 1 if LDT (with the right shape and size) exists
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_exists( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.ldt_exists( topRec, ldtBinName );
  else
    return llist.ldt_exists( topRec, ldtBinName );
  end
end

-- ========================================================================
-- ldt_validate() -- return 1 if LDT is in good shape
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function ldt_validate( topRec, ldtBinName )
  if (IS_LIST == false) then
    return lmap.validate( topRec, ldtBinName );
  else
    return llist.validate( topRec, ldtBinName );
  end
end

-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
-- Developer Functions
-- (*) dump()
-- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> -- <D> <D> <D> 
--
-- ========================================================================
-- dump()
-- ========================================================================
-- Dump the full contents of the LDT (structure and all).
--
-- Dump the full contents of the Large Map, with Separate Hash Groups
-- shown in the result. Unlike scan which simply returns the contents of all 
-- the bins, this routine gives a tree-walk through or map walk-through of the
-- entire lmap structure. 
-- Return a LIST of lists -- with Each List marked with it's Hash Name.
-- ========================================================================
function dump( topRec, ldtBinName )
    -- Set up the Sub-Rec Context to track open Sub-Records.
  local src = ldt_common.createSubRecContext();

  return lmap.dump( topRec, ldtBinName, src );
end


-- =======================================================================
-- Bulk Number Load Operations
-- =======================================================================
-- Add significant amounts to an LDT -- to aid in testing LMAP.
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
  local valueString;
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
    valueString = "ABC" .. value;
    rc = lmap.put( topRec, ldtBinName, value, valueString, createSpec, src );
    if ( rc < 0 ) then
        warn("<%s:%s>RC (%d) from PUT: Name(%s) Val(%s)",MOD, meth, rc,
          tostring(value), tostring(valueString));
        error("INTERNAL ERROR");
    end
  end

  info("[EXIT]<%s:%s> RC(%d)", MOD, meth, rc );
  return rc;
end -- bulk_number_load()

-- ========================================================================
--   _     ___  ___  ___  ______ 
--  | |    |  \/  | / _ \ | ___ \
--  | |    | .  . |/ /_\ \| |_/ /
--  | |    | |\/| ||  _  ||  __/ 
--  | |____| |  | || | | || |    
--  \_____/\_|  |_/\_| |_/\_|    (EXTERNAL)
--                               
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
