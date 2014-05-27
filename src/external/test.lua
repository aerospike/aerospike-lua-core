-- UDF Test Operations
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

-- Used for version tracking in logging/debugging
local MOD = "test:2014_05_27.A";

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  UDF Test Operations >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the TEST module:
--
-- ======================================================================
-- Reference the LDT and UDF Library Modules:
local lib_test = require('ldt/lib_test');
local lmap     = require('ldt/lib_lmap');
local lset     = require('ldt/lib_lset');
local llist    = require('ldt/lib_llist');
local lstack   = require('ldt/lib_lstack');

-- ========================================================================
-- test_one() -- Return a single value
-- test_one_lib() -- Return a single value -- using our library
-- ========================================================================
function test_one( topRec, ldtBinName )
  return 1;
end

function test_one_lib( topRec, ldtBinName )
  return lib_test.one( topRec, ldtBinName );
end

-- ========================================================================
-- test_same()      -- Return Val parm.  Used for perf measurement.
-- test_same_lib()  -- Return Val parm.  -- Using our Library
-- ========================================================================
function test_same( topRec, ldtBinName, val )
  if( val == nil or type(val) ~= "number") then
    return 1;
  else
    return val;
  end
end -- test_same()

function test_same_lib( topRec, ldtBinName, val )
    return lib_test.same( topRec, ldtBinName, val );
end -- test_same_lib()

-- ========================================================================
-- test_list()     -- Create a LIST of N elements and return it.
-- test_list_lib() -- Create a LIST of N elements, using our Library
-- ========================================================================
function test_list( topRec, ldtBinName, size )
  -- local meth = "test_list()";
  -- info("[ENTER]<%s:%s> ldtBinName(%s) size(%s)", MOD, meth,
  --   tostring(ldtBinName), tostring(size))
  if( size == nil or type(size) ~= "number") then
    error("BAD SIZE PARAMETER");
  end

  resultList = list();

  for i = 1, size, 1 do
    list.append(resultList, i);
  end

  -- info("[EXIT]<%s:%s> ResultList(%s)", MOD, meth, tostring(resultMap));
  return resultList;
end

function test_list_lib( topRec, ldtBinName, size )
  return lib_test.list_test( topRec, ldtBinName, size );
end

-- ========================================================================
-- test_map()      -- Create a MAP of N elements and return it.
-- test_map_lib()  -- Create a MAP of N elements, using our Library.
-- ========================================================================
function test_map( topRec, ldtBinName, size )
  -- local meth = "test_map()";
  -- info("[ENTER]<%s:%s> ldtBinName(%s) size(%s)",
    -- MOD, meth, tostring(ldtBinName), tostring(size))
  if( size == nil or type(size) ~= "number") then
    error("BAD SIZE PARAMETER");
  end
  local resultMap = map();
  local newMap;

  for i = 1, size, 1 do
    newMap = map();
    newMap[i]   = i + 10000;
    newMap[i+1] = i + 20000;
    newMap[i+2] = i + 30000;
    resultMap[i] = newMap;
  end

  -- info("[EXIT]<%s:%s> ResultMap(%s)", MOD, meth, tostring(resultMap));
  return resultMap;
end -- test.map_test()

-- ========================================================================
function test_map_lib( topRec, ldtBinName, size )
  return lib_test.map_test( topRec, ldtBinName, size );
end

-- ========================================================================
-- lmap_load( topRec, ldtBinName, nameSeed, mapSeed )
-- ========================================================================
-- Generate a load test from LUA.
-- ========================================================================
function lmap_load( topRec, ldtBinName, nameSeed, valueSeed, count )
  local meth = "lmap_load";
  info("[ENTER]<%s:%s>BinNameType(%s) NSeedType(%s) VSeedType(%s) CountT(%s)",
  MOD, meth, type(ldtBinName), type(nameSeed), type(valueSeed), type(count));

  info("[ENTER]<%s:%s> ldtBinName(%s) Nseed(%s), Vseed(%s) count(%s)",
    MOD, meth, ldtBinName,
    tostring(nameSeed), tostring(valueSeed), tostring(count) );

  local name;
  local value;
  for i = 1, count, 1 do
    name = 200 + i;
    value = tostring(valueSeed) .. tostring(nameSeed);
    lmap.put( topRec, ldtBinName, name, value );
  end -- for

  info("[EXIT]<%s:%s>", MOD, meth );
end -- lmap_load()

-- ========================================================================
-- ========================================================================
function llist_load( topRec, ldtBinName, valueSeed, count )
  local meth = "llist_load";

  info("[ENTER]<%s:%s>BinNameType(%s) VSeedType(%s) CountT(%s)",
    MOD, meth, type(ldtBinName), type(valueSeed), type(count));

  info("[ENTER]<%s:%s> ldtBinName(%s) Vseed(%s) count(%s)",
    MOD, meth, tostring(ldtBinName), tostring(valueSeed), tostring(count) );

  local value;
  for i = 1, count, 1 do
    value = 200 + i;
    llist.add( topRec, ldtBinName, value );
  end -- for

  info("[EXIT]<%s:%s>", MOD, meth );
end -- llist_load()

-- ========================================================================
-- ========================================================================
function lset_load( topRec, ldtBinName, valueSeed, count )
  local meth = "lset_load";
  info("[ENTER]<%s:%s>BinNameType(%s) VSeedType(%s) CountT(%s)",
    MOD, meth, type(ldtBinName), type(valueSeed), type(count));

  info("[ENTER]<%s:%s> ldtBinName(%s) Vseed(%s) count(%s)",
    MOD, meth, tostring(ldtBinName), tostring(valueSeed), tostring(count) );

  local value;
  for i = 1, count, 1 do
    value = 200 + i;
    llist.add( topRec, ldtBinName, value );
  end -- for

  info("[EXIT]<%s:%s>", MOD, meth );
end -- lset_load()

-- ========================================================================
-- ========================================================================
function lstack_load( topRec, ldtBinName, valueSeed, count )
  local meth = "lstack_load";
  info("[ENTER]<%s:%s>BinNameType(%s) VSeedType(%s) CountT(%s)",
    MOD, meth, type(ldtBinName), type(valueSeed), type(count));

  info("[ENTER]<%s:%s> ldtBinName(%s) Vseed(%s) count(%s)",
    MOD, meth, tostring(ldtBinName), tostring(valueSeed), tostring(count) );

  local value;
  for i = 1, count, 1 do
    value = 200 + i;
    llist.add( topRec, ldtBinName, value );
  end -- for

  info("[EXIT]<%s:%s>", MOD, meth );
end -- lstack_load()

-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- |||||||||  CAPPED COLLECTION TEST FUNCTIONS ||||||||||||||||||||||||||||
-- ========================================================================
-- ========================================================================
-- cc_create()
-- ========================================================================
-- Create an Ordered Capped Collection, with the specified Max Item Limit.
-- the bin named <ccBinName>.
-- ========================================================================
function cc_create( topRec, ccBinName, maxLimit )
    return lib_test.cc_create( topRec, ccBinName, maxLimit )
end
-- ========================================================================
-- cc_read()
-- ========================================================================
-- Read a value from the Capped Collection.  The CC is searched for the
-- key value, and the Payload is returned (if it is found).
-- ========================================================================
function cc_read( topRec, ccBinName, key )
    return lib_test.cc_read( topRec, ccBinName, key );
end
-- ========================================================================
-- cc_write()
-- ========================================================================
-- Write a value into an Ordered Capped Collection, which corresponds with
-- the bin named <ccBinName>. The Ordered Capped Collection takes two
-- parameters:  A key value (which is the ordering value) and a "value",
-- which is the "payload" that gets stored with the key.  The PayLoad is
-- commonly a complex object, such as a List or a Map, but it can be any
-- type (number, string, bytes, list, map).
--
-- A Capped Collection can be explicitly created with a cc_create() call,
-- or it can be implicitly created with the cc_write() call.  If the
-- creation is implicit, then it is advised to submit a configuration
-- map for the "createSpec" parameter, which will set(override) several
-- important values, such as the CC Max Limit.
-- ========================================================================
function cc_write( topRec, ccBinName, key, value, createSpec)
    return lib_test.cc_write( topRec, ccBinName, key, value, createSpec);
end
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
-- ========================================================================
--
-- /$$$$$$$$ /$$$$$$$$  /$$$$$$  /$$$$$$$$
-- |__  $$__/| $$_____/ /$$__  $$|__  $$__/
--    | $$   | $$      | $$  \__/   | $$   
--    | $$   | $$$$$   |  $$$$$$    | $$   
--    | $$   | $$__/    \____  $$   | $$   
--    | $$   | $$       /$$  \ $$   | $$   
--    | $$   | $$$$$$$$|  $$$$$$/   | $$   
--    |__/   |________/ \______/    |__/   External
--                                                           
-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
