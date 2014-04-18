-- UDF Test Operations
local MOD = "test:2014_04_20.A";

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  UDF Test Operations >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the TEST module:
--
-- ======================================================================
-- Reference the TEST UDF Library Module:
local lib_test = require('ldt/lib_test');

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
  info("[ENTER]<test_list()> ldtBinName(%s) size(%s)",
    tostring(ldtBinName), tostring(size))
  if( size == nil or type(size) ~= "number") then
    error("BAD SIZE PARAMETER");
  end

  resultList = list();

  for i = 1, size, 1 do
    list.append(resultList, i);
  end
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
  info("[ENTER]<test_map()> ldtBinName(%s) size(%s)",
    tostring(ldtBinName), tostring(size))
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
  return resultMap;

end -- test.map_test()

function test_map_lib( topRec, ldtBinName, size )
  return lib_test.map_test( topRec, ldtBinName, size );
end

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
