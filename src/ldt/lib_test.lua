-- Test Facility for trying things out.
local MOD="lib_test_2014_04_04.A";

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
local GP;      -- Global Print Instrument
local F=true; -- Set F (flag) to true to turn ON global print
local E=true; -- Set E (ENTER/EXIT) to true to turn ON Enter/Exit print
local B=true; -- Set B (Banners) to true to turn ON Banner Print
local GD;     -- Global Debug instrument.
local DEBUG=true; -- turn on for more elaborate state dumps.

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
