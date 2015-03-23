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
local MOD="ext_llist_2014_11_20.A";

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LLIST Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- The following external functions are defined in the LLIST module:
--
-- (*) Status = add   (topRec, ldtBinName, newValue, createSpec)
-- (*) Status = update(topRec, ldtBinName, newValue, createSpec)
-- (*) List   = find  (topRec, bin, keyList, module, filter, fargs )
-- (*) List   = find_first (topRec, ldtBinName, count, module, filter, fargs)
-- (*) List   = find_last  (topRec, ldtBinName, count, module, filter, fargs)
-- (*) List   = find_range (topRec,ldtBinName, minKey, maxKey, filterModule, filter, fargs)
-- (*) List   = exists (topRec, ldtBinName, value)
-- (*) Status = remove (topRec, ldtBinName, searchKey) 
-- (*) Status = remove_range (topRec, ldtBinName, minKey, maxKey)
-- (*) Status = destroy (topRec, ldtBinName)
-- (*) Number = size    (topRec, ldtBinName)
-- (*) Map    = config  (topRec, ldtBinName)
-- (*) Map    = setPageSize  (topRec, ldtBinName, size)
-- (*) Number = ldt_exists   (topRec, ldtBinName)
-- (*) Number = ldt_validate (topRec, ldtBinName)
-- ======================================================================
-- Reference the LLIST LDT Library Module:
local llist = require('ldt/lib_llist');

-- Reference the common routines, which include the Sub-Rec Context
-- functions for managing the open sub-recs.
local ldt_common = require('ldt/ldt_common');
-- ======================================================================
-- These values should be "built-in" for our Lua, but it is either missing
-- or inconsistent, so we define it here.  We use this when we check to see
-- if a value is a LIST or a MAP.
local Map  = getmetatable( map() );
local List = getmetatable( list() );

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
  if (getmetatable(newValue) == List) then
    return llist.add_all(topRec, ldtBinName, newValue, createSpec, nil)
  else
    return llist.add( topRec, ldtBinName, newValue, createSpec, nil)
  end
end

-- =======================================================================
-- update() -- insert a new value into an ordered list, if the value is
--             not already present, or if it is present (and uniqueness is
--             set), then overwrite the old value with the new value.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) newValue: The value to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function update( topRec, ldtBinName, newValue, createSpec )
  if (getmetatable(newValue) == List) then
    return llist.update_all( topRec, ldtBinName, newValue, createSpec, nil)
  else
    return llist.update( topRec, ldtBinName, newValue, createSpec, nil)
  end
end

-- =======================================================================
-- exist() -- Check if the object(s) associated with searchKey. 0/1
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) searchKey: The key value for the objects to be found
-- =======================================================================
function exists(topRec, ldtBinName, searchKeyList)
  return llist.exists(topRec, ldtBinName, searchKeyList, nil);
end

-- =======================================================================
-- find() -- Locate the object(s) associated with searchKey.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) searchKey: The key value for the objects to be found
-- =======================================================================
function find(topRec, ldtBinName, searchKeyList, filterModule, filter, fargs)
  return llist.find( topRec, ldtBinName, searchKeyList, filterModule, filter, fargs, nil);
end
-- Find and Remove
function take(topRec, ldtBinName, searchKeyList, filterModule, filter, fargs)
  return llist.find( topRec, ldtBinName, searchKeyList, filterModule, filter, fargs, nil, true);
end

-- =======================================================================
-- find_from() -- Locate the object(s) between minKey and minKey+count
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) from: The key value for the beginning of the search
-- (*) count: count number of values from key
-- (*) filterModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function find_from( topRec, ldtBinName, fromKey, count, filterModule, filter, fargs )
  return llist.range( topRec, ldtBinName, fromKey, nil, count, filterModule,
                      filter, fargs, nil);
end
-- Find and Remove
function take_from( topRec, ldtBinName, fromKey, count, filterModule, filter, fargs )
  return llist.range( topRec, ldtBinName, fromKey, nil, count, filterModule,
                      filter, fargs, nil, true);
end


-- =======================================================================
-- find_first() -- Return the first "count" objects in the large list.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) count: The Number of elements to return from the front of the LLIST.
-- =======================================================================
function find_first(topRec, ldtBinName, count, filterModule, filter, fargs)
  return llist.find_first(topRec, ldtBinName, count, filterModule, filter, fargs, nil);
end
-- Find and remove
function take_first(topRec, ldtBinName, count, filterModule, filter, fargs)
  return llist.find_first(topRec, ldtBinName, count, filterModule, filter, fargs, nil, true);
end

-- =======================================================================
-- find_last() -- Return the last "count" objects in the large list.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) count: The Number of elements to return from the end of the LLIST.
-- =======================================================================
function find_last( topRec, ldtBinName, count, filterModule, filter, fargs )
  return llist.find_last(topRec, ldtBinName, count, filterModule, filter, fargs, nil);
end
-- Find and Remove
function take_last( topRec, ldtBinName, count, filterModule, filter, fargs )
  return llist.find_last(topRec, ldtBinName, count, filterModule, filter, fargs, nil, true);
end

-- =======================================================================
-- find_range() -- Locate the object(s) between minKey and maxKey
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) minKey: The key value for the beginning of the range search
-- (*) maxKey: The key value for the END       of the range search
-- (*) filterModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function find_range( topRec, ldtBinName, minKey, maxKey, filterModule, filter, fargs )
  return llist.range( topRec, ldtBinName, minKey, maxKey, nil, filterModule,
                      filter, fargs, nil);
end
-- Find and Remove
function take_range( topRec, ldtBinName, minKey, maxKey, filterModule, filter, fargs )
  return llist.range( topRec, ldtBinName, minKey, maxKey, nil, filterModule,
                      filter, fargs, nil, true);
end

-- =======================================================================
-- find_range_lim() -- Locate the object(s) between minKey and minKey+count
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) minKey: The key value for the beginning of the range search
-- (*) count:  count number of values from minKey
-- (*) filterModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function find_range_lim( topRec, ldtBinName, minKey, maxKey, count, filterModule, filter, fargs )
  return llist.range(topRec, ldtBinName, minKey, maxKey, count, filterModule, filter, fargs, nil);
end
-- Find and Remove
function find_range_lim( topRec, ldtBinName, minKey, maxKey, count, filterModule, filter, fargs )
  return llist.range(topRec, ldtBinName, minKey, maxKey, count, filterModule, filter, fargs, nil, true);
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
  if (getmetatable(key) == List) then
    return llist.remove_all( topRec, ldtBinName, key, nil);
  else
    return llist.remove( topRec, ldtBinName, key, nil);
  end
end

-- (*) Status = remove_range(topRec,ldtBinName,minKey,maxKey)
-- ======================================================================
-- remove_range(): Remove all items in the given range
-- ======================================================================
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName
-- (3) minKey
-- (4) maxKey
-- ======================================================================
function remove_range( topRec, ldtBinName, minKey, maxKey )
  return llist.remove_range( topRec, ldtBinName, minKey, maxKey, nil);
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
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
end

-- ========================================================================
-- config() -- return the config settings
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function setPageSize( topRec, ldtBinName, pageSize )
  return llist.setPageSize( topRec, ldtBinName, pageSize );
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
  return llist.dump( topRec, ldtBinName, nil );
end


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <<  LLIST Main Functions >>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- (*) Map    = get_config( topRec, ldtBinName )
-- (*) Status = remove_all( topRec, ldtBinName, valueList ) 
-- (*) Status = add_all( topRec, ldtBinName, valueList, createSpec )
-- (*) Status = update_all( topRec, ldtBinName, valueList, createSpec )
-- (*) List   = scan( topRec, ldtBinName )
-- (*) List   = filter( topRec, ldtBinName, key, filterModule, filter, fargs )
-- (*) List   = range(topRec,ldtBinName, minKey, maxKey,filterModule,filter,fargs)
-- ======================================================================
-- create()
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
-- add_all() -- Insert a list of values into the LLIST.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) valueList: The list of values to be inserted
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function add_all( topRec, ldtBinName, valueList, createSpec )
  return llist.add_all( topRec, ldtBinName, valueList, createSpec, nil);
end

-- =======================================================================
-- update_all() -- Update a list of values in LLIST.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) valueList: The list of values to be updated.
-- (*) createSpec: The map or module that contains Create Settings
-- =======================================================================
function update_all( topRec, ldtBinName, valueList, createSpec )
  return llist.update_all( topRec, ldtBinName, valueList, createSpec, nil)
end

-- =======================================================================
-- range() -- Locate the object(s) between minKey and maxKey
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) minKey: The key value for the beginning of the range search
-- (*) maxKey: The key value for the END       of the range search
-- (*) filterModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function range( topRec, ldtBinName, minKey, maxKey, filterModule, filter, fargs )
  return llist.range( topRec, ldtBinName, minKey, maxKey, nil, filterModule,
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

-- filter(): Pass all matching elements thru the filter and return all
-- that qualify.
-- =======================================================================
-- Use the library find() call.  Note that passing in a nil key to do a
-- full scan.
-- =======================================================================
-- Parms:
-- (*) topRec: the user-level record holding the LDT Bin
-- (*) ldtBinName: The user's chosen name for the LDT bin
-- (*) key: Search key for a value, or nil to match all values.
-- (*) filterModule: The User's UDF that contains filter functions
-- (*) filter: User Defined Function (UDF) that returns passing values
-- (*) fargs: Arguments passed in to the filter function.
-- =======================================================================
function filter( topRec, ldtBinName, keyList, filterModule, filter, fargs )
  return llist.find( topRec, ldtBinName, keyList, filterModule, filter, fargs, nil);
end

-- ======================================================================
-- remove_all(): Remove all items corresponding to the Value List.
-- ======================================================================
-- Remove (Delete) the item(s) that correspond to "key".
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName
-- (3) valueList: Could be values, could be keys
-- ======================================================================
function remove_all( topRec, ldtBinName, valueList )
  return llist.remove_all( topRec, ldtBinName, valueList, nil);
end

-- ========================================================================
-- get_config() -- return the config settings
-- ========================================================================
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) ldtBinName: The name of the LDT Bin
-- ========================================================================
function get_config( topRec, ldtBinName )
  return llist.config( topRec, ldtBinName );
end
