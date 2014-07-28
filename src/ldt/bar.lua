
-- ======================================================================
-- searchKeyListBinary(): Search the Key list in a Root or Inner Node
-- ======================================================================
-- Search the key list, return the index of the value that represents the
-- child pointer that we should follow.  Notice that this is DIFFERENT
-- from the Leaf Search, which treats the EQUAL case differently.
-- ALSO -- the Objects in the Leaves may be TRANSFORMED (e.g. compressed),
-- so they potentially need to be UN-TRANSFORMED before they can be
-- read.
--
-- For this example:
--              +---+---+---+---+
-- KeyList      |111|222|333|444|
--              +---+---+---+---+
-- DigestList   A   B   C   D   E
--
-- Search Key 100:  Position 1 :: Follow Child Ptr A
-- Search Key 111:  Position 2 :: Follow Child Ptr B
-- Search Key 200:  Position 2 :: Follow Child Ptr B
-- Search Key 222:  Position 2 :: Follow Child Ptr C
-- Search Key 555:  Position 5 :: Follow Child Ptr E
--
-- Note that in the case of Duplicate values (and some of the dups may make
-- it up into the parent node key lists), we have to ALWAYS get the LEFT-MOST
-- value (regardless of ascending/descending values) so that we get ALL of
-- them (in case we're searching for a set of values).
--
-- Parms:
-- (*) ldtMap: Main control Map
-- (*) keyList: The list of keys (from root or inner node)
-- (*) searchKey: if nil, then is always LESS THAN the list
-- Return:
-- OK: Return the Position of the Digest Pointer that we want
-- ERRORS: Return ERR_GENERAL (bad compare)
-- ======================================================================
local function searchKeyListBinary( ldtMap, keyList, searchKey )
  local meth = "searchKeyListBinary()";
  GP=E and trace("[ENTER]<%s:%s>searchKey(%s)", MOD,meth,tostring(searchKey));

  GP=D and trace("[DEBUG]<%s:%s>KeyList(%s)", MOD,meth,tostring(keyList));

  -- Note that the parent caller has already checked for nil search key.

  -- Binary Search of the KeyList.  Find the appropriate entry and return
  -- the index.  Note that we're assuming ASCENDING values first, then will
  -- generalize later for ASCENDING and DESCENDING (a dynamic compare function
  -- will help us make that pluggable).
  local resultIndex = 0;
  local compareResult = 0;
  local listSize = list.size( keyList );
  local entryKey;
  local foundStart = 0; -- shows where the value chain started, or zero

  --  Initialize the Start, Middle and End numbers
  local iStart,iEnd,iMid = 1,listSize,0
  local finalState = 0; -- Shows where iMid ends up pointing.
  while iStart <= iEnd do
    -- calculate middle
    iMid = math.floor( (iStart+iEnd)/2 );
    -- get compare value from the DB List (no translate for keys)
    local entryKey = keyList[iMid];
    compareResult = keyCompare( searchKey, entryKey );

    GP=F and trace("[Loop]<%s:%s>Key(%s) S(%d) M(%d) E(%d) EK(%s) CR(%d)",
      MOD, meth, tostring(searchKey), iStart, iMid, iEnd, tostring(entryKey),
      compareResult);

    if compareResult == CR_EQUAL then
      foundStart = iMid;
      -- If we're UNIQUE, then we're done. Otherwise, we have to look LEFT
      -- to find the first NON-matching position.
      if( ldtMap[R_KeyUnique] == AS_TRUE ) then
        GP=F and trace("[FOUND KEY]: <%s:%s> : SrchValue(%s) Index(%d)",
          MOD, meth, tostring(searchKey), iMid);
      else
        -- There might be duplicates.  Scan left to find left-most matching
        -- key.  Note that if we fall off the front, keyList[0] should
        -- (in theory) be defined to be NIL, so the compare just fails and
        -- we stop.
        entryKey = keyList[iMid - 1];
        while searchKey == entryKey do
          iMid = iMid - 1;
          entryKey = keyList[iMid - 1];
        end
        GP=F and trace("[FOUND DUP KEY]: <%s:%s> : SrchValue(%s) Index(%d)",
          MOD, meth, tostring(searchKey), iMid);
      end
      return iMid + 1; -- Right Child Pointer that goes with iMid
    end -- if found, we've returned.

    -- Keep Searching
    if compareResult == CR_LESS_THAN then
      iEnd = iMid - 1;
      finalState = 0; -- At the end, iMid points at the Insert Point.
    else
      iStart = iMid + 1;
      finalState = 1; -- At the end, iMid points BEFORE the Insert Point.
    end
  end -- while binary search


  -- If we're here, then iStart > iEnd, so we have to return the index of
  -- the correct child pointer that matches the search.
  -- Final state shows us where we are relative to the last compare.  If our
  -- last compare:: Cmp(searchKey, entryKey) shows SK < EK, then the value
  -- of iMid 
  resultIndex = iMid + finalState;

  GP=F and trace("[Result]<%s:%s> iStart(%d) iMid(%d) iEnd(%d)", MOD, meth,
    iStart, iMid, iEnd );
  GP=F and trace("[Result]<%s:%s> Final(%d) ResultIndex(%d)",
    MOD, meth, finalState, resultIndex);

  if DEBUG then
    entryKey = keyList[iMid + finalState];
    GP=F and trace("[Result]<%s:%s> ResultIndex(%d) EntryKey at RI(%s)",
      MOD, meth, resultIndex, tostring(entryKey));
  end

  GP=F and trace("[FOUND Insert Point]: <%s:%s> SKey(%s) KeyList(%s)", 
    MOD, meth, tostring(searchKey), tostring(keyList));

  return resultIndex;
end -- searchKeyListBinary()
