/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Utility functions to help with dealing with ID hashes in Chord. */
/*                                                                           */

package chord

import (
	"testing"
)

type between func (nodeX, nodeA, nodeB []byte) bool

func TestBetween(t *testing.T) {
	a := IntToBytes(3)
	x := IntToBytes(5)
	b := IntToBytes(10)
	check(Between, x, a, b, true, t)
	check(BetweenRightIncl, x, a, b, true, t)

	x = IntToBytes(11)
	check(Between, x, a, b, false, t)
	check(BetweenRightIncl, x, a, b, false, t)

	x = IntToBytes(10)
	check(Between, x, a, b, false, t)
	check(BetweenRightIncl, x, a, b, true, t)

	a = IntToBytes(10)
	x = IntToBytes(11)
	b = IntToBytes(3)

	check(Between, x, a, b, true, t)
	check(BetweenRightIncl, x, a, b, true, t)

	x = IntToBytes(2)
	check(Between, x, a, b, true, t)
	check(BetweenRightIncl, x, a, b, true, t)

	x = IntToBytes(4)
	check(Between, x, a, b, false, t)
	check(BetweenRightIncl, x, a, b, false, t)

	x = IntToBytes(3)
	check(Between, x, a, b, false, t)
	check(BetweenRightIncl, x, a, b, true, t)

	a , b, x = IntToBytes(3), IntToBytes(3), IntToBytes(3)
	check(Between, x, a, b, false, t)
	check(BetweenRightIncl, x, a, b, true, t)

}

func check(fn between, x, a, b []byte, expectedResult bool, t *testing.T) {
	isBetween := fn(x, a, b)
	if isBetween != expectedResult {
		t.Errorf("wrong result", fn, x, a, b, expectedResult)
	}
}
