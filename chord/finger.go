/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Finger table related functions for a given Chord node.          */
/*                                                                           */

package chord

import (
	"fmt"
	"time"
	"math/big"
	"log"
)

/* A single finger table entry */
type FingerEntry struct {
	Start []byte       /* ID hash of (n + 2^i) mod (2^m)  */
	Node  *RemoteNode  /* RemoteNode that Start points to */
}

/* Create initial finger table that only points to itself, will be fixed later */
func (node *Node) initFingerTable() {
	node.FingerTable  = make([]FingerEntry, KEY_LENGTH)
	for i, _ := range node.FingerTable {
		finger := FingerEntry{}
		finger.Start = fingerMath(node.Id, i, KEY_LENGTH)
		finger.Node = node.RemoteSelf
		node.FingerTable[i] = finger
	}
	node.Successor = node.RemoteSelf
}

/* Called periodically (in a seperate go routine) to fix entries in our finger table. */
func (node *Node) fixNextFinger(ticker *time.Ticker) {
	i := -1
		for _ = range ticker.C {
			i = (i + 1) % KEY_LENGTH
			finger := node.FingerTable[i]
			nextStart, err := node.findSuccessor(finger.Start, false)
			if err == nil {
				finger.Node = nextStart
				node.FingerTable[i] = finger
			} else {
				log.Println("Error to find next finger", err)
			}

		}
}

/* (n + 2^i) mod (2^m) */
func fingerMath(n []byte, i int, m int) []byte {
	nInt := &big.Int{}
	nInt.SetBytes(n)
	iInt := big.NewInt(int64(i))
	mInt := big.NewInt(int64(m))

	sum := big.Int{}
	absolute := sum.Add(nInt, (&big.Int{}).Exp(big.NewInt(int64(2)), iInt, big.NewInt(int64(0))))
	finger :=  (&big.Int{}).Mod(absolute, (&big.Int{}).Exp(big.NewInt(int64(2)), mInt, big.NewInt(int64(0))))
	return finger.Bytes()
}

/* Print contents of a node's finger table */
func PrintFingerTable(node *Node) {
	fmt.Printf("[%v] FingerTable:\n", HashStr(node.Id))
	for _, val := range node.FingerTable {
		fmt.Printf("\t{start:%v\tnodeLoc:%v %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr)
	}
}
