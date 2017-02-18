/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Local Chord node functions to interact with the Chord ring.     */
/*                                                                           */

package chord

import (
	"fmt"
	"time"
	"log"
)

// This node is trying to join an existing ring that a remote node is a part of (i.e., other)
func (node *Node) join(other *RemoteNode) error {
	// for first node other is nil
	if other == nil {
		return nil
	}
	node.Predecessor = nil
	successor, err := FindSuccessor_RPC(other, node.Id)
	node.Successor = successor

	return err
}

// Thread 2: Psuedocode from figure 7 of chord paper
func (node *Node) stabilize(ticker *time.Ticker) {
	for _ = range ticker.C {
		if node.IsShutdown {
			fmt.Printf("[%v-stabilize] Shutting down stabilize timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}

		x, err := GetPredecessorId_RPC(node.Successor)
		if err != nil {
			log.Fatal("error", err)
		}

		if x != nil && BetweenRightIncl(x.Id, node.RemoteSelf.Id, node.Successor.Id) {
			node.Successor = x
		}

		// corner case
		if node.Predecessor != nil && EqualIds(node.Id, node.Successor.Id) && !EqualIds(node.Id, node.Predecessor.Id) {
			node.Successor = node.Predecessor
		}

		Notify_RPC(node.Successor, node.RemoteSelf)

	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {
	predecessor := node.Predecessor
	if predecessor == nil || Between(remoteNode.Id, predecessor.Id, node.Id) {
		// corner case: if remotenode == node => do not set predecessor
		if !EqualIds(remoteNode.Id, node.Id) {
			node.Predecessor = remoteNode
			//TransferKeys_RPC(node.RemoteSelf, predecessor, remoteNode.Id)
		}
	}
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	// corner case: if node == node.successor then return this node
	if EqualIds(node.Id, node.Successor.Id) {
		return node.Successor, nil
	}
	predecessor, err := node.findPredecessor(id)
	if err != nil {
		return nil, err
	}
	return GetSuccessorId_RPC(predecessor)
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	n1 := node.RemoteSelf
	successor, err := GetSuccessorId_RPC(n1)
	if err != nil {
		return nil, err
	}
	// make linear search for now
	//TODO change to finger table
	for !BetweenRightIncl(id, n1.Id, successor.Id) {
		n1 = successor
		successor, err = GetSuccessorId_RPC(n1)
		if err != nil {
			return nil, err
		}
	}
	return n1, nil
}
