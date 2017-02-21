/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: RPC API implementation, these are the functions that actually   */
/*           get executed on a destination Chord node when a *_RPC()         */
/*           function is called.                                             */
/*                                                                           */

package chord

import (
	"bytes"
	"errors"
	"fmt"
)

/* Validate that we're executing this RPC on the intended node */
func validateRpc(node *Node, reqId []byte) error {
	if !bytes.Equal(node.Id, reqId) {
		errStr := fmt.Sprintf("Node ids do not match %v, %v", node.Id, reqId)
		return errors.New(errStr)
	}
	return nil
}

/* RPC */
func (node *Node) GetPredecessorId(req *RemoteId, reply *IdReply) error {
	if err := validateRpc(node, req.Id); err != nil {
		return err
	}
	// Predecessor may be nil, which is okay.
	if node.Predecessor == nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
	} else {
		reply.Id = node.Predecessor.Id
		reply.Addr = node.Predecessor.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) GetSuccessorId(req *RemoteId, reply *IdReply) error {
	if err := validateRpc(node, req.Id); err != nil {
		return err
	}

	reply.Id = node.Successor.Id
	reply.Addr = node.Successor.Addr
	reply.Valid = true
	return nil
}

/* RPC */
func (node *Node) Notify(remoteNode *RemoteNode, reply *RpcOkay) error {
	node.notify(remoteNode)
	return nil
}

/* RPC */
func (node *Node) FindSuccessor(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	successor, err := node.findSuccessor(query.Id, false)
	reply.Id = successor.Id
	reply.Addr = successor.Addr
	reply.Valid = true

	return err
}

/* RPC */
func (node *Node) ClosestPrecedingFinger(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}

	id := query.Id

	for i:= KEY_LENGTH - 1; i > -1; i-- {
		finger := node.FingerTable[i].Node
		if Between(finger.Id, node.Id, id) {
			reply.Id =  finger.Id
			reply.Addr = finger.Addr
			reply.Valid = true
			return nil
		}
	}

	reply.Id = node.Id
	reply.Addr = node.Addr
	reply.Valid = true
	return nil
}
