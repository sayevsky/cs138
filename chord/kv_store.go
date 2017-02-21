/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: API and interal functions to interact with the Key-Value store  */
/*           that the Chord ring is providing.                               */
/*                                                                           */

package chord

import (
	"fmt"
	"log"
)

/*                             */
/* External API Into Datastore */
/*                             */

/* Get a value in the datastore, provided an abitrary node in the ring */
func Get(node *Node, key string) (string, error) {
	remoteNode, err := node.locate(key)
	if err !=nil {
		return "", err
	}
	value, err := Get_RPC(remoteNode, key)

	return value, nil
}

/* Put a key/value in the datastore, provided an abitrary node in the ring */
func Put(node *Node, key string, value string) error {
	log.Println("Put", key, value)
	remoteNode, err := node.locate(key)
	log.Println("located node", remoteNode.Id, "hashed key is", HashKey(key))

	if err !=nil {
		return err
	}
	err = Put_RPC(remoteNode, key, value)

	return err
}

/* Internal helper method to find the appropriate node in the ring */
func (node *Node) locate(key string) (*RemoteNode, error) {

	remoteNode, err := node.findSuccessor(HashKey(key), true)

	return remoteNode, err
}

/*                                                         */
/* RPCs to assist with interfacing with the datastore ring */
/*                                                         */

/* RPC */
func (node *Node) GetLocal(req *KeyValueReq, reply *KeyValueReply) error {
	if err := validateRpc(node, req.NodeId); err != nil {
		return err
	}
	key := req.Key
	node.dsLock.RLock()
	value := node.dataStore[key]
	node.dsLock.RUnlock()
	reply.Key = key
	reply.Value = value
	return nil
}

/* RPC */
func (node *Node) PutLocal(req *KeyValueReq, reply *KeyValueReply) error {
	if err := validateRpc(node, req.NodeId); err != nil {
		return err
	}
	key := req.Key
	value := req.Value

	node.dsLock.Lock()
	node.dataStore[key] = value
	node.dsLock.Unlock()

	reply.Key = key
	reply.Value = value
	return nil
}

/* RPC */
/*
req.NodeId is a successor that has keys we want to transfer to joined node (req.PredId)
which are between
 */
func (node *Node) TransferKeys(req *TransferReq, reply *RpcOkay) error {
	if err := validateRpc(node, req.NodeId); err != nil {
		return err
	}

	joined, errFind := node.findSuccessor(req.FromId, false)
	if errFind != nil {
		return errFind
	}
	predId := req.PredId

	node.dsLock.Lock()
	for k, v := range node.dataStore {
		if BetweenRightIncl(HashKey(k), predId, req.FromId){
			Put_RPC(joined, k, v)
			delete(node.dataStore, k)
		}
	}

	node.dsLock.Unlock()

	reply.Ok = true

	return nil
}

/* Print the contents of a node's data store */
func PrintDataStore(node *Node) {
	fmt.Printf("Node-%v datastore: %v\n", HashStr(node.Id), node.dataStore)
}
