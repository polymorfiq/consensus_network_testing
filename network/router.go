package network

import (
	"fmt"
	"iter"
	"maps"
	"sync"
)

type RouterAddress string
type Router[T any] struct {
	connections    map[RouterAddress]Connection[T]
	membershipLock sync.Mutex
	incoming       chan RouterReceipt[T]
	outgoing       chan RouterMessage[T]
	dropIncoming   bool
	dropOutgoing   bool
}

func NewRouter[T any]() *Router[T] {
	router := Router[T]{
		connections: make(map[RouterAddress]Connection[T]),
		incoming:    make(chan RouterReceipt[T], 10000),
		outgoing:    make(chan RouterMessage[T], 10000),
	}

	return &router
}

func (router *Router[T]) Incoming() <-chan RouterReceipt[T] {
	return router.incoming
}

func (router *Router[T]) Send(outgoing *RouterMessage[T]) error {
	var targets []Connection[T]
	if outgoing.RouterDeliveryType == BroadcastAll {
		targets = make([]Connection[T], 0, len(router.connections))

		for conn := range maps.Values(router.connections) {
			targets = append(targets, conn)
		}
	} else if outgoing.RouterDeliveryType == BroadcastTargets {
		for _, targetAddr := range outgoing.Targets {
			conn, ok := router.connections[targetAddr]
			if !ok {
				continue
			}

			targets = append(targets, conn)
		}
	}

	if router.dropOutgoing {
		return nil
	}

	for _, target := range targets {
		go func() {
			target.Outgoing() <- outgoing.Message
		}()
	}

	return nil
}

func (router *Router[T]) NumConnections() int {
	return len(router.connections)
}

func (router *Router[T]) Add(id RouterAddress, conn Connection[T]) {
	router.membershipLock.Lock()
	defer router.membershipLock.Unlock()

	connAddr := id
	go func() {
		for {
			msg, ok := <-conn.Incoming()

			if !ok {
				return
			}
			if router.dropIncoming {
				return
			}

			router.incoming <- RouterReceipt[T]{
				FromAddress: connAddr,
				Message:     msg,
			}
		}
	}()

	go func() {
		for {
			select {
			case readErr := <-conn.ReadErrors():
				fmt.Printf("ReadError: %v\n", readErr)
			case writeErr := <-conn.WriteErrors():
				fmt.Printf("WriteError: %v\n", writeErr)
			}
		}
	}()

	router.connections[connAddr] = conn
}

func (router *Router[T]) Remove(connAddr RouterAddress) {
	router.membershipLock.Lock()
	defer router.membershipLock.Unlock()
	delete(router.connections, connAddr)
}

func (router *Router[T]) Addresses() iter.Seq[RouterAddress] {
	return maps.Keys(router.connections)
}

func (router *Router[T]) Connections() iter.Seq[Connection[T]] {
	return maps.Values(router.connections)
}

func (router *Router[T]) SetDropIncoming(enabled bool) {
	router.dropIncoming = enabled
}

func (router *Router[T]) SetDropOutgoing(enabled bool) {
	router.dropOutgoing = enabled
}

type RouterDeliveryType int

const (
	BroadcastAll RouterDeliveryType = iota
	BroadcastTargets
)

type RouterMessage[T any] struct {
	RouterDeliveryType RouterDeliveryType
	Targets            []RouterAddress
	Message            T
}

func NewRouterMessage[T any]() *RouterMessage[T] {
	return &RouterMessage[T]{}
}

type RouterReceipt[T any] struct {
	FromAddress RouterAddress
	Message     T
}

func NewRouterReceipt[T any]() *RouterReceipt[T] {
	return &RouterReceipt[T]{}
}
