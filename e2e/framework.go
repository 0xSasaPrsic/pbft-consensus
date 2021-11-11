package e2e

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/0xPolygon/ibft-consensus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func initTracer(name string) *sdktrace.TracerProvider {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String(name),
		),
	)
	if err != nil {
		panic("failed to create resource")
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		panic("failed to trace exporter")
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider
}

type cluster struct {
	t      *testing.T
	nodes  map[string]*node
	tracer *sdktrace.TracerProvider
	hook   transportHook
}

func newIBFTCluster(t *testing.T, name, prefix string, count int, hook ...transportHook) *cluster {
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("%s_%d", prefix, i)
	}

	tt := &transport{}
	if len(hook) == 1 {
		tt.addHook(hook[0])
	}

	c := &cluster{
		t:      t,
		nodes:  map[string]*node{},
		tracer: initTracer("fuzzy_" + name),
		hook:   tt.hook,
	}
	for _, name := range names {
		trace := c.tracer.Tracer(name)
		n, _ := newIBFTNode(name, names, trace, tt)
		n.c = c
		c.nodes[name] = n
	}
	return c
}

func (c *cluster) syncWithNetwork(ourselves string) (uint64, []*ibft.SealedProposal) {
	var height uint64
	var proposals []*ibft.SealedProposal

	for _, n := range c.nodes {
		if n.name == ourselves {
			continue
		}
		if c.hook != nil {
			// we need to see if this transport does allow those two nodes to be connected
			// Otherwise, that node should not be elegible to sync
			if !c.hook.Connects(ibft.NodeID(ourselves), ibft.NodeID(n.name)) {
				continue
			}
		}
		localHeight, data := n.getProposals()
		if localHeight > height {
			height = localHeight
			proposals = data
		}
	}
	return height, proposals
}

func (c *cluster) resolveNodes(nodes ...[]string) []string {
	queryNodes := []string{}
	if len(nodes) == 1 {
		for _, n := range nodes[0] {
			if _, ok := c.nodes[n]; !ok {
				panic("node not found in query")
			}
		}
		queryNodes = nodes[0]
	} else {
		for n := range c.nodes {
			queryNodes = append(queryNodes, n)
		}
	}
	return queryNodes
}

func (c *cluster) IsStuck(timeout time.Duration, nodes ...[]string) {
	queryNodes := c.resolveNodes(nodes...)

	nodeHeight := map[string]uint64{}
	isStuck := func() bool {
		for _, n := range queryNodes {
			height := c.nodes[n].currentHeight()
			if lastHeight, ok := nodeHeight[n]; ok {
				if lastHeight != height {
					return false
				}
			} else {
				nodeHeight[n] = height
			}
		}
		return true
	}

	timer := time.NewTimer(timeout)
	for {
		select {
		case <-time.After(200 * time.Millisecond):
			if !isStuck() {
				c.t.Fatal("it is not stuck")
			}
		case <-timer.C:
			return
		}
	}
}

func (c *cluster) WaitForHeight(num uint64, timeout time.Duration, nodes ...[]string) {
	// we need to check every node in the ensemble?
	// yes, this should test if everyone can agree on the final set.
	// note, if we include drops, we need to do sync otherwise this will never work

	queryNodes := c.resolveNodes(nodes...)

	enough := func() bool {
		for _, name := range queryNodes {
			if c.nodes[name].currentHeight() < num {
				return false
			}
		}
		return true
	}

	timer := time.NewTimer(timeout)
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			if enough() {
				return
			}
		case <-timer.C:
			c.t.Fatal("timeout")
		}
	}
}

func (c *cluster) Start() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *cluster) StartNode(name string) {
	c.nodes[name].Start()
}

func (c *cluster) StopNode(name string) {
	c.nodes[name].Stop()
}

func (c *cluster) Stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
	if err := c.tracer.Shutdown(context.Background()); err != nil {
		panic("failed to shutdown TracerProvider")
	}
}

type node struct {
	lock sync.Mutex

	c *cluster

	name     string
	ibft     *ibft.Ibft
	cancelFn context.CancelFunc
	stopped  uint64

	// validator nodes
	nodes []string

	// list of proposals
	proposals []*ibft.SealedProposal
}

func newIBFTNode(name string, nodes []string, trace trace.Tracer, tt *transport) (*node, error) {
	kk := key(name)
	con := ibft.New(kk, tt, ibft.WithTracer(trace))

	tt.Register(ibft.NodeID(name), func(msg *ibft.MessageReq) {
		// pipe messages from mock transport to ibft
		con.PushMessage(msg)
	})

	n := &node{
		nodes:     nodes,
		proposals: []*ibft.SealedProposal{},
		name:      name,
		ibft:      con,
		stopped:   0,
	}
	return n, nil
}

func (n *node) isStuck(num uint64) (uint64, bool) {
	// get max heigh in the network
	height, _ := n.c.syncWithNetwork(n.name)

	if height > num {
		return height, true
	}
	return 0, false
}

func (n *node) lastProposer() ibft.NodeID {
	lastProposer := ibft.NodeID("")
	if len(n.proposals) != 0 {
		lastProposer = n.proposals[len(n.proposals)-1].Proposer
	}
	return lastProposer
}

func (n *node) getProposals() (uint64, []*ibft.SealedProposal) {
	n.lock.Lock()
	defer n.lock.Unlock()

	res := []*ibft.SealedProposal{}
	res = append(res, n.proposals...)

	number := uint64(0)
	if len(res) != 0 {
		number = uint64(res[len(res)-1].Number)
	}
	return number, res
}

func (n *node) currentHeight() uint64 {
	n.lock.Lock()
	defer n.lock.Unlock()

	number := uint64(1) // initial height is always 1 since 0 is the genesis
	if len(n.proposals) != 0 {
		number = n.proposals[len(n.proposals)-1].Number
	}
	return number
}

func (n *node) Insert(pp *ibft.SealedProposal) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.proposals = append(n.proposals, pp)
	return nil
}

func (n *node) Start() {
	if n.cancelFn != nil {
		panic("already started")
	}

	// create the ctx and the cancelFn
	ctx, cancelFn := context.WithCancel(context.Background())
	n.cancelFn = cancelFn

	go func() {
	SYNC:
		// 'sync up' with the network
		_, history := n.c.syncWithNetwork(n.name)
		n.proposals = history

		for {
			fsm := &fsm{
				n:            n,
				nodes:        n.nodes,
				lastProposer: n.lastProposer(),

				// important: in this iteration of the fsm we have increased our height
				height: n.currentHeight() + 1,
			}
			if err := n.ibft.SetBackend(fsm); err != nil {
				panic(err)
			}

			// start the execution
			n.ibft.Run(ctx)

			switch n.ibft.GetState() {
			case ibft.SyncState:
				// we need to go back to sync
				goto SYNC
			case ibft.DoneState:
				// everything worked, move to the next iteration
			default:
				// stopped
				return
			}
		}
	}()
}

func (n *node) IsRunning() bool {
	return n.cancelFn != nil
}

func (n *node) Stop() {
	if n.cancelFn == nil {
		panic("already stopped")
	}
	n.cancelFn()
	n.cancelFn = nil
}

type key string

func (k key) NodeID() ibft.NodeID {
	return ibft.NodeID(k)
}

func (k key) Sign(b []byte) ([]byte, error) {
	return b, nil
}

// -- fsm --

type fsm struct {
	n            *node
	nodes        []string
	lastProposer ibft.NodeID
	height       uint64
}

func (f *fsm) Height() uint64 {
	return f.height
}

func (f *fsm) IsStuck(num uint64) (uint64, bool) {
	return f.n.isStuck(num)
}

func (f *fsm) BuildProposal() (*ibft.Proposal, error) {
	proposal := &ibft.Proposal{
		Data: []byte{byte(f.Height())},
		Time: time.Now().Add(1 * time.Second),
	}
	return proposal, nil
}

func (f *fsm) Validate(proposal []byte) error {
	// always validate for now
	return nil
}

func (f *fsm) Insert(pp *ibft.SealedProposal) error {
	return f.n.Insert(pp)
}

func (f *fsm) ValidatorSet() ibft.ValidatorSet {
	valsAsNode := []ibft.NodeID{}
	for _, i := range f.nodes {
		valsAsNode = append(valsAsNode, ibft.NodeID(i))
	}
	vv := valString{
		nodes:        valsAsNode,
		lastProposer: f.lastProposer,
	}
	return &vv
}

func (f *fsm) Hash(p []byte) []byte {
	h := sha1.New()
	h.Write(p)
	return h.Sum(nil)
}

type valString struct {
	nodes        []ibft.NodeID
	lastProposer ibft.NodeID
}

func (v *valString) CalcProposer(round uint64) ibft.NodeID {
	seed := uint64(0)
	if v.lastProposer == ibft.NodeID("") {
		seed = round
	} else {
		offset := 0
		if indx := v.Index(v.lastProposer); indx != -1 {
			offset = indx
		}
		seed = uint64(offset) + round + 1
	}

	pick := seed % uint64(v.Len())
	return (v.nodes)[pick]
}

func (v *valString) Index(addr ibft.NodeID) int {
	for indx, i := range v.nodes {
		if i == addr {
			return indx
		}
	}
	return -1
}

func (v *valString) Includes(id ibft.NodeID) bool {
	for _, i := range v.nodes {
		if i == id {
			return true
		}
	}
	return false
}

func (v *valString) Len() int {
	return len(v.nodes)
}