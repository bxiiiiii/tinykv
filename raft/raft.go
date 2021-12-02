// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"math/rand"
	"time"

	//"github.com/pingcap-incubator/tinykv/raft"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// 自到达上次 heartbeatTimeout 以来的滴答数。
	// 只有领导者保持 heartbeatElapsed。
	heartbeatElapsed int
	// 当它是领导者或候选人时，自从它到达上次选举超时以来的滴答声。 
	// 自从它达到上次选举超时或从当前领导者作为跟随者时收到有效消息以来的滴答数。
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peers uint64
	electionRandomTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	peer_map := make(map[uint64]*Progress)
	vote_map := make(map[uint64]bool)
	peers := 0
	index, _ := c.Storage.LastIndex()
	for _ , i := range c.peers {
		peers++
		pro := new(Progress)
		pro.Match = 0
		pro.Next = index + 1
		peer_map[i] = pro
	}
	hard_state, _, _ := c.Storage.InitialState()
	return &Raft{
		id: c.ID,
		Term: hard_state.Term,
		Vote: hard_state.Vote,
		RaftLog: newLog(c.Storage),
		Prs: peer_map,
		State: StateFollower,
		votes: vote_map,
		electionTimeout: c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		electionElapsed: 0,
		heartbeatElapsed: 0,
		electionRandomTimeout: c.ElectionTick,
		peers: uint64(peers),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var term uint64

	entries := make([]*pb.Entry, 0)
	for _, i := range r.RaftLog.entries[r.Prs[to].Next-1:] {
		tem := new(pb.Entry)
		*tem = i
		entries = append(entries, tem)
	}

	if r.Prs[to].Next == 1 {
		term = 0
	} else {
		term = r.RaftLog.entries[r.Prs[to].Next-2].Term
	}

	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Entries: entries,
		LogTerm: term,
		Index: entries[0].Index-1,
		Commit: r.RaftLog.committed,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionRandomTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup})
			r.electionElapsed = 0
			rand.Seed(time.Now().UnixMicro())
			r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
		}
		
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionRandomTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup})
			r.electionElapsed = 0
			rand.Seed(time.Now().UnixMicro())
			r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
		}

	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			r.heartbeatElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.votes = make(map[uint64]bool)
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	entries := []*pb.Entry{{Data: nil}}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: entries,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
				case pb.MessageType_MsgHup:
					r.becomeCandidate()
					var Votes uint64
					Votes = 0
					for i := range r.votes {
						if r.votes[i] {
							Votes++
						}
					}
					
					if r.peers%2 == 1 {
						if Votes >= (r.peers/2+1) {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
						}
					} else {
						if Votes > r.peers/2 {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
						}
					}
					
					for i := range r.Prs {
						if i == r.id {
							continue
						}
						var index uint64
						if r.RaftLog.LastIndex() == 0 {
							index = 0
						} else {
							index = r.RaftLog.entries[r.RaftLog.LastIndex() - 1].Term
						}
						r.msgs = append(r.msgs, pb.Message{
							From: r.id,
							To: i,
							MsgType: pb.MessageType_MsgRequestVote,
							Term: r.Term,
							Index: r.RaftLog.LastIndex(),
							LogTerm: index,
						})
					}
				case pb.MessageType_MsgAppend:
					if r.Term < m.Term {
						r.becomeFollower(m.Term,m.From)
					}
					r.handleAppendEntries(m)
				case pb.MessageType_MsgRequestVote:
					if r.Term > m.Term {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Reject: true,
							From: m.To,
							To: m.From,
							Term: m.Term,
						})
					}else {
						if r.Term < m.Term {
							r.State = StateFollower
							r.Term = m.Term
							r.Vote = None
						}
						var is_vote bool
						if r.Vote == None {
							is_vote = false
							r.Vote = m.From
						} else {
							if r.Vote == m.From {
								is_vote = false
							} else {
								is_vote = true
							}
						}
						var term uint64
						if r.RaftLog.LastIndex() == 0 {
							term = 0
						} else {
							term = r.RaftLog.entries[r.RaftLog.LastIndex()-1].Term
						}
						if m.LogTerm < term {
							is_vote = true
						}
						if m.LogTerm == term {
							if m.Index < r.RaftLog.LastIndex() {
								is_vote = true
							}
						}
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Reject: is_vote,
							From: m.To,
							To: m.From,
							Term: m.Term,
						})
					}
		
				case pb.MessageType_MsgHeartbeat:
					r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
				case pb.MessageType_MsgAppend:
					if r.Term <= m.Term {
						r.becomeFollower(m.Term, m.From)
					}
					r.handleAppendEntries(m)
				case pb.MessageType_MsgHeartbeat:
					r.handleHeartbeat(m)
				case pb.MessageType_MsgHup:
					r.becomeCandidate()
					var Votes uint64
					Votes = 0
					for i := range r.votes {
						if r.votes[i] {
							Votes++
						}
					}
					if r.peers%2 == 1 {
						if Votes >= (r.peers/2+1) {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
						}
					} else {
						if Votes > r.peers/2 {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
						}
					}
					for i := range r.Prs {
						if i == r.id {
							continue
						}
						var index uint64
						if r.RaftLog.LastIndex() == 0 {
							index = 0
						} else {
							index = r.RaftLog.entries[r.RaftLog.LastIndex() - 1].Term
						}
						r.msgs = append(r.msgs, pb.Message{
							From: r.id,
							To: i,
							MsgType: pb.MessageType_MsgRequestVote,
							Term: r.Term,
							Index: r.RaftLog.LastIndex(),
							LogTerm: index,
						})
					}
				case pb.MessageType_MsgRequestVote:
					if r.Term > m.Term {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Reject: true,
							From: m.To,
							To: m.From,
							Term: m.Term,
						})
					} else {
						if m.Term > r.Term {
							r.State = StateFollower
							r.Term = m.Term
							r.Vote = None
						}
						var is_vote bool
						if r.Vote == None {
							is_vote = false
							r.Vote = m.From
						} else {
							if r.Vote == m.From {
								is_vote = false
							} else {
								is_vote = true
							}
						}
						var term uint64
						if r.RaftLog.LastIndex() == 0 {
							term = 0
						} else {
							term = r.RaftLog.entries[r.RaftLog.LastIndex()-1].Term
						}
						if m.LogTerm < term {
							is_vote = true
						}
						if m.LogTerm == term {
							if m.Index < r.RaftLog.LastIndex() {
								is_vote = true
							}
						}
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Reject: is_vote,
							From: m.To,
							To: m.From,
							Term: m.Term,
						})
					}
					
				case pb.MessageType_MsgRequestVoteResponse:
					r.votes[m.From] = !m.Reject
					var Votes_true uint64
					var Votes_false uint64
					Votes_true = 0
					Votes_false = 0
					for i := range r.votes {
						if r.votes[i] {
							Votes_true++
						} else {
							Votes_false++
						}
					} 
					if r.peers%2 == 1 {
						if Votes_true >= (r.peers / 2 + 1) {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
							 
						} else if Votes_false >= (r.peers / 2 + 1) {
							r.becomeFollower(m.Term, r.Lead)
						}
					} else {
						if Votes_true > r.peers / 2 {
							r.becomeLeader()
							r.votes = make(map[uint64]bool)
							r.Vote = 0
						} else if Votes_false > r.peers / 2 {
							r.becomeFollower(m.Term, r.Lead)
						}
					}
					if(Votes_false > r.peers) {
						r.becomeFollower(m.Term, r.Lead)
					}
		} 
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for i := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendHeartbeat(i)
			}
		case pb.MessageType_MsgAppend:
			if r.Term < m.Term {
				r.becomeFollower(m.Term, m.From)
			}
			//send reject

		case pb.MessageType_MsgRequestVote:
			if r.Term > m.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: true,
					From: m.To,
					To: m.From,
					Term: m.Term,
				})
			}else {
				if r.Term < m.Term {
					r.State = StateFollower
					r.Term = m.Term
					r.Vote = None
				}
				var is_vote bool
				if r.Vote == None {
					is_vote = false
					r.Vote = m.From
				} else {
					if r.Vote == m.From {
						is_vote = false
					} else {
						is_vote = true
					}
				}
				var term uint64
				if r.RaftLog.LastIndex() == 0 {
					term = 0
				} else {
					term = r.RaftLog.entries[r.RaftLog.LastIndex()-1].Term
				}
				if m.LogTerm < term {
					is_vote = true
				}
				if m.LogTerm == term {
					if m.Index < r.RaftLog.LastIndex() {
						is_vote = true
					}
				}
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: is_vote,
					From: m.To,
					To: m.From,
					Term: m.Term,
				})
			}
		case pb.MessageType_MsgPropose:
			for i := range m.Entries {
				r.RaftLog.index_entries = r.RaftLog.LastIndex()
				r.RaftLog.index_entries++
				m.Entries[i].EntryType = pb.EntryType_EntryNormal
				m.Entries[i].Index = r.RaftLog.index_entries
				m.Entries[i].Term = r.Term
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				r.Prs[r.id].Match = r.RaftLog.index_entries
				r.Prs[r.id].Next = r.Prs[r.id].Match+1
				if r.peers == 1{
					r.RaftLog.committed = m.Entries[i].Index
				}
			}

			for j := range r.Prs {
				if r.id == j {
					continue
				}
				r.sendAppend(j)
			}
		case pb.MessageType_MsgAppendResponse:
			if m.Reject {
				r.Prs[m.From].Next--
				r.sendAppend(m.From)
			} else {
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index+1
				if m.Index != 0 && r.RaftLog.entries[m.Index-1].Term == r.Term{
					for _, i := range r.RaftLog.entries[r.RaftLog.committed:] {
						count := 1
						for j := range r.Prs {
							if j == r.id {
								continue
							}
							if r.Prs[j].Match >= i.Index {
								count++
							}
						}
						if r.peers%2 == 1 {
							if uint64(count) >= (r.peers/2+1) {
								r.RaftLog.committed = i.Index
								for i := range r.Prs {
									if i == r.id {
										continue
									}
									var index uint64
									if r.Prs[i].Match == 0 {
										index = 0
									} else {
										index = r.RaftLog.entries[r.Prs[i].Match-1].Term
									}
									r.msgs = append(r.msgs, pb.Message{
										From: r.id,
										To: i,
										Term: r.Term,
										MsgType: pb.MessageType_MsgAppend,
										Entries: nil,
										Commit: r.RaftLog.committed,
										Index: r.Prs[i].Match,
										LogTerm: index,
									})
								}
							}
						} else {
							if  uint64(count) > r.peers/2 {
								r.RaftLog.committed = i.Index
								r.RaftLog.committed = i.Index
								for i := range r.Prs {
									if i == r.id {
										continue
									}
									var index uint64
									if r.Prs[i].Match == 0 {
										index = 0
									} else {
										index = r.RaftLog.entries[r.Prs[i].Match-1].Term
									}
									r.msgs = append(r.msgs, pb.Message{
										From: r.id,
										To: i,
										Term: r.Term,
										MsgType: pb.MessageType_MsgAppend,
										Entries: nil,
										Commit: r.RaftLog.committed,
										Index: r.Prs[i].Match,
										LogTerm: index,
									})
								}
							}
						}
					}
				}
			}
			
		case pb.MessageType_MsgHeartbeatResponse:
			if r.RaftLog.committed > m.Commit {
				r.sendAppend(m.From)
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	ice := false
	var pmin uint64

	if m.Index == 0 && m.LogTerm == 0 {
		ice = true
	}
	for _, i := range r.RaftLog.entries {
		if i.Index == m.Index && i.Term == m.LogTerm {
			ice = true
		}
	}

	if ice {
		if len(r.RaftLog.entries) <= int(m.Index) {
			for _, i := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *i)
			}
		} else {
			if len(m.Entries) != 0 {
				if r.RaftLog.entries[m.Entries[0].Index-1].Term != m.Entries[0].Term {
					if r.RaftLog.stabled >= m.Index {
						r.RaftLog.stabled = m.Index
					}
					r.RaftLog.entries = r.RaftLog.entries[:m.Index]
					for _, i := range m.Entries {
						r.RaftLog.entries = append(r.RaftLog.entries, *i)
					}
				} else {
					for _, i := range m.Entries {
						if r.RaftLog.LastIndex() >= i.Index {
							continue
						} 
						r.RaftLog.entries = append(r.RaftLog.entries, *i)
					}
				}
			}
		}

		if len(m.Entries) == 0 {
			pmin = m.Index
		} else {
			pmin = m.Entries[len(m.Entries)-1].Index
		}

		if r.RaftLog.LastIndex() <  m.Commit {
			r.RaftLog.committed = min(r.RaftLog.LastIndex(), pmin)
		} else {
			r.RaftLog.committed = m.Commit
		}

		r.msgs = append(r.msgs, pb.Message{
			From: m.To,
			To: m.From,
			Term: m.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index: r.RaftLog.LastIndex(),
		})
		
	} else {
		r.msgs = append(r.msgs, pb.Message{
			Reject: !ice,
			From: r.id,
			To: m.From,
			Term: r.Term,
			MsgType:  pb.MessageType_MsgAppendResponse,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
		r.electionElapsed = 0
		r.msgs = append(r.msgs, pb.Message{
			From: m.To,
			To: m.From,
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Commit: r.RaftLog.committed,
		})

	case StateCandidate:
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
			r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
			r.electionElapsed = 0
		}
		r.msgs = append(r.msgs, pb.Message{
			From: m.To,
			To: m.From,
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Commit: r.RaftLog.committed,
		})
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
