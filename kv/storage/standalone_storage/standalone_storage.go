package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engins engine_util.Engines
	config  *config.Config
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	KvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	//snapPath := filepath.Join(dbPath, "snap")
	kvDB := engine_util.CreateDB(KvPath, true)
	raftDB := engine_util.CreateDB(raftPath, false)
	engins := *engine_util.NewEngines(kvDB, raftDB, KvPath, raftPath)
	
	return &StandAloneStorage{
		engins: engins,
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engins.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	Txn := s.engins.Kv.NewTransaction(false)

	return &StandAloneStorage{
		txn: Txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	//bat := new(engine_util.WriteBatch)
	for _, i := range batch {
		switch i.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.engins.Kv, i.Cf(), i.Key(), i.Value())
			//bat.SetCF(i.Cf(), i.Key(), i.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.engins.Kv, i.Cf(), i.Key())
			//bat.DeleteCF(i.Cf(), i.Key())
		}
	}
	//err = bat.WriteToDB(s.engins.Kv)
	return err
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ :=  engine_util.GetCFFromTxn(s.txn, cf, key)
	return val, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorage) Close() {
	s.txn.Discard()
}
