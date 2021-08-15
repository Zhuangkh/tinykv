package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	kv     *badger.DB
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kv := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		kv:     kv,
		config: conf,
	}

}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.kv.Close()
	if err != nil {
		panic(err)
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.kv.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.kv.NewTransaction(true)
	defer txn.Discard()

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			if err := engine_util.PutCF(s.kv, put.Cf, put.Key, put.Value); err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			if err := engine_util.DeleteCF(s.kv, del.Cf, del.Key); err != nil {
				return err
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s StandAloneStorageReader) Close() {
	s.txn.Discard()
}
