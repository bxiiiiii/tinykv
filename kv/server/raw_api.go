package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var Notfound bool
	sto_reader, _ := server.storage.Reader(nil)
	value, _ := sto_reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil {
		Notfound = true
	} else {
		Notfound = false
	}

	return &kvrpcpb.RawGetResponse{
		NotFound: Notfound,
		Value: value,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:  req.Cf,
				Key: req.Key,
				Value: req.Value,
			},
		},
	})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf: req.Cf,
				Key: req.Key,
			},
		},
	})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	sto_reader, _ := server.storage.Reader(nil)
	db_iter := sto_reader.IterCF(req.Cf) 
	arr := make([]*kvrpcpb.KvPair, 0)

	num := 0
	i := req.StartKey
	for db_iter.Seek(i); db_iter.Valid() ;db_iter.Next() {
		if num >= int(req.Limit) {
			break
		}
		
		kvpair := new(kvrpcpb.KvPair)
		kvpair.Key = db_iter.Item().Key()
		kvpair.Value, _ = db_iter.Item().Value()
		arr = append(arr, kvpair)
		num++
		//db_iter.Item().KeyCopy(i)
	}

	db_iter.Close()
	return &kvrpcpb.RawScanResponse{
		Kvs: arr,
	}, nil
}
