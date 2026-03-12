package assets

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hyperledger-labs/cc-tools/errors"
	sw "github.com/hyperledger-labs/cc-tools/stubwrapper"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

type SearchResponse struct {
	Result   []map[string]interface{}  `json:"result"`
	Metadata *pb.QueryResponseMetadata `json:"metadata"`
}

func Search(stub *sw.StubWrapper, request map[string]interface{}, privateCollection string, resolve bool) (*SearchResponse, errors.ICCError) {
	var bookmark string
	var pageSize int32

	bookmarkInt, bookmarkExists := request["bookmark"]
	limit, limitExists := request["limit"]

	if limitExists {
		limit64, ok := limit.(float64)
		if !ok {
			return nil, errors.NewCCError("limit must be an integer", 400)
		}
		pageSize = int32(limit64)
	}

	if bookmarkExists {
		var ok bool
		bookmark, ok = bookmarkInt.(string)
		if !ok {
			return nil, errors.NewCCError("bookmark must be a string", 400)
		}
	}

	delete(request, "bookmark")
	delete(request, "limit")

	query, err := json.Marshal(request)
	if err != nil {
		return nil, errors.WrapErrorWithStatus(err, "failed marshaling JSON-encoded asset", 500)
	}
	queryString := string(query)

	var resultsIterator shim.StateQueryIteratorInterface
	var responseMetadata *pb.QueryResponseMetadata

	levelDBFallback := func() (*SearchResponse, errors.ICCError) {
		return searchFallback(stub, request, privateCollection, resolve, limitExists, pageSize, bookmark)
	}

	if !limitExists {
		if privateCollection == "" {
			resultsIterator, err = stub.GetQueryResult(queryString)
		} else {
			resultsIterator, err = stub.GetPrivateDataQueryResult(privateCollection, queryString)
		}
	} else {
		if privateCollection != "" {
			return nil, errors.NewCCError("private data pagination is not implemented", 501)
		}
		resultsIterator, responseMetadata, err = stub.GetQueryResultWithPagination(queryString, pageSize, bookmark)
	}
	if err != nil {
		if privateCollection == "" {
			if fallbackResp, fbErr := levelDBFallback(); fbErr == nil {
				return fallbackResp, nil
			} else if isLevelDBQueryError(err) {
				return nil, fbErr
			}
		}
		return nil, errors.WrapErrorWithStatus(err, "failed to get query result", 500)
	}
	defer resultsIterator.Close()

	searchResult := make([]map[string]interface{}, 0)

	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, errors.WrapErrorWithStatus(err, "error iterating response", 500)
		}

		var data map[string]interface{}

		err = json.Unmarshal(queryResponse.Value, &data)
		if err != nil {
			return nil, errors.WrapErrorWithStatus(err, "failed to unmarshal queryResponse values", 500)
		}

		if resolve {
			key, err := NewKey(data)
			if err != nil {
				return nil, errors.WrapError(err, "failed to create key object to resolve result")
			}
			asset, err := key.GetRecursive(stub)
			if err != nil {
				return nil, errors.WrapError(err, "failed to resolve result")
			}
			data = asset
		}

		searchResult = append(searchResult, data)
	}

	response := SearchResponse{
		Result:   searchResult,
		Metadata: responseMetadata,
	}

	return &response, nil
}

func isLevelDBQueryError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "executequery") && strings.Contains(msg, "leveldb")
}

func searchFallback(
	stub *sw.StubWrapper,
	request map[string]interface{},
	privateCollection string,
	resolve bool,
	limitExists bool,
	pageSize int32,
	bookmark string,
) (*SearchResponse, errors.ICCError) {
	if privateCollection != "" {
		return nil, errors.NewCCError("rich queries on private data are not available without CouchDB", 501)
	}
	if bookmark != "" {
		return nil, errors.NewCCError("bookmark pagination is not supported when CouchDB is unavailable", 501)
	}

	rawSelector, ok := request["selector"]
	if !ok {
		return nil, errors.NewCCError("missing selector", 400)
	}

	selectorMap, ok := rawSelector.(map[string]interface{})
	if !ok {
		return nil, errors.NewCCError("selector must be an object", 400)
	}

	stateIter, err := stub.Stub.GetStateByRange("", "")
	if err != nil {
		return nil, errors.WrapErrorWithStatus(err, "failed to scan ledger for fallback search", 500)
	}
	defer stateIter.Close()

	matches := make([]map[string]interface{}, 0)

	for stateIter.HasNext() {
		kv, iterErr := stateIter.Next()
		if iterErr != nil {
			return nil, errors.WrapErrorWithStatus(iterErr, "failed iterating ledger during fallback search", 500)
		}

		var doc map[string]interface{}
		if jsonErr := json.Unmarshal(kv.Value, &doc); jsonErr != nil {
			return nil, errors.WrapErrorWithStatus(jsonErr, "failed to unmarshal state value during fallback search", 500)
		}

		match, matchErr := matchesSelector(doc, selectorMap)
		if matchErr != nil {
			return nil, matchErr
		}
		if !match {
			continue
		}

		if resolve {
			key, keyErr := NewKey(doc)
			if keyErr != nil {
				return nil, errors.WrapError(keyErr, "failed to create key object to resolve result")
			}
			asset, assetErr := key.GetRecursive(stub)
			if assetErr != nil {
				return nil, errors.WrapError(assetErr, "failed to resolve result")
			}
			doc = asset
		}

		matches = append(matches, doc)

		if limitExists && int32(len(matches)) >= pageSize {
			break
		}
	}

	metadata := &pb.QueryResponseMetadata{
		FetchedRecordsCount: int32(len(matches)),
		Bookmark:            "",
	}

	return &SearchResponse{
		Result:   matches,
		Metadata: metadata,
	}, nil
}

func matchesSelector(doc map[string]interface{}, selector map[string]interface{}) (bool, errors.ICCError) {
	for key, value := range selector {
		if strings.HasPrefix(key, "$") {
			switch strings.ToLower(key) {
			case "$and":
				conditions, ok := value.([]interface{})
				if !ok {
					return false, errors.NewCCError("$and operator expects an array of conditions", 400)
				}
				for _, cond := range conditions {
					condMap, ok := cond.(map[string]interface{})
					if !ok {
						return false, errors.NewCCError("$and operator expects each condition to be an object", 400)
					}
					match, err := matchesSelector(doc, condMap)
					if err != nil {
						return false, err
					}
					if !match {
						return false, nil
					}
				}
			case "$or":
				conditions, ok := value.([]interface{})
				if !ok {
					return false, errors.NewCCError("$or operator expects an array of conditions", 400)
				}
				orMatch := false
				for _, cond := range conditions {
					condMap, ok := cond.(map[string]interface{})
					if !ok {
						return false, errors.NewCCError("$or operator expects each condition to be an object", 400)
					}
					match, err := matchesSelector(doc, condMap)
					if err != nil {
						return false, err
					}
					if match {
						orMatch = true
						break
					}
				}
				if !orMatch {
					return false, nil
				}
			default:
				return false, errors.NewCCError(fmt.Sprintf("operator %s is not supported without CouchDB", key), 501)
			}
			continue
		}

		docValue, exists := getNestedValue(doc, key)
		if !exists {
			return false, nil
		}

		if !valuesEqual(docValue, value) {
			return false, nil
		}
	}

	return true, nil
}

func getNestedValue(doc map[string]interface{}, path string) (interface{}, bool) {
	current := interface{}(doc)
	for _, part := range strings.Split(path, ".") {
		switch typed := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = typed[part]
			if !ok {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

func valuesEqual(a interface{}, b interface{}) bool {
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
		return fmt.Sprint(a) == fmt.Sprint(b)
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case int:
			return av == float64(bv)
		case int32:
			return av == float64(bv)
		case int64:
			return av == float64(bv)
		default:
			return fmt.Sprint(a) == fmt.Sprint(b)
		}
	case int, int32, int64:
		return fmt.Sprint(a) == fmt.Sprint(b)
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
		return fmt.Sprint(a) == fmt.Sprint(b)
	default:
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}
