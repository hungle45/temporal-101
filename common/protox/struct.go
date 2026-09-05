package protox

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func ToStructPB(v any) (*structpb.Struct, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var s structpb.Struct
	if err := protojson.Unmarshal(b, &s); err != nil {
		return nil, err
	}

	return &s, nil
}

func FromStructPB(s *structpb.Struct, v any) error {
	b, err := protojson.Marshal(s)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}
