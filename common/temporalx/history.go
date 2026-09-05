package temporalx

import (
	"bytes"
	"embed"

	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
)

func LoadHistory(fs embed.FS, historyPath string) (*history.History, error) {
	data, err := fs.ReadFile(historyPath)
	if err != nil {
		return nil, err
	}
	return client.HistoryFromJSON(bytes.NewReader(data), client.HistoryJSONOptions{})
}
