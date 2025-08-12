package processing

import (
	"errors"
	"strings"
)

// LowerCaseStage implements the QueryStage interface to convert the query to lowercase.
type LowerCaseStage struct{}

// Process converts the input query string to lowercase.
func (s *LowerCaseStage) Process(query string, config map[string]interface{}) (string, error) {
	return strings.ToLower(query), nil
}

// TokenizeStage implements the QueryStage interface to split the query into tokens.
type TokenizeStage struct{}

// Process splits the input query string into tokens based on whitespace.
// It returns a space-separated string of tokens.
func (s *TokenizeStage) Process(query string, config map[string]interface{}) (string, error) {
	if query == "" {
		return "", nil
	}
	// Simple whitespace tokenizer. More advanced tokenization would involve regex or libraries.
	tokens := strings.Fields(query)
	return strings.Join(tokens, " "), nil
}

// RemoveStopwordsStage implements the QueryStage interface to remove stopwords from the query.
type RemoveStopwordsStage struct{}

// Process removes predefined stopwords from the query.
// Stopwords are expected in the config map under the "stopwords" key as a []string.
func (s *RemoveStopwordsStage) Process(query string, config map[string]interface{}) (string, error) {
	if query == "" {
		return "", nil
	}

	stopwordsInterface, ok := config["stopwords"]
	if !ok {
		// If no stopwords are provided in config, simply return the original query.
		// Alternatively, this could return an error or use a default list.
		return query, nil
	}

	stopwordsList, ok := stopwordsInterface.([]string)
	if !ok {
		return "", errors.New("stopwords config must be a list of strings")
	}

	stopwordMap := make(map[string]struct{})
	for _, sw := range stopwordsList {
		stopwordMap[sw] = struct{}{}
	}

	// Assuming the query is already tokenized by a previous stage or is space-separated.
	tokens := strings.Fields(query)
	filteredTokens := make([]string, 0, len(tokens))

	for _, token := range tokens {
		if _, isStopword := stopwordMap[token]; !isStopword {
			filteredTokens = append(filteredTokens, token)
		}
	}

	return strings.Join(filteredTokens, " "), nil
}

// SynonymExpansionStage implements the QueryStage interface for synonym expansion.
// This is a placeholder and would require a more complex lookup mechanism.
type SynonymExpansionStage struct{}

// Process currently returns the query as is, demonstrating a placeholder.
// In a real scenario, this would expand terms based on a synonym dictionary.
func (s *SynonymExpansionStage) Process(query string, config map[string]interface{}) (string, error) {
	// For demonstration, let's say "pc" expands to "personal computer"
	// This logic would typically come from a configurable synonym map
	if strings.Contains(query, "pc") {
		query = strings.ReplaceAll(query, "pc", "pc personal computer")
	}
	return query, nil
}
