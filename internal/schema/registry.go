package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	ErrSchemaNotFound = errors.New("schema: not registered")
	ErrEmptyPayload   = errors.New("schema: payload is empty")
)

const schemaFileSuffix = ".schema.json"

type Definition struct {
	Name        string `json:"name"`
	Version     int    `json:"version"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Source      string `json:"source,omitempty"`
}

type entry struct {
	definition Definition
	compiled   *jsonschema.Schema
}

type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*entry)}
}

func (r *Registry) Register(name string, document []byte) (Definition, error) {
	if name == "" {
		return Definition{}, errors.New("schema: name is required")
	}

	decoded, err := jsonschema.UnmarshalJSON(strings.NewReader(string(document)))
	if err != nil {
		return Definition{}, fmt.Errorf("schema: parse %s: %w", name, err)
	}

	compiler := jsonschema.NewCompiler()
	resource := name + schemaFileSuffix

	if err := compiler.AddResource(resource, decoded); err != nil {
		return Definition{}, fmt.Errorf("schema: add %s: %w", name, err)
	}

	compiled, err := compiler.Compile(resource)
	if err != nil {
		return Definition{}, fmt.Errorf("schema: compile %s: %w", name, err)
	}

	definition := Definition{
		Name:        name,
		Version:     versionOf(document),
		Title:       compiled.Title,
		Description: compiled.Description,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries[name] = &entry{definition: definition, compiled: compiled}

	return definition, nil
}

func (r *Registry) LoadDirectory(root string) ([]Definition, error) {
	loaded := make([]Definition, 0)

	err := filepath.WalkDir(root, func(path string, dir fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if dir.IsDir() || !strings.HasSuffix(dir.Name(), schemaFileSuffix) {
			return nil
		}

		document, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("schema: read %s: %w", path, err)
		}

		name := strings.TrimSuffix(dir.Name(), schemaFileSuffix)

		definition, err := r.Register(name, document)
		if err != nil {
			return err
		}

		definition.Source = path
		loaded = append(loaded, definition)

		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Name < loaded[j].Name })

	return loaded, nil
}

func (r *Registry) Definitions() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	definitions := make([]Definition, 0, len(r.entries))
	for _, registered := range r.entries {
		definitions = append(definitions, registered.definition)
	}

	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })

	return definitions
}

func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.entries[name]

	return ok
}

func (r *Registry) Validate(name string, payload json.RawMessage) error {
	if name == "" {
		return nil
	}

	r.mu.RLock()
	registered, ok := r.entries[name]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %s", ErrSchemaNotFound, name)
	}

	if len(payload) == 0 {
		return &ValidationError{Schema: name, Violations: []Violation{{
			Path:   "",
			Detail: ErrEmptyPayload.Error(),
		}}}
	}

	document, err := jsonschema.UnmarshalJSON(strings.NewReader(string(payload)))
	if err != nil {
		return &ValidationError{Schema: name, Violations: []Violation{{
			Path:   "",
			Detail: "payload is not valid JSON",
		}}}
	}

	if err := registered.compiled.Validate(document); err != nil {
		var validationErr *jsonschema.ValidationError
		if errors.As(err, &validationErr) {
			return &ValidationError{Schema: name, Violations: collectViolations(validationErr)}
		}

		return err
	}

	return nil
}

func versionOf(document []byte) int {
	var header struct {
		Version int `json:"x-schema-version"`
	}

	if err := json.Unmarshal(document, &header); err != nil {
		return 1
	}

	if header.Version < 1 {
		return 1
	}

	return header.Version
}
